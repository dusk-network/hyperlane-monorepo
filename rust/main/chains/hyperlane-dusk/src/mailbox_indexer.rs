use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, HyperlaneMessage, Indexed, Indexer, LogMeta, SequenceAwareIndexer, H256, H512,
    U256,
};
use hyperlane_dusk_types::{events, message};

use crate::rues::{exact_event_data, rkyv_deserialize, rkyv_serialize};
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::RuesClient;

/// What the Mailbox stored for one dispatch.
struct StoredDispatch {
    nonce: u32,
    block_number: u64,
    message_id: [u8; 32],
}

/// Dusk mailbox indexer for dispatch and delivery events.
///
/// Uses sequence-based indexing, keyed on:
/// - Dispatch nonce for `Indexer<HyperlaneMessage>`
/// - Process index for `Indexer<H256>` (delivery)
#[derive(Debug, Clone)]
pub struct DuskMailboxIndexer {
    rues: Arc<RuesClient>,
    mailbox_id: [u8; 32],
    mailbox_address: H256,
    local_domain: u32,
}

impl DuskMailboxIndexer {
    /// Create a dispatch indexer for the Mailbox of `local_domain`.
    ///
    /// Only messages whose origin is `local_domain` are indexed. Agents pass
    /// the configured domain, which startup checks against the Mailbox's own
    /// `local_domain`.
    pub fn new(rues: Arc<RuesClient>, mailbox_id: H256, local_domain: u32) -> Self {
        Self {
            rues,
            mailbox_id: mailbox_id.into(),
            mailbox_address: mailbox_id,
            local_domain,
        }
    }

    async fn dispatch_height(&self, sequence: u32) -> ChainResult<u64> {
        Ok(self
            .rues
            .contract_query(&self.mailbox_id, "dispatched_block_height", &(sequence,))
            .await?)
    }

    async fn first_dispatch_at_or_after(&self, count: u32, block_height: u64) -> ChainResult<u32> {
        let mut low = 0u32;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.dispatch_height(middle).await? < block_height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn dispatch_range_at_block(
        &self,
        count: u32,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = self.first_dispatch_at_or_after(count, block_height).await?;
        if first == count || self.dispatch_height(first).await? != block_height {
            return Ok(None);
        }
        let after = self
            .first_dispatch_at_or_after(count, block_height.saturating_add(1))
            .await?;
        Ok(Some(crate::bounded_block_range(first, after, "dispatch")?))
    }

    async fn finalized_dispatch_count(&self, current_count: u32) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self
            .first_dispatch_at_or_after(current_count, u64::from(finalized_tip) + 1)
            .await?;
        Ok((finalized_count, finalized_tip))
    }
}

// =============================================================================
// Dispatch indexing: Indexer<HyperlaneMessage>
// =============================================================================

#[async_trait]
impl Indexer<HyperlaneMessage> for DuskMailboxIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<HyperlaneMessage>, LogMeta)>> {
        // Mailbox.nonce is the next dispatch nonce; messages exist for [0, nonce).
        let current_nonce: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        let (finalized_nonce, _) = self.finalized_dispatch_count(current_nonce).await?;

        // The Mailbox stores only each message's ID and block. The message
        // itself is in the finalized Dispatch event that hashes to that ID.
        let mut stored = Vec::new();
        for nonce in range {
            if nonce >= finalized_nonce {
                break;
            }
            let block_number: u64 = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_block_height", &(nonce,))
                .await?;
            ensure_cursor_height(block_number)?;
            let message_id: [u8; 32] = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_id", &(nonce,))
                .await?;
            stored.push(StoredDispatch {
                nonce,
                block_number,
                message_id,
            });
        }

        // One archive pass per block, however many dispatches it holds.
        let mut results = Vec::with_capacity(stored.len());
        let local_domain = self.local_domain;
        for block in stored.chunk_by(|a, b| a.block_number == b.block_number) {
            let Some(first) = block.first() else {
                continue;
            };
            let ids: HashMap<u32, [u8; 32]> = block
                .iter()
                .map(|dispatch| (dispatch.nonce, dispatch.message_id))
                .collect();
            let sequences: Vec<usize> = block
                .iter()
                .map(|dispatch| dispatch.nonce as usize)
                .collect();
            let matches = self
                .rues
                .finalized_contract_events_at_block(
                    &self.mailbox_id,
                    events::Dispatch::TOPIC,
                    first.block_number,
                    &sequences,
                    |data| {
                        // Route by the event's own nonce, then hold it to
                        // everything the Mailbox stored for that nonce.
                        dispatch_nonce(data)
                            .filter(|nonce| {
                                ids.get(nonce).is_some_and(|message_id| {
                                    decode_dispatch(data, *nonce, message_id, local_domain)
                                        .is_some()
                                })
                            })
                            .map(|nonce| vec![nonce as usize])
                            .unwrap_or_default()
                    },
                )
                .await?;

            for (dispatch, matched) in block.iter().zip(matches) {
                let nonce = dispatch.nonce;
                let provenance = matched.provenance;
                let dusk_msg =
                    decode_dispatch(&matched.data, nonce, &dispatch.message_id, local_domain)
                        .ok_or_else(|| {
                            crate::HyperlaneDuskError::Other(format!(
                                "Matched Dusk Dispatch event for nonce {nonce} no longer decodes"
                            ))
                        })?;

                // Convert to hyperlane-core HyperlaneMessage.
                let core_msg = HyperlaneMessage {
                    version: dusk_msg.version,
                    nonce: dusk_msg.nonce,
                    origin: dusk_msg.origin,
                    sender: H256::from_slice(&dusk_msg.sender),
                    destination: dusk_msg.destination,
                    recipient: H256::from_slice(&dusk_msg.recipient),
                    body: dusk_msg.body,
                };

                let log_meta = LogMeta {
                    address: self.mailbox_address,
                    block_number: provenance.block_height,
                    block_hash: provenance.block_hash,
                    transaction_id: provenance.transaction_id,
                    transaction_index: 0,
                    log_index: U256::from(nonce),
                };
                let indexed = Indexed::from(core_msg).with_sequence(nonce);
                results.push((indexed, log_meta));
            }
        }

        debug!(count = results.len(), "Fetched dispatch logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(self.rues.finalized_block_number().await?)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<HyperlaneMessage>, LogMeta)>> {
        let tx_id = h512_to_dusk_tx_id(&tx_hash)?;
        let Some(block_height) = self.rues.transaction_block_height(&tx_id).await? else {
            return Ok(vec![]);
        };
        ensure_cursor_height(block_height)?;
        if block_height > self.rues.finalized_block_height().await? {
            return Ok(vec![]);
        }
        let count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        let Some(range) = self.dispatch_range_at_block(count, block_height).await? else {
            return Ok(vec![]);
        };
        crate::ensure_tx_hash_lookup_budget(&range, "dispatch")?;
        let mut logs = Vec::new();
        for chunk in crate::block_range_chunks(range) {
            logs.extend(
                self.fetch_logs_in_range(chunk)
                    .await?
                    .into_iter()
                    .filter(|(_, meta)| meta.transaction_id == tx_hash),
            );
        }
        Ok(logs)
    }

    fn parse_tx_hash(&self, tx_hash: &str) -> ChainResult<H512> {
        Ok(dusk_tx_id_to_h512(tx_hash)?)
    }
}

#[async_trait]
impl SequenceAwareIndexer<HyperlaneMessage> for DuskMailboxIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let nonce: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        let (finalized_nonce, finalized_tip) = self.finalized_dispatch_count(nonce).await?;
        Ok((Some(finalized_nonce), finalized_tip))
    }
}

// =============================================================================
// Delivery indexing: Indexer<H256> (processed message IDs)
// =============================================================================

/// Dusk delivery indexer — indexes processed (delivered) message IDs.
#[derive(Debug, Clone)]
pub struct DuskDeliveryIndexer {
    rues: Arc<RuesClient>,
    mailbox_id: [u8; 32],
    mailbox_address: H256,
}

impl DuskDeliveryIndexer {
    /// Create a new delivery indexer.
    pub fn new(rues: Arc<RuesClient>, mailbox_id: H256) -> Self {
        Self {
            rues,
            mailbox_id: mailbox_id.into(),
            mailbox_address: mailbox_id,
        }
    }

    async fn process_height(&self, sequence: u32) -> ChainResult<u64> {
        Ok(self
            .rues
            .contract_query(
                &self.mailbox_id,
                "processed_block_height_at_index",
                &(sequence,),
            )
            .await?)
    }

    async fn first_process_at_or_after(&self, count: u32, block_height: u64) -> ChainResult<u32> {
        let mut low = 0u32;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.process_height(middle).await? < block_height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn process_range_at_block(
        &self,
        count: u32,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = self.first_process_at_or_after(count, block_height).await?;
        if first == count || self.process_height(first).await? != block_height {
            return Ok(None);
        }
        let after = self
            .first_process_at_or_after(count, block_height.saturating_add(1))
            .await?;
        Ok(Some(crate::bounded_block_range(first, after, "delivery")?))
    }

    async fn finalized_process_count(&self, current_count: u32) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self
            .first_process_at_or_after(current_count, u64::from(finalized_tip) + 1)
            .await?;
        Ok((finalized_count, finalized_tip))
    }
}

#[async_trait]
impl Indexer<H256> for DuskDeliveryIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<H256>, LogMeta)>> {
        // Messages exist for [0, processed_count).
        let processed_count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "processed_count", &())
            .await?;
        let (finalized_count, _) = self.finalized_process_count(processed_count).await?;

        let mut stored = Vec::new();
        for index in range {
            if index >= finalized_count {
                break;
            }
            let message_id: [u8; 32] = self
                .rues
                .contract_query(&self.mailbox_id, "processed_at_index", &(index,))
                .await?;

            let block_number: u64 = self
                .rues
                .contract_query(
                    &self.mailbox_id,
                    "processed_block_height_at_index",
                    &(index,),
                )
                .await?;
            ensure_cursor_height(block_number)?;
            stored.push((index, block_number, message_id));
        }

        // One archive pass per block, however many deliveries it holds.
        let mut results = Vec::with_capacity(stored.len());
        for block in stored.chunk_by(|a, b| a.1 == b.1) {
            let Some(&(_, block_number, _)) = block.first() else {
                continue;
            };
            let mut expected = Vec::with_capacity(block.len());
            for (index, _, message_id) in block {
                let message_id = *message_id;
                expected.push((
                    *index as usize,
                    rkyv_serialize(&events::ProcessId { message_id })?,
                ));
            }
            let sequences: Vec<usize> = expected.iter().map(|(index, _)| *index).collect();
            let matches = self
                .rues
                .finalized_contract_events_at_block(
                    &self.mailbox_id,
                    events::ProcessId::TOPIC,
                    block_number,
                    &sequences,
                    exact_event_data(expected),
                )
                .await?;

            for (&(index, _, message_id), matched) in block.iter().zip(matches) {
                let provenance = matched.provenance;
                let log_meta = LogMeta {
                    address: self.mailbox_address,
                    block_number: provenance.block_height,
                    block_hash: provenance.block_hash,
                    transaction_id: provenance.transaction_id,
                    transaction_index: 0,
                    log_index: U256::from(index),
                };

                let h256_id = H256::from_slice(&message_id);
                let indexed = Indexed::from(h256_id).with_sequence(index);
                results.push((indexed, log_meta));
            }
        }

        debug!(count = results.len(), "Fetched delivery logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(self.rues.finalized_block_number().await?)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<H256>, LogMeta)>> {
        let tx_id = h512_to_dusk_tx_id(&tx_hash)?;
        let Some(block_height) = self.rues.transaction_block_height(&tx_id).await? else {
            return Ok(vec![]);
        };
        ensure_cursor_height(block_height)?;
        if block_height > self.rues.finalized_block_height().await? {
            return Ok(vec![]);
        }
        let count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "processed_count", &())
            .await?;
        let Some(range) = self.process_range_at_block(count, block_height).await? else {
            return Ok(vec![]);
        };
        crate::ensure_tx_hash_lookup_budget(&range, "delivery")?;
        let mut logs = Vec::new();
        for chunk in crate::block_range_chunks(range) {
            logs.extend(
                self.fetch_logs_in_range(chunk)
                    .await?
                    .into_iter()
                    .filter(|(_, meta)| meta.transaction_id == tx_hash),
            );
        }
        Ok(logs)
    }

    fn parse_tx_hash(&self, tx_hash: &str) -> ChainResult<H512> {
        Ok(dusk_tx_id_to_h512(tx_hash)?)
    }
}

fn ensure_cursor_height(block_height: u64) -> ChainResult<()> {
    if block_height > u64::from(u32::MAX) {
        return Err(crate::HyperlaneDuskError::Other(format!(
            "Dusk block height {block_height} exceeds the shared u32 cursor range"
        ))
        .into());
    }
    Ok(())
}

/// Decode a `Dispatch` event as the message the Mailbox stored at `nonce`.
///
/// Returns the message only when the event is exactly the one the Mailbox
/// emits for it: the message hashes to the stored `message_id`, carries this
/// nonce and the Mailbox's origin domain, the event's routing fields repeat
/// the message's, and the event bytes are the canonical encoding of that
/// event. Any other event data is not this dispatch.
fn decode_dispatch(
    event_data: &[u8],
    nonce: u32,
    message_id: &[u8; 32],
    local_domain: u32,
) -> Option<message::HyperlaneMessage> {
    let event: events::Dispatch = rkyv_deserialize(event_data).ok()?;
    if message::id(&event.message) != *message_id {
        return None;
    }
    let decoded = message::decode(&event.message)?;
    if decoded.nonce != nonce
        || decoded.origin != local_domain
        || decoded.sender != event.sender
        || decoded.destination != event.destination
        || decoded.recipient != event.recipient
    {
        return None;
    }
    // The ID pins the message and the fields pin the rest of the event.
    // Requiring the canonical bytes keeps this as strict as comparing against
    // the event rebuilt from contract state.
    if rkyv_serialize(&event).ok()? != event_data {
        return None;
    }
    Some(decoded)
}

/// The nonce a `Dispatch` event's message claims. Only picks which stored
/// dispatch to hold the event to; `decode_dispatch` decides whether it is
/// that dispatch.
fn dispatch_nonce(event_data: &[u8]) -> Option<u32> {
    let event: events::Dispatch = rkyv_deserialize(event_data).ok()?;
    Some(message::decode(&event.message)?.nonce)
}

#[async_trait]
impl SequenceAwareIndexer<H256> for DuskDeliveryIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "processed_count", &())
            .await?;
        let (finalized_count, finalized_tip) = self.finalized_process_count(count).await?;
        Ok((Some(finalized_count), finalized_tip))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ORIGIN: u32 = 4242;

    fn stored(nonce: u32, body: &[u8]) -> Vec<u8> {
        message::encode(3, nonce, ORIGIN, [7; 32], 42, [8; 32], body)
    }

    fn event(message: Vec<u8>) -> events::Dispatch {
        events::Dispatch {
            sender: [7; 32],
            destination: 42,
            recipient: [8; 32],
            message,
        }
    }

    fn bytes(event: &events::Dispatch) -> Vec<u8> {
        rkyv_serialize(event).unwrap()
    }

    #[test]
    fn dispatch_event_decodes_only_as_the_stored_message() {
        let message = stored(5, &[1]);
        let id = message::id(&message);
        let decoded = decode_dispatch(&bytes(&event(message.clone())), 5, &id, ORIGIN).unwrap();
        assert_eq!(decoded.encode(), message);

        // Same nonce, origin and routing with another body: only the ID differs.
        assert!(decode_dispatch(&bytes(&event(stored(5, &[2]))), 5, &id, ORIGIN).is_none());
        // A message whose own ID is stored, but for another nonce or origin.
        let other_nonce = stored(6, &[1]);
        let other_origin = message::encode(3, 5, ORIGIN + 1, [7; 32], 42, [8; 32], &[1]);
        for foreign in [other_nonce, other_origin] {
            let foreign_id = message::id(&foreign);
            assert!(decode_dispatch(&bytes(&event(foreign)), 5, &foreign_id, ORIGIN).is_none());
        }
        // The event's routing fields must repeat the message's.
        for tampered in [
            events::Dispatch {
                sender: [9; 32],
                ..event(message.clone())
            },
            events::Dispatch {
                destination: 43,
                ..event(message.clone())
            },
            events::Dispatch {
                recipient: [9; 32],
                ..event(message.clone())
            },
        ] {
            assert!(decode_dispatch(&bytes(&tampered), 5, &id, ORIGIN).is_none());
        }
    }

    #[test]
    fn dispatch_event_bytes_must_be_canonical() {
        let message = stored(5, &[1]);
        let id = message::id(&message);
        // rkyv reads the root from the end of the buffer, so leading bytes
        // still decode to the same event.
        let mut padded = vec![0u8; 16];
        padded.extend_from_slice(&bytes(&event(message)));
        assert!(rkyv_deserialize::<events::Dispatch>(&padded).is_ok());
        assert!(decode_dispatch(&padded, 5, &id, ORIGIN).is_none());
        assert!(decode_dispatch(b"not a dispatch event", 5, &id, ORIGIN).is_none());
    }
}
