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

use crate::rues::{rkyv_deserialize, rkyv_serialize, EventLog, LogEntry};
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::RuesClient;

/// What the Mailbox stored for one dispatch.
struct StoredDispatch {
    nonce: u32,
    block_number: u64,
    message_id: [u8; 32],
}

/// Dusk mailbox indexer for dispatch events, by dispatch nonce.
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
        // The transaction's own Dispatch rows name the nonces it used, so
        // only those are looked up, from the block's rows read once. Rows
        // that name no nonce are not evidence that it dispatched nothing:
        // contract state decides that below.
        if let Some(rows) = self
            .rues
            .finalized_rows_of_transaction(
                &self.mailbox_id,
                events::Dispatch::TOPIC,
                block_height,
                &crate::dusk_transaction_bytes(&tx_hash),
            )
            .await?
        {
            let nonces: std::collections::BTreeSet<u32> = rows
                .iter()
                .filter_map(|data| dispatch_nonce(data))
                .collect();
            if !nonces.is_empty() {
                return crate::transaction_logs(self, nonces, tx_hash, block_height, "dispatch")
                    .await;
            }
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
// Delivery indexing: Indexer<H256> (delivered message IDs)
// =============================================================================

/// Archive pages a delivery indexer reads per call to catch up, on to the
/// end of the block being read.
const DELIVERY_LOG_PAGES: usize = 16;

/// The finalized event log of a Mailbox's deliveries: its `ProcessId` rows,
/// keyed by message ID.
pub(crate) fn delivery_log(mailbox_id: [u8; 32]) -> EventLog {
    EventLog {
        contract_id: mailbox_id,
        topic: events::ProcessId::TOPIC,
        key_of: delivery_key,
    }
}

/// The message ID a `ProcessId` event names, if its data is exactly the
/// event's encoding.
fn delivery_key(data: &[u8]) -> Option<Vec<u8>> {
    let event: events::ProcessId = rkyv_deserialize(data).ok()?;
    (rkyv_serialize(&event).ok()? == data).then(|| event.message_id.to_vec())
}

/// Dusk delivery indexer: the message IDs the Mailbox delivered.
///
/// The Mailbox keeps no list of deliveries, only a set of delivered IDs, so
/// deliveries come from its finalized `ProcessId` events. Their sequence is
/// their number in the delivery log (`delivery_log`): every non-reverted
/// `ProcessId` row the archive has finalized, in block order and, within a
/// block, in archive order (by transaction hash, then by emission). It is
/// the same for every agent and endpoint whose archive holds the same
/// blocks, but it is not the order of execution within a block.
#[derive(Debug, Clone)]
pub struct DuskDeliveryIndexer {
    rues: Arc<RuesClient>,
    log: EventLog,
    mailbox_address: H256,
}

impl DuskDeliveryIndexer {
    /// Create a new delivery indexer.
    pub fn new(rues: Arc<RuesClient>, mailbox_id: H256) -> Self {
        Self {
            rues,
            log: delivery_log(mailbox_id.into()),
            mailbox_address: mailbox_id,
        }
    }

    /// The deliveries of `entries`, each in a block that
    /// `checkBlock(onlyFinalized)` still accepts. A block it no longer
    /// accepts means the endpoint does not agree with the log, which is
    /// dropped and rebuilt.
    async fn deliveries(
        &self,
        entries: Vec<LogEntry>,
    ) -> ChainResult<Vec<(Indexed<H256>, LogMeta)>> {
        let mut checked: HashMap<(u64, H256), bool> = HashMap::new();
        let mut logs = Vec::with_capacity(entries.len());
        for entry in entries {
            let block = (entry.block_height, entry.block_hash);
            let finalized = match checked.get(&block) {
                Some(finalized) => *finalized,
                None => {
                    let finalized = self
                        .rues
                        .check_finalized_block_hash(entry.block_height, entry.block_hash)
                        .await?;
                    checked.insert(block, finalized);
                    finalized
                }
            };
            if !finalized {
                self.rues.reset_event_log(&self.log).await?;
                return Err(crate::HyperlaneDuskError::FinalizedEventMismatch(format!(
                    "Dusk delivery {} is in block {} ({:?}), which checkBlock(onlyFinalized) no longer accepts; the delivery log is rebuilt",
                    entry.sequence, entry.block_height, entry.block_hash
                ))
                .into());
            }
            ensure_cursor_height(entry.block_height)?;
            let message_id = delivery_key(&entry.data)
                .and_then(|id| <[u8; 32]>::try_from(id).ok())
                .ok_or_else(|| {
                    crate::HyperlaneDuskError::Other(format!(
                        "Dusk delivery log row {} is not a ProcessId event",
                        entry.sequence
                    ))
                })?;
            let sequence = u32::try_from(entry.sequence).map_err(|_| {
                crate::HyperlaneDuskError::Other(format!(
                    "Dusk delivery sequence {} exceeds the shared u32 cursor range",
                    entry.sequence
                ))
            })?;
            let mut transaction_id = [0u8; 64];
            transaction_id[32..].copy_from_slice(&entry.origin);
            let log_meta = LogMeta {
                address: self.mailbox_address,
                block_number: entry.block_height,
                block_hash: entry.block_hash,
                transaction_id: H512::from(transaction_id),
                transaction_index: 0,
                log_index: U256::from(sequence),
            };
            logs.push((
                Indexed::from(H256::from(message_id)).with_sequence(sequence),
                log_meta,
            ));
        }
        Ok(logs)
    }
}

#[async_trait]
impl Indexer<H256> for DuskDeliveryIndexer {
    /// The deliveries numbered `range` that the log holds. Contract sync
    /// asks only for sequences below the count `latest_sequence_count_and_tip`
    /// reported.
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<H256>, LogMeta)>> {
        let entries = self.rues.event_log_entries(
            &self.log,
            u64::from(*range.start()),
            u64::from(*range.end()),
        )?;
        let logs = self.deliveries(entries).await?;
        debug!(count = logs.len(), "Fetched delivery logs");
        Ok(logs)
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
        let pair = self.rues.block_pair().await?;
        let state = self
            .rues
            .advance_event_log(&self.log, DELIVERY_LOG_PAGES, Some(&pair))
            .await?;
        if state
            .complete_through
            .is_none_or(|through| through < block_height)
        {
            return Err(crate::HyperlaneDuskError::Other(format!(
                "Dusk delivery log holds the archive's finalized deliveries up to block {:?}, short of block {block_height}; retrying later",
                state.complete_through
            ))
            .into());
        }
        let transaction = crate::dusk_transaction_bytes(&tx_hash);
        let entries = self
            .rues
            .event_log_block(&self.log, block_height)?
            .into_iter()
            .filter(|entry| entry.origin == transaction)
            .collect();
        self.deliveries(entries).await
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
    /// The deliveries the log holds after reading on in the archive, and
    /// the height up to which it holds every finalized delivery.
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let pair = self.rues.block_pair().await?;
        let state = self
            .rues
            .advance_event_log(&self.log, DELIVERY_LOG_PAGES, Some(&pair))
            .await?;
        let count = u32::try_from(state.count).map_err(|_| {
            crate::HyperlaneDuskError::Other(format!(
                "Dusk delivery count {} exceeds the shared u32 cursor range",
                state.count
            ))
        })?;
        let tip = state.complete_through.unwrap_or_default();
        ensure_cursor_height(tip)?;
        Ok((Some(count), tip as u32))
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
