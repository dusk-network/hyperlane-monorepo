use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, HyperlaneMessage, Indexed, Indexer, LogMeta, SequenceAwareIndexer, H256, H512,
    U256,
};
use hyperlane_dusk_types::events;

use crate::rues::rkyv_serialize;
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::RuesClient;

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
}

impl DuskMailboxIndexer {
    /// Create a new indexer.
    pub fn new(rues: Arc<RuesClient>, mailbox_id: H256) -> Self {
        Self {
            rues,
            mailbox_id: mailbox_id.into(),
            mailbox_address: mailbox_id,
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
        Ok(Some(first..=after - 1))
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
        let mut results = Vec::new();

        // Mailbox.nonce is the next dispatch nonce; messages exist for [0, nonce).
        let current_nonce: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        let (finalized_nonce, _) = self.finalized_dispatch_count(current_nonce).await?;

        for nonce in range {
            if nonce >= finalized_nonce {
                break;
            }
            let block_number: u64 = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_block_height", &(nonce,))
                .await?;
            ensure_cursor_height(block_number)?;
            // Query the encoded dispatched message at this nonce.
            let encoded: Vec<u8> = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_message", &(nonce,))
                .await?;

            // Decode the Dusk-format message.
            let dusk_msg = hyperlane_dusk_types::message::decode(&encoded).ok_or_else(|| {
                crate::HyperlaneDuskError::Other(format!(
                    "Failed to decode dispatched message at nonce {nonce}"
                ))
            })?;
            if dusk_msg.nonce != nonce {
                return Err(crate::HyperlaneDuskError::Other(format!(
                    "Dispatched message nonce {} does not match queried sequence {nonce}",
                    dusk_msg.nonce
                ))
                .into());
            }

            let expected_event = rkyv_serialize(&events::Dispatch {
                sender: dusk_msg.sender,
                destination: dusk_msg.destination,
                recipient: dusk_msg.recipient,
                message: encoded.clone(),
            })?;
            let provenance = self
                .rues
                .finalized_contract_event(
                    &self.mailbox_id,
                    events::Dispatch::TOPIC,
                    nonce as usize,
                    block_number,
                    &expected_event,
                )
                .await?;

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
        let mut logs = self.fetch_logs_in_range(range).await?;
        logs.retain(|(_, meta)| meta.transaction_id == tx_hash);
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
        Ok(Some(first..=after - 1))
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
        let mut results = Vec::new();

        // Messages exist for [0, processed_count).
        let processed_count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "processed_count", &())
            .await?;
        let (finalized_count, _) = self.finalized_process_count(processed_count).await?;

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

            let expected_event = rkyv_serialize(&events::ProcessId { message_id })?;
            let provenance = self
                .rues
                .finalized_contract_event(
                    &self.mailbox_id,
                    events::ProcessId::TOPIC,
                    index as usize,
                    block_number,
                    &expected_event,
                )
                .await?;

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
        let mut logs = self.fetch_logs_in_range(range).await?;
        logs.retain(|(_, meta)| meta.transaction_id == tx_hash);
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
