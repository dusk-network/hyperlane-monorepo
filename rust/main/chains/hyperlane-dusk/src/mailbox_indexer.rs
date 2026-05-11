use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, HyperlaneMessage, Indexed, Indexer, LogMeta, SequenceAwareIndexer, H256, H512,
    U256,
};

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

    fn log_meta_for_sequence(&self, sequence: u32, block_number: u64) -> LogMeta {
        LogMeta {
            address: self.mailbox_address,
            block_number,
            block_hash: H256::zero(),
            transaction_id: H512::zero(),
            transaction_index: 0,
            log_index: U256::from(sequence),
        }
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

        for nonce in range {
            if nonce >= current_nonce {
                break;
            }
            let block_number: u64 = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_block_height", &(nonce,))
                .await
                .unwrap_or_default();
            // Query the encoded dispatched message at this nonce.
            let encoded: Vec<u8> = self
                .rues
                .contract_query(&self.mailbox_id, "dispatched_message", &(nonce,))
                .await?;

            // Decode the Dusk-format message.
            let dusk_msg = hyperlane_dusk_types::message::decode(&encoded)
                .ok_or_else(|| {
                    crate::HyperlaneDuskError::Other(format!(
                        "Failed to decode dispatched message at nonce {nonce}"
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

            let log_meta = self.log_meta_for_sequence(nonce, block_number);
            let indexed = Indexed::from(core_msg).with_sequence(nonce);
            results.push((indexed, log_meta));
        }

        debug!(count = results.len(), "Fetched dispatch logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        // Not used for sequence-based indexing.
        Ok(0)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        _tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<HyperlaneMessage>, LogMeta)>> {
        Ok(vec![])
    }
}

#[async_trait]
impl SequenceAwareIndexer<HyperlaneMessage> for DuskMailboxIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let nonce: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        // tip = 0 since we use sequence-based indexing (no block concept)
        Ok((Some(nonce), 0))
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

        for index in range {
            if index >= processed_count {
                break;
            }
            let message_id: [u8; 32] = self
                .rues
                .contract_query(&self.mailbox_id, "processed_at_index", &(index,))
                .await?;

            let block_number: u64 = self
                .rues
                .contract_query(&self.mailbox_id, "processed_block_height_at_index", &(index,))
                .await
                .unwrap_or_default();

            let log_meta = LogMeta {
                address: self.mailbox_address,
                block_number,
                block_hash: H256::zero(),
                transaction_id: H512::zero(),
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
        Ok(0)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        _tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<H256>, LogMeta)>> {
        Ok(vec![])
    }
}

#[async_trait]
impl SequenceAwareIndexer<H256> for DuskDeliveryIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let count: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "processed_count", &())
            .await?;
        Ok((Some(count), 0))
    }
}
