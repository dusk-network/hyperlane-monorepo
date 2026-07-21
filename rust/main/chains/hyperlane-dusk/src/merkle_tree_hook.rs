use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, Indexed, Indexer, LogMeta, MerkleTreeInsertion, SequenceAwareIndexer, H256, H512,
    U256,
};
use hyperlane_dusk_types::events;

use crate::rues::rkyv_serialize;
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::RuesClient;

/// Dusk MerkleTreeHook indexer anchored to the hook's own state and events.
#[derive(Debug, Clone)]
pub struct DuskMerkleTreeHookIndexer {
    rues: Arc<RuesClient>,
    hook_id: [u8; 32],
    hook_address: H256,
}

impl DuskMerkleTreeHookIndexer {
    /// Create an indexer for a deployed MerkleTreeHook contract.
    pub fn new(rues: Arc<RuesClient>, hook_id: H256) -> Self {
        Self {
            rues,
            hook_id: hook_id.into(),
            hook_address: hook_id,
        }
    }

    async fn insertion_height(&self, sequence: u32) -> ChainResult<u64> {
        Ok(self
            .rues
            .contract_query(&self.hook_id, "inserted_block_height", &(sequence,))
            .await?)
    }

    async fn first_insertion_at_or_after(&self, count: u32, block_height: u64) -> ChainResult<u32> {
        let mut low = 0u32;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.insertion_height(middle).await? < block_height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn insertion_range_at_block(
        &self,
        count: u32,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = self
            .first_insertion_at_or_after(count, block_height)
            .await?;
        if first == count || self.insertion_height(first).await? != block_height {
            return Ok(None);
        }
        let after = self
            .first_insertion_at_or_after(count, block_height.saturating_add(1))
            .await?;
        Ok(Some(first..=after - 1))
    }

    async fn finalized_insertion_count(&self, current_count: u32) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self
            .first_insertion_at_or_after(current_count, u64::from(finalized_tip) + 1)
            .await?;
        Ok((finalized_count, finalized_tip))
    }
}

#[async_trait]
impl Indexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
        let count: u32 = self
            .rues
            .contract_query(&self.hook_id, "count", &())
            .await?;
        let (finalized_count, _) = self.finalized_insertion_count(count).await?;
        let mut results = Vec::new();

        for index in range {
            if index >= finalized_count {
                break;
            }
            let message_id: [u8; 32] = self
                .rues
                .contract_query(&self.hook_id, "message_id_at", &(index,))
                .await?;
            let block_number = self.insertion_height(index).await?;
            ensure_cursor_height(block_number)?;

            let expected_event = rkyv_serialize(&events::InsertedIntoTree { message_id, index })?;
            let provenance = self
                .rues
                .finalized_contract_event(
                    &self.hook_id,
                    events::InsertedIntoTree::TOPIC,
                    index as usize,
                    block_number,
                    &expected_event,
                )
                .await?;

            let insertion = MerkleTreeInsertion::new(index, H256::from_slice(&message_id));
            let indexed = Indexed::from(insertion).with_sequence(index);
            let meta = LogMeta {
                address: self.hook_address,
                block_number: provenance.block_height,
                block_hash: provenance.block_hash,
                transaction_id: provenance.transaction_id,
                transaction_index: 0,
                log_index: U256::from(index),
            };
            results.push((indexed, meta));
        }

        debug!(count = results.len(), "Fetched merkle tree insertion logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(self.rues.finalized_block_number().await?)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
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
            .contract_query(&self.hook_id, "count", &())
            .await?;
        let Some(range) = self.insertion_range_at_block(count, block_height).await? else {
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
impl SequenceAwareIndexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let count: u32 = self
            .rues
            .contract_query(&self.hook_id, "count", &())
            .await?;
        let (finalized_count, finalized_tip) = self.finalized_insertion_count(count).await?;
        Ok((Some(finalized_count), finalized_tip))
    }
}
