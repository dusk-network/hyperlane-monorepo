use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, Indexed, Indexer, LogMeta, MerkleTreeInsertion, SequenceAwareIndexer, H256, H512,
    U256,
};
use hyperlane_dusk_types::events;

use crate::final_records::FinalRecords;
use crate::rues::{exact_event_data, rkyv_deserialize, rkyv_serialize};
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::RuesClient;

/// Most message IDs one `message_ids` query returns.
const MESSAGE_ID_PAGE_SIZE: u64 = 256;

/// Dusk MerkleTreeHook indexer anchored to the hook's own state and events.
#[derive(Debug, Clone)]
pub struct DuskMerkleTreeHookIndexer {
    rues: Arc<RuesClient>,
    hook_id: [u8; 32],
    hook_address: H256,
    /// Where the insertions last crossed a final height.
    insertions: Arc<FinalRecords>,
}

impl DuskMerkleTreeHookIndexer {
    /// Create an indexer for a deployed MerkleTreeHook contract.
    pub fn new(rues: Arc<RuesClient>, hook_id: H256) -> Self {
        Self {
            hook_id: hook_id.into(),
            hook_address: hook_id,
            insertions: rues.final_records(hook_id.into(), "inserted_block_height"),
            rues,
        }
    }

    async fn insertion_height(&self, sequence: u32) -> ChainResult<u64> {
        Ok(self
            .rues
            .contract_query(&self.hook_id, "inserted_block_height", &(sequence,))
            .await?)
    }

    async fn insertion_count(&self) -> ChainResult<u32> {
        Ok(self
            .rues
            .contract_query(&self.hook_id, "count", &())
            .await?)
    }

    /// How many insertions are at or below final block `block_height`.
    async fn insertions_at_or_below(&self, block_height: u64) -> ChainResult<u32> {
        self.insertions
            .count_at_or_below(
                block_height,
                || self.insertion_count(),
                |sequence| self.insertion_height(sequence),
            )
            .await
    }

    /// The insertions of final block `block_height`.
    async fn insertion_range_at_block(
        &self,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = match block_height.checked_sub(1) {
            Some(below) => self.insertions_at_or_below(below).await?,
            None => 0,
        };
        let after = self.insertions_at_or_below(block_height).await?;
        if after == first {
            return Ok(None);
        }
        Ok(Some(crate::bounded_block_range(
            first,
            after,
            "merkle insertion",
        )?))
    }

    /// The final insertions and the last final block.
    async fn finalized_insertion_count(&self) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self
            .insertions_at_or_below(u64::from(finalized_tip))
            .await?;
        Ok((finalized_count, finalized_tip))
    }

    /// The message IDs the hook stores at `range`, a page of at most
    /// `MESSAGE_ID_PAGE_SIZE` per query, each page as long as asked.
    async fn message_ids(&self, range: RangeInclusive<u32>) -> ChainResult<Vec<[u8; 32]>> {
        let (start, end) = (u64::from(*range.start()), u64::from(*range.end()));
        let mut ids = Vec::new();
        let mut next = start;
        while next <= end {
            let limit = (end - next + 1).min(MESSAGE_ID_PAGE_SIZE);
            let page: Vec<[u8; 32]> = self
                .rues
                .contract_query(&self.hook_id, "message_ids", &(next as u32, limit as u32))
                .await?;
            if page.len() as u64 != limit {
                return Err(crate::HyperlaneDuskError::Other(format!(
                    "MerkleTreeHook returned {} message IDs for page start={next} limit={limit}",
                    page.len()
                ))
                .into());
            }
            ids.extend(page);
            next += limit;
        }
        Ok(ids)
    }
}

#[async_trait]
impl Indexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
        let (finalized_count, _) = self.finalized_insertion_count().await?;

        // The final insertions in the range: their IDs a page at a time,
        // and each one's block.
        let mut stored = Vec::new();
        let start = *range.start();
        let end = (*range.end()).min(finalized_count.saturating_sub(1));
        if start < finalized_count {
            for (index, message_id) in (start..=end).zip(self.message_ids(start..=end).await?) {
                let block_number = self.insertion_height(index).await?;
                ensure_cursor_height(block_number)?;
                stored.push((index, block_number, message_id));
            }
        }

        // One archive pass per block, however many insertions it holds. The
        // expected event bytes carry both the message ID and the tree index.
        let mut results = Vec::with_capacity(stored.len());
        for block in stored.chunk_by(|a, b| a.1 == b.1) {
            let Some(&(_, block_number, _)) = block.first() else {
                continue;
            };
            let mut expected = Vec::with_capacity(block.len());
            for &(index, _, message_id) in block {
                expected.push((
                    index as usize,
                    rkyv_serialize(&events::InsertedIntoTree { message_id, index })?,
                ));
            }
            let sequences: Vec<usize> = expected.iter().map(|(index, _)| *index).collect();
            let matches = self
                .rues
                .finalized_contract_events_at_block(
                    &self.hook_id,
                    events::InsertedIntoTree::TOPIC,
                    block_number,
                    &sequences,
                    exact_event_data(expected),
                )
                .await?;

            for (&(index, _, message_id), matched) in block.iter().zip(matches) {
                let provenance = matched.provenance;
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
        // The transaction's own rows name the tree indices it inserted, so
        // only those are looked up, from the block's rows read once. Rows
        // that name no index are not evidence that it inserted nothing:
        // contract state decides that below.
        if let Some(rows) = self
            .rues
            .finalized_rows_of_transaction(
                &self.hook_id,
                events::InsertedIntoTree::TOPIC,
                block_height,
                &crate::dusk_transaction_bytes(&tx_hash),
            )
            .await?
        {
            let indices: std::collections::BTreeSet<u32> = rows
                .iter()
                .filter_map(|data| rkyv_deserialize::<events::InsertedIntoTree>(data).ok())
                .map(|event| event.index)
                .collect();
            if !indices.is_empty() {
                return crate::transaction_logs(
                    self,
                    indices,
                    tx_hash,
                    block_height,
                    "merkle insertion",
                )
                .await;
            }
        }
        let Some(range) = self.insertion_range_at_block(block_height).await? else {
            return Ok(vec![]);
        };
        crate::ensure_tx_hash_lookup_budget(&range, "merkle insertion")?;
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

#[async_trait]
impl SequenceAwareIndexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    /// The final insertions and the last final block. While the node has
    /// finalized no new block, this reads only the shared `lastBlockPair`.
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let (finalized_count, finalized_tip) = self.finalized_insertion_count().await?;
        Ok((Some(finalized_count), finalized_tip))
    }
}
