use std::ops::RangeInclusive;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, HyperlaneMessage, Indexed, Indexer, LogMeta, MerkleTreeInsertion,
    SequenceAwareIndexer, H512,
};

use crate::DuskMailboxIndexer;

/// Dusk MerkleTreeHook indexer — wraps the mailbox dispatch indexer
/// and converts `HyperlaneMessage` to `MerkleTreeInsertion`.
#[derive(Debug, Clone)]
pub struct DuskMerkleTreeHookIndexer {
    inner: DuskMailboxIndexer,
}

impl DuskMerkleTreeHookIndexer {
    /// Create a new merkle tree hook indexer wrapping a mailbox indexer.
    pub fn new(inner: DuskMailboxIndexer) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Indexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
        let messages = Indexer::<HyperlaneMessage>::fetch_logs_in_range(&self.inner, range).await?;
        let results = convert_messages(messages)?;
        debug!(count = results.len(), "Fetched merkle tree insertion logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(0)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
        let messages =
            Indexer::<HyperlaneMessage>::fetch_logs_by_tx_hash(&self.inner, tx_hash).await?;
        convert_messages(messages)
    }

    fn parse_tx_hash(&self, tx_hash: &str) -> ChainResult<H512> {
        Indexer::<HyperlaneMessage>::parse_tx_hash(&self.inner, tx_hash)
    }
}

fn convert_messages(
    messages: Vec<(Indexed<HyperlaneMessage>, LogMeta)>,
) -> ChainResult<Vec<(Indexed<MerkleTreeInsertion>, LogMeta)>> {
    let mut results = Vec::with_capacity(messages.len());
    for (indexed_msg, meta) in messages {
        let sequence = indexed_msg.sequence.ok_or_else(|| {
            crate::HyperlaneDuskError::Other(
                "Dusk mailbox message is missing its queried sequence".into(),
            )
        })?;
        let msg = indexed_msg.inner();
        if msg.nonce != sequence {
            return Err(crate::HyperlaneDuskError::Other(format!(
                "Dusk mailbox message nonce {} does not match queried sequence {sequence}",
                msg.nonce
            ))
            .into());
        }
        let insertion = MerkleTreeInsertion::new(sequence, msg.id());
        let indexed = Indexed::from(insertion).with_sequence(sequence);
        results.push((indexed, meta));
    }
    Ok(results)
}

#[async_trait]
impl SequenceAwareIndexer<MerkleTreeInsertion> for DuskMerkleTreeHookIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        SequenceAwareIndexer::<HyperlaneMessage>::latest_sequence_count_and_tip(&self.inner).await
    }
}
