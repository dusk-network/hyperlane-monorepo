use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use hyperlane_core::{
    accumulator::incremental::IncrementalMerkle, BatchResult, ChainResult, Checkpoint,
    CheckpointAtBlock, FixedPointNumber, HyperlaneChain, HyperlaneContract, HyperlaneDomain,
    HyperlaneMessage, HyperlaneProvider, IncrementalMerkleAtBlock, Mailbox, MerkleTreeHook,
    Metadata, QueueOperation, ReorgPeriod, TxCostEstimate, TxOutcome, H256, U256,
};

use crate::rues::rkyv_serialize;
use crate::{ConnectionConf, DuskProvider, DuskSigner, HyperlaneDuskError, RuesClient};

const TX_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Dusk Mailbox — implements both `Mailbox` and `MerkleTreeHook` traits.
#[derive(Debug, Clone)]
pub struct DuskMailbox {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    mailbox_id: [u8; 32],
    merkle_tree_hook_id: [u8; 32],
    domain: HyperlaneDomain,
    signer: Option<DuskSigner>,
    conn: ConnectionConf,
}

impl DuskMailbox {
    /// Create a new DuskMailbox.
    pub fn new(
        provider: Arc<DuskProvider>,
        rues: Arc<RuesClient>,
        mailbox_id: H256,
        merkle_tree_hook_id: H256,
        domain: HyperlaneDomain,
        signer: Option<DuskSigner>,
        conn: ConnectionConf,
    ) -> Self {
        Self {
            provider,
            rues,
            mailbox_id: mailbox_id.into(),
            merkle_tree_hook_id: merkle_tree_hook_id.into(),
            domain,
            signer,
            conn,
        }
    }

    /// Encode a HyperlaneMessage to the Dusk wire format.
    fn encode_message(message: &HyperlaneMessage) -> Vec<u8> {
        hyperlane_dusk_types::message::encode(
            message.version,
            message.nonce,
            message.origin,
            message.sender.into(),
            message.destination,
            message.recipient.into(),
            &message.body,
        )
    }

    async fn merkle_tree_count(&self) -> ChainResult<u32> {
        Ok(self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "count", &())
            .await?)
    }

    async fn merkle_insertion_height(&self, index: u32) -> ChainResult<u64> {
        Ok(self
            .rues
            .contract_query(
                &self.merkle_tree_hook_id,
                "inserted_block_height",
                &(index,),
            )
            .await?)
    }

    async fn first_merkle_insertion_at_or_after(
        &self,
        count: u32,
        block_height: u64,
    ) -> ChainResult<u32> {
        let mut low = 0u32;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.merkle_insertion_height(middle).await? < block_height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn merkle_count_at_height(&self, count: u32, block_height: u64) -> ChainResult<u32> {
        self.first_merkle_insertion_at_or_after(count, block_height.saturating_add(1))
            .await
    }

    async fn finalized_merkle_view(&self) -> ChainResult<(u32, u64)> {
        let finalized_height = self.rues.finalized_block_height().await?;
        let count = self.merkle_tree_count().await?;
        let finalized_count = self.merkle_count_at_height(count, finalized_height).await?;
        Ok((finalized_count, finalized_height))
    }

    async fn merkle_root_at(&self, index: u32) -> ChainResult<H256> {
        let root: [u8; 32] = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "root_at", &(index,))
            .await?;
        Ok(H256::from_slice(&root))
    }

    async fn finalized_tree(&self) -> ChainResult<IncrementalMerkleAtBlock> {
        let (count, finalized_height) = self.finalized_merkle_view().await?;
        let mut tree = IncrementalMerkle::default();
        for index in 0..count {
            let message_id: [u8; 32] = self
                .rues
                .contract_query(&self.merkle_tree_hook_id, "message_id_at", &(index,))
                .await?;
            tree.ingest(H256::from_slice(&message_id));
        }

        if count > 0 {
            let expected_root = self.merkle_root_at(count - 1).await?;
            if tree.root() != expected_root {
                return Err(HyperlaneDuskError::Other(format!(
                    "Reconstructed finalized merkle root mismatch: on-chain={expected_root:?} local={:?} count={count}",
                    tree.root()
                ))
                .into());
            }
        }

        Ok(IncrementalMerkleAtBlock {
            tree,
            block_height: Some(finalized_height),
        })
    }

    async fn checkpoint_at_height(&self, block_height: u64) -> ChainResult<CheckpointAtBlock> {
        let finalized_height = self.rues.finalized_block_height().await?;
        let effective_height = block_height.min(finalized_height);
        let current_count = self.merkle_tree_count().await?;
        let count = self
            .merkle_count_at_height(current_count, effective_height)
            .await?;
        if count == 0 {
            return Err(HyperlaneDuskError::Other(format!(
                "MerkleTreeHook has no insertion at or before finalized block {effective_height}"
            ))
            .into());
        }
        let index = count - 1;
        Ok(CheckpointAtBlock {
            checkpoint: Checkpoint {
                merkle_tree_hook_address: H256::from_slice(&self.merkle_tree_hook_id),
                mailbox_domain: self.domain.id(),
                root: self.merkle_root_at(index).await?,
                index,
            },
            block_height: Some(effective_height),
        })
    }
}

impl HyperlaneChain for DuskMailbox {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new((*self.provider).clone())
    }
}

impl HyperlaneContract for DuskMailbox {
    fn address(&self) -> H256 {
        H256::from_slice(&self.mailbox_id)
    }
}

#[async_trait]
impl Mailbox for DuskMailbox {
    fn domain_hash(&self) -> H256 {
        // Compute domain_hash = keccak256(local_domain || mailbox_address || "HYPERLANE")
        let mut preimage = Vec::with_capacity(4 + 32 + 9);
        preimage.extend_from_slice(&self.domain.id().to_be_bytes());
        preimage.extend_from_slice(&self.mailbox_id);
        preimage.extend_from_slice(b"HYPERLANE");
        let hash = hyperlane_dusk_types::message::keccak256(&preimage);
        H256::from_slice(&hash)
    }

    async fn count(&self, _reorg_period: &ReorgPeriod) -> ChainResult<u32> {
        let nonce: u32 = self
            .rues
            .contract_query(&self.mailbox_id, "nonce", &())
            .await?;
        Ok(nonce)
    }

    async fn delivered(&self, id: H256) -> ChainResult<bool> {
        let id_bytes: [u8; 32] = id.into();
        let delivered: bool = self
            .rues
            .contract_query(&self.mailbox_id, "delivered", &(id_bytes,))
            .await?;
        Ok(delivered)
    }

    async fn default_ism(&self) -> ChainResult<H256> {
        let ism_bytes: [u8; 32] = self
            .rues
            .contract_query(&self.mailbox_id, "default_ism", &())
            .await?;
        Ok(H256::from_slice(&ism_bytes))
    }

    async fn recipient_ism(&self, recipient: H256) -> ChainResult<H256> {
        let recipient_bytes: [u8; 32] = recipient.into();
        let ism_bytes: [u8; 32] = self
            .rues
            .contract_query(&self.mailbox_id, "recipient_ism", &(recipient_bytes,))
            .await?;
        Ok(H256::from_slice(&ism_bytes))
    }

    async fn process(
        &self,
        message: &HyperlaneMessage,
        metadata: &Metadata,
        tx_gas_limit: Option<U256>,
    ) -> ChainResult<TxOutcome> {
        let signer = self
            .signer
            .as_ref()
            .ok_or(HyperlaneDuskError::SignerUnavailable)?;

        let encoded = Self::encode_message(message);
        let metadata_bytes = metadata.to_owned();

        info!(
            message_id = ?message.id(),
            nonce = message.nonce,
            "Processing message on Dusk via dusk-tx"
        );

        let args = crate::tx_sender::process_args(&metadata_bytes, &encoded)?;

        let gas_limit = tx_gas_limit
            .map(|limit| {
                if limit > U256::from(u64::MAX) {
                    return Err(HyperlaneDuskError::Other(format!(
                        "Dusk transaction gas limit {limit} exceeds u64"
                    )));
                }
                Ok(limit.low_u64())
            })
            .transpose()?;

        let res = crate::tx_sender::dusk_tx_call(
            &self.conn,
            signer,
            &self.mailbox_id,
            "process",
            &args,
            gas_limit,
        )
        .await?;

        let tx_id = res.get("tx_id").and_then(|v| v.as_str()).ok_or_else(|| {
            HyperlaneDuskError::Other(format!("dusk-tx response is missing string tx_id: {res}"))
        })?;
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(tx_id)?;
        let confirmed = self
            .rues
            .wait_for_tx(tx_id, TX_CONFIRMATION_TIMEOUT)
            .await?;
        let executed = confirmed.error.is_none();
        if let Some(error) = &confirmed.error {
            warn!(tx_id, %error, "Dusk mailbox transaction execution failed");
        }

        Ok(TxOutcome {
            transaction_id,
            executed,
            gas_used: U256::from(confirmed.gas_spent),
            gas_price: FixedPointNumber::from(self.conn.gas_price),
        })
    }

    fn supports_batching(&self) -> bool {
        false
    }

    async fn process_batch<'a>(&self, _ops: Vec<&'a QueueOperation>) -> ChainResult<BatchResult> {
        Err(ChainCommunicationError::BatchingFailed)
    }

    async fn process_estimate_costs(
        &self,
        _message: &HyperlaneMessage,
        _metadata: &Metadata,
    ) -> ChainResult<TxCostEstimate> {
        Ok(TxCostEstimate {
            gas_limit: U256::from(self.conn.gas_limit),
            gas_price: FixedPointNumber::from(self.conn.gas_price),
            l2_gas_limit: None,
        })
    }

    async fn process_calldata(
        &self,
        message: &HyperlaneMessage,
        metadata: &Metadata,
    ) -> ChainResult<Vec<u8>> {
        let encoded = Self::encode_message(message);
        let metadata_bytes = metadata.to_owned();
        Ok(rkyv_serialize(&(metadata_bytes, encoded))?)
    }

    fn delivered_calldata(&self, message_id: H256) -> ChainResult<Option<Vec<u8>>> {
        let id_bytes: [u8; 32] = message_id.into();
        Ok(Some(rkyv_serialize(&(id_bytes,))?))
    }
}

// Also implement MerkleTreeHook, since the Dusk mailbox struct holds
// both mailbox and merkle tree hook contract IDs.
#[async_trait]
impl MerkleTreeHook for DuskMailbox {
    async fn tree(&self, _reorg_period: &ReorgPeriod) -> ChainResult<IncrementalMerkleAtBlock> {
        // Rusk does not expose historical contract-state queries. Reconstruct
        // the one-time validator start tree from hook-owned insertion history,
        // capped at consensus finality, and verify it against the stored root.
        self.finalized_tree().await
    }

    async fn count(&self, _reorg_period: &ReorgPeriod) -> ChainResult<u32> {
        Ok(self.finalized_merkle_view().await?.0)
    }

    async fn latest_checkpoint(
        &self,
        _reorg_period: &ReorgPeriod,
    ) -> ChainResult<CheckpointAtBlock> {
        let finalized_height = self.rues.finalized_block_height().await?;
        self.checkpoint_at_height(finalized_height).await
    }

    async fn latest_checkpoint_at_block(&self, height: u64) -> ChainResult<CheckpointAtBlock> {
        self.checkpoint_at_height(height).await
    }
}

// Bring ChainCommunicationError into scope for process_batch
use hyperlane_core::ChainCommunicationError;
