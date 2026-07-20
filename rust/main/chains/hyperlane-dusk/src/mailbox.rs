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
        _tx_gas_limit: Option<U256>,
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

        let res =
            crate::tx_sender::dusk_tx_call(&self.conn, signer, &self.mailbox_id, "process", &args)
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
        // Prefer reading the tree state directly from the MerkleTreeHook
        // contract (O(1) query). This avoids reconstructing the tree by replaying
        // all dispatched messages (O(count) queries).
        let dusk_tree: hyperlane_dusk_types::merkle::IncrementalMerkle = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "tree", &())
            .await?;

        let mut tree = IncrementalMerkle::default();
        tree.count = dusk_tree.count as usize;
        for (i, node) in dusk_tree.branch.iter().enumerate() {
            tree.branch[i] = H256::from_slice(node);
        }

        // Sanity-check the loaded tree matches the on-chain root.
        let onchain_root: [u8; 32] = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "root", &())
            .await?;
        let onchain_root = H256::from_slice(&onchain_root);
        if tree.root() != onchain_root {
            return Err(HyperlaneDuskError::Other(format!(
                "Loaded merkle tree root mismatch: on-chain={onchain_root:?} local={:?} count={}",
                tree.root(),
                dusk_tree.count
            ))
            .into());
        }

        Ok(IncrementalMerkleAtBlock {
            tree,
            block_height: None,
        })
    }

    async fn count(&self, _reorg_period: &ReorgPeriod) -> ChainResult<u32> {
        let count: u32 = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "count", &())
            .await?;
        Ok(count)
    }

    async fn latest_checkpoint(
        &self,
        _reorg_period: &ReorgPeriod,
    ) -> ChainResult<CheckpointAtBlock> {
        let (root, index): ([u8; 32], u32) = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "latest_checkpoint", &())
            .await?;

        Ok(CheckpointAtBlock {
            checkpoint: Checkpoint {
                merkle_tree_hook_address: H256::from_slice(&self.merkle_tree_hook_id),
                mailbox_domain: self.domain.id(),
                root: H256::from_slice(&root),
                index,
            },
            block_height: None,
        })
    }

    async fn latest_checkpoint_at_block(&self, _height: u64) -> ChainResult<CheckpointAtBlock> {
        // Dusk does not support point-in-time queries.
        // Return the current checkpoint.
        self.latest_checkpoint(&ReorgPeriod::None).await
    }
}

// Bring ChainCommunicationError into scope for process_batch
use hyperlane_core::ChainCommunicationError;
