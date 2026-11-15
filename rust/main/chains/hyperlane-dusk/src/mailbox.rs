use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use hyperlane_core::{
    accumulator::incremental::IncrementalMerkle, BatchResult, ChainResult, Checkpoint,
    CheckpointAtBlock, FixedPointNumber, HyperlaneChain, HyperlaneContract, HyperlaneDomain,
    HyperlaneMessage, HyperlaneProvider, IncrementalMerkleAtBlock, Mailbox, MerkleTreeHook,
    Metadata, QueueOperation, ReorgPeriod, TxCostEstimate, TxOutcome, H256, U256,
};

use hyperlane_dusk_types::events;
use hyperlane_dusk_types::merkle::{IncrementalMerkle as HookTree, TREE_DEPTH};

use crate::rues::{
    bounded_body, bounded_error, bounded_json, bounded_text, rkyv_deserialize, rkyv_serialize,
    LogLookup,
};
use crate::{ConnectionConf, DuskProvider, DuskSigner, HyperlaneDuskError, RuesClient};

const TX_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);
/// Most message IDs one `message_ids` query returns.
const MERKLE_HISTORY_PAGE_SIZE: u32 = 256;
/// Archive pages a delivery check reads to bring the delivery log up to
/// date, on to the end of the block being read.
const DELIVERED_LOG_PAGES: usize = 4;

/// Dusk Mailbox contract adapter.
#[derive(Debug, Clone)]
pub struct DuskMailbox {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    mailbox_id: [u8; 32],
    merkle_tree_hook_id: [u8; 32],
    domain: HyperlaneDomain,
    signer: Option<DuskSigner>,
    conn: ConnectionConf,
    /// The hook's Merkle tree as far as checkpoints have read it, shared by
    /// every clone.
    merkle: Arc<tokio::sync::Mutex<MerkleCache>>,
}

/// The MerkleTreeHook keeps no root per insertion, so checkpoints compute
/// roots from the message IDs it stores: this is the tree of its first
/// `tree.count()` leaves. It only ever holds leaves of final insertions,
/// which no rollback can change, so it is extended, never rebuilt, unless a
/// check against the hook fails. It takes 32 branch nodes and a count,
/// however many leaves it holds.
#[derive(Debug, Default)]
struct MerkleCache {
    tree: IncrementalMerkle,
    /// The last leaf ingested, `None` for an empty tree.
    last: Option<H256>,
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
            merkle: Arc::new(tokio::sync::Mutex::new(MerkleCache::default())),
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

    /// Run `process` as an unsigned contract query before paying for it.
    ///
    /// Dusk charges a failed transaction its whole gas limit, so a delivery
    /// that would revert must not be submitted. Rusk runs contract queries in
    /// a throwaway session on the current tip: writes are discarded and
    /// nothing is signed, so an endpoint gets no transaction it could replay.
    /// A contract panic comes back as an HTTP error.
    ///
    /// This filters out deliveries that fail; it cannot prove one will
    /// succeed. A query runs with the node's block gas limit rather than the
    /// transaction's, and a recipient can tell a query from a transaction.
    ///
    /// `process` returns `()`, so a successful query replies with exactly its
    /// encoding. Any other success reply, such as a proxy's error page sent
    /// with status 200, did not come from running `process` and fails the
    /// simulation.
    async fn simulate_process(&self, message: &HyperlaneMessage, args: &[u8]) -> ChainResult<()> {
        let unit = rkyv_serialize(&())?;
        let failure = match self
            .rues
            .contract_query_raw(&self.mailbox_id, "process", args)
            .await
        {
            Ok(reply) if reply == unit => return Ok(()),
            Ok(reply) => format!(
                "replied with {} bytes instead of the encoding of (): {}",
                reply.len(),
                bounded_body(&reply)
            ),
            Err(error) => error.to_string(),
        };
        warn!(
            message_id = ?message.id(),
            error = %failure,
            "Dusk process simulation failed; not submitting the message"
        );
        Err(ChainCommunicationError::SimulationFailed(format!(
            "Dusk Mailbox process query failed for message {:?}: {failure}",
            message.id()
        )))
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

    async fn finalized_merkle_view(&self, reorg_period: &ReorgPeriod) -> ChainResult<(u32, u64)> {
        let finalized_height = self.rues.checkpoint_block_height(reorg_period).await?;
        let count = self.merkle_tree_count().await?;
        let finalized_count = self.merkle_count_at_height(count, finalized_height).await?;
        Ok((finalized_count, finalized_height))
    }

    /// The tree of the hook's first `count` leaves, ingested from the
    /// message IDs it stores, and checked against the hook (`verify_tree`).
    ///
    /// The shared tree is extended page by page, so a call that is cancelled
    /// or fails to read keeps the pages it read. A tree of fewer leaves than
    /// the shared one holds, for an earlier height, is built on its own. A
    /// failed check drops the shared tree, so the next call rebuilds it from
    /// the hook's first leaf: no root is computed from IDs that disagree
    /// with the hook.
    async fn tree_at(&self, count: u32) -> ChainResult<IncrementalMerkle> {
        let mut shared = self.merkle.lock().await;
        let (tree, last) = if shared.tree.count() <= count as usize {
            self.extend_tree(&mut shared, count).await?;
            (shared.tree.clone(), shared.last)
        } else {
            let mut earlier = MerkleCache::default();
            self.extend_tree(&mut earlier, count).await?;
            (earlier.tree, earlier.last)
        };
        if let Err(detail) = self.verify_tree(&tree, last).await? {
            *shared = MerkleCache::default();
            warn!(
                hook = %hex::encode(self.merkle_tree_hook_id),
                leaves = tree.count(),
                %detail,
                "Dusk Merkle tree computed from the hook's stored IDs disagrees with the hook; discarding it"
            );
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk Merkle tree of {} leaves computed from MerkleTreeHook message IDs disagrees with the hook: {detail}",
                tree.count()
            ))
            .into());
        }
        Ok(tree)
    }

    /// Ingest the hook's stored message IDs from `cache`'s leaf count up to
    /// `count`, a page of at most `MERKLE_HISTORY_PAGE_SIZE` at a time, each
    /// page as long as asked.
    async fn extend_tree(&self, cache: &mut MerkleCache, count: u32) -> ChainResult<()> {
        loop {
            let start = u32::try_from(cache.tree.count()).map_err(|_| {
                HyperlaneDuskError::Other("Dusk Merkle tree count exceeds u32".into())
            })?;
            if start >= count {
                return Ok(());
            }
            let limit = (count - start).min(MERKLE_HISTORY_PAGE_SIZE);
            let message_ids: Vec<[u8; 32]> = self
                .rues
                .contract_query(&self.merkle_tree_hook_id, "message_ids", &(start, limit))
                .await?;
            if message_ids.len() != limit as usize {
                return Err(HyperlaneDuskError::Other(format!(
                    "MerkleTreeHook returned {} message IDs for page start={start} limit={limit}",
                    message_ids.len()
                ))
                .into());
            }
            for message_id in message_ids {
                let leaf = H256::from(message_id);
                cache.tree.ingest(leaf);
                cache.last = Some(leaf);
            }
        }
    }

    /// Check a tree computed from the hook's stored IDs against the hook
    /// itself: `Ok(Err(detail))` when they disagree. Its last leaf must be
    /// the ID the hook stores at that index (`message_id_at`). When the hook
    /// holds exactly these leaves, its own tree (`tree`, one read of its
    /// branch and count, so a dispatch in between is told apart from a
    /// mismatch) must have the same count, root and root-making branch nodes
    /// (`matches_hook_tree`). A tree of fewer leaves than the hook holds can
    /// only be checked by its last ID: the hook keeps no earlier roots.
    async fn verify_tree(
        &self,
        tree: &IncrementalMerkle,
        last: Option<H256>,
    ) -> ChainResult<Result<(), String>> {
        let Some(index) = tree.count().checked_sub(1) else {
            return Ok(Ok(()));
        };
        let index = u32::try_from(index)
            .map_err(|_| HyperlaneDuskError::Other("Dusk Merkle tree index exceeds u32".into()))?;
        let stored: [u8; 32] = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "message_id_at", &(index,))
            .await?;
        if last != Some(H256::from(stored)) {
            return Ok(Err(format!(
                "message_id_at({index}) is {:?}, the last ingested ID {last:?}",
                H256::from(stored)
            )));
        }
        let hook: HookTree = self
            .rues
            .contract_query(&self.merkle_tree_hook_id, "tree", &())
            .await?;
        if hook.count as usize != tree.count() {
            return Ok(Ok(()));
        }
        if !matches_hook_tree(tree, &hook) {
            return Ok(Err(format!(
                "the hook's own tree of {} leaves has root {:?}, the computed one {:?}",
                hook.count,
                H256::from(hook.root()),
                tree.root()
            )));
        }
        Ok(Ok(()))
    }

    async fn finalized_tree(
        &self,
        reorg_period: &ReorgPeriod,
    ) -> ChainResult<IncrementalMerkleAtBlock> {
        let (count, finalized_height) = self.finalized_merkle_view(reorg_period).await?;
        Ok(IncrementalMerkleAtBlock {
            tree: self.tree_at(count).await?,
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
        let tree = self.tree_at(count).await?;
        Ok(CheckpointAtBlock {
            checkpoint: Checkpoint {
                merkle_tree_hook_address: H256::from_slice(&self.merkle_tree_hook_id),
                mailbox_domain: self.domain.id(),
                root: tree.root(),
                index: count - 1,
            },
            block_height: Some(effective_height),
        })
    }
}

/// Whether `tree`, computed from the hook's stored IDs, is the hook's own
/// tree `hook`: the same leaf count, the same branch nodes at the levels the
/// count's set bits select, and the same root. Those levels alone determine
/// the root. The others are not compared: the hook starts every branch node
/// at zero, where hyperlane-core starts each at the empty subtree's hash, so
/// two trees of the same leaves differ there.
fn matches_hook_tree(tree: &IncrementalMerkle, hook: &HookTree) -> bool {
    let count = hook.count as usize;
    count == tree.count()
        && (0..TREE_DEPTH)
            .filter(|level| (count >> level) & 1 == 1)
            .all(|level| hook.branch[level] == tree.branch[level].0)
        && H256::from(hook.root()) == tree.root()
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

/// Identity-correct MerkleTreeHook adapter.
///
/// The Mailbox and hook share query helpers, but Hyperlane checkpoints bind to
/// the hook contract address. Returning the Mailbox identity here creates a
/// deterministic false-reorg at validator startup.
#[derive(Debug, Clone)]
pub struct DuskMerkleTreeHook {
    inner: DuskMailbox,
}

impl DuskMerkleTreeHook {
    /// Create a hook adapter over the same Dusk connection and topology. A
    /// client made `with_identity` checks the endpoint's deployment before
    /// its first read, so an offline quorum endpoint keeps its voting slot
    /// and can recover on a later read.
    pub fn new(inner: DuskMailbox) -> Self {
        Self { inner }
    }
}

impl HyperlaneChain for DuskMerkleTreeHook {
    fn domain(&self) -> &HyperlaneDomain {
        self.inner.domain()
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        self.inner.provider()
    }
}

impl HyperlaneContract for DuskMerkleTreeHook {
    fn address(&self) -> H256 {
        H256::from_slice(&self.inner.merkle_tree_hook_id)
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

    async fn count(&self, reorg_period: &ReorgPeriod) -> ChainResult<u32> {
        Ok(self.finalized_merkle_view(reorg_period).await?.0)
    }

    /// Whether the message is delivered in a final block.
    ///
    /// The relayer stops retrying a message once this returns `true`, but
    /// the `delivered` query reads the node's tip, which Rusk can still roll
    /// back. So a delivery the tip holds counts only once the agent's
    /// delivery log holds its `ProcessId` row in a block that
    /// `checkBlock(onlyFinalized)` accepts: no rollback can undo it, whoever
    /// delivered the message. Without that row this fails with `NotFinal`,
    /// whether the log holds every final block, does not reach the last one
    /// yet, cannot be read, or the client keeps none, and the relayer checks
    /// again later; a delivery that is rolled back reads as not delivered,
    /// and the message is relayed again.
    ///
    /// The blocks above the last final one never make a delivery count. They
    /// are read only where the log holds every final block, to name the
    /// block that holds the delivery while it is not final: a check reads
    /// the chain up to a tip that is older than the state it read, so a
    /// block accepted and rolled back within its requests leaves no trace
    /// there.
    async fn delivered(&self, id: H256) -> ChainResult<bool> {
        let id_bytes: [u8; 32] = id.into();
        let delivered_at_tip = || async {
            self.rues
                .contract_query::<_, bool>(&self.mailbox_id, "delivered", &(id_bytes,))
                .await
        };
        if !delivered_at_tip().await? {
            return Ok(false);
        }
        // Read the chain before the state the check holds, so that no
        // rollback in between goes unnoticed.
        let pair = self.rues.block_pair().await?;
        if !delivered_at_tip().await? {
            return Ok(false);
        }
        let lookup = self
            .rues
            .finalized_event_log_lookup(
                &crate::mailbox_indexer::delivery_log(self.mailbox_id),
                &id_bytes,
                &pair,
                DELIVERED_LOG_PAGES,
            )
            .await;
        // Only a finalized delivery counts.
        let final_block = match lookup {
            Ok(LogLookup::Found(_)) => return Ok(true),
            Ok(LogLookup::Absent { final_block }) => final_block,
            Ok(LogLookup::Unknown(reason)) => {
                debug!(message_id = ?id, %reason, "Dusk delivery log cannot show the delivery final yet");
                return Err(HyperlaneDuskError::NotFinal(format!(
                    "Message {id:?} is delivered at the Dusk tip, but the delivery log cannot show it final yet: {reason}"
                ))
                .into());
            }
            Err(error) => {
                let error = bounded_error(&error);
                debug!(message_id = ?id, %error, "Dusk delivery log cannot be read; the delivery does not count yet");
                return Err(HyperlaneDuskError::NotFinal(format!(
                    "Message {id:?} is delivered at the Dusk tip, but the delivery log that would show it final cannot be read: {error}"
                ))
                .into());
            }
        };
        let pending = self
            .rues
            .non_final_events(&pair, &self.mailbox_id, events::ProcessId::TOPIC)
            .await
            .map_err(|error| {
                HyperlaneDuskError::NotFinal(format!(
                    "Message {id:?} is delivered at the Dusk tip, but its finality cannot be checked yet: {error}"
                ))
            })?;
        for event in pending {
            let processed = rkyv_deserialize::<events::ProcessId>(&event.data).map_err(|error| {
                HyperlaneDuskError::NotFinal(format!(
                    "Message {id:?} is delivered at the Dusk tip, but non-final block {} holds an undecodable ProcessId event: {error}",
                    event.block_height
                ))
            })?;
            if processed.message_id == id_bytes {
                return Err(HyperlaneDuskError::NotFinal(format!(
                    "Message {id:?} is delivered in Dusk block {} ({:?}, transaction {}), which is not final yet",
                    event.block_height,
                    event.block_hash,
                    bounded_text(&event.origin)
                ))
                .into());
            }
        }
        // In no final block and in no block above them: a block that held
        // it was rolled back during this check, and the next one reads it
        // as not delivered, or the archive skipped the final block that
        // holds it and this stays so.
        warn!(
            message_id = ?id,
            final_block,
            "Dusk Mailbox reports a delivery that neither the archive's finalized deliveries nor the blocks above the last final one hold"
        );
        Err(HyperlaneDuskError::NotFinal(format!(
            "Message {id:?} is delivered at the Dusk tip, but no finalized block up to block {final_block}, the last final one, holds its delivery, and no block above it does either: the block that did was rolled back during this check, or the node's archive skipped it"
        ))
        .into())
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
        let args = crate::tx_sender::process_args(metadata, &encoded)?;

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

        // The relayer simulates through process_estimate_costs before calling
        // this. Simulate again here, at the only call that spends gas, so no
        // caller submits a delivery that would revert.
        self.simulate_process(message, &args).await?;

        info!(
            message_id = ?message.id(),
            nonce = message.nonce,
            "Processing message on Dusk via dusk-tx"
        );

        let call_result = crate::tx_sender::dusk_tx_call(
            &self.conn,
            signer,
            &self.mailbox_id,
            "process",
            &args,
            gas_limit,
        )
        .await;

        let tx_id = match call_result {
            Ok(response) => response
                .get("tx_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    HyperlaneDuskError::Other(format!(
                        "dusk-tx response is missing string tx_id: {}",
                        bounded_json(&response)
                    ))
                })?
                .to_owned(),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, detail })
            | Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id, detail }) => {
                warn!(%tx_id, %detail, "Reconciling Dusk submission receipt by exact hash");
                tx_id
            }
            Err(error) => return Err(error.into()),
        };
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(&tx_id)?;
        let confirmed = self
            .rues
            .wait_for_tx(&tx_id, TX_CONFIRMATION_TIMEOUT)
            .await?;
        let executed = confirmed.error.is_none();
        if let Some(error) = &confirmed.error {
            let error = bounded_text(error);
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
        message: &HyperlaneMessage,
        metadata: &Metadata,
    ) -> ChainResult<TxCostEstimate> {
        // Rusk's transaction simulation takes a signed transaction that a
        // remote endpoint could replay through propagation, so simulate with
        // an unsigned query of `process` instead. A failure here is a failed
        // estimate: the relayer retries later without submitting. The query
        // measures no gas, so the configured gas limit remains the ceiling.
        let encoded = Self::encode_message(message);
        let args = crate::tx_sender::process_args(metadata, &encoded)?;
        self.simulate_process(message, &args).await?;
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
        Ok(crate::tx_sender::process_args(metadata, &encoded)?)
    }

    fn delivered_calldata(&self, message_id: H256) -> ChainResult<Option<Vec<u8>>> {
        let id_bytes: [u8; 32] = message_id.into();
        Ok(Some(rkyv_serialize(&(id_bytes,))?))
    }
}

#[async_trait]
impl MerkleTreeHook for DuskMerkleTreeHook {
    async fn tree(&self, reorg_period: &ReorgPeriod) -> ChainResult<IncrementalMerkleAtBlock> {
        // Rusk does not expose historical contract-state queries. Build the
        // tree from the IDs the hook stores, up to the count at consensus
        // finality, and check it against the hook (`tree_at`).
        self.inner.finalized_tree(reorg_period).await
    }

    async fn count(&self, reorg_period: &ReorgPeriod) -> ChainResult<u32> {
        Ok(self.inner.finalized_merkle_view(reorg_period).await?.0)
    }

    async fn latest_checkpoint(
        &self,
        reorg_period: &ReorgPeriod,
    ) -> ChainResult<CheckpointAtBlock> {
        let finalized_height = self
            .inner
            .rues
            .checkpoint_block_height(reorg_period)
            .await?;
        self.inner.checkpoint_at_height(finalized_height).await
    }

    async fn latest_checkpoint_at_block(&self, height: u64) -> ChainResult<CheckpointAtBlock> {
        self.inner.checkpoint_at_height(height).await
    }
}

// Bring ChainCommunicationError into scope for process_batch
use hyperlane_core::ChainCommunicationError;

#[cfg(test)]
mod tests {
    use super::*;
    use hyperlane_core::config::OpSubmissionConfig;
    use hyperlane_core::{HyperlaneDomainProtocol, HyperlaneDomainTechnicalStack, NativeToken};
    use url::Url;

    /// Insert leaves `0..count` into a tree built the hook's way and one
    /// built hyperlane-core's way.
    fn both_trees(count: usize) -> (HookTree, IncrementalMerkle) {
        let mut hook = HookTree::new();
        let mut core = IncrementalMerkle::default();
        for leaf in 0..count {
            let id = hyperlane_dusk_types::message::keccak256(&(leaf as u64).to_be_bytes());
            hook.insert(id);
            core.ingest(H256::from(id));
        }
        (hook, core)
    }

    /// The hook's tree (`types/src/merkle.rs` in the Dusk repository)
    /// starts every branch node at zero, hyperlane-core's at the empty
    /// subtree's hash. Their roots agree at every count, and so do the
    /// branch nodes the count's set bits select, but not all branch nodes.
    #[test]
    fn hook_and_core_trees_agree_on_roots_and_on_the_levels_that_make_them() {
        for count in [
            1usize, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 100, 127, 128, 129,
            255, 256, 257, 1000, 1023, 1024, 1025,
        ] {
            let (hook, core) = both_trees(count);
            assert_eq!(hook.count as usize, core.count(), "count {count}");
            assert_eq!(H256::from(hook.root()), core.root(), "count {count}");
            assert!(matches_hook_tree(&core, &hook), "count {count}");
            for level in (0..TREE_DEPTH).filter(|level| (count >> level) & 1 == 1) {
                assert_eq!(hook.branch[level], core.branch[level].0, "count {count}");
            }
        }
        // After one leaf, levels 1 to 31 hold zero in the hook's tree and the
        // empty subtrees' hashes in hyperlane-core's: comparing whole branch
        // arrays would reject trees with the same root.
        let (hook, core) = both_trees(1);
        assert_ne!(hook.branch[1], core.branch[1].0);
        assert_eq!(hook.branch[1], [0u8; 32]);
    }

    #[test]
    fn a_hook_tree_with_another_count_or_root_level_does_not_match() {
        let (hook, core) = both_trees(5);
        // 5 = 0b101: levels 0 and 2 make the root.
        let mut tampered = hook.clone();
        tampered.branch[2][0] ^= 1;
        assert!(!matches_hook_tree(&core, &tampered));
        let mut tampered = hook.clone();
        tampered.branch[0][31] ^= 1;
        assert!(!matches_hook_tree(&core, &tampered));
        let mut tampered = hook.clone();
        tampered.count = 6;
        assert!(!matches_hook_tree(&core, &tampered));
        // A level the count does not select leaves the root as it is.
        let mut unused = hook;
        unused.branch[1] = [7u8; 32];
        assert!(matches_hook_tree(&core, &unused));
    }

    #[test]
    fn mailbox_and_hook_adapters_report_distinct_contract_identities() {
        let mailbox_id = H256::from([1u8; 32]);
        let hook_id = H256::from([2u8; 32]);
        let rues = Arc::new(RuesClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap());
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-test",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        let mailbox = DuskMailbox::new(
            provider,
            rues,
            mailbox_id,
            hook_id,
            domain,
            None,
            ConnectionConf {
                url: Url::parse("http://127.0.0.1:1").unwrap(),
                chain_id: 1,
                event_cursor_dir: std::env::temp_dir(),
                gas_limit: 1,
                gas_price: 1,
                native_token: NativeToken::default(),
                op_submission_config: OpSubmissionConfig::default(),
                query_rate_limit: crate::QueryRateLimit::default(),
            },
        );
        let hook = DuskMerkleTreeHook::new(mailbox.clone());
        assert_eq!(mailbox.address(), mailbox_id);
        assert_eq!(hook.address(), hook_id);
        assert_ne!(mailbox.address(), hook.address());
    }
}
