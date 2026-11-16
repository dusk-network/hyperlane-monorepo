use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt::Debug;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
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

use crate::final_records::FinalRecords;
use crate::rues::{
    bounded_body, bounded_error, bounded_json, bounded_text, fits_one_aggregate, list_reply,
    rkyv_deserialize, rkyv_serialize, BlockPair, CheckHold, LogLookup, Read, SentDelivery,
    TransactionStatus, BOOL_REPLY, ID_REPLY, U32_REPLY, U64_REPLY,
};
use crate::{
    AccountStatus, ConnectionConf, DuskProvider, DuskSigner, HyperlaneDuskError, RuesClient,
};

const TX_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);
/// Most message IDs one `message_ids` query returns.
const MERKLE_HISTORY_PAGE_SIZE: u32 = 256;
/// Pages of message IDs one aggregate carries.
const MERKLE_PAGES_PER_AGGREGATE: usize = 7;
/// The bytes of the hook's `tree` reply.
const HOOK_TREE_REPLY: usize = std::mem::size_of::<rkyv::Archived<HookTree>>();
/// Archive pages a delivery check reads to bring the delivery log up to
/// date, on to the end of the block being read.
const DELIVERED_LOG_PAGES: usize = 4;
/// Messages whose last delivery check a Mailbox keeps.
const MAX_DELIVERY_CHECKS: usize = 4096;
/// How long a passed `process` simulation stands for the same arguments.
/// The relayer simulates a delivery when it prepares it, again just before
/// it submits it, and the Mailbox did a third time in `process`: when the
/// submission follows the preparation at once, the three ran within a few
/// hundred milliseconds of each other on the same state. A Dusk block takes
/// about 10 s, and the transaction waits as long again for its block.
const SIMULATION_REUSE: Duration = Duration::from_secs(2);
/// Simulations a Mailbox remembers.
const MAX_REMEMBERED_SIMULATIONS: usize = 1024;
/// Rusk 1.7.3's block gas limit (`DEFAULT_BLOCK_GAS_LIMIT`). The mempool
/// admits a transaction with a higher limit, but no block can hold it.
pub const RUSK_BLOCK_GAS_LIMIT: u64 = 3_000_000_000;
/// How long a batch waits for the node to hold one delivery before it sends
/// the next.
const HELD_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a batch sends deliveries: it sends none after this, and leaves
/// the rest to be sent one at a time.
const BATCH_SEND_TIMEOUT: Duration = Duration::from_secs(60);
/// Rusk 1.7.3 queues at most this many transactions of one account whose
/// earlier nonces it has not seen yet (`max_moonlight_future_nonce_per_account`).
const MAX_PIPELINED_DELIVERIES: usize = 64;
/// How long after it was sent a delivery's transaction may still be on its
/// way into the node's mempool.
const DROP_GRACE: Duration = Duration::from_secs(60);
/// Most deliveries a signer has outstanding: sent, and in no block yet.
/// Rusk bounds only the transactions it holds back for an earlier nonce,
/// not those it admitted in nonce order.
const MAX_OUTSTANDING_DELIVERIES: usize = 64;

/// What a delivery check found.
#[derive(Debug, Clone)]
enum Delivery {
    /// In a final block, for good.
    Final,
    /// Not delivered at the tip.
    Absent,
    /// Delivered at the tip in a block that is not final yet, which stays
    /// so until the node finalizes another block; why.
    Pending(String),
    /// Delivered at the tip, but the check could not settle whether it is
    /// final: the delivery log cannot be read or does not reach the last
    /// final block, or the blocks above it cannot be read; why.
    Unsettled(String),
}

impl Delivery {
    /// How long the check holds, made at last final block `finalized`
    /// with the delivery log read up to `progress`.
    fn hold(&self, finalized: u64, progress: impl FnOnce() -> Option<LogProgress>) -> CheckHold {
        match self {
            Self::Final => CheckHold::Forever,
            Self::Absent | Self::Pending(_) => CheckHold::FinalBlock(finalized),
            Self::Unsettled(_) => CheckHold::unsettled(finalized, progress()),
        }
    }
}

/// How far the delivery log reaches, as `RuesClient::event_log_progress`
/// gives it.
type LogProgress = crate::rues::LogState;

/// The last delivery check of each message and how long it holds. Until
/// the node finalizes another block, a check that settled finds the same:
/// the relayer asks again every half second while it waits for signatures.
/// One the delivery log could not settle holds only briefly, and only while
/// the log has not read on: the next check tries the archive again, or
/// reads on in it.
#[derive(Debug, Default)]
struct DeliveryChecks {
    checks: HashMap<H256, (CheckHold, Delivery)>,
}

impl DeliveryChecks {
    /// The check of `id` that still holds at last final block `finalized`,
    /// the delivery log having read up to what `progress` returns.
    fn get(
        &self,
        id: H256,
        finalized: u64,
        progress: impl FnOnce() -> Option<LogProgress>,
    ) -> Option<Delivery> {
        let (hold, delivery) = self.checks.get(&id)?;
        hold.holds(finalized, progress).then(|| delivery.clone())
    }

    fn insert(&mut self, id: H256, finalized: u64, hold: CheckHold, delivery: Delivery) {
        if self.checks.len() >= MAX_DELIVERY_CHECKS && !self.checks.contains_key(&id) {
            // Checks made at earlier final blocks no longer hold; a final
            // delivery that goes is checked again.
            self.checks.retain(|_, (hold, _)| hold.at(finalized));
            if self.checks.len() >= MAX_DELIVERY_CHECKS {
                self.checks.clear();
            }
        }
        self.checks.insert(id, (hold, delivery));
    }
}

/// A delivery ready to be sent: its place in the batch, its message, and
/// its `process` arguments and gas limit.
struct Ready {
    index: usize,
    message_id: H256,
    args: Vec<u8>,
    gas_limit: u64,
}

/// Where a delivery the signer sent stands, reconciled with the node.
#[derive(Debug, Clone)]
enum Standing {
    /// Its transaction is in a block that is not final yet: no other is
    /// sent for it, and the transaction is followed.
    Landed(String),
    /// Its transaction may still be in a block: no other is sent for it,
    /// and the transaction, if the helper named it, is followed.
    InFlight(Option<String>),
    /// Its transaction is in neither the mempool nor a block, and the nonce
    /// it owns is unused: it goes again with that nonce, so that at most
    /// one of its transactions can ever be in a block.
    Dropped(u64),
}

/// The signer's account, and its deliveries that have not settled by
/// message: the nonce each owns, and where it stands.
struct Owned {
    account: AccountStatus,
    standing: HashMap<H256, (u64, Standing)>,
}

impl Owned {
    /// The deliveries whose transactions are not in a block: those the
    /// bound of outstanding deliveries counts.
    fn outstanding(&self) -> usize {
        self.standing
            .values()
            .filter(|(_, standing)| !matches!(standing, Standing::Landed(_)))
            .count()
    }

    /// Whether a delivery in flight owns `nonce`.
    fn in_flight(&self, nonce: u64) -> bool {
        self.standing
            .values()
            .any(|(owned, standing)| *owned == nonce && matches!(standing, Standing::InFlight(_)))
    }

    /// The nonce the next new delivery takes: the account's next one, past
    /// those deliveries in flight own.
    fn next_free_nonce(&self) -> u64 {
        let mut nonce = self.account.next_nonce;
        while self.in_flight(nonce) {
            nonce = nonce.saturating_add(1);
        }
        nonce
    }

    /// The deliveries to send, each with its nonce and whether it goes
    /// again, in nonce order: consecutive from the account's next nonce, as
    /// the node admits them. One the node dropped (`reused`, by the nonce it
    /// owns) goes with its nonce, a nonce a delivery in flight owns is
    /// passed over, and new ones (`fresh`) take the other nonces while the
    /// signer has fewer than `outstanding` deliveries outstanding. The rest
    /// are left out, and so is one whose nonce comes after a gap: the node
    /// would hold it back.
    fn plan(
        &self,
        mut reused: BTreeMap<u64, Ready>,
        mut fresh: VecDeque<Ready>,
        outstanding: usize,
        left_out: &mut Vec<usize>,
    ) -> Vec<(u64, Ready, bool)> {
        let mut allowed = outstanding.saturating_sub(self.outstanding());
        let mut plan = Vec::new();
        let mut nonce = self.account.next_nonce;
        while plan.len() < MAX_PIPELINED_DELIVERIES {
            if let Some(delivery) = reused.remove(&nonce) {
                plan.push((nonce, delivery, true));
            } else if !self.in_flight(nonce) {
                if allowed == 0 {
                    break;
                }
                let Some(delivery) = fresh.pop_front() else {
                    break;
                };
                allowed -= 1;
                plan.push((nonce, delivery, false));
            }
            nonce = nonce.saturating_add(1);
        }
        left_out.extend(reused.into_values().map(|delivery| delivery.index));
        left_out.extend(fresh.into_iter().map(|delivery| delivery.index));
        plan
    }
}

/// The key a signer's deliveries are kept under: its address.
fn signer_key(signer: &DuskSigner) -> [u8; 32] {
    signer.eth_address().into()
}

/// An endpoint, a Mailbox and a signer whose delivery mode was logged.
type LoggedMode = (String, H256, [u8; 32]);

/// Say once per process, for each endpoint, Mailbox and signer, how this
/// agent submits deliveries.
fn log_delivery_mode(
    domain: &HyperlaneDomain,
    mailbox: &H256,
    signer: &DuskSigner,
    conn: &ConnectionConf,
) {
    static LOGGED: std::sync::OnceLock<StdMutex<std::collections::HashSet<LoggedMode>>> =
        std::sync::OnceLock::new();
    let key = (conn.url.as_str().to_owned(), *mailbox, signer_key(signer));
    let first = LOGGED
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .insert(key);
    if !first {
        return;
    }
    let max_batch_size = conn.op_submission_config.max_batch_size;
    if conn.pipelines_deliveries() {
        warn!(
            domain = %domain.name(),
            max_batch_size,
            "Dusk deliveries are pipelined (pipelineDeliveries, experimental): a batch goes as consecutive transactions of the signer, with nonces this agent chooses and keeps in eventCursorDir; no other process may sign with this key"
        );
    } else {
        info!(
            domain = %domain.name(),
            max_batch_size,
            pipeline_deliveries = conn.pipeline_deliveries,
            "Dusk deliveries go one at a time: dusk-tx signs each with the account's next nonce and waits for its block (pipelined deliveries are off; they need pipelineDeliveries and a maxBatchSize above 1)"
        );
    }
}

/// Milliseconds since the Unix epoch, by the wall clock.
fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// How long a Mailbox waits while it submits deliveries, and how many it
/// keeps outstanding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmissionLimits {
    /// For a sent transaction to be in a block.
    pub receipt: Duration,
    /// For the node to hold a pipelined transaction before the next is
    /// sent.
    pub held: Duration,
    /// For a batch to send its transactions: it sends none after this.
    pub batch: Duration,
    /// For a sent transaction to reach the node's mempool: one in neither
    /// the mempool nor a block counts as dropped only after this.
    pub drop_grace: Duration,
    /// Most deliveries the signer has outstanding: sent, and in no block
    /// yet.
    pub outstanding: usize,
}

impl Default for SubmissionLimits {
    fn default() -> Self {
        Self {
            receipt: TX_CONFIRMATION_TIMEOUT,
            held: HELD_TIMEOUT,
            batch: BATCH_SEND_TIMEOUT,
            drop_grace: DROP_GRACE,
            outstanding: MAX_OUTSTANDING_DELIVERIES,
        }
    }
}

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
    /// Where the hook's insertions last crossed a final height.
    insertions: Arc<FinalRecords>,
    /// The last delivery check of each message, shared by every clone.
    delivery_checks: Arc<StdMutex<DeliveryChecks>>,
    /// When `process` last passed its simulation, by the keccak256 of its
    /// arguments, shared by every clone.
    simulations: Arc<StdMutex<HashMap<[u8; 32], std::time::Instant>>>,
    limits: SubmissionLimits,
    /// Whether deliveries are pipelined (`ConnectionConf::pipelines_deliveries`).
    /// Otherwise each goes one at a time: `dusk-tx` signs it with the
    /// account's nonce plus one, as the node reports it, and waits for its
    /// block.
    pipelined: bool,
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
    /// The tree has passed `verify_tree` with this many leaves. Its leaves
    /// are final, so the check would read the same answers again.
    verified: Option<usize>,
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
        let pipelined = conn.pipelines_deliveries();
        if let Some(signer) = &signer {
            log_delivery_mode(&domain, &mailbox_id, signer, &conn);
        }
        Self {
            provider,
            insertions: rues.final_records(merkle_tree_hook_id.into(), "inserted_block_height"),
            rues,
            mailbox_id: mailbox_id.into(),
            merkle_tree_hook_id: merkle_tree_hook_id.into(),
            domain,
            signer,
            conn,
            merkle: Arc::new(tokio::sync::Mutex::new(MerkleCache::default())),
            delivery_checks: Arc::default(),
            simulations: Arc::default(),
            limits: SubmissionLimits::default(),
            pipelined,
        }
    }

    /// This Mailbox, submitting deliveries within `limits`, for tests that
    /// cannot wait minutes.
    #[doc(hidden)]
    pub fn with_submission_limits(mut self, limits: SubmissionLimits) -> Self {
        self.limits = limits;
        self
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
    ///
    /// A simulation of the same arguments that passed less than
    /// `SIMULATION_REUSE` ago stands for this one: it ran on the state this
    /// one would read, as far as a delivery can tell.
    async fn simulate_process(&self, message: &HyperlaneMessage, args: &[u8]) -> ChainResult<()> {
        let key = hyperlane_dusk_types::message::keccak256(args);
        let passed_recently = self
            .simulations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&key)
            .is_some_and(|passed| passed.elapsed() < SIMULATION_REUSE);
        if passed_recently {
            debug!(message_id = ?message.id(), "Dusk process simulation passed moments ago; not running it again");
            return Ok(());
        }
        let unit = rkyv_serialize(&())?;
        let failure = match self
            .rues
            .contract_query_raw(&self.mailbox_id, "process", args)
            .await
        {
            Ok(reply) if reply == unit => {
                let mut simulations = self
                    .simulations
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                if simulations.len() >= MAX_REMEMBERED_SIMULATIONS {
                    simulations.retain(|_, passed| passed.elapsed() < SIMULATION_REUSE);
                }
                simulations.insert(key, std::time::Instant::now());
                return Ok(());
            }
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

    /// Submit `deliveries`, each a message, its metadata and the relayer's
    /// gas limit, as consecutive transactions of the signer's account, so
    /// that several share a block.
    ///
    /// Each delivery is simulated first (`simulate_process`), and one that
    /// fails is left out. Then the signer's deliveries that have not
    /// settled are reconciled with the node (`reconcile`): a delivery whose
    /// transaction may still be in a block is not sent again, but followed;
    /// one the node dropped goes again with the nonce it owns; the others
    /// take the account's next nonces, from the node's `next_nonce`, which
    /// counts the account's transactions in its mempool, as long as the
    /// signer has fewer than `SubmissionLimits::outstanding` deliveries
    /// sent and in no block yet. They are sent in nonce order, each once the node holds
    /// the one before: Rusk checks a transaction's nonce against its
    /// mempool. Each delivery's nonce, and then its transaction, is kept in
    /// the event store before and after the helper sends it. A delivery
    /// the node refuses ends the batch there, and so does one whose
    /// submission outcome is unknown, so that no later nonce waits behind a
    /// gap. Then the batch follows every transaction it sent or follows to
    /// its block, under one deadline.
    ///
    /// Returns the deliveries it left out, which the relayer submits one at
    /// a time through `process`, and, when it sent or followed any, one
    /// outcome for all of them, as the relayer's batches have: the first
    /// transaction's ID, the gas they spent together, which the relayer
    /// shares out among them, and whether every one executed. A delivery
    /// whose transaction is in no block by the deadline still counts as
    /// sent: the relayer checks that it was delivered before it gives up on
    /// it, and submits it again if not, which follows that transaction
    /// instead of sending another. Once one transaction is sent this never
    /// fails, so the relayer never sends the batch again.
    ///
    /// Unless deliveries are pipelined (`ConnectionConf::pipelines_deliveries`),
    /// or with a helper that cannot sign with a given nonce
    /// (`tx_sender::helper_pipelines`), every delivery is left out, and the
    /// relayer sends each on its own.
    pub async fn process_deliveries(
        &self,
        deliveries: &[(HyperlaneMessage, Metadata, Option<U256>)],
    ) -> ChainResult<BatchResult> {
        let signer = self
            .signer
            .as_ref()
            .ok_or(HyperlaneDuskError::SignerUnavailable)?;
        // Without the opt-in, the relayer sends each one on its own.
        if !self.pipelined || !crate::tx_sender::helper_pipelines().await {
            return Ok(BatchResult::failed(deliveries.len()));
        }
        let mut left_out = Vec::new();
        let mut ready = Vec::new();
        for (index, (message, metadata, gas_limit)) in deliveries.iter().enumerate() {
            if ready.len() == MAX_PIPELINED_DELIVERIES {
                left_out.push(index);
                continue;
            }
            let encoded = Self::encode_message(message);
            let args = match crate::tx_sender::process_args(metadata, &encoded) {
                Ok(args) => args,
                Err(error) => {
                    warn!(message_id = ?message.id(), %error, "Dusk delivery cannot be sent");
                    left_out.push(index);
                    continue;
                }
            };
            if self.simulate_process(message, &args).await.is_err() {
                left_out.push(index);
                continue;
            }
            ready.push(Ready {
                index,
                message_id: message.id(),
                args,
                gas_limit: self.submission_gas_limit(message, *gas_limit),
            });
        }
        if ready.is_empty() {
            return Ok(BatchResult {
                outcome: None,
                failed_indexes: left_out,
            });
        }

        let key = signer_key(signer);
        let lock = self.rues.submission_lock(key);
        let submission = lock.lock().await;
        // Nothing is sent before the signer's deliveries and the account's
        // next nonce are known.
        self.rues.ensure_identity().await?;
        let owned = self.reconcile(signer).await?;
        let mut followed = Vec::new();
        let mut reused = BTreeMap::new();
        let mut fresh = VecDeque::new();
        for delivery in ready {
            match owned.standing.get(&delivery.message_id) {
                Some((_, Standing::Landed(tx_id) | Standing::InFlight(Some(tx_id)))) => {
                    info!(
                        message_id = ?delivery.message_id,
                        %tx_id,
                        "Dusk delivery's transaction may still be in a block; following it instead of sending another"
                    );
                    followed.push(tx_id.clone());
                }
                Some((_, Standing::InFlight(None))) => {
                    info!(
                        message_id = ?delivery.message_id,
                        "Dusk delivery's transaction may still be in a block, and the helper never named it; it is checked again later"
                    );
                    left_out.push(delivery.index);
                }
                Some((_, Standing::Dropped(nonce))) => {
                    reused.insert(*nonce, delivery);
                }
                None => fresh.push_back(delivery),
            }
        }
        let plan = owned.plan(reused, fresh, self.limits.outstanding, &mut left_out);

        let mut sent = Vec::new();
        let mut stopped = false;
        // Every wait of the batch, for the helper or the node, ends by this.
        let send_deadline = tokio::time::Instant::now() + self.limits.batch;
        for (nonce, delivery, reuse) in plan {
            let message_id = delivery.message_id;
            if !stopped && tokio::time::Instant::now() >= send_deadline {
                warn!(
                    ?message_id,
                    "Dusk delivery batch has used its time to send; its later deliveries are sent one at a time"
                );
                stopped = true;
            }
            if stopped {
                left_out.push(delivery.index);
                continue;
            }
            if !reuse && !self.own_nonce(&key, message_id, nonce) {
                left_out.push(delivery.index);
                stopped = true;
                continue;
            }
            let result = crate::tx_sender::dusk_tx_send_with_nonce(
                &self.conn,
                signer,
                &self.mailbox_id,
                "process",
                &delivery.args,
                delivery.gas_limit,
                nonce,
            )
            .await;
            let tx_id = match result {
                // The helper's result names the transaction it prepared.
                Ok(response) => response
                    .get("tx_id")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_owned(),
                Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, detail })
                | Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id, detail }) => {
                    warn!(
                        ?message_id,
                        %tx_id,
                        nonce,
                        %detail,
                        "Dusk delivery submission outcome is unknown; reconciling its exact hash and sending no later nonce"
                    );
                    stopped = true;
                    tx_id
                }
                Err(error) => {
                    warn!(
                        ?message_id,
                        nonce,
                        %error,
                        "Dusk node refused a pipelined delivery; the batch's later deliveries are sent one at a time"
                    );
                    // A delivery sent again keeps the nonce its earlier
                    // transaction may still take.
                    if !reuse {
                        self.release_nonce(&key, message_id);
                    }
                    left_out.push(delivery.index);
                    stopped = true;
                    continue;
                }
            };
            self.name_transaction(&key, message_id, nonce, &tx_id);
            info!(?message_id, %tx_id, nonce, "Dusk delivery sent");
            if !stopped {
                let wait = self
                    .limits
                    .held
                    .min(send_deadline.saturating_duration_since(tokio::time::Instant::now()));
                let held = self.rues.wait_until_held(&tx_id, wait).await;
                if !matches!(held, Ok(true)) {
                    warn!(
                        %tx_id,
                        ?held,
                        "Dusk node does not hold a pipelined delivery yet; sending no later nonce"
                    );
                    stopped = true;
                }
            }
            sent.push(tx_id);
        }
        // Following them needs none of the signer's nonces.
        drop(submission);
        left_out.sort_unstable();
        sent.extend(followed);
        let Some(first) = sent.first() else {
            return Ok(BatchResult {
                outcome: None,
                failed_indexes: left_out,
            });
        };

        // Follow every sent transaction to its block.
        let deadline = tokio::time::Instant::now() + self.limits.receipt;
        let mut gas_spent = 0u64;
        let mut executed = true;
        for tx_id in &sent {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.rues.wait_for_tx(tx_id, remaining).await {
                Ok(receipt) => {
                    gas_spent = gas_spent.saturating_add(receipt.gas_spent);
                    if let Some(error) = &receipt.error {
                        executed = false;
                        let error = bounded_text(error);
                        warn!(tx_id, %error, "Dusk delivery transaction execution failed");
                    }
                }
                Err(error) => {
                    executed = false;
                    warn!(
                        tx_id,
                        %error,
                        "Dusk delivery transaction is in no block yet; the relayer checks the delivery before it relays it again"
                    );
                }
            }
        }
        Ok(BatchResult {
            outcome: Some(TxOutcome {
                transaction_id: crate::tx_sender::dusk_tx_id_to_h512(first).unwrap_or_default(),
                executed,
                gas_used: U256::from(gas_spent),
                gas_price: FixedPointNumber::from(self.conn.gas_price),
            }),
            failed_indexes: left_out,
        })
    }

    /// The signer's account and its deliveries that have not settled, each
    /// reconciled with the node. Run with the signer's submission lock
    /// held. A delivery settles, and is forgotten, once its transaction is
    /// in a final block, whether it executed or not, or once the account
    /// has used its nonce while the transaction is in no block: then it
    /// never can be. Until then, one whose transaction is in a block that
    /// is not final yet has landed, and one stays in flight while its
    /// transaction is in the mempool, or is too recent to tell, or the
    /// node's mempool holds its nonce for another transaction; one in
    /// neither, past `SubmissionLimits::drop_grace`, with its nonce unused,
    /// was dropped, and goes again with that nonce. A delivery whose
    /// transaction cannot be looked up stays in flight.
    async fn reconcile(&self, signer: &DuskSigner) -> ChainResult<Owned> {
        let key = signer_key(signer);
        let account = self.rues.account_status(&signer.address_string()).await?;
        let finalized = self.rues.block_pair().await?.finalized();
        let now = unix_millis();
        let grace = u64::try_from(self.limits.drop_grace.as_millis()).unwrap_or(u64::MAX);
        let mut standing = HashMap::new();
        for (message_id, sent) in self.rues.sent_deliveries(&key)? {
            let tx_id = sent.tx_id.map(hex::encode);
            let status = match &tx_id {
                Some(tx_id) => match self.rues.transaction_status(tx_id).await {
                    Ok(status) => Some(status),
                    Err(error) => {
                        debug!(?message_id, %tx_id, error = %bounded_error(&error), "Dusk delivery transaction cannot be looked up; it stays in flight");
                        None
                    }
                },
                None => None,
            };
            let state = match status {
                Some(TransactionStatus::Included { block_height, .. })
                    if block_height <= finalized =>
                {
                    self.release_nonce(&key, message_id);
                    continue;
                }
                Some(TransactionStatus::Included { .. }) => {
                    Standing::Landed(tx_id.unwrap_or_default())
                }
                Some(TransactionStatus::Pending) => Standing::InFlight(tx_id),
                // Not looked up.
                None if tx_id.is_some() => Standing::InFlight(tx_id),
                // In neither the mempool nor a block, or not named.
                _ if account.nonce >= sent.nonce => {
                    debug!(
                        ?message_id,
                        nonce = sent.nonce,
                        "Dusk account used a delivery's nonce for another transaction; it settled"
                    );
                    self.release_nonce(&key, message_id);
                    continue;
                }
                _ if sent.nonce < account.next_nonce
                    || now.saturating_sub(sent.sent_at) < grace =>
                {
                    Standing::InFlight(tx_id)
                }
                _ => Standing::Dropped(sent.nonce),
            };
            standing.insert(message_id, (sent.nonce, state));
        }
        Ok(Owned { account, standing })
    }

    /// Keep that `message_id`'s delivery owns `nonce` before a transaction
    /// is signed with it. Whether it was kept: nothing is sent otherwise.
    fn own_nonce(&self, key: &[u8; 32], message_id: H256, nonce: u64) -> bool {
        let sent = SentDelivery {
            nonce,
            tx_id: None,
            sent_at: unix_millis(),
        };
        match self.rues.record_sent_delivery(key, message_id, &sent) {
            Ok(()) => true,
            Err(error) => {
                warn!(?message_id, nonce, error = %bounded_error(&error), "Dusk delivery cannot be kept in eventCursorDir; not sending it");
                false
            }
        }
    }

    /// Keep that `message_id`'s delivery sent `tx_id` with `nonce`. One
    /// that cannot be kept stays with its nonce, and settles by it.
    fn name_transaction(&self, key: &[u8; 32], message_id: H256, nonce: u64, tx_id: &str) {
        let Some(tx) = hex::decode(tx_id)
            .ok()
            .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        else {
            warn!(?message_id, tx_id = %bounded_text(tx_id), "Dusk delivery transaction ID is not 32 bytes of hex; the delivery settles by its nonce");
            return;
        };
        let sent = SentDelivery {
            nonce,
            tx_id: Some(tx),
            sent_at: unix_millis(),
        };
        if let Err(error) = self.rues.record_sent_delivery(key, message_id, &sent) {
            warn!(?message_id, %tx_id, error = %bounded_error(&error), "Dusk delivery transaction cannot be kept in eventCursorDir; the delivery settles by its nonce");
        }
    }

    /// Forget `message_id`'s delivery: it settled, or its nonce was never
    /// used.
    fn release_nonce(&self, key: &[u8; 32], message_id: H256) {
        if let Err(error) = self.rues.forget_sent_delivery(key, message_id) {
            warn!(?message_id, error = %bounded_error(&error), "Dusk delivery that settled cannot be forgotten in eventCursorDir");
        }
    }

    /// Submit one delivery, one at a time, as Dusk relayers always have:
    /// `dusk-tx` signs it with the account's nonce plus one, as the node
    /// reports it, sends it and waits for its block. No nonce is chosen or
    /// kept here. While an earlier transaction of the signer is in the
    /// mempool, the node refuses another with its nonce at preverification,
    /// so a delivery submitted again while its first transaction waits is
    /// refused, and at most one of them can be in a block. When the
    /// helper's outcome is unknown, the transaction it prepared is followed
    /// by its exact hash.
    async fn process_one(
        &self,
        signer: &DuskSigner,
        args: &[u8],
        gas_limit: u64,
    ) -> ChainResult<TxOutcome> {
        let call_result = crate::tx_sender::dusk_tx_call(
            &self.conn,
            signer,
            &self.mailbox_id,
            "process",
            args,
            Some(gas_limit),
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
        self.outcome_of(&tx_id).await
    }

    /// Submit one delivery when deliveries are pipelined: it follows the
    /// rule a batch does (`process_deliveries`), under the signer's
    /// submission lock: one in flight is followed, one dropped goes again
    /// with its own nonce, and a new one takes the next free nonce.
    async fn process_pipelined(
        &self,
        signer: &DuskSigner,
        message_id: H256,
        args: &[u8],
        gas_limit: u64,
    ) -> ChainResult<TxOutcome> {
        // The helper signs with the account's nonces: no batch may be using
        // them meanwhile. A delivery follows the rule a batch does: one in
        // flight is followed, one dropped goes again with its own nonce.
        let key = signer_key(signer);
        let lock = self.rues.submission_lock(key);
        let submission = lock.lock().await;
        let owned = self.reconcile(signer).await?;
        let (nonce, reuse) = match owned.standing.get(&message_id) {
            Some((_, Standing::Landed(tx_id) | Standing::InFlight(Some(tx_id)))) => {
                let tx_id = tx_id.clone();
                drop(submission);
                info!(
                    ?message_id,
                    %tx_id,
                    "Dusk delivery's transaction may still be in a block; following it instead of sending another"
                );
                return self.outcome_of(&tx_id).await;
            }
            Some((_, Standing::InFlight(None))) => {
                return Err(HyperlaneDuskError::Other(format!(
                    "Dusk delivery of message {message_id:?} may still be in a block, and the helper never named its transaction; checking it again later"
                ))
                .into());
            }
            Some((_, Standing::Dropped(nonce))) => (*nonce, true),
            None => {
                if owned.outstanding() >= self.limits.outstanding {
                    return Err(HyperlaneDuskError::Other(format!(
                        "The Dusk signer has {} deliveries outstanding, its most; message {message_id:?} goes once they are in blocks",
                        owned.outstanding()
                    ))
                    .into());
                }
                (owned.next_free_nonce(), false)
            }
        };
        // The node admits consecutive nonces only, and a helper without
        // `--nonce` signs with the account's next one.
        let pipelines = crate::tx_sender::helper_pipelines().await;
        let next = if pipelines {
            owned.next_free_nonce()
        } else {
            owned.account.nonce.saturating_add(1)
        };
        if nonce != next {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk delivery of message {message_id:?} goes with nonce {nonce}, once the signer's transactions before it are in; the next one is {next}"
            ))
            .into());
        }
        if !reuse && !self.own_nonce(&key, message_id, nonce) {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk delivery of message {message_id:?} cannot be kept in eventCursorDir; not sending it"
            ))
            .into());
        }
        let call_result = if pipelines {
            crate::tx_sender::dusk_tx_send_with_nonce(
                &self.conn,
                signer,
                &self.mailbox_id,
                "process",
                args,
                gas_limit,
                nonce,
            )
            .await
        } else {
            crate::tx_sender::dusk_tx_call(
                &self.conn,
                signer,
                &self.mailbox_id,
                "process",
                args,
                Some(gas_limit),
            )
            .await
        };

        let tx_id = match call_result {
            // The delivery keeps its nonce if the result names no
            // transaction, and settles by it.
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
            Err(error) => {
                // Nothing was sent; one sent again keeps the nonce its
                // earlier transaction may still take.
                if !reuse {
                    self.release_nonce(&key, message_id);
                }
                return Err(error.into());
            }
        };
        self.name_transaction(&key, message_id, nonce, &tx_id);
        drop(submission);
        self.outcome_of(&tx_id).await
    }

    /// The outcome of delivery transaction `tx_id`, once it is in a block
    /// within `SubmissionLimits::receipt`.
    async fn outcome_of(&self, tx_id: &str) -> ChainResult<TxOutcome> {
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(tx_id)?;
        let confirmed = self.rues.wait_for_tx(tx_id, self.limits.receipt).await?;
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

    /// One delivery check of message `id`, whose state is read after
    /// `pair`, the node's tip and last final block. Fails only when the
    /// `delivered` query does; a delivery in a block that is not final yet
    /// is `Pending`, and everything else the check cannot show is
    /// `Unsettled`.
    async fn check_delivery(&self, id: H256, pair: &BlockPair) -> ChainResult<Delivery> {
        let id_bytes: [u8; 32] = id.into();
        // Read after `pair`'s last final block was final. When batching, it
        // can come from the endpoint's reads of that block, made before
        // `pair` itself was read: such a reply can only be as stale as a
        // check this keeps for the whole block, since a delivery counts
        // only once its finalized `ProcessId` row does.
        let read = Read::new(self.mailbox_id, "delivered", &(id_bytes,), BOOL_REPLY)?;
        let delivered: bool =
            rkyv_deserialize(&self.rues.tick_read(pair.finalized(), read).await?)?;
        if !delivered {
            return Ok(Delivery::Absent);
        }
        let lookup = self
            .rues
            .finalized_event_log_lookup(
                &crate::mailbox_indexer::delivery_log(self.mailbox_id),
                &id_bytes,
                pair,
                DELIVERED_LOG_PAGES,
            )
            .await;
        // Only a finalized delivery counts.
        let final_block = match lookup {
            Ok(LogLookup::Found(_)) => return Ok(Delivery::Final),
            Ok(LogLookup::Absent { final_block }) => final_block,
            Ok(LogLookup::Unknown(reason)) => {
                debug!(message_id = ?id, %reason, "Dusk delivery log cannot show the delivery final yet");
                return Ok(Delivery::Unsettled(format!(
                    "Message {id:?} is delivered at the Dusk tip, but the delivery log cannot show it final yet: {reason}"
                )));
            }
            Err(error) => {
                let error = bounded_error(&error);
                debug!(message_id = ?id, %error, "Dusk delivery log cannot be read; the delivery does not count yet");
                return Ok(Delivery::Unsettled(format!(
                    "Message {id:?} is delivered at the Dusk tip, but the delivery log that would show it final cannot be read: {error}"
                )));
            }
        };
        let pending = match self
            .rues
            .non_final_events(pair, &self.mailbox_id, events::ProcessId::TOPIC)
            .await
        {
            Ok(pending) => pending,
            Err(error) => {
                return Ok(Delivery::Unsettled(format!(
                    "Message {id:?} is delivered at the Dusk tip, but its finality cannot be checked yet: {error}"
                )))
            }
        };
        for event in pending {
            let processed = match rkyv_deserialize::<events::ProcessId>(&event.data) {
                Ok(processed) => processed,
                Err(error) => {
                    return Ok(Delivery::Unsettled(format!(
                        "Message {id:?} is delivered at the Dusk tip, but non-final block {} holds an undecodable ProcessId event: {error}",
                        event.block_height
                    )))
                }
            };
            if processed.message_id == id_bytes {
                return Ok(Delivery::Pending(format!(
                    "Message {id:?} is delivered in Dusk block {} ({:?}, transaction {}), which is not final yet",
                    event.block_height,
                    event.block_hash,
                    bounded_text(&event.origin)
                )));
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
        Ok(Delivery::Unsettled(format!(
            "Message {id:?} is delivered at the Dusk tip, but no finalized block up to block {final_block}, the last final one, holds its delivery, and no block above it does either: the block that did was rolled back during this check, or the node's archive skipped it"
        )))
    }

    /// The gas limit a delivery is submitted with: the relayer's, which
    /// follows the gas the sender paid for, but at most the configured
    /// `gasLimit` and Rusk's block gas limit. A failed Dusk transaction pays
    /// its whole limit, and one above the block gas limit stays in the
    /// mempool for good.
    fn submission_gas_limit(&self, message: &HyperlaneMessage, requested: Option<U256>) -> u64 {
        let ceiling = self.conn.gas_limit.min(RUSK_BLOCK_GAS_LIMIT);
        let Some(requested) = requested else {
            return ceiling;
        };
        if requested > U256::from(ceiling) {
            debug!(
                message_id = ?message.id(),
                requested = %requested,
                ceiling,
                "Dusk delivery gas limit capped at the configured gasLimit"
            );
            return ceiling;
        }
        requested.low_u64()
    }

    /// The hook's insertion count, read once per final block.
    async fn merkle_tree_count(&self) -> ChainResult<u32> {
        let read = Read::new(self.merkle_tree_hook_id, "count", &(), U32_REPLY)?;
        Ok(rkyv_deserialize(&self.rues.final_tick_read(read).await?)?)
    }

    async fn merkle_insertion_heights(&self, indices: Vec<u32>) -> ChainResult<Vec<u64>> {
        Ok(self
            .rues
            .read_indexed(
                self.merkle_tree_hook_id,
                "inserted_block_height",
                &indices,
                U64_REPLY,
            )
            .await?)
    }

    /// How many of the hook's insertions are at or below block
    /// `block_height`, which is at or below the last final block. Shared
    /// with the hook's indexer on the same endpoint, and free for a height
    /// asked before.
    async fn merkle_count_at_height(&self, block_height: u64) -> ChainResult<u32> {
        let probes = self.rues.batch_capacity().await?;
        self.insertions
            .count_at_or_below(
                block_height,
                || self.merkle_tree_count(),
                probes,
                |indices| self.merkle_insertion_heights(indices),
            )
            .await
    }

    async fn finalized_merkle_view(&self, reorg_period: &ReorgPeriod) -> ChainResult<(u32, u64)> {
        let finalized_height = self.rues.checkpoint_block_height(reorg_period).await?;
        let finalized_count = self.merkle_count_at_height(finalized_height).await?;
        Ok((finalized_count, finalized_height))
    }

    /// The tree of the hook's first `count` leaves, the insertions at or
    /// below block `at_height`, ingested from the message IDs it stores, and
    /// checked against the hook (`verify_tree`, or `verify_snapshot` when
    /// one aggregate reads the checks at one state).
    ///
    /// The shared tree is extended page by page, so a call that is cancelled
    /// or fails to read keeps the pages it read. A tree of fewer leaves than
    /// the shared one holds, for an earlier height, is built on its own. A
    /// failed check drops the shared tree, so the next call rebuilds it from
    /// the hook's first leaf: no root is computed from IDs that disagree
    /// with the hook.
    async fn tree_at(&self, count: u32, at_height: u64) -> ChainResult<IncrementalMerkle> {
        let mut shared = self.merkle.lock().await;
        if shared.verified == Some(count as usize) && shared.tree.count() == count as usize {
            return Ok(shared.tree.clone());
        }
        let extends_shared = shared.tree.count() <= count as usize;
        let mut earlier = MerkleCache::default();
        let cache = if extends_shared {
            &mut *shared
        } else {
            &mut earlier
        };
        let verdict = match self.snapshot_tree(cache, count, at_height).await? {
            Some(verdict) => verdict,
            None => {
                self.extend_tree(cache, count).await?;
                self.verify_tree(&cache.tree, cache.last).await?
            }
        };
        let tree = cache.tree.clone();
        if let Err(detail) = verdict {
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
        if extends_shared {
            shared.verified = Some(tree.count());
        }
        Ok(tree)
    }

    /// Ingest the hook's stored message IDs from `cache`'s leaf count up to
    /// `count`, a page of at most `MERKLE_HISTORY_PAGE_SIZE` at a time, each
    /// page as long as asked.
    async fn extend_tree(&self, cache: &mut MerkleCache, count: u32) -> ChainResult<()> {
        loop {
            let start = cache_count(cache)?;
            if start >= count {
                return Ok(());
            }
            let limit = (count - start).min(MERKLE_HISTORY_PAGE_SIZE);
            let page = self
                .rues
                .contract_query_raw(
                    &self.merkle_tree_hook_id,
                    "message_ids",
                    &rkyv_serialize(&(start, limit))?,
                )
                .await?;
            ingest_page(cache, &page, start, limit)?;
        }
    }

    /// The `message_ids` pages from leaf `start` up to `count`: each page's
    /// start, length and read.
    fn tree_pages(&self, start: u32, count: u32) -> ChainResult<Vec<(u32, u32, Read)>> {
        let mut pages = Vec::new();
        let mut next = start;
        while next < count {
            let limit = (count - next).min(MERKLE_HISTORY_PAGE_SIZE);
            pages.push((
                next,
                limit,
                Read::new(
                    self.merkle_tree_hook_id,
                    "message_ids",
                    &(next, limit),
                    list_reply(limit as usize, 32),
                )?,
            ));
            next += limit;
        }
        Ok(pages)
    }

    /// Extend `cache` to the hook's first `count` leaves and check it
    /// against the hook, reading the checks (`count`, `root`, `tree` and
    /// `message_id_at(count - 1)`), with the last pages of IDs that fit
    /// beside them, in one aggregate: at one state, which must be at or
    /// above block `at_height`, whose insertions the leaves are. `None`
    /// when no aggregate can, and the caller reads them one query at a
    /// time.
    ///
    /// The state's height only shows that it is not older than the block
    /// the leaves were counted at. It does not name a block, and the leaves
    /// are final by their count, found as before.
    async fn snapshot_tree(
        &self,
        cache: &mut MerkleCache,
        count: u32,
        at_height: u64,
    ) -> ChainResult<Option<Result<(), String>>> {
        let Some(index) = count.checked_sub(1) else {
            return Ok(None);
        };
        if self.rues.batch_capacity().await? == 1 {
            return Ok(None);
        }
        let hook = self.merkle_tree_hook_id;
        let checks = [
            Read::new(hook, "count", &(), U32_REPLY)?,
            Read::new(hook, "root", &(), ID_REPLY)?,
            Read::new(hook, "tree", &(), HOOK_TREE_REPLY)?,
            Read::new(hook, "message_id_at", &(index,), ID_REPLY)?,
        ];
        let pages = self.tree_pages(cache_count(cache)?, count)?;
        // The pages before those that fit beside the checks go first, a few
        // to an aggregate, each ingested once read.
        let mut lead = pages.len();
        while lead > 0 {
            let candidate: Vec<Read> = pages[lead - 1..]
                .iter()
                .map(|(_, _, read)| read.clone())
                .chain(checks.iter().cloned())
                .collect();
            if !fits_one_aggregate(&candidate) {
                break;
            }
            lead -= 1;
        }
        for chunk in pages[..lead].chunks(MERKLE_PAGES_PER_AGGREGATE) {
            let reads: Vec<Read> = chunk.iter().map(|(_, _, read)| read.clone()).collect();
            let replies = self.rues.read_all(&reads).await?;
            for ((start, limit, _), reply) in chunk.iter().zip(replies) {
                ingest_page(cache, &reply, *start, *limit)?;
            }
        }
        let mut reads: Vec<Read> = pages[lead..]
            .iter()
            .map(|(_, _, read)| read.clone())
            .collect();
        reads.extend(checks);
        let Some(snapshot) = self.rues.read_snapshot(&reads).await? else {
            return Ok(None);
        };
        if snapshot.height < at_height {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk endpoint's state at height {} is below block {at_height}, whose insertions the checkpoint holds; reading it again later",
                snapshot.height
            ))
            .into());
        }
        let mut results = snapshot.results.into_iter();
        let mut next = || {
            results.next().unwrap_or_else(|| {
                Err(HyperlaneDuskError::Other(
                    "Dusk multicall snapshot returned too few results".into(),
                ))
            })
        };
        for (start, limit, _) in &pages[lead..] {
            ingest_page(cache, &next()?, *start, *limit)?;
        }
        let hook_count: u32 = rkyv_deserialize(&next()?)?;
        let root: [u8; 32] = rkyv_deserialize(&next()?)?;
        let hook_tree: HookTree = rkyv_deserialize(&next()?)?;
        let stored: [u8; 32] = rkyv_deserialize(&next()?)?;
        Ok(Some(verify_snapshot(
            &cache.tree,
            cache.last,
            index,
            hook_count,
            root,
            &hook_tree,
            stored,
        )))
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
            tree: self.tree_at(count, finalized_height).await?,
            block_height: Some(finalized_height),
        })
    }

    async fn checkpoint_at_height(&self, block_height: u64) -> ChainResult<CheckpointAtBlock> {
        let finalized_height = self.rues.finalized_block_height().await?;
        let effective_height = block_height.min(finalized_height);
        let count = self.merkle_count_at_height(effective_height).await?;
        if count == 0 {
            return Err(HyperlaneDuskError::Other(format!(
                "MerkleTreeHook has no insertion at or before finalized block {effective_height}"
            ))
            .into());
        }
        let tree = self.tree_at(count, effective_height).await?;
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

/// The leaves `cache` holds.
fn cache_count(cache: &MerkleCache) -> ChainResult<u32> {
    Ok(u32::try_from(cache.tree.count())
        .map_err(|_| HyperlaneDuskError::Other("Dusk Merkle tree count exceeds u32".into()))?)
}

/// Ingest a `message_ids` reply from leaf `start`, which must hold the
/// `limit` IDs asked.
fn ingest_page(cache: &mut MerkleCache, reply: &[u8], start: u32, limit: u32) -> ChainResult<()> {
    let message_ids: Vec<[u8; 32]> = rkyv_deserialize(reply)?;
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
    Ok(())
}

/// `verify_tree`'s check of `tree`, whose last leaf is `last`, from the
/// hook's replies read at one state: its `count`, `root`, `tree` and the
/// ID it stores at `index`, the tree's last. `Err(detail)` when they
/// disagree with the tree or with each other.
fn verify_snapshot(
    tree: &IncrementalMerkle,
    last: Option<H256>,
    index: u32,
    count: u32,
    root: [u8; 32],
    hook: &HookTree,
    stored: [u8; 32],
) -> Result<(), String> {
    if hook.count != count {
        return Err(format!(
            "the hook's count {count} and its tree's {} differ at one state",
            hook.count
        ));
    }
    if last != Some(H256::from(stored)) {
        return Err(format!(
            "message_id_at({index}) is {:?}, the last ingested ID {last:?}",
            H256::from(stored)
        ));
    }
    if hook.count as usize != tree.count() {
        return Ok(());
    }
    if !matches_hook_tree(tree, hook) {
        return Err(format!(
            "the hook's own tree of {} leaves has root {:?}, the computed one {:?}",
            hook.count,
            H256::from(hook.root()),
            tree.root()
        ));
    }
    if H256::from(root) != tree.root() {
        return Err(format!(
            "the hook's root {:?} is not the computed one {:?}",
            H256::from(root),
            tree.root()
        ));
    }
    Ok(())
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
    ///
    /// A check that settled holds until the node finalizes another block:
    /// asked again before that, it is answered without a request. A final
    /// delivery is answered without a request for good. A check the
    /// delivery log could not settle, because the archive failed or the log
    /// does not reach the last final block yet, holds for at most
    /// `UNSETTLED_REUSE`, and only while the log has not read on: the next
    /// one tries the archive again, or reads on in it.
    async fn delivered(&self, id: H256) -> ChainResult<bool> {
        // The chain first, then the state the check holds, so that no
        // rollback in between goes unnoticed.
        let pair = self.rues.block_pair().await?;
        let finalized = pair.finalized();
        let log = crate::mailbox_indexer::delivery_log(self.mailbox_id);
        let known = self
            .delivery_checks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(id, finalized, || self.rues.event_log_progress(&log));
        let delivery = match known {
            Some(delivery) => delivery,
            None => {
                let delivery = self.check_delivery(id, &pair).await?;
                if let (Delivery::Final, Some(signer), true) =
                    (&delivery, &self.signer, self.pipelined)
                {
                    // Whoever delivered it, no delivery of it is in flight.
                    self.release_nonce(&signer_key(signer), id);
                }
                let hold = delivery.hold(finalized, || self.rues.event_log_progress(&log));
                self.delivery_checks
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .insert(id, finalized, hold, delivery.clone());
                delivery
            }
        };
        match delivery {
            Delivery::Final => Ok(true),
            Delivery::Absent => Ok(false),
            Delivery::Pending(reason) | Delivery::Unsettled(reason) => {
                Err(HyperlaneDuskError::NotFinal(reason).into())
            }
        }
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
        // Messages to one recipient prepared together ask it together. The
        // Mailbox runs the recipient's own getter, so the read goes after
        // the deployment's in an aggregate.
        let read = Read::new(
            self.mailbox_id,
            "recipient_ism",
            &(recipient_bytes,),
            ID_REPLY,
        )?
        .untrusted();
        let ism_bytes: [u8; 32] = rkyv_deserialize(&self.rues.coalesced_read(read).await?)?;
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
        let gas_limit = self.submission_gas_limit(message, tx_gas_limit);

        // The relayer simulates through process_estimate_costs before calling
        // this, and that simulation stands when it passed moments ago.
        // Otherwise it runs here, at the only call that spends gas, so no
        // caller submits a delivery that would revert.
        self.simulate_process(message, &args).await?;

        info!(
            message_id = ?message.id(),
            nonce = message.nonce,
            "Processing message on Dusk via dusk-tx"
        );

        if self.pipelined {
            return self
                .process_pipelined(signer, message.id(), &args, gas_limit)
                .await;
        }
        self.process_one(signer, &args, gas_limit).await
    }

    /// Batches are sent as consecutive transactions of the signer's
    /// account (`process_deliveries`), only when deliveries are pipelined
    /// (`ConnectionConf::pipelines_deliveries`). Otherwise the relayer
    /// submits every delivery on its own, through `process`.
    fn supports_batching(&self) -> bool {
        self.signer.is_some() && self.pipelined
    }

    async fn process_batch<'a>(&self, ops: Vec<&'a QueueOperation>) -> ChainResult<BatchResult> {
        // Without pipelining, every delivery goes on its own, through
        // `process`.
        if !self.pipelined {
            return Ok(BatchResult::failed(ops.len()));
        }
        let deliveries = ops
            .iter()
            .map(|op| {
                let item = op.try_batch()?;
                Ok((
                    item.data,
                    item.submission_data.metadata,
                    Some(item.submission_data.gas_limit),
                ))
            })
            .collect::<ChainResult<Vec<_>>>()?;
        self.process_deliveries(&deliveries).await
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
                pipeline_deliveries: false,
            },
        );
        let hook = DuskMerkleTreeHook::new(mailbox.clone());
        assert_eq!(mailbox.address(), mailbox_id);
        assert_eq!(hook.address(), hook_id);
        assert_ne!(mailbox.address(), hook.address());
    }

    #[test]
    fn a_delivery_gas_limit_is_at_most_gas_limit_and_the_block_gas_limit() {
        let mailbox = |gas_limit: u64| {
            let rues =
                Arc::new(RuesClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap());
            let domain = HyperlaneDomain::from_config(
                4242,
                "dusk-test",
                HyperlaneDomainProtocol::Dusk,
                HyperlaneDomainTechnicalStack::Other,
            )
            .unwrap();
            let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
            DuskMailbox::new(
                provider,
                rues,
                H256::from([1u8; 32]),
                H256::from([2u8; 32]),
                domain,
                None,
                ConnectionConf {
                    url: Url::parse("http://127.0.0.1:1").unwrap(),
                    chain_id: 1,
                    event_cursor_dir: std::env::temp_dir(),
                    gas_limit,
                    gas_price: 1,
                    native_token: NativeToken::default(),
                    op_submission_config: OpSubmissionConfig::default(),
                    query_rate_limit: crate::QueryRateLimit::default(),
                    pipeline_deliveries: false,
                },
            )
        };
        let message = HyperlaneMessage::default();
        let five_million = mailbox(5_000_000);
        let limit =
            |requested: Option<U256>| five_million.submission_gas_limit(&message, requested);
        assert_eq!(limit(None), 5_000_000);
        assert_eq!(limit(Some(U256::from(2_000_000))), 2_000_000);
        // A sender that paid for more gas does not raise what a failed
        // delivery pays, nor one over u64.
        assert_eq!(limit(Some(U256::from(9_000_000_000u64))), 5_000_000);
        assert_eq!(limit(Some(U256::MAX)), 5_000_000);
        // No block holds more than the block gas limit, whatever the
        // configuration says (the parser refuses such a gasLimit).
        assert_eq!(
            mailbox(u64::MAX).submission_gas_limit(&message, None),
            RUSK_BLOCK_GAS_LIMIT
        );
    }
}
