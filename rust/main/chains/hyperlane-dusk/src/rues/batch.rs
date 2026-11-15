//! Batched contract reads through Multicall for Dusk.
//!
//! A chain that names a multicall (`batchContractAddress`) has its clients
//! send reads that do not depend on each other as one `aggregate` query,
//! which answers them all at one state, instead of one query each. An
//! aggregate is one request to the endpoint's pace and rate limit. Without
//! a multicall, or while it is unusable, every read is one query, exactly
//! as before.
//!
//! - **Probe.** Before its first aggregate, a process checks that the
//!   multicall answers `version` with 1 and an empty `aggregate` with no
//!   results. If it does not, or later misbehaves (a reply that is not an
//!   aggregate of its calls, or an aggregate of one read that fails while
//!   the read itself answers), the agent warns once, reads one query at a
//!   time, and probes again 10 minutes later.
//! - **Limits.** A batch goes in runs of at most 256 calls whose argument
//!   and reply fit the multicall's 64 KiB even if every call fails with the
//!   longest panic message, by the reply size each read declares. An
//!   aggregate that fails as a whole, as one whose replies pass 64 KiB
//!   does, is split in two and each half sent again; a half of one read is
//!   sent as a single query. Rusk 1.7.3 answers every failed query with
//!   `500 Internal server error`, so the agent cannot tell the multicall's
//!   "reply exceeds" panic from other failures, and when both halves fail
//!   it reads the run one query at a time instead of splitting further.
//! - **Results.** A call that fails in an aggregate gives the error a single
//!   query of it gives: `500`, here with the reason. One that runs out of
//!   gas is sent again as a single query, which has the query's whole gas
//!   bound instead of a share of it.
//! - **Order.** A call can write state that the calls after it in the same
//!   query read. Reads of contracts outside the configured deployment, such
//!   as an ISM a recipient chose, go after the others. Such a contract can
//!   also answer the multicall otherwise than a query, so only failures of
//!   the deployment's own reads set the multicall aside.
//!
//! Every view the agent batches answers its caller alike, so a read in an
//! aggregate replies with the bytes a single query of it would.

use std::future::Future;
use std::pin::Pin;

use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::info;

use super::*;
use crate::multicall::{self, Aggregate, Call, CallError};

/// How long the agent reads one query at a time after the multicall failed
/// its probe or misbehaved, before it probes it again.
const REPROBE_INTERVAL: Duration = Duration::from_secs(600);
/// Most reads a final block's batch carries because they were asked at the
/// blocks before it.
const MAX_WATCHED_READS: usize = 1024;
/// Most bytes of replies, with their arguments, kept for a final block.
const MAX_TICK_REPLY_BYTES: usize = 1024 * 1024;

/// Reply sizes of the views the agent batches, in bytes.
pub(crate) const BOOL_REPLY: usize = 1;
pub(crate) const U8_REPLY: usize = 1;
pub(crate) const U32_REPLY: usize = 4;
pub(crate) const U64_REPLY: usize = 8;
pub(crate) const ID_REPLY: usize = 32;

/// The reply size of a list of at most `len` items of `item` bytes each.
pub(crate) const fn list_reply(len: usize, item: usize) -> usize {
    16 + len * item
}

/// One contract read, which a batch can carry.
#[derive(Debug, Clone)]
pub(crate) struct Read {
    contract: [u8; 32],
    method: &'static str,
    args: Vec<u8>,
    /// The longest reply the view gives, in bytes. Batches keep their
    /// replies within the multicall's buffer by it; a reply past it can
    /// only cost an aggregate that fails and is sent again in parts.
    max_reply: usize,
    /// Whether the callee is a contract of the configured deployment.
    /// Other reads go after these in an aggregate, and do not count against
    /// the multicall when their aggregate fails.
    trusted: bool,
}

/// What names a read: its contract, view and argument.
type ReadKey = ([u8; 32], &'static str, Vec<u8>);
/// A read's reply, or the error its query gives.
pub(crate) type ReadResult = Result<Vec<u8>, HyperlaneDuskError>;

impl Read {
    /// A read of `method` of `contract` with `args`, whose reply is at most
    /// `max_reply` bytes.
    pub(crate) fn new<I>(
        contract: [u8; 32],
        method: &'static str,
        args: &I,
        max_reply: usize,
    ) -> Result<Self, HyperlaneDuskError>
    where
        I: Serialize<AllocSerializer<256>>,
    {
        Ok(Self {
            contract,
            method,
            args: rkyv_serialize(args)?,
            max_reply,
            trusted: true,
        })
    }

    /// This read, of a contract outside the configured deployment.
    pub(crate) fn untrusted(mut self) -> Self {
        self.trusted = false;
        self
    }

    fn key(&self) -> ReadKey {
        (self.contract, self.method, self.args.clone())
    }

    fn call(&self) -> Call {
        Call {
            contract: self.contract,
            fn_name: self.method.to_owned(),
            fn_args: self.args.clone(),
            gas_limit: 0,
        }
    }
}

/// Whether a batch fails at its first failed read or reports each read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Fail with the first failed read, and send no read after it when
    /// reading one at a time.
    All,
    /// One result per read.
    Each,
}

/// Replies read together at one state.
#[derive(Debug)]
pub(crate) struct Snapshot {
    /// The height of the block whose state the reads saw. It does not name
    /// the block: a block rolled back and the one replacing it share it.
    pub(crate) height: u64,
    /// One result per read, in order.
    pub(crate) results: Vec<ReadResult>,
}

/// What every client of an endpoint in this process knows of one
/// multicall.
#[derive(Debug)]
pub(crate) struct Multicall {
    contract: [u8; 32],
    /// Set while the multicall is usable, so that reads need no lock.
    ready: AtomicBool,
    availability: tokio::sync::Mutex<Availability>,
    /// The reads of the last final block, see `tick_read`.
    tick: tokio::sync::Mutex<TickReads>,
    /// Reads waiting for the next aggregate, see `coalesced_read`.
    queue: StdMutex<Queue>,
}

impl Multicall {
    pub(super) fn new(contract: [u8; 32]) -> Self {
        Self {
            contract,
            ready: AtomicBool::new(false),
            availability: tokio::sync::Mutex::new(Availability::Unprobed),
            tick: tokio::sync::Mutex::new(TickReads::default()),
            queue: StdMutex::new(Queue::default()),
        }
    }

    fn queue(&self) -> std::sync::MutexGuard<'_, Queue> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug, Clone, Copy)]
enum Availability {
    Unprobed,
    Ready,
    /// Read one query at a time until `retry_at`, then probe again.
    Unavailable {
        retry_at: Instant,
    },
}

/// The reads asked at the endpoint's last final block, and the reads asked
/// at the blocks before it, which the next block's batch carries.
///
/// Replies are kept only for watched reads, and dropped with them, at most
/// `MAX_TICK_REPLY_BYTES` with their arguments: while finality stalls, ever
/// new reads asked at the same final block, such as delivery checks of new
/// messages, cannot grow them without bound.
#[derive(Debug, Default)]
struct TickReads {
    /// Final blocks seen, counting from one.
    epoch: u64,
    final_block: Option<u64>,
    /// The replies of watched reads, read after `final_block` was final.
    replies: HashMap<ReadKey, Vec<u8>>,
    /// The bytes of `replies` and of their reads' arguments.
    reply_bytes: usize,
    /// Reads asked lately, with the epoch they were last asked at and when,
    /// counted in reads asked, so that the least recently asked goes first.
    watched: HashMap<ReadKey, (Read, u64, u64)>,
    /// Reads asked so far.
    asked: u64,
}

impl TickReads {
    fn watch(&mut self, read: &Read) {
        let key = read.key();
        self.asked += 1;
        self.watched
            .insert(key.clone(), (read.clone(), self.epoch, self.asked));
        if self.watched.len() > MAX_WATCHED_READS {
            let oldest = self
                .watched
                .iter()
                .filter(|(watched, _)| **watched != key)
                .min_by_key(|(_, (_, epoch, asked))| (*epoch, *asked))
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.watched.remove(&oldest);
                self.drop_reply(&oldest);
            }
        }
    }

    /// Keep `reply` of the read `key` for this final block, if the read is
    /// watched and the reply fits the bound.
    fn keep(&mut self, key: ReadKey, reply: Vec<u8>) {
        if !self.watched.contains_key(&key) {
            return;
        }
        self.drop_reply(&key);
        let bytes = key.2.len().saturating_add(reply.len());
        if self.reply_bytes.saturating_add(bytes) > MAX_TICK_REPLY_BYTES {
            return;
        }
        self.reply_bytes += bytes;
        self.replies.insert(key, reply);
    }

    fn drop_reply(&mut self, key: &ReadKey) {
        if let Some(reply) = self.replies.remove(key) {
            self.reply_bytes = self
                .reply_bytes
                .saturating_sub(key.2.len().saturating_add(reply.len()));
        }
    }

    /// Read at final block `final_block` from now on: the replies of the
    /// block before go, and so do the reads not asked at the last blocks.
    fn next_block(&mut self, final_block: u64) {
        self.epoch += 1;
        self.final_block = Some(final_block);
        self.replies.clear();
        self.reply_bytes = 0;
        let epoch = self.epoch;
        self.watched
            .retain(|_, (_, asked, _)| asked.saturating_add(2) >= epoch);
    }
}

#[derive(Debug, Default)]
struct Queue {
    pending: Vec<(Read, oneshot::Sender<ReadResult>)>,
    /// Whether a task is sending the pending reads.
    draining: bool,
}

/// How an aggregate failed.
enum AggregateFailure {
    /// No answer from the node, such as a timeout or a rate limit. Nothing
    /// is known of the reads.
    Unanswered(HyperlaneDuskError),
    /// The node ran the query and it failed as a whole, as one whose
    /// replies pass the buffer does.
    Failed(HyperlaneDuskError),
    /// The reply is not an aggregate of the calls sent.
    Malformed(String),
}

/// Whether a failed request got no answer from the node itself.
fn unanswered(error: &HyperlaneDuskError) -> bool {
    match error {
        HyperlaneDuskError::RuesHttp { .. }
        | HyperlaneDuskError::RateLimited { .. }
        | HyperlaneDuskError::IdentityUnverified(_) => true,
        // A proxy in front of the node, saying it cannot reach it.
        HyperlaneDuskError::RuesResponse { status, .. } => matches!(status, 502..=504),
        _ => false,
    }
}

/// The error a single query of `read` gives where its call in an aggregate
/// failed with `error`: Rusk answers a query whose contract fails with
/// `500`.
fn call_failed(read: &Read, error: &CallError) -> HyperlaneDuskError {
    let reason = match error {
        CallError::Panic(message) => format!("the contract panicked: {message}"),
        CallError::OutOfGas => "it ran out of gas".to_owned(),
        CallError::DoesNotExist => "no contract has this ID".to_owned(),
        CallError::Unknown => "it failed (a missing function or a trap)".to_owned(),
    };
    HyperlaneDuskError::RuesResponse {
        status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
        body: bounded_text(&format!(
            "Dusk contract query {} of {} failed in a multicall: {reason}",
            read.method,
            hex::encode(read.contract)
        )),
    }
}

type Boxed<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

impl RuesClient {
    /// This client, batching independent contract reads through the
    /// multicall at `contract`, if any; see the module documentation.
    /// Every client of the endpoint in this process shares what is known of
    /// the multicall.
    pub fn with_multicall(mut self, contract: Option<[u8; 32]>) -> Self {
        self.multicall = contract.map(|contract| self.endpoint.multicall(contract));
        self
    }

    /// How many reads one aggregate takes: `multicall::MAX_CALLS` while the
    /// multicall is usable, otherwise 1.
    pub(crate) async fn batch_capacity(&self) -> Result<usize, HyperlaneDuskError> {
        self.ensure_identity().await?;
        Ok(match self.ready_multicall().await? {
            Some(_) => multicall::MAX_CALLS,
            None => 1,
        })
    }

    /// The reply of every read, or the first error. Through the multicall,
    /// in as few aggregates as the limits allow; otherwise one query each,
    /// in order, sending none after a failed one.
    pub(crate) async fn read_all(
        &self,
        reads: &[Read],
    ) -> Result<Vec<Vec<u8>>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        self.read_batch(reads, Mode::All)
            .await?
            .into_iter()
            .collect()
    }

    /// One result per read. Fails only when an aggregate got no answer.
    pub(crate) async fn read_each(
        &self,
        reads: &[Read],
    ) -> Result<Vec<ReadResult>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        self.read_batch(reads, Mode::Each).await
    }

    /// `reads` in one aggregate, all at one state, with the height of that
    /// state. `None` when the multicall is not usable, the reads do not fit
    /// one aggregate, or the aggregate failed: the caller then reads them
    /// as it would without a multicall. A call that fails gives the error a
    /// single query gives.
    pub(crate) async fn read_snapshot(
        &self,
        reads: &[Read],
    ) -> Result<Option<Snapshot>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        let Some(multicall) = self.ready_multicall().await? else {
            return Ok(None);
        };
        let ordered = trusted_first(reads);
        let runs = multicall::plan(ordered.iter().map(|&index| plan_entry(&reads[index])));
        if runs.len() != 1 {
            return Ok(None);
        }
        let calls: Vec<&Read> = ordered.iter().map(|&index| &reads[index]).collect();
        match self.send_aggregate(&multicall, &calls).await {
            Ok(aggregate) => {
                let mut results: Vec<Option<ReadResult>> = reads.iter().map(|_| None).collect();
                for (&index, result) in ordered.iter().zip(aggregate.results) {
                    results[index] =
                        Some(result.map_err(|error| call_failed(&reads[index], &error)));
                }
                Ok(Some(Snapshot {
                    height: aggregate.height,
                    results: results.into_iter().flatten().collect(),
                }))
            }
            Err(AggregateFailure::Unanswered(error)) => Err(error),
            Err(AggregateFailure::Malformed(reason)) => {
                self.multicall_misbehaved(&multicall, &reason).await;
                Ok(None)
            }
            Err(AggregateFailure::Failed(error)) => {
                debug!(error = %bounded_error(&error), "Dusk multicall snapshot failed as a whole; reading one query at a time");
                Ok(None)
            }
        }
    }

    /// `read`'s reply, read after `final_block` was the node's last final
    /// block, for reads asked once per final block, such as record counts
    /// and delivery checks.
    ///
    /// The first such read after the node finalizes another block reads,
    /// in the same aggregate, every read asked at the two blocks before, and
    /// keeps the replies for that block: a record count or a delivery check
    /// read later at that block answers from them. Each was read after the
    /// block was final, as a read of its own would be. Without a usable
    /// multicall this is one query.
    pub(crate) async fn tick_read(
        &self,
        final_block: u64,
        read: Read,
    ) -> Result<Vec<u8>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        let Some(multicall) = self.ready_multicall().await? else {
            return self.query_read(&read).await;
        };
        let key = read.key();
        let mut tick = multicall.tick.lock().await;
        match tick.final_block {
            Some(current) if current == final_block => {
                tick.watch(&read);
                if let Some(reply) = tick.replies.get(&key) {
                    return Ok(reply.clone());
                }
                drop(tick);
                // Not read at this block yet: with whatever else is asked
                // meanwhile.
                let reply = self.coalesced(&multicall, read).await?;
                let mut tick = multicall.tick.lock().await;
                if tick.final_block == Some(final_block) {
                    tick.keep(key, reply.clone());
                }
                Ok(reply)
            }
            // The endpoint already reads a later block.
            Some(current) if current > final_block => {
                drop(tick);
                self.query_read(&read).await
            }
            _ => {
                tick.next_block(final_block);
                let mut reads = vec![read.clone()];
                reads.extend(
                    tick.watched
                        .iter()
                        .filter(|(watched, _)| **watched != key)
                        .map(|(_, (watched, _, _))| watched.clone()),
                );
                tick.watch(&read);
                // Others asking at this block wait for these replies.
                let results = self.read_batch(&reads, Mode::Each).await?;
                let mut own = None;
                for (read, result) in reads.iter().zip(results) {
                    if own.is_none() {
                        if let Ok(reply) = &result {
                            tick.keep(read.key(), reply.clone());
                        }
                        own = Some(result);
                    } else if let Ok(reply) = result {
                        tick.keep(read.key(), reply);
                    }
                }
                own.unwrap_or_else(|| {
                    Err(HyperlaneDuskError::Other(
                        "Dusk multicall batch returned no result for its first read".into(),
                    ))
                })
            }
        }
    }

    /// `read`'s reply as `tick_read` gives it at the node's last final
    /// block, which is only read when batching: without a usable multicall
    /// this is one query.
    pub(crate) async fn final_tick_read(&self, read: Read) -> Result<Vec<u8>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        if self.ready_multicall().await?.is_none() {
            return self.query_read(&read).await;
        }
        let final_block = self.block_pair().await?.finalized();
        self.tick_read(final_block, read).await
    }

    /// `method` of `contract` for each of `indices`, its `u32` argument,
    /// decoded, through `read_all`.
    pub(crate) async fn read_indexed<O>(
        &self,
        contract: [u8; 32],
        method: &'static str,
        indices: &[u32],
        max_reply: usize,
    ) -> Result<Vec<O>, HyperlaneDuskError>
    where
        O: Archive,
        O::Archived: Deserialize<O, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
    {
        let reads = indices
            .iter()
            .map(|index| Read::new(contract, method, &(*index,), max_reply))
            .collect::<Result<Vec<_>, _>>()?;
        self.read_all(&reads)
            .await?
            .iter()
            .map(|reply| rkyv_deserialize(reply))
            .collect()
    }

    /// `read`'s reply. Through the multicall, in one aggregate with the
    /// reads other tasks ask while one is in flight, each read sent once
    /// however many ask it; without a usable multicall, one query.
    pub(crate) async fn coalesced_read(&self, read: Read) -> Result<Vec<u8>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        match self.ready_multicall().await? {
            Some(multicall) => self.coalesced(&multicall, read).await,
            None => self.query_read(&read).await,
        }
    }

    async fn coalesced(
        &self,
        multicall: &Arc<Multicall>,
        read: Read,
    ) -> Result<Vec<u8>, HyperlaneDuskError> {
        let (sender, receiver) = oneshot::channel();
        let start = {
            let mut queue = multicall.queue();
            queue.pending.push((read, sender));
            !std::mem::replace(&mut queue.draining, true)
        };
        if start {
            let client = self.clone();
            let multicall = multicall.clone();
            tokio::spawn(async move { client.drain(multicall).await });
        }
        receiver.await.unwrap_or_else(|_| {
            Err(HyperlaneDuskError::Other(
                "The Dusk multicall batch holding this read was dropped".into(),
            ))
        })
    }

    /// Send the queued reads, a batch at a time, until none are left. Reads
    /// queued while a batch is in flight go in the next one.
    async fn drain(&self, multicall: Arc<Multicall>) {
        /// A drain that ends before it emptied the queue, which only a
        /// dropped task does, fails the reads still queued, which then retry,
        /// and lets the next read start a drain.
        struct Draining {
            multicall: Arc<Multicall>,
            done: bool,
        }
        impl Drop for Draining {
            fn drop(&mut self) {
                if !self.done {
                    let mut queue = self.multicall.queue();
                    queue.draining = false;
                    queue.pending.clear();
                }
            }
        }
        let mut draining = Draining {
            multicall,
            done: false,
        };
        loop {
            let pending = {
                let mut queue = draining.multicall.queue();
                if queue.pending.is_empty() {
                    // Under the lock a read is queued with: one queued
                    // after this starts the next drain.
                    queue.draining = false;
                    draining.done = true;
                    return;
                }
                std::mem::take(&mut queue.pending)
            };
            let mut reads: Vec<Read> = Vec::new();
            let mut positions: HashMap<ReadKey, usize> = HashMap::new();
            let mut waiters = Vec::with_capacity(pending.len());
            for (read, sender) in pending {
                let position = *positions.entry(read.key()).or_insert_with(|| {
                    reads.push(read);
                    reads.len() - 1
                });
                waiters.push((position, sender));
            }
            let results = self.read_batch(&reads, Mode::Each).await;
            for (position, sender) in waiters {
                let result = match &results {
                    Ok(results) => results.get(position).cloned().unwrap_or_else(|| {
                        Err(HyperlaneDuskError::Other(
                            "Dusk multicall batch returned too few results".into(),
                        ))
                    }),
                    Err(error) => Err(error.clone()),
                };
                // The reader may have gone.
                let _ = sender.send(result);
            }
        }
    }

    /// Replies of the prefetched `reads`, read in one batch when a
    /// multicall is usable; otherwise none, and each is read when asked, as
    /// without a multicall.
    pub(super) async fn prefetch(&self, reads: &[Read]) -> Result<Prefetch, HyperlaneDuskError> {
        if self.multicall.is_none() || self.ready_multicall().await?.is_none() {
            return Ok(Prefetch::default());
        }
        let results = self.read_batch(reads, Mode::Each).await?;
        Ok(Prefetch {
            replies: reads.iter().map(Read::key).zip(results).collect(),
        })
    }

    /// One query of `read`, through `send`'s pace.
    pub(super) async fn query_read(&self, read: &Read) -> ReadResult {
        self.contract_query_raw_unchecked(&read.contract, read.method, &read.args)
            .await
    }

    /// The multicall to batch reads through, once it has passed its probe.
    /// `None` reads one query at a time. Fails when the probe got no answer.
    async fn ready_multicall(&self) -> Result<Option<Arc<Multicall>>, HyperlaneDuskError> {
        let Some(multicall) = &self.multicall else {
            return Ok(None);
        };
        if multicall.ready.load(Ordering::Acquire) {
            return Ok(Some(multicall.clone()));
        }
        let mut availability = multicall.availability.lock().await;
        let again = match *availability {
            Availability::Ready => return Ok(Some(multicall.clone())),
            Availability::Unavailable { retry_at } if Instant::now() < retry_at => return Ok(None),
            Availability::Unavailable { .. } => true,
            Availability::Unprobed => false,
        };
        let contract = hex::encode(multicall.contract);
        match self.probe_multicall(multicall).await? {
            Ok(()) => {
                if again {
                    info!(multicall = %contract, "Dusk multicall is usable again; batching independent contract reads through it");
                } else {
                    info!(multicall = %contract, "Dusk multicall answers ABI version 1; batching independent contract reads through it");
                }
                *availability = Availability::Ready;
                multicall.ready.store(true, Ordering::Release);
                Ok(Some(multicall.clone()))
            }
            Err(reason) => {
                if again {
                    debug!(multicall = %contract, %reason, "Dusk multicall is still not usable");
                } else {
                    warn!(multicall = %contract, %reason, "Dusk multicall (batchContractAddress) is not usable; reading contracts one query at a time and probing it again every 10 minutes");
                }
                *availability = Availability::Unavailable {
                    retry_at: Instant::now() + REPROBE_INTERVAL,
                };
                Ok(None)
            }
        }
    }

    /// Whether the multicall answers `version` with 1 and an empty
    /// `aggregate` with no results: `Ok(Err(reason))` when it does not.
    async fn probe_multicall(
        &self,
        multicall: &Multicall,
    ) -> Result<Result<(), String>, HyperlaneDuskError> {
        let version = match self
            .contract_query_raw_unchecked(&multicall.contract, "version", &[])
            .await
        {
            Ok(version) => version,
            Err(error) if unanswered(&error) => return Err(error),
            Err(error) => {
                return Ok(Err(format!(
                    "its version query failed: {}",
                    bounded_error(&error)
                )))
            }
        };
        match rkyv_deserialize::<u32>(&version) {
            Ok(multicall::VERSION) if version.len() == 4 => {}
            Ok(other) if version.len() == 4 => {
                return Ok(Err(format!(
                    "it reports ABI version {other}, and this agent reads version {}",
                    multicall::VERSION
                )))
            }
            _ => {
                return Ok(Err(format!(
                    "its version reply is not a version: {}",
                    bounded_body(&version)
                )))
            }
        }
        match self.send_aggregate(multicall, &[]).await {
            Ok(_) => Ok(Ok(())),
            Err(AggregateFailure::Unanswered(error)) => Err(error),
            Err(AggregateFailure::Failed(error)) => Ok(Err(format!(
                "an empty aggregate failed: {}",
                bounded_error(&error)
            ))),
            Err(AggregateFailure::Malformed(reason)) => {
                Ok(Err(format!("an empty aggregate {reason}")))
            }
        }
    }

    /// Stop batching through a multicall that answered wrongly, warning
    /// once, until the next probe.
    async fn multicall_misbehaved(&self, multicall: &Multicall, reason: &str) {
        let mut availability = multicall.availability.lock().await;
        multicall.ready.store(false, Ordering::Release);
        if matches!(*availability, Availability::Ready) {
            warn!(
                multicall = %hex::encode(multicall.contract),
                %reason,
                "Dusk multicall answered wrongly; reading contracts one query at a time and probing it again in 10 minutes"
            );
        }
        *availability = Availability::Unavailable {
            retry_at: Instant::now() + REPROBE_INTERVAL,
        };
    }

    /// One aggregate of `reads`.
    async fn send_aggregate(
        &self,
        multicall: &Multicall,
        reads: &[&Read],
    ) -> Result<Aggregate, AggregateFailure> {
        let calls: Vec<Call> = reads.iter().map(|read| read.call()).collect();
        let body = rkyv_serialize(&calls).map_err(AggregateFailure::Failed)?;
        let reply = match self
            .contract_query_raw_unchecked(&multicall.contract, "aggregate", &body)
            .await
        {
            Ok(reply) => reply,
            Err(error) if unanswered(&error) => return Err(AggregateFailure::Unanswered(error)),
            Err(error @ HyperlaneDuskError::RuesResponse { .. }) => {
                return Err(AggregateFailure::Failed(error))
            }
            Err(error) => return Err(AggregateFailure::Malformed(bounded_error(&error))),
        };
        let aggregate: Aggregate = rkyv_deserialize(&reply).map_err(|error| {
            AggregateFailure::Malformed(format!(
                "replied with bytes that are not an aggregate: {error}"
            ))
        })?;
        if aggregate.results.len() != calls.len() {
            return Err(AggregateFailure::Malformed(format!(
                "answered {} results to {} calls",
                aggregate.results.len(),
                calls.len()
            )));
        }
        Ok(aggregate)
    }

    /// `reads`, through the multicall when it is usable and there are at
    /// least two, otherwise one query each. In `Mode::All` the first failed
    /// read fails the batch.
    async fn read_batch(
        &self,
        reads: &[Read],
        mode: Mode,
    ) -> Result<Vec<ReadResult>, HyperlaneDuskError> {
        let multicall = match reads.len() {
            0 | 1 => None,
            _ => self.ready_multicall().await?,
        };
        let Some(multicall) = multicall else {
            let reads: Vec<&Read> = reads.iter().collect();
            return self.one_by_one(&reads, mode).await;
        };
        let ordered = trusted_first(reads);
        let mut results: Vec<Option<ReadResult>> = reads.iter().map(|_| None).collect();
        for run in multicall::plan(ordered.iter().map(|&index| plan_entry(&reads[index]))) {
            let calls: Vec<&Read> = ordered[run.clone()]
                .iter()
                .map(|&index| &reads[index])
                .collect();
            let answered = self.read_run(&multicall, &calls, mode).await?;
            for (&index, result) in ordered[run].iter().zip(answered) {
                results[index] = Some(result);
            }
        }
        Ok(results
            .into_iter()
            .map(|result| {
                result.unwrap_or_else(|| {
                    Err(HyperlaneDuskError::Other(
                        "Dusk multicall batch returned too few results".into(),
                    ))
                })
            })
            .collect())
    }

    /// One query per read, in order. In `Mode::All` the first failure ends
    /// the batch with it.
    async fn one_by_one(
        &self,
        reads: &[&Read],
        mode: Mode,
    ) -> Result<Vec<ReadResult>, HyperlaneDuskError> {
        let mut results = Vec::with_capacity(reads.len());
        for read in reads {
            match (mode, self.query_read(read).await) {
                (Mode::All, Err(error)) => return Err(error),
                (_, result) => results.push(result),
            }
        }
        Ok(results)
    }

    /// One run of reads, which the limits let one aggregate take.
    async fn read_run(
        &self,
        multicall: &Arc<Multicall>,
        reads: &[&Read],
        mode: Mode,
    ) -> Result<Vec<ReadResult>, HyperlaneDuskError> {
        match self.send_aggregate(multicall, reads).await {
            Ok(aggregate) => self.settle(reads, aggregate.results, mode).await,
            Err(AggregateFailure::Unanswered(error)) => Err(error),
            Err(AggregateFailure::Malformed(reason)) => {
                self.multicall_misbehaved(multicall, &reason).await;
                self.one_by_one(reads, mode).await
            }
            Err(AggregateFailure::Failed(_)) => self.read_failed_run(multicall, reads, mode).await,
        }
    }

    /// The results of an aggregate's calls as single queries give them.
    async fn settle(
        &self,
        reads: &[&Read],
        results: Vec<Result<Vec<u8>, CallError>>,
        mode: Mode,
    ) -> Result<Vec<ReadResult>, HyperlaneDuskError> {
        let mut settled = Vec::with_capacity(reads.len());
        for (read, result) in reads.iter().zip(results) {
            let result = match result {
                Ok(reply) => Ok(reply),
                // A call gets a share of the query's gas; a single query
                // gets all of it.
                Err(CallError::OutOfGas) => self.query_read(read).await,
                Err(error) => Err(call_failed(read, &error)),
            };
            match (mode, result) {
                (Mode::All, Err(error)) => return Err(error),
                (_, result) => settled.push(result),
            }
        }
        Ok(settled)
    }

    /// Reads of an aggregate that failed as a whole: each half again, and
    /// a half of one read as a single query. When both halves fail, the run
    /// holds no single reply too long for the buffer, and is read one query
    /// at a time.
    fn read_failed_run<'a>(
        &'a self,
        multicall: &'a Arc<Multicall>,
        reads: &'a [&'a Read],
        mode: Mode,
    ) -> Boxed<'a, Result<Vec<ReadResult>, HyperlaneDuskError>> {
        Box::pin(async move {
            if let [read] = reads {
                let result = self.query_read(read).await;
                // A contract outside the deployment can answer the
                // multicall otherwise than a query: only the deployment's
                // own reads tell against the multicall.
                if read.trusted && matches!(&result, Ok(reply) if reply.len() <= read.max_reply) {
                    self.multicall_misbehaved(
                        multicall,
                        "an aggregate of one read failed, and the read itself answered within its size",
                    )
                    .await;
                }
                return match (mode, result) {
                    (Mode::All, Err(error)) => Err(error),
                    (_, result) => Ok(vec![result]),
                };
            }
            let (left, right) = reads.split_at(reads.len() / 2);
            let left_answer = self.send_aggregate(multicall, left).await;
            let right_answer = self.send_aggregate(multicall, right).await;
            match (left_answer, right_answer) {
                (Err(AggregateFailure::Unanswered(error)), _)
                | (_, Err(AggregateFailure::Unanswered(error))) => Err(error),
                (Err(AggregateFailure::Malformed(reason)), _)
                | (_, Err(AggregateFailure::Malformed(reason))) => {
                    self.multicall_misbehaved(multicall, &reason).await;
                    self.one_by_one(reads, mode).await
                }
                (Err(AggregateFailure::Failed(_)), Err(AggregateFailure::Failed(_))) => {
                    debug!(
                        reads = reads.len(),
                        "Both halves of a failed Dusk multicall aggregate failed; reading them one query at a time"
                    );
                    let results = self.one_by_one(reads, mode).await?;
                    // Replies within their sizes fit the aggregates, which
                    // should then have answered.
                    let fit = results.len() == reads.len()
                        && reads.iter().zip(&results).all(|(read, result)| {
                            read.trusted
                                && matches!(result, Ok(reply) if reply.len() <= read.max_reply)
                        });
                    if fit {
                        self.multicall_misbehaved(
                            multicall,
                            "aggregates failed whose reads each answered within their sizes",
                        )
                        .await;
                    }
                    Ok(results)
                }
                (Err(AggregateFailure::Failed(_)), Ok(answer)) => {
                    let mut results = self.read_failed_run(multicall, left, mode).await?;
                    results.extend(self.settle(right, answer.results, mode).await?);
                    Ok(results)
                }
                (Ok(answer), Err(AggregateFailure::Failed(_))) => {
                    let mut results = self.settle(left, answer.results, mode).await?;
                    results.extend(self.read_failed_run(multicall, right, mode).await?);
                    Ok(results)
                }
                (Ok(left_answer), Ok(right_answer)) => {
                    let mut results = self.settle(left, left_answer.results, mode).await?;
                    results.extend(self.settle(right, right_answer.results, mode).await?);
                    Ok(results)
                }
            }
        })
    }
}

/// The positions of `reads`, those of the configured deployment's
/// contracts first, each group in its order.
fn trusted_first(reads: &[Read]) -> Vec<usize> {
    let mut order: Vec<usize> = (0..reads.len()).collect();
    order.sort_by_key(|&index| !reads[index].trusted);
    order
}

/// A read's argument bytes and reply size, as `multicall::plan` takes them.
fn plan_entry(read: &Read) -> (usize, usize) {
    (
        multicall::argument_bytes(read.method, &read.args),
        read.max_reply,
    )
}

/// Whether one aggregate takes `reads` within its limits, whatever each
/// call does.
pub(crate) fn fits_one_aggregate(reads: &[Read]) -> bool {
    multicall::plan(reads.iter().map(plan_entry)).len() <= 1
}

/// Replies read ahead, see `RuesClient::prefetch`.
#[derive(Debug, Default)]
pub(super) struct Prefetch {
    replies: HashMap<ReadKey, ReadResult>,
}

impl Prefetch {
    /// `read`'s reply: the one read ahead, or else one query now.
    pub(super) async fn raw(&mut self, client: &RuesClient, read: &Read) -> ReadResult {
        match self.replies.remove(&read.key()) {
            Some(result) => result,
            None => client.query_read(read).await,
        }
    }

    /// `read`'s reply, decoded.
    pub(super) async fn query<O>(
        &mut self,
        client: &RuesClient,
        read: &Read,
    ) -> Result<O, HyperlaneDuskError>
    where
        O: Archive,
        O::Archived: Deserialize<O, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
    {
        rkyv_deserialize(&self.raw(client, read).await?)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader, Read as _, Write};
    use std::net::{TcpListener, TcpStream};
    use std::thread::{self, JoinHandle};

    use super::*;

    const MULTICALL: [u8; 32] = [0x93; 32];
    /// A contract of test views; see `view`.
    const VIEWS: [u8; 32] = [7; 32];
    /// No contract has this ID.
    const ABSENT: [u8; 32] = [8; 32];
    const HEIGHT: u64 = 4_633_849;

    /// How the node's multicall behaves.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Behaviour {
        Works,
        Missing,
        Version(u32),
        /// Replies to `aggregate` with bytes that are not an aggregate.
        Garbage,
        /// Fails every `aggregate` of at least one call.
        Fails,
    }

    struct State {
        multicall: Behaviour,
        /// Each request: `aggregate:<calls>`, or a query's method.
        requests: Vec<String>,
        /// Each call of every aggregate: its method and argument.
        calls: Vec<(String, Vec<u8>)>,
    }

    /// The views of `VIEWS`. `echo` returns its argument, `bytes` as many
    /// bytes as its `u32` argument says, `boom` panics, and `heavy` needs
    /// more gas than a share of a batch's.
    fn view(method: &str, args: &[u8], whole_query: bool) -> Result<Vec<u8>, CallError> {
        match method {
            "echo" => Ok(args.to_vec()),
            "bytes" => Ok(vec![
                7;
                u32::from_le_bytes(args[..4].try_into().unwrap())
                    as usize
            ]),
            "boom" => Err(CallError::Panic("boom".into())),
            "heavy" if whole_query => Ok(b"heavy".to_vec()),
            "heavy" => Err(CallError::OutOfGas),
            // Answers a query, but the multicall with more than it holds.
            "shy" if whole_query => Ok(b"shy".to_vec()),
            "shy" => Ok(vec![0; 70_000]),
            _ => Err(CallError::Unknown),
        }
    }

    const FAILED: (u16, &[u8]) = (500, br#"{"error":"Internal server error"}"#);

    fn answer(state: &mut State, path: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let (contract, method) = path
            .strip_prefix("/on/contracts:")
            .and_then(|rest| rest.split_once('/'))
            .unwrap();
        let contract: [u8; 32] = hex::decode(contract).unwrap().try_into().unwrap();
        let failed = (FAILED.0, FAILED.1.to_vec());
        if contract == MULTICALL {
            if method == "version" {
                state.requests.push("version".into());
            }
            match (state.multicall, method) {
                (Behaviour::Missing, _) => return failed,
                (Behaviour::Version(version), "version") => {
                    return (200, rkyv_serialize(&version).unwrap())
                }
                (_, "version") => return (200, rkyv_serialize(&multicall::VERSION).unwrap()),
                _ => {}
            }
            let calls: Vec<Call> = rkyv_deserialize(body).unwrap();
            state.requests.push(format!("aggregate:{}", calls.len()));
            match state.multicall {
                Behaviour::Garbage => return (200, b"not an aggregate".to_vec()),
                Behaviour::Fails if !calls.is_empty() => return failed,
                _ => {}
            }
            if calls.len() > multicall::MAX_CALLS {
                return failed;
            }
            let results: Vec<Result<Vec<u8>, CallError>> = calls
                .iter()
                .map(|call| {
                    state
                        .calls
                        .push((call.fn_name.clone(), call.fn_args.clone()));
                    if call.contract == VIEWS {
                        view(&call.fn_name, &call.fn_args, false)
                    } else {
                        Err(CallError::DoesNotExist)
                    }
                })
                .collect();
            // The contract's own check: "reply exceeds 65536 bytes".
            let out_of_line = results.iter().map(multicall::out_of_line).sum();
            if multicall::reply_len(calls.len(), out_of_line) > multicall::ARGBUF_LEN {
                return failed;
            }
            return (
                200,
                rkyv_serialize(&Aggregate {
                    height: HEIGHT,
                    results,
                })
                .unwrap(),
            );
        }
        state.requests.push(method.to_owned());
        match (contract == VIEWS).then(|| view(method, body, true)) {
            Some(Ok(reply)) => (200, reply),
            _ => failed,
        }
    }

    /// A node serving the multicall and `VIEWS`, until it is dropped.
    struct Node {
        url: Url,
        state: Arc<StdMutex<State>>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl Node {
        fn start(multicall: Behaviour) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
            let state = Arc::new(StdMutex::new(State {
                multicall,
                requests: Vec::new(),
                calls: Vec::new(),
            }));
            let stop = Arc::new(AtomicBool::new(false));
            let worker = {
                let (state, stop) = (state.clone(), stop.clone());
                thread::spawn(move || {
                    for stream in listener.incoming() {
                        if stop.load(Ordering::SeqCst) {
                            break;
                        }
                        let Ok(mut stream) = stream else {
                            continue;
                        };
                        let mut reader = BufReader::new(&mut stream);
                        let mut first = String::new();
                        reader.read_line(&mut first).unwrap();
                        let path = first.split_whitespace().nth(1).unwrap().to_owned();
                        let mut length = 0;
                        loop {
                            let mut header = String::new();
                            reader.read_line(&mut header).unwrap();
                            if header == "\r\n" {
                                break;
                            }
                            if let Some(value) =
                                header.to_ascii_lowercase().strip_prefix("content-length:")
                            {
                                length = value.trim().parse().unwrap();
                            }
                        }
                        let mut body = vec![0; length];
                        reader.read_exact(&mut body).unwrap();
                        let (status, reply) = answer(&mut state.lock().unwrap(), &path, &body);
                        let _ = write!(
                            stream,
                            "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                            reply.len()
                        );
                        let _ = stream.write_all(&reply);
                    }
                })
            };
            Self {
                url,
                state,
                stop,
                worker: Some(worker),
            }
        }

        fn client(&self) -> RuesClient {
            RuesClient::new(self.url.clone())
                .unwrap()
                .with_multicall(Some(MULTICALL))
        }

        /// The requests since the last call.
        fn requests(&self) -> Vec<String> {
            std::mem::take(&mut self.state.lock().unwrap().requests)
        }

        fn set(&self, multicall: Behaviour) {
            self.state.lock().unwrap().multicall = multicall;
        }
    }

    impl Drop for Node {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.url.socket_addrs(|| None).unwrap()[0]);
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    fn echo(value: u32) -> Read {
        Read::new(VIEWS, "echo", &(value,), U32_REPLY).unwrap()
    }

    fn reply(value: u32) -> Vec<u8> {
        value.to_le_bytes().to_vec()
    }

    fn strings(requests: &[&str]) -> Vec<String> {
        requests.iter().map(|request| request.to_string()).collect()
    }

    /// Let the multicall be probed again at once.
    async fn reprobe_now(client: &RuesClient) {
        let multicall = client.multicall.as_ref().unwrap();
        let mut availability = multicall.availability.lock().await;
        assert!(matches!(*availability, Availability::Unavailable { .. }));
        *availability = Availability::Unavailable {
            retry_at: Instant::now(),
        };
    }

    #[tokio::test]
    async fn reads_go_in_aggregates_of_at_most_256_calls() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        let reads: Vec<Read> = (0..600).map(echo).collect();
        let replies = client.read_all(&reads).await.unwrap();
        assert_eq!(replies, (0..600).map(reply).collect::<Vec<_>>());
        // The probe, then the reads in three aggregates: one request each.
        assert_eq!(
            node.requests(),
            strings(&[
                "version",
                "aggregate:0",
                "aggregate:256",
                "aggregate:256",
                "aggregate:88"
            ])
        );
        // A single read needs no aggregate.
        assert_eq!(client.read_all(&[echo(7)]).await.unwrap(), [reply(7)]);
        assert_eq!(node.requests(), strings(&["echo"]));
    }

    #[tokio::test]
    async fn without_a_multicall_each_read_is_one_query_as_before() {
        let node = Node::start(Behaviour::Works);
        let client = RuesClient::new(node.url.clone()).unwrap();
        assert_eq!(client.batch_capacity().await.unwrap(), 1);
        let replies = client.read_all(&[echo(1), echo(2), echo(3)]).await.unwrap();
        assert_eq!(replies, [reply(1), reply(2), reply(3)]);
        assert_eq!(node.requests(), strings(&["echo", "echo", "echo"]));
        // All of them fail at the first failure, and send nothing after it.
        let boom = Read::new(VIEWS, "boom", &(), U8_REPLY).unwrap();
        assert!(client
            .read_all(&[echo(1), boom.clone(), echo(3)])
            .await
            .is_err());
        assert_eq!(node.requests(), strings(&["echo", "boom"]));
        // Each of them reports each.
        let results = client.read_each(&[echo(1), boom, echo(3)]).await.unwrap();
        assert!(results[0].is_ok() && results[1].is_err() && results[2].is_ok());
        assert_eq!(node.requests(), strings(&["echo", "boom", "echo"]));
        assert_eq!(client.coalesced_read(echo(4)).await.unwrap(), reply(4));
        assert_eq!(client.tick_read(10, echo(5)).await.unwrap(), reply(5));
        assert_eq!(
            client
                .read_snapshot(&[echo(6), echo(7)])
                .await
                .unwrap()
                .map(|_| ()),
            None
        );
        assert_eq!(node.requests(), strings(&["echo", "echo"]));
    }

    #[tokio::test]
    async fn a_multicall_that_fails_its_probe_is_set_aside_and_probed_again_later() {
        for refusal in [
            Behaviour::Missing,
            Behaviour::Version(2),
            Behaviour::Garbage,
        ] {
            let node = Node::start(refusal);
            let client = node.client();
            assert_eq!(
                client.read_all(&[echo(1), echo(2)]).await.unwrap(),
                [reply(1), reply(2)]
            );
            let probe = match refusal {
                Behaviour::Garbage => vec!["version", "aggregate:0"],
                _ => vec!["version"],
            };
            assert_eq!(
                node.requests(),
                strings(&[probe.as_slice(), &["echo", "echo"]].concat())
            );
            // Until the next probe is due, reads go one at a time, unprobed.
            assert_eq!(client.batch_capacity().await.unwrap(), 1);
            client.read_all(&[echo(1), echo(2)]).await.unwrap();
            assert_eq!(node.requests(), strings(&["echo", "echo"]));
            // Once due, a multicall that answers is used again.
            node.set(Behaviour::Works);
            reprobe_now(&client).await;
            client.read_all(&[echo(1), echo(2)]).await.unwrap();
            assert_eq!(
                node.requests(),
                strings(&["version", "aggregate:0", "aggregate:2"])
            );
        }
    }

    #[tokio::test]
    async fn a_probe_without_an_answer_fails_the_read_and_is_tried_again() {
        let node = Node::start(Behaviour::Works);
        let url = node.url.clone();
        drop(node);
        let client = RuesClient::new(url)
            .unwrap()
            .with_multicall(Some(MULTICALL));
        let error = client.read_all(&[echo(1), echo(2)]).await.unwrap_err();
        assert!(unanswered(&error), "{error}");
        let multicall = client.multicall.as_ref().unwrap();
        assert!(matches!(
            *multicall.availability.lock().await,
            Availability::Unprobed
        ));
    }

    #[tokio::test]
    async fn an_aggregate_whose_replies_pass_the_buffer_is_split_down_to_the_long_reply() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        // A read that declares a one-byte reply and returns 65,500 bytes,
        // which an aggregate holds only on its own.
        let long = Read::new(VIEWS, "bytes", &(65_500u32,), U8_REPLY).unwrap();
        let mut reads: Vec<Read> = (0..9).map(echo).collect();
        reads.insert(4, long);
        let replies = client.read_all(&reads).await.unwrap();
        assert_eq!(replies[4].len(), 65_500);
        for (index, value) in [0usize, 1, 2, 3, 5, 6, 7, 8, 9].into_iter().zip(0..) {
            assert_eq!(replies[index], reply(value));
        }
        // Halves, and the half with the long reply in halves again.
        assert_eq!(
            node.requests(),
            strings(&[
                "version",
                "aggregate:0",
                "aggregate:10",
                "aggregate:5",
                "aggregate:5",
                "aggregate:2",
                "aggregate:3",
                "aggregate:1",
                "aggregate:2",
                "aggregate:1",
                "aggregate:1",
            ])
        );
        // 65,520 bytes fit a single query's buffer but no aggregate's: the
        // read alone goes as a single query.
        let longer = Read::new(VIEWS, "bytes", &(65_520u32,), U8_REPLY).unwrap();
        let replies = client
            .read_all(&[echo(0), longer, echo(2), echo(3)])
            .await
            .unwrap();
        assert_eq!(replies[1].len(), 65_520);
        assert_eq!(
            node.requests(),
            strings(&[
                "aggregate:4",
                "aggregate:2",
                "aggregate:2",
                "aggregate:1",
                "aggregate:1",
                "bytes",
            ])
        );
        // The callee answered past its declared size: the multicall is not
        // to blame.
        assert_eq!(client.batch_capacity().await.unwrap(), multicall::MAX_CALLS);
    }

    #[tokio::test]
    async fn calls_that_fail_give_the_errors_their_queries_give() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        let reads = [
            echo(1),
            Read::new(VIEWS, "boom", &(), U8_REPLY).unwrap(),
            Read::new(VIEWS, "heavy", &(), U8_REPLY).unwrap(),
            Read::new(ABSENT, "echo", &(), U8_REPLY).unwrap(),
            Read::new(VIEWS, "missing", &(), U8_REPLY).unwrap(),
        ];
        let results = client.read_each(&reads).await.unwrap();
        assert_eq!(results[0].as_ref().unwrap(), &reply(1));
        for (index, reason) in [
            (1, "the contract panicked: boom"),
            (3, "no contract has this ID"),
            (4, "a missing function or a trap"),
        ] {
            match &results[index] {
                Err(HyperlaneDuskError::RuesResponse { status: 500, body }) => {
                    assert!(body.contains(reason), "{body}")
                }
                other => unreachable!("{other:?}"),
            }
        }
        // A call short of gas in a batch is read again on its own, with the
        // query's whole gas bound.
        assert_eq!(results[2].as_ref().unwrap(), b"heavy");
        assert_eq!(
            node.requests(),
            strings(&["version", "aggregate:0", "aggregate:5", "heavy"])
        );
        // All of them fail with the first failure.
        let error = client.read_all(&reads).await.unwrap_err();
        assert!(error.to_string().contains("panicked: boom"), "{error}");
    }

    #[tokio::test]
    async fn a_multicall_that_answers_wrongly_is_set_aside() {
        // A reply that is not an aggregate.
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        client.read_all(&[echo(1), echo(2)]).await.unwrap();
        node.requests();
        node.set(Behaviour::Garbage);
        assert_eq!(
            client.read_all(&[echo(1), echo(2)]).await.unwrap(),
            [reply(1), reply(2)]
        );
        assert_eq!(node.requests(), strings(&["aggregate:2", "echo", "echo"]));
        client.read_all(&[echo(1), echo(2)]).await.unwrap();
        assert_eq!(node.requests(), strings(&["echo", "echo"]));

        // Aggregates that fail while each of their reads answers.
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        client.read_all(&[echo(1), echo(2)]).await.unwrap();
        node.requests();
        node.set(Behaviour::Fails);
        let reads: Vec<Read> = (0..4).map(echo).collect();
        assert_eq!(
            client.read_all(&reads).await.unwrap(),
            (0..4).map(reply).collect::<Vec<_>>()
        );
        assert_eq!(
            node.requests(),
            strings(&[
                "aggregate:4",
                "aggregate:2",
                "aggregate:2",
                "echo",
                "echo",
                "echo",
                "echo"
            ])
        );
        client.read_all(&reads).await.unwrap();
        assert_eq!(node.requests(), strings(&["echo"; 4]));
        reprobe_now(&client).await;
    }

    #[tokio::test]
    async fn only_the_deployments_own_reads_tell_against_the_multicall() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        let shy = Read::new(VIEWS, "shy", &(), U64_REPLY).unwrap();
        // A contract a recipient chose that fails every aggregate it is in:
        // read on its own, and the multicall kept.
        let replies = client
            .read_all(&[echo(1), shy.clone().untrusted()])
            .await
            .unwrap();
        assert_eq!(replies, [reply(1), b"shy".to_vec()]);
        assert_eq!(
            node.requests(),
            strings(&[
                "version",
                "aggregate:0",
                "aggregate:2",
                "aggregate:1",
                "aggregate:1",
                "shy"
            ])
        );
        assert_eq!(client.batch_capacity().await.unwrap(), multicall::MAX_CALLS);
        // A contract of the deployment that does so shows the multicall
        // answering wrongly.
        client.read_all(&[echo(1), shy]).await.unwrap();
        assert_eq!(client.batch_capacity().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn reads_of_contracts_outside_the_deployment_go_after_the_others() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        let reads = [echo(1).untrusted(), echo(2), echo(3).untrusted(), echo(4)];
        let replies = client.read_all(&reads).await.unwrap();
        assert_eq!(replies, [reply(1), reply(2), reply(3), reply(4)]);
        let order: Vec<Vec<u8>> = node
            .state
            .lock()
            .unwrap()
            .calls
            .iter()
            .map(|(_, args)| args.clone())
            .collect();
        assert_eq!(order, [reply(2), reply(4), reply(1), reply(3)]);
    }

    #[tokio::test]
    async fn reads_asked_together_go_in_one_aggregate_each_read_once() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        assert_eq!(client.batch_capacity().await.unwrap(), multicall::MAX_CALLS);
        node.requests();
        let mut tasks = tokio::task::JoinSet::new();
        for task in 0..20u32 {
            let client = client.clone();
            tasks.spawn(async move {
                let value = task % 10;
                (value, client.coalesced_read(echo(value)).await.unwrap())
            });
        }
        while let Some(joined) = tasks.join_next().await {
            let (value, read) = joined.unwrap();
            assert_eq!(read, reply(value));
        }
        assert_eq!(node.requests(), strings(&["aggregate:10"]));
    }

    #[tokio::test]
    async fn reads_of_a_final_block_carry_the_reads_asked_at_the_blocks_before() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        client.batch_capacity().await.unwrap();
        node.requests();
        let tick = |block: u64, value: u32| {
            let client = client.clone();
            async move { client.tick_read(block, echo(value)).await.unwrap() }
        };
        // Block 10: each read on its own, the second one answered again
        // from memory.
        assert_eq!(tick(10, 1).await, reply(1));
        assert_eq!(tick(10, 2).await, reply(2));
        assert_eq!(tick(10, 2).await, reply(2));
        assert_eq!(node.requests(), strings(&["echo", "echo"]));
        // Block 11: the first read carries the other.
        assert_eq!(tick(11, 2).await, reply(2));
        assert_eq!(tick(11, 1).await, reply(1));
        assert_eq!(node.requests(), strings(&["aggregate:2"]));
        // A read no longer asked rides along for two more blocks.
        for block in [12, 13] {
            assert_eq!(tick(block, 1).await, reply(1));
            assert_eq!(node.requests(), strings(&["aggregate:2"]));
        }
        assert_eq!(tick(14, 1).await, reply(1));
        assert_eq!(node.requests(), strings(&["echo"]));
        // A block older than the one read: one query.
        assert_eq!(tick(13, 2).await, reply(2));
        assert_eq!(node.requests(), strings(&["echo"]));
    }

    /// What a client keeps of the reads of its last final block: how many
    /// reads it watches, how many replies it keeps, whether every reply is
    /// of a watched read, and the bytes of the replies and their arguments.
    async fn kept_tick_reads(client: &RuesClient) -> (usize, usize, bool, usize) {
        let multicall = client.multicall.as_ref().unwrap();
        let tick = multicall.tick.lock().await;
        let bytes = tick
            .replies
            .iter()
            .map(|((_, _, args), reply)| args.len() + reply.len())
            .sum();
        (
            tick.watched.len(),
            tick.replies.len(),
            tick.replies
                .keys()
                .all(|key| tick.watched.contains_key(key)),
            bytes,
        )
    }

    #[tokio::test]
    async fn a_finality_stall_keeps_a_bounded_number_of_replies() {
        let node = Node::start(Behaviour::Works);
        let client = node.client().with_query_rate_limit(QueryRateLimit {
            per_second: 1_000_000,
            burst: 1_000_000,
        });
        client.batch_capacity().await.unwrap();
        // The node finalizes nothing while delivery checks of ever new
        // messages are asked at its last final block.
        let asked = MAX_WATCHED_READS as u32 * 3;
        for value in 0..asked {
            assert_eq!(
                client.tick_read(10, echo(value)).await.unwrap(),
                reply(value)
            );
        }
        let (watched, replies, all_watched, _) = kept_tick_reads(&client).await;
        assert!(watched <= MAX_WATCHED_READS, "{watched} reads watched");
        assert!(replies <= MAX_WATCHED_READS, "{replies} replies kept");
        assert!(all_watched, "a reply outlived its watched read");
        // The latest read is kept: asked again at the block, it is answered
        // from memory.
        node.requests();
        assert_eq!(
            client.tick_read(10, echo(asked - 1)).await.unwrap(),
            reply(asked - 1)
        );
        assert!(node.requests().is_empty());

        // Long replies are kept within a bound of bytes.
        let node = Node::start(Behaviour::Works);
        let client = node.client().with_query_rate_limit(QueryRateLimit {
            per_second: 1_000_000,
            burst: 1_000_000,
        });
        client.batch_capacity().await.unwrap();
        for len in 60_000u32..60_100 {
            let read = Read::new(VIEWS, "bytes", &(len,), U8_REPLY).unwrap();
            assert_eq!(
                client.tick_read(10, read).await.unwrap().len(),
                len as usize
            );
        }
        let (_, _, all_watched, bytes) = kept_tick_reads(&client).await;
        assert!(all_watched);
        assert!(
            bytes <= MAX_TICK_REPLY_BYTES,
            "{bytes} bytes of replies kept"
        );
    }

    #[tokio::test]
    async fn a_snapshot_is_one_aggregate_with_the_height_of_its_state() {
        let node = Node::start(Behaviour::Works);
        let client = node.client();
        let snapshot = client
            .read_snapshot(&[echo(1), Read::new(VIEWS, "boom", &(), U8_REPLY).unwrap()])
            .await
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.height, HEIGHT);
        assert_eq!(snapshot.results[0].as_ref().unwrap(), &reply(1));
        assert!(snapshot.results[1].is_err());
        assert_eq!(
            node.requests(),
            strings(&["version", "aggregate:0", "aggregate:2"])
        );
        // More than one aggregate holds: none.
        let many: Vec<Read> = (0..300).map(echo).collect();
        assert!(client.read_snapshot(&many).await.unwrap().is_none());
        assert!(fits_one_aggregate(&many[..256]) && !fits_one_aggregate(&many));
        // An aggregate that fails: none, for the caller to read one at a
        // time.
        node.set(Behaviour::Fails);
        assert!(client
            .read_snapshot(&[echo(1), echo(2)])
            .await
            .unwrap()
            .is_none());
        assert_eq!(node.requests(), strings(&["aggregate:2"]));
    }
}
