use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock, PoisonError};
use std::time::Duration;

use base64::engine::{general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::StatusCode;
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{check_archived_root, Archive, Deserialize, Infallible, Serialize};
use rocksdb::{Direction, IteratorMode, Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use serde_json::Value as JsonValue;
use tracing::{debug, error, warn};
use url::Url;

use hyperlane_core::{ReorgPeriod, H256, H512};

use crate::HyperlaneDuskError;

mod event_log;
use event_log::EventLogLocks;
pub(crate) use event_log::{EventLog, LogEntry, LogLookup, BLOCK_LIMITS};

const MAX_RUES_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Rows per `finalizedEvents` page: the most Rusk 1.7.3 returns
/// (`MAX_LIMIT` in its GraphQL archive module).
const FINALIZED_EVENT_PAGE_SIZE: usize = 200;
const MAX_EVENT_TOPIC_BYTES: usize = 256;
/// Archive rows of one contract between two persisted scan anchors.
const SCAN_ANCHOR_SPACING: u64 = FINALIZED_EVENT_PAGE_SIZE as u64;
/// Most bytes of a response body that go into an error or a log line.
const MAX_ERROR_BODY_BYTES: usize = 1024;
/// The Mailbox state version this agent reads: dispatched message IDs
/// (`dispatched_id`), and deliveries from `ProcessId` events only.
const MAILBOX_STATE_VERSION: u32 = 5;
/// The MerkleTreeHook state version this agent reads: stored message IDs,
/// from which checkpoints compute their roots (no `root_at`).
const MERKLE_TREE_HOOK_STATE_VERSION: u32 = 2;
/// The ValidatorAnnounce state version this agent reads: location hashes,
/// with the locations in `ValidatorAnnouncement` events.
const VALIDATOR_ANNOUNCE_STATE_VERSION: u32 = 2;
const TX_POLL_INTERVAL: Duration = Duration::from_secs(2);
/// Most non-final blocks a finality check reads. Rusk finalizes a block a
/// few blocks after accepting it; a longer non-final chain means finality
/// has stalled, and checks wait for it to catch up instead.
const MAX_NON_FINAL_BLOCKS: u64 = 64;
/// Blocks whose archived events of one contract and topic a client keeps for
/// finality checks.
const NON_FINAL_EVENT_CACHE_BLOCKS: usize = 256;

type SharedRuesClients = HashMap<(String, PathBuf), RuesClient>;
static SHARED_RUES_CLIENTS: OnceLock<StdMutex<SharedRuesClients>> = OnceLock::new();

/// How often the same critical condition, such as a missing block, is
/// reported as a critical error. The lookup fails, and contract sync retries
/// it, far more often.
const ARCHIVE_GAP_REPORT_INTERVAL: Duration = Duration::from_secs(600);
/// Most critical conditions whose last report is remembered.
const ARCHIVE_GAP_REPORT_KEYS: usize = 1024;

type ArchiveGapKey = ([u8; 32], String, u64);
static ARCHIVE_GAP_REPORTS: OnceLock<StdMutex<HashMap<ArchiveGapKey, std::time::Instant>>> =
    OnceLock::new();

/// Whether a critical condition of a contract and topic at a block, such as
/// a missing block or a halted event log, is due to be reported again,
/// recording the report if so.
fn critical_report_due(contract_id: &[u8; 32], topic: &str, block_height: u64) -> bool {
    let reports = ARCHIVE_GAP_REPORTS.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut reports = reports.lock().unwrap_or_else(PoisonError::into_inner);
    let now = std::time::Instant::now();
    let key = (*contract_id, topic.to_owned(), block_height);
    if reports
        .get(&key)
        .is_some_and(|last| now.duration_since(*last) < ARCHIVE_GAP_REPORT_INTERVAL)
    {
        return false;
    }
    if reports.len() >= ARCHIVE_GAP_REPORT_KEYS {
        reports.retain(|_, last| now.duration_since(*last) < ARCHIVE_GAP_REPORT_INTERVAL);
        if reports.len() >= ARCHIVE_GAP_REPORT_KEYS {
            reports.clear();
        }
    }
    reports.insert(key, now);
    true
}

/// The Hyperlane deployment an agent is configured for on a Dusk chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeploymentIdentity {
    /// Dusk chain ID the endpoint must report.
    pub chain_id: u8,
    /// Hyperlane domain of the Mailbox and ValidatorAnnounce.
    pub domain: u32,
    /// Mailbox contract ID.
    pub mailbox: [u8; 32],
    /// ValidatorAnnounce contract ID, bound to the Mailbox.
    pub validator_announce: [u8; 32],
    /// MerkleTreeHook contract ID, the Mailbox's required hook or in it.
    pub merkle_tree_hook: [u8; 32],
}

/// Whether an endpoint has been shown to serve a deployment, shared by the
/// clients that require it.
#[derive(Debug)]
struct IdentityGate {
    identity: DeploymentIdentity,
    verified: AtomicBool,
    /// Attempts that have finished, and the last one's failure. Callers that
    /// waited while an attempt ran share its result instead of each making
    /// one, so an endpoint that does not answer is asked once at a time.
    finished: AtomicU64,
    attempt: tokio::sync::Mutex<Option<String>>,
}

type IdentityGates = HashMap<DeploymentIdentity, Arc<IdentityGate>>;

/// RUES (Rusk Unified Event System) HTTP client.
///
/// Communicates with Dusk nodes using the RUES protocol:
/// - Contract queries: `POST /on/contracts:{hex_id}/{method}`
/// - Transaction propagation: `POST /on/transactions/propagate`
#[derive(Clone)]
pub struct RuesClient {
    client: reqwest::Client,
    base_url: Url,
    event_store: Option<Arc<DB>>,
    /// Serializes read-modify-write updates of the persisted scan anchors.
    scan_anchors: Arc<StdMutex<()>>,
    /// Finalized blocks' archive rows, shared by every clone of the client.
    block_cache: Arc<StdMutex<BlockCache>>,
    /// Archived events of blocks that finality checks read.
    non_final_cache: Arc<StdMutex<NonFinalEventCache>>,
    /// Serializes the advances of each finalized event log.
    event_logs: Arc<StdMutex<EventLogLocks>>,
    /// The deployment every request first checks the endpoint serves, if
    /// any (`with_identity`).
    identity: Option<Arc<IdentityGate>>,
    /// The gates of the deployments this client's clones require, so they
    /// verify each deployment once.
    identity_gates: Arc<StdMutex<IdentityGates>>,
}

impl fmt::Debug for RuesClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuesClient")
            .field("base_url", &"<redacted>")
            .field("event_store_configured", &self.event_store.is_some())
            .finish_non_exhaustive()
    }
}

/// Account status returned by Rusk for a Moonlight account.
#[derive(Debug, Clone, SerdeDeserialize)]
pub struct AccountStatus {
    /// Account balance in LUX.
    pub balance: u64,
    /// Current confirmed nonce.
    pub nonce: u64,
    /// Next available nonce accounting for mempool in-flight txs.
    pub next_nonce: u64,
}

/// Contract status returned by Rusk for a contract's balance.
#[derive(Debug, Clone, SerdeDeserialize)]
pub struct ContractStatus {
    /// Contract balance in LUX.
    pub balance: u64,
}

/// VM metadata returned for a contract.
#[derive(Debug, Clone, SerdeDeserialize)]
pub struct ContractMetadata {
    /// Hex-encoded owner bytes. Empty when the contract does not exist.
    pub contract_owner: String,
}

/// Gas price stats returned by Rusk.
#[derive(Debug, Clone, SerdeDeserialize)]
pub struct GasPriceStats {
    pub average: u64,
    pub max: u64,
    pub median: u64,
    pub min: u64,
}

/// A transaction that has been included in the Dusk ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfirmedTransaction {
    /// Gas actually consumed by execution.
    pub gas_spent: u64,
    /// Contract execution error, if execution reverted.
    pub error: Option<String>,
}

/// Contract-scoped finalized event returned by Rusk's archive GraphQL API.
#[derive(Debug, Clone, PartialEq, Eq, SerdeDeserialize, SerdeSerialize)]
struct FinalizedContractEvent {
    id: i64,
    block_height: u64,
    block_hash: String,
    pub origin: String,
    pub topic: String,
    pub source: String,
    pub data: String,
    pub reverted: bool,
}

#[derive(Debug, Clone, SerdeDeserialize)]
#[serde(rename_all = "camelCase")]
struct FinalizedEventPage {
    events: Vec<FinalizedContractEvent>,
    start_cursor: Option<String>,
    end_cursor: Option<String>,
    has_next_page: bool,
}

/// The answer to one `finalizedEvents` request.
enum PageReply {
    /// The page, and the size of its reply in bytes.
    Page(FinalizedEventPage, usize),
    /// The reply was too large to read.
    TooLarge {
        /// Why, for an error once one row is too many.
        reason: String,
        /// Whether the agent's own byte limit refused it, rather than the
        /// endpoint or a proxy in front of it.
        by_agent: bool,
    },
}

/// How many rows a scan asks each `finalizedEvents` request for.
///
/// Rusk returns up to 200 rows a page and sends the page whole, but the agent
/// reads at most `MAX_RUES_RESPONSE_BYTES` of a reply. An event carries up to
/// 64 KiB of data, 128 KiB as hex, so 200 rows of large dispatches do not
/// fit. A reply over the limit is asked for again at the same cursor with
/// half as many rows, down to one.
///
/// The smaller size holds for the rest of the scan, since rows next to large
/// ones are often large too, with one exception: the agent knows its own
/// limit, so after a full page that used at most a quarter of it the size
/// doubles again, up to 200, and one block of large events does not slow
/// down the rest of a long scan. An endpoint that refuses pages itself does
/// not say how much it accepts, so after such a refusal the size only
/// shrinks. Each scan starts at 200; finding the size again costs a few
/// refused replies, which the agent refuses from their declared length
/// without reading them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageSize {
    rows: usize,
    may_grow: bool,
}

impl PageSize {
    const fn largest() -> Self {
        Self {
            rows: FINALIZED_EVENT_PAGE_SIZE,
            may_grow: true,
        }
    }

    fn rows(self) -> usize {
        self.rows
    }

    /// Halve after a reply over the limit. `false` when a single row was
    /// already too large.
    fn shrink(&mut self, by_agent: bool) -> bool {
        self.may_grow &= by_agent;
        if self.rows <= 1 {
            return false;
        }
        self.rows /= 2;
        true
    }

    /// Double after a full page of `rows` rows whose reply took `bytes`
    /// bytes, if twice as many rows of that size use at most half the limit.
    fn after_page(&mut self, rows: usize, bytes: usize) {
        if self.may_grow && rows == self.rows && bytes <= MAX_RUES_RESPONSE_BYTES / 4 {
            self.rows = (self.rows * 2).min(FINALIZED_EVENT_PAGE_SIZE);
        }
    }
}

/// A persisted position in one contract's archive rows, just after the row
/// `fence_id`. Every row up to and including that row has a block height
/// below `before_block`, so a scan for any block at or above it may start
/// here instead of at the contract's first row.
///
/// Anchors only decide where a scan starts, never whether a row is accepted.
/// Archive IDs belong to the endpoint's archive: a rebuilt or replaced archive
/// may number its rows differently. A scan that resumes from an anchor must
/// therefore see the fence row first, unchanged and below `before_block`.
///
/// Rusk numbers rows in insertion order and never deletes one, so an archive
/// that only lags behind the one an anchor came from returns the fence row
/// first whenever it returns any row from the fence's ID on. When the archive
/// returns another row first, it numbers its rows differently: the
/// contract's anchors are dropped and the scan starts over. When it returns
/// no row from the fence's ID on, it may only be behind, and the anchors are
/// kept, with the lookup failing until it catches up, only on positive
/// evidence of that: the archive has not finalized the fence row's block
/// (Rusk does so in the same transaction that inserts the block's rows), the
/// node's ledger holds that block under the anchor's hash, and the node has
/// not finalized the block yet or the archive has not finalized the node's
/// last final block either. Otherwise the archive will never return the
/// fence row, and the scan starts over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ScanAnchor {
    before_block: u64,
    /// Archive ID of the last row before the position.
    fence_id: i64,
    /// keccak256 of that row as the archive returned it.
    fence_digest: [u8; 32],
    /// How many of the contract's rows precede the position.
    rank: u64,
    /// Height of the fence row's block, below `before_block`.
    fence_block: u64,
    /// Hash of the fence row's block.
    fence_block_hash: [u8; 32],
}

impl ScanAnchor {
    const ENCODED_LEN: usize = 8 + 8 + 32 + 8 + 8 + 32;

    /// An anchor just after `row`, the `rank`th row of the contract. `None`
    /// when the row's block hash does not parse, since the anchor could not
    /// be checked against the archive's finalized blocks.
    fn after_row(
        row: &FinalizedContractEvent,
        before_block: u64,
        rank: u64,
    ) -> Result<Option<Self>, HyperlaneDuskError> {
        let Ok(fence_block_hash) = parse_h256_hex(&row.block_hash, "archive row block hash") else {
            return Ok(None);
        };
        Ok(Some(Self {
            before_block,
            fence_id: row.id,
            fence_digest: archive_row_digest(row)?,
            rank,
            fence_block: row.block_height,
            fence_block_hash: fence_block_hash.0,
        }))
    }

    /// Whether `row`, which the archive returned at `fence_id`, is the row
    /// this anchor was taken after.
    fn is_fence(&self, row: &FinalizedContractEvent) -> Result<bool, HyperlaneDuskError> {
        Ok(row.id == self.fence_id
            && archive_row_digest(row)? == self.fence_digest
            && row.block_height == self.fence_block
            && row.block_height < self.before_block
            && parse_h256_hex(&row.block_hash, "archive row block hash")
                .is_ok_and(|hash| hash.0 == self.fence_block_hash))
    }

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::ENCODED_LEN);
        bytes.extend_from_slice(&self.before_block.to_be_bytes());
        bytes.extend_from_slice(&self.fence_id.to_be_bytes());
        bytes.extend_from_slice(&self.fence_digest);
        bytes.extend_from_slice(&self.rank.to_be_bytes());
        bytes.extend_from_slice(&self.fence_block.to_be_bytes());
        bytes.extend_from_slice(&self.fence_block_hash);
        bytes
    }

    /// Anchors that do not decode, including ones whose fence row is not
    /// below `before_block`, are dropped as unreadable.
    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let (before_block, rest) = bytes.split_at(8);
        let (fence_id, rest) = rest.split_at(8);
        let (fence_digest, rest) = rest.split_at(32);
        let (rank, rest) = rest.split_at(8);
        let (fence_block, fence_block_hash) = rest.split_at(8);
        let anchor = Self {
            before_block: u64::from_be_bytes(before_block.try_into().ok()?),
            fence_id: i64::from_be_bytes(fence_id.try_into().ok()?),
            fence_digest: fence_digest.try_into().ok()?,
            rank: u64::from_be_bytes(rank.try_into().ok()?),
            fence_block: u64::from_be_bytes(fence_block.try_into().ok()?),
            fence_block_hash: fence_block_hash.try_into().ok()?,
        };
        (anchor.fence_id >= 0 && anchor.rank > 0 && anchor.fence_block < anchor.before_block)
            .then_some(anchor)
    }
}

/// What one pass over a contract's rows found for one block.
struct BlockScan {
    /// The rows matched to wanted sequences.
    found: ScannedRows,
    /// The block's rows, when the pass read all of them within the cache
    /// bound and each parsed.
    block: Option<CachedBlock>,
    /// The first row of a later block the pass read, if any: the archive
    /// has finalized a block above the scanned one.
    later: Option<FinalizedContractEvent>,
}

/// How a scan from an anchor, or from the contract's first row, ended.
enum ScanOutcome {
    /// The scan ran.
    Done(BlockScan),
    /// The archive's first row from the anchor's fence ID on is not the
    /// fence row, or the archive has finalized the fence row's block without
    /// that row, so the anchor does not belong to this archive.
    WrongAnchor,
    /// The archive has not reached the anchor's fence row yet.
    ArchiveBehind(ScanAnchor),
}

/// A row of the scanned block that can be a sequence's record: not
/// reverted, of the scan's topic, with its provenance and event data parsed
/// here once. Any other row never matches.
#[derive(Debug, Clone, PartialEq, Eq)]
struct BlockRow {
    block_hash: H256,
    /// Hash of the transaction that emitted the row.
    origin: [u8; 32],
    data: Vec<u8>,
}

impl BlockRow {
    /// The row's provenance and data, or why they do not parse. A row
    /// whose block hash or origin is not 32 bytes of hex, or whose data is
    /// not hex, cannot be any sequence's record.
    fn parse(event: &FinalizedContractEvent) -> Result<Self, HyperlaneDuskError> {
        Ok(Self {
            block_hash: parse_h256_hex(&event.block_hash, "archive row block hash")?,
            origin: parse_h256_hex(&event.origin, "archive row origin")?.0,
            data: hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Archive row has invalid data: {}",
                    bounded_error(error)
                ))
            })?,
        })
    }

    /// Memory the row takes, for the cache bound.
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.len()
    }

    /// The row as the record of `sequence`, which carries the sequence as
    /// its ID.
    fn record(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        sequence: usize,
    ) -> Result<FinalizedContractEvent, HyperlaneDuskError> {
        Ok(FinalizedContractEvent {
            id: i64::try_from(sequence).map_err(|_| {
                HyperlaneDuskError::Other(
                    "Dusk finalized-event sequence exceeds i64 provenance range".into(),
                )
            })?,
            block_height,
            block_hash: hex::encode(self.block_hash),
            origin: hex::encode(self.origin),
            topic: topic.to_owned(),
            source: hex::encode(contract_id),
            data: hex::encode(&self.data),
            reverted: false,
        })
    }
}

/// Give each wanted sequence that has no row yet the first row, in archive
/// order, whose data `matching_sequences` assigns to it. A scan and the
/// block cache match rows only through this.
fn assign_row<F>(
    row: &BlockRow,
    contract_id: &[u8; 32],
    topic: &str,
    block_height: u64,
    wanted: &HashSet<usize>,
    matching_sequences: &F,
    found: &mut ScannedRows,
) -> Result<(), HyperlaneDuskError>
where
    F: Fn(&[u8]) -> Vec<usize>,
{
    for sequence in matching_sequences(&row.data) {
        if wanted.contains(&sequence) && !found.contains_key(&sequence) {
            found.insert(
                sequence,
                row.record(contract_id, topic, block_height, sequence)?,
            );
        }
    }
    Ok(())
}

/// Every row of one finalized block, contract and topic that can be a
/// record, in archive order, all with the block hash that
/// `checkBlock(onlyFinalized)` accepted when the block was cached.
#[derive(Debug)]
struct CachedBlock {
    block_hash: H256,
    rows: Vec<BlockRow>,
    bytes: usize,
}

/// The scanned block's rows that a scan collects for the block cache.
struct BlockCapture {
    /// `None` once the rows grew past the bound, or once a row of the block
    /// that could be a record did not parse.
    rows: Option<Vec<BlockRow>>,
    bytes: usize,
    max_bytes: usize,
    /// Whether a row of the block that could be a record did not parse.
    malformed: bool,
    /// Whether the scan read any row of the block, of any topic.
    seen: bool,
    /// Whether the scan read every row of the block. Rusk archives a
    /// finalized block's rows in one transaction, and blocks in height
    /// order, so the block's rows end at a row of a later block or at the
    /// end of an archive that holds one of them.
    complete: bool,
}

impl BlockCapture {
    fn new(max_bytes: usize) -> Self {
        Self {
            rows: Some(Vec::new()),
            bytes: 0,
            max_bytes,
            malformed: false,
            seen: false,
            complete: false,
        }
    }

    fn collecting(&self) -> bool {
        self.rows.is_some()
    }

    fn push(&mut self, row: BlockRow) {
        let Some(rows) = &mut self.rows else {
            return;
        };
        self.bytes = self.bytes.saturating_add(row.bytes());
        if self.bytes > self.max_bytes {
            self.rows = None;
        } else {
            rows.push(row);
        }
    }

    /// A row of the block that could be a record did not parse. The block's
    /// readable rows are then not all of its records, so it is not cached,
    /// and a lookup by transaction cannot rely on them.
    fn reject_malformed(&mut self) {
        self.malformed = true;
        self.rows = None;
    }

    /// The block's rows, if the scan read all of them within the bound,
    /// every one parsed, and they name one block hash.
    fn finish(self) -> Option<CachedBlock> {
        let rows = self.rows?;
        let block_hash = rows.first()?.block_hash;
        (self.complete && !self.malformed && rows.iter().all(|row| row.block_hash == block_hash))
            .then_some(CachedBlock {
                block_hash,
                rows,
                bytes: self.bytes,
            })
    }
}

/// Bounds of a client's cache of finalized blocks' rows.
#[derive(Debug, Clone, Copy)]
struct BlockCacheLimits {
    /// Most blocks held at once.
    blocks: usize,
    /// Most bytes held at once.
    bytes: usize,
    /// Most bytes of one block; a larger block is scanned per lookup.
    block_bytes: usize,
}

/// The cache holds the blocks that indexing is working through: a forward
/// and a backward cursor and the transaction lookups of each indexed
/// contract and topic. 32 MiB fits about 500 dispatches of 64 KiB, the
/// largest event Rusk emits, or 100,000 Merkle insertions.
const BLOCK_CACHE_LIMITS: BlockCacheLimits = BlockCacheLimits {
    blocks: 64,
    bytes: 64 * 1024 * 1024,
    block_bytes: 32 * 1024 * 1024,
};

/// One block of one contract and topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BlockKey {
    contract_id: [u8; 32],
    topic: String,
    block_height: u64,
}

impl BlockKey {
    fn new(contract_id: &[u8; 32], topic: &str, block_height: u64) -> Self {
        Self {
            contract_id: *contract_id,
            topic: topic.to_owned(),
            block_height,
        }
    }
}

/// Finalized blocks' rows that one scan read whole, so that other lookups
/// in the same blocks, such as a cursor that asks for one sequence at a
/// time or a lookup per transaction, match against them instead of reading
/// the archive again. Rows are matched exactly as a scan matches them, and
/// every lookup still checks contract state and `checkBlock` itself.
/// Bounded by blocks and bytes; the least recently used block goes first.
#[derive(Debug)]
struct BlockCache {
    limits: BlockCacheLimits,
    /// Each block, and when it was last used.
    blocks: HashMap<BlockKey, (Arc<CachedBlock>, u64)>,
    bytes: usize,
    clock: u64,
}

impl BlockCache {
    fn new(limits: BlockCacheLimits) -> Self {
        Self {
            limits,
            blocks: HashMap::new(),
            bytes: 0,
            clock: 0,
        }
    }

    fn get(&mut self, key: &BlockKey) -> Option<Arc<CachedBlock>> {
        self.clock += 1;
        let (block, used) = self.blocks.get_mut(key)?;
        *used = self.clock;
        Some(block.clone())
    }

    fn insert(&mut self, key: BlockKey, block: Arc<CachedBlock>) {
        self.remove(&key);
        if block.bytes > self.limits.block_bytes || self.limits.blocks == 0 {
            return;
        }
        while self.blocks.len() >= self.limits.blocks
            || self.bytes.saturating_add(block.bytes) > self.limits.bytes
        {
            let Some(oldest) = self
                .blocks
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| key.clone())
            else {
                break;
            };
            self.remove(&oldest);
        }
        self.clock += 1;
        self.bytes += block.bytes;
        self.blocks.insert(key, (block, self.clock));
    }

    fn remove(&mut self, key: &BlockKey) {
        if let Some((block, _)) = self.blocks.remove(key) {
            self.bytes -= block.bytes;
        }
    }

    fn forget_contract(&mut self, contract_id: &[u8; 32]) {
        let keys: Vec<BlockKey> = self
            .blocks
            .keys()
            .filter(|key| key.contract_id == *contract_id)
            .cloned()
            .collect();
        for key in keys {
            self.remove(&key);
        }
    }
}

/// The rows of one block that a scan matched, keyed by sequence.
type ScannedRows = HashMap<usize, FinalizedContractEvent>;

/// Row-owned provenance for one finalized, state-matched contract event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizedEventProvenance {
    pub block_height: u64,
    pub block_hash: H256,
    pub transaction_id: H512,
    pub event_id: i64,
}

/// One finalized, state-matched contract event: its provenance and the event
/// data that matched.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizedEventMatch {
    pub provenance: FinalizedEventProvenance,
    pub data: Vec<u8>,
}

/// A non-reverted contract event in a block that is not final yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NonFinalEvent {
    pub block_height: u64,
    pub block_hash: H256,
    /// Hash of the transaction that emitted the event, as the archive
    /// returned it.
    pub origin: String,
    pub data: Vec<u8>,
}

/// A block above the last finalized one.
#[derive(Debug, Clone, Copy)]
struct NonFinalBlock {
    height: u64,
    hash: H256,
}

/// One event row of `contractEvents`, as Rusk's archive returns it.
#[derive(Debug, SerdeDeserialize)]
struct ArchivedBlockEvent {
    origin: String,
    topic: String,
    source: String,
    data: String,
    reverted: bool,
}

/// The archived events of one contract and topic in one block.
type NonFinalEventKey = (H256, [u8; 32], String);

/// The archived events of blocks that finality checks read, so that a check
/// reads only the blocks that are new to it. The archive stores a block's
/// events at once and never changes them, so an entry stays right for as
/// long as the block exists. The oldest entry goes first.
#[derive(Debug, Default)]
struct NonFinalEventCache {
    entries: HashMap<NonFinalEventKey, Arc<Vec<NonFinalEvent>>>,
    order: VecDeque<NonFinalEventKey>,
}

impl NonFinalEventCache {
    fn get(&self, key: &NonFinalEventKey) -> Option<Arc<Vec<NonFinalEvent>>> {
        self.entries.get(key).cloned()
    }

    fn insert(&mut self, key: NonFinalEventKey, events: Arc<Vec<NonFinalEvent>>) {
        if self.entries.insert(key.clone(), events).is_none() {
            self.order.push_back(key);
        }
        while self.order.len() > NON_FINAL_EVENT_CACHE_BLOCKS {
            if let Some(oldest) = self.order.pop_front() {
                self.entries.remove(&oldest);
            }
        }
    }
}

impl RuesClient {
    /// Create a new RUES client.
    pub fn new(url: Url) -> Result<Self, HyperlaneDuskError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err(HyperlaneDuskError::Other(
                "Dusk RUES URL must use HTTP or HTTPS".into(),
            ));
        }
        let base_url = url;
        Ok(Self {
            client,
            base_url,
            event_store: None,
            scan_anchors: Arc::new(StdMutex::new(())),
            block_cache: Arc::new(StdMutex::new(BlockCache::new(BLOCK_CACHE_LIMITS))),
            non_final_cache: Arc::new(StdMutex::new(NonFinalEventCache::default())),
            event_logs: Arc::new(StdMutex::new(HashMap::new())),
            identity: None,
            identity_gates: Arc::new(StdMutex::new(HashMap::new())),
        })
    }

    /// This client, but every request it sends first checks, once, that the
    /// endpoint serves `identity` (`validate_chain_identity`): building it
    /// makes no request. Until a check succeeds, every request fails without
    /// being sent and the next one checks again; callers that wait while a
    /// check runs share its result. Clones share the outcome.
    pub fn with_identity(&self, identity: DeploymentIdentity) -> Self {
        let gate = self
            .identity_gates
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(identity)
            .or_insert_with(|| {
                Arc::new(IdentityGate {
                    identity,
                    verified: AtomicBool::new(false),
                    finished: AtomicU64::new(0),
                    attempt: tokio::sync::Mutex::new(None),
                })
            })
            .clone();
        Self {
            identity: Some(gate),
            ..self.clone()
        }
    }

    /// Check, once, that the endpoint serves the deployment this client
    /// requires, if any. Fails, without retrying, until a check succeeds.
    pub async fn ensure_identity(&self) -> Result<(), HyperlaneDuskError> {
        let Some(gate) = &self.identity else {
            return Ok(());
        };
        if gate.verified.load(Ordering::Acquire) {
            return Ok(());
        }
        let observed = gate.finished.load(Ordering::Acquire);
        let mut last_failure = gate.attempt.lock().await;
        if gate.verified.load(Ordering::Acquire) {
            return Ok(());
        }
        if gate.finished.load(Ordering::Acquire) != observed {
            // A check finished while this call waited for it.
            return Err(HyperlaneDuskError::IdentityUnverified(
                last_failure.clone().unwrap_or_default(),
            ));
        }
        let identity = gate.identity;
        let result = self
            .validate_chain_identity(
                identity.chain_id,
                identity.domain,
                &identity.mailbox,
                &identity.validator_announce,
                &identity.merkle_tree_hook,
            )
            .await;
        gate.finished.fetch_add(1, Ordering::AcqRel);
        match result {
            Ok(()) => {
                gate.verified.store(true, Ordering::Release);
                *last_failure = None;
                Ok(())
            }
            Err(error) => {
                let detail = bounded_error(&error);
                warn!(error = %detail, "Dusk endpoint does not serve the configured deployment yet");
                *last_failure = Some(detail.clone());
                Err(HyperlaneDuskError::IdentityUnverified(detail))
            }
        }
    }

    /// Create a RUES client whose validated contract event rows and archive
    /// scan positions survive restart. A scan position is checked against
    /// the archive before it is reused. The configured directory must be
    /// owned by one agent process.
    pub fn new_with_event_cursor_dir(
        url: Url,
        event_cursor_dir: PathBuf,
    ) -> Result<Self, HyperlaneDuskError> {
        let key = shared_client_key(&url, &event_cursor_dir);
        let shared = SHARED_RUES_CLIENTS.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut shared = shared.lock().map_err(|_| {
            HyperlaneDuskError::Other("Shared Dusk RUES client registry is poisoned".into())
        })?;
        if let Some(client) = shared.get(&key) {
            return Ok(client.clone());
        }
        let mut options = Options::default();
        options.create_if_missing(true);
        let event_store = DB::open(&options, &event_cursor_dir).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to open exclusive Dusk event cursor store {}: {error}",
                event_cursor_dir.display()
            ))
        })?;
        let mut client = Self::new(url)?;
        client.event_store = Some(Arc::new(event_store));
        shared.insert(key, client.clone());
        Ok(client)
    }

    /// Forget this process's shared client of `url` and `event_cursor_dir`,
    /// as an agent's exit would: its event store closes once the last clone
    /// of the client is dropped, and `new_with_event_cursor_dir` opens it
    /// again. For tests that restart an agent within one process. Whether
    /// there was such a client.
    #[doc(hidden)]
    pub fn forget_shared_client(url: &Url, event_cursor_dir: &Path) -> bool {
        SHARED_RUES_CLIENTS.get().is_some_and(|shared| {
            shared
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&shared_client_key(url, event_cursor_dir))
                .is_some()
        })
    }

    fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{}{path}", url.path().trim_end_matches('/')));
        url.set_fragment(None);
        url
    }

    /// Query the native Dusk chain ID from the transfer contract.
    pub async fn chain_id(&self) -> Result<u8, HyperlaneDuskError> {
        self.ensure_identity().await?;
        self.chain_id_unchecked().await
    }

    async fn chain_id_unchecked(&self) -> Result<u8, HyperlaneDuskError> {
        let mut transfer_contract = [0u8; 32];
        transfer_contract[0] = 1;
        let bytes = self
            .contract_query_raw_unchecked(&transfer_contract, "chain_id", &[])
            .await?;
        match bytes.as_slice() {
            [chain_id] => Ok(*chain_id),
            _ => Err(HyperlaneDuskError::Other(format!(
                "Unexpected Dusk chain_id response length: {}",
                bytes.len()
            ))),
        }
    }

    /// Validate the endpoint and deployed Hyperlane contracts before an agent
    /// indexes or submits anything. Its own queries do not wait for
    /// `ensure_identity`, which runs this.
    pub async fn validate_chain_identity(
        &self,
        expected_chain_id: u8,
        expected_domain: u32,
        mailbox_id: &[u8; 32],
        validator_announce_id: &[u8; 32],
        merkle_tree_hook_id: &[u8; 32],
    ) -> Result<(), HyperlaneDuskError> {
        let observed_chain_id = self.chain_id_unchecked().await?;
        if observed_chain_id != expected_chain_id {
            return Err(HyperlaneDuskError::Other(format!(
                "Configured Dusk chainId {expected_chain_id} does not match endpoint chain ID {observed_chain_id}"
            )));
        }

        for (label, contract_id) in [
            ("Mailbox", mailbox_id),
            ("ValidatorAnnounce", validator_announce_id),
        ] {
            let observed_domain: u32 = self
                .contract_query_unchecked(contract_id, "local_domain", &())
                .await
                .map_err(|error| {
                    HyperlaneDuskError::Other(format!(
                        "Failed to query Dusk {label} local_domain: {error}"
                    ))
                })?;
            if observed_domain != expected_domain {
                return Err(HyperlaneDuskError::Other(format!(
                    "Configured Dusk domainId {expected_domain} does not match {label} local_domain {observed_domain}"
                )));
            }
        }
        self.validate_state_version(
            mailbox_id,
            "Mailbox",
            MAILBOX_STATE_VERSION,
            "stores dispatched message IDs (dispatched_id) and leaves deliveries to ProcessId events",
        )
        .await?;
        self.validate_state_version(
            validator_announce_id,
            "ValidatorAnnounce",
            VALIDATOR_ANNOUNCE_STATE_VERSION,
            "stores location hashes (get_announced_storage_location_hashes) and leaves the locations to ValidatorAnnouncement events",
        )
        .await?;
        self.validate_mailbox_binding(validator_announce_id, mailbox_id, "ValidatorAnnounce")
            .await?;

        // The supported deployments attach the Merkle hook directly, or as a
        // child of the required static aggregation. A matching local domain
        // alone cannot identify the deployment whose checkpoints we sign.
        let required: [u8; 32] = self
            .contract_query_unchecked(mailbox_id, "required_hook", &())
            .await?;
        let hook_type: u8 = self
            .contract_query_unchecked(merkle_tree_hook_id, "hook_type", &())
            .await?;
        if hook_type != 3 {
            return Err(HyperlaneDuskError::Other(
                "Configured Dusk MerkleTreeHook does not have MerkleTree hook_type 3".into(),
            ));
        }
        self.validate_state_version(
            merkle_tree_hook_id,
            "MerkleTreeHook",
            MERKLE_TREE_HOOK_STATE_VERSION,
            "stores the inserted message IDs (message_ids) that checkpoint roots are computed from",
        )
        .await?;
        if required == *merkle_tree_hook_id {
            self.validate_mailbox_binding(merkle_tree_hook_id, mailbox_id, "MerkleTreeHook")
                .await?;
        } else {
            let required_type: u8 = self
                .contract_query_unchecked(&required, "hook_type", &())
                .await?;
            if required_type != 2 {
                return Err(HyperlaneDuskError::Other(
                    "Dusk Mailbox required_hook must be the configured MerkleTreeHook or its static aggregation".into(),
                ));
            }
            self.validate_mailbox_binding(&required, mailbox_id, "required aggregation hook")
                .await?;
            let hooks: Vec<[u8; 32]> = self
                .contract_query_unchecked(&required, "hooks", &())
                .await?;
            if hooks
                .iter()
                .filter(|hook| *hook == merkle_tree_hook_id)
                .count()
                != 1
            {
                return Err(HyperlaneDuskError::Other(
                    "Dusk required aggregation must contain the configured MerkleTreeHook exactly once".into(),
                ));
            }
            self.validate_mailbox_binding(merkle_tree_hook_id, &required, "MerkleTreeHook")
                .await?;
        }
        Ok(())
    }

    async fn validate_mailbox_binding(
        &self,
        contract: &[u8; 32],
        expected_mailbox: &[u8; 32],
        label: &str,
    ) -> Result<(), HyperlaneDuskError> {
        let mailbox: [u8; 32] = self
            .contract_query_unchecked(contract, "mailbox", &())
            .await?;
        if mailbox != *expected_mailbox {
            return Err(HyperlaneDuskError::Other(format!(
                "Configured Dusk {label} mailbox does not match the configured deployment"
            )));
        }
        Ok(())
    }

    /// Each state version names the queries and events a contract offers,
    /// and contracts cannot be upgraded. An agent reading another version
    /// fails later on a query that is gone, with an unexplained node error,
    /// so only the version this agent reads is accepted.
    async fn validate_state_version(
        &self,
        contract: &[u8; 32],
        label: &str,
        expected: u32,
        what: &str,
    ) -> Result<(), HyperlaneDuskError> {
        let state_version: u32 = self
            .contract_query_unchecked(contract, "state_version", &())
            .await
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to query Dusk {label} state_version (this agent needs state_version {expected}): {error}"
                ))
            })?;
        if state_version != expected {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk {label} state_version {state_version} is not supported: this agent needs state_version {expected}, which {what}. Deploy contracts that match this agent's hyperlane-dusk-types pin"
            )));
        }
        Ok(())
    }

    /// Query a contract method, sending and receiving raw bytes.
    pub async fn contract_query_raw(
        &self,
        contract_id: &[u8; 32],
        method: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, HyperlaneDuskError> {
        self.ensure_identity().await?;
        self.contract_query_raw_unchecked(contract_id, method, body)
            .await
    }

    async fn contract_query_raw_unchecked(
        &self,
        contract_id: &[u8; 32],
        method: &str,
        body: &[u8],
    ) -> Result<Vec<u8>, HyperlaneDuskError> {
        let contract_hex = hex::encode(contract_id);
        let url = self.endpoint(&format!("/on/contracts:{contract_hex}/{method}"));

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(body.to_vec())
            .send()
            .await?;

        let status = response.status();
        let response_body = read_bounded_body(response, "contract query").await?;
        if !status.is_success() {
            return Err(rues_response_error(status, &response_body));
        }

        Ok(response_body)
    }

    /// Query VM metadata for a contract without invoking contract code.
    pub async fn contract_metadata(
        &self,
        contract_id: &[u8; 32],
    ) -> Result<ContractMetadata, HyperlaneDuskError> {
        self.ensure_identity().await?;
        let contract_hex = hex::encode(contract_id);
        let url = self.endpoint(&format!("/on/contract:{contract_hex}/metadata"));

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(Vec::new())
            .send()
            .await?;

        let status = response.status();
        let response_body = read_bounded_body(response, "contract metadata query").await?;
        if !status.is_success() {
            return Err(rues_response_error(status, &response_body));
        }

        serde_json::from_slice::<ContractMetadata>(&response_body).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse contract metadata JSON: {}. Body: {}",
                bounded_error(error),
                bounded_body(&response_body)
            ))
        })
    }

    /// Query a contract method with typed rkyv serialization/deserialization.
    pub async fn contract_query<I, O>(
        &self,
        contract_id: &[u8; 32],
        method: &str,
        args: &I,
    ) -> Result<O, HyperlaneDuskError>
    where
        I: Serialize<AllocSerializer<256>>,
        O: Archive,
        O::Archived: Deserialize<O, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
    {
        let body = rkyv_serialize(args)?;
        let response_bytes = self.contract_query_raw(contract_id, method, &body).await?;
        rkyv_deserialize(&response_bytes)
    }

    async fn contract_query_unchecked<I, O>(
        &self,
        contract_id: &[u8; 32],
        method: &str,
        args: &I,
    ) -> Result<O, HyperlaneDuskError>
    where
        I: Serialize<AllocSerializer<256>>,
        O: Archive,
        O::Archived: Deserialize<O, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
    {
        let body = rkyv_serialize(args)?;
        let response_bytes = self
            .contract_query_raw_unchecked(contract_id, method, &body)
            .await?;
        rkyv_deserialize(&response_bytes)
    }

    /// Propagate a serialized transaction to the network.
    pub async fn propagate_tx(&self, tx_bytes: &[u8]) -> Result<(), HyperlaneDuskError> {
        self.ensure_identity().await?;
        // Preverify first
        let preverify_url = self.endpoint("/on/transactions/preverify");
        let response = self
            .client
            .post(preverify_url)
            .headers(self.default_headers())
            .body(tx_bytes.to_vec())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = read_bounded_body(response, "transaction preverification").await?;
            return Err(rues_response_error(status, &body));
        }

        let url = self.endpoint("/on/transactions/propagate");

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(tx_bytes.to_vec())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = read_bounded_body(response, "transaction propagation").await?;
            return Err(rues_response_error(status, &body));
        }

        Ok(())
    }

    /// Execute a GraphQL query against the node's `/on/graphql/query` endpoint.
    pub async fn graphql_query(&self, query: &str) -> Result<JsonValue, HyperlaneDuskError> {
        let (payload, _) = self.graphql_reply(query).await?;
        graphql_data(payload)
    }

    /// Send a GraphQL query and parse its JSON reply, without looking at any
    /// GraphQL errors in it. Also returns the size of the reply in bytes.
    async fn graphql_reply(&self, query: &str) -> Result<(JsonValue, usize), HyperlaneDuskError> {
        self.ensure_identity().await?;
        let url = self.endpoint("/on/graphql/query");

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(query.as_bytes().to_vec())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "GraphQL query").await?;
        if !status.is_success() {
            return Err(rues_response_error(status, &body));
        }

        let payload = serde_json::from_slice::<JsonValue>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse GraphQL JSON response: {}. Body: {}",
                bounded_error(e),
                bounded_body(&body)
            ))
        })?;
        Ok((payload, body.len()))
    }

    /// Match the contract-state records of one block to their finalized
    /// archive events, reading the block's archive rows once.
    ///
    /// `matching_sequences(data)` must return the sequences among `sequences`
    /// whose contract state implies exactly this event data. For each
    /// sequence, the first finalized, non-reverted row of `topic` at
    /// `block_height` that it is assigned becomes the record. Archive IDs
    /// order a block's rows by transaction hash, not by contract execution,
    /// so the sequence, not the row's position, names the durable record.
    ///
    /// Each record's block must pass `checkBlock(onlyFinalized)`. A record
    /// persisted by an earlier lookup is checked against `matching_sequences`
    /// and `checkBlock` again before it is served, and dropped when it no
    /// longer matches. Returns one match per sequence, in the order given.
    pub(crate) async fn finalized_contract_events_at_block<F>(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        sequences: &[usize],
        matching_sequences: F,
    ) -> Result<Vec<FinalizedEventMatch>, HyperlaneDuskError>
    where
        F: Fn(&[u8]) -> Vec<usize> + Send + Sync,
    {
        let mut rows = HashMap::new();
        let mut stale = Vec::new();
        let mut first_mismatch = None;
        for &sequence in sequences {
            let Some(event) = self.load_finalized_event(contract_id, topic, sequence)? else {
                continue;
            };
            match check_event_against_state(
                contract_id,
                topic,
                sequence,
                block_height,
                &matching_sequences,
                &event,
            ) {
                Ok(()) => {
                    rows.insert(sequence, event);
                }
                Err(error) => {
                    stale.push(sequence);
                    first_mismatch.get_or_insert(error);
                }
            }
        }
        if let Some(error) = first_mismatch {
            self.invalidate_finalized_events(contract_id, topic, &stale)?;
            return Err(error);
        }

        let wanted: HashSet<usize> = sequences
            .iter()
            .copied()
            .filter(|sequence| !rows.contains_key(sequence))
            .collect();
        let mut scanned = HashSet::new();
        let key = BlockKey::new(contract_id, topic, block_height);
        let mut scanned_block = None;
        let mut later_row = None;
        if !wanted.is_empty() {
            let cached = self.block_cache().get(&key);
            let found = match cached {
                Some(block) => {
                    let mut found = ScannedRows::new();
                    for row in &block.rows {
                        assign_row(
                            row,
                            contract_id,
                            topic,
                            block_height,
                            &wanted,
                            &matching_sequences,
                            &mut found,
                        )?;
                        if found.len() == wanted.len() {
                            break;
                        }
                    }
                    if found.len() < wanted.len() {
                        // The block lacks a record that contract state
                        // implies, so the next lookup reads the archive.
                        self.block_cache().remove(&key);
                    }
                    found
                }
                None => {
                    let scan = self
                        .scan_block_rows(
                            contract_id,
                            topic,
                            block_height,
                            &wanted,
                            &matching_sequences,
                        )
                        .await?;
                    scanned_block = scan.block;
                    later_row = scan.later;
                    scan.found
                }
            };
            scanned.extend(found.keys().copied());
            rows.extend(found);
        }

        // Every row of one block names the same block, so one check serves
        // them all. A transport failure keeps persisted rows for a retry.
        let mut block_checks: HashMap<(u64, String), bool> = HashMap::new();
        let mut matches = HashMap::new();
        let mut unfinalized = Vec::new();
        let mut first_unfinalized = None;
        for &sequence in sequences {
            let Some(event) = rows.get(&sequence) else {
                continue;
            };
            let block_hash = match parse_h256_hex(&event.block_hash, "finalized event block hash") {
                Ok(block_hash) => block_hash,
                Err(error) => {
                    // A row that does not parse never serves another lookup
                    // from memory.
                    self.block_cache().remove(&key);
                    return Err(error);
                }
            };
            let block = (
                event.block_height,
                strip_hex_prefix(&event.block_hash).to_owned(),
            );
            let finalized = match block_checks.get(&block) {
                Some(finalized) => *finalized,
                None => {
                    let finalized = self.check_finalized_block(block.0, &block.1).await?;
                    block_checks.insert(block, finalized);
                    finalized
                }
            };
            if !finalized {
                unfinalized.push(sequence);
                first_unfinalized.get_or_insert_with(|| {
                    HyperlaneDuskError::FinalizedEventMismatch(format!(
                        "Finalized Dusk event block {}/{} failed checkBlock",
                        event.block_height, event.block_hash
                    ))
                });
                continue;
            }
            match finalized_event_match(event, block_hash) {
                Ok(matched) => {
                    matches.insert(sequence, matched);
                }
                Err(error) => {
                    self.block_cache().remove(&key);
                    return Err(error);
                }
            }
        }
        if let Some(error) = first_unfinalized {
            self.invalidate_finalized_events(contract_id, topic, &unfinalized)?;
            return Err(error);
        }

        // Only rows that passed the state and checkBlock checks are durable.
        // Page peers and scan anchors never gain authority.
        let fresh: Vec<(usize, &FinalizedContractEvent)> = sequences
            .iter()
            .filter(|sequence| scanned.contains(*sequence))
            .filter_map(|sequence| rows.get(sequence).map(|event| (*sequence, event)))
            .collect();
        self.persist_finalized_events(contract_id, topic, &fresh)?;

        // A block's rows are cached only once checkBlock(onlyFinalized)
        // accepted the hash they all carry.
        if let Some(block) = scanned_block {
            let check = (block_height, hex::encode(block.block_hash));
            if block_checks.get(&check) == Some(&true) {
                self.block_cache().insert(key, Arc::new(block));
            }
        }

        let missing: Vec<usize> = sequences
            .iter()
            .copied()
            .filter(|sequence| !matches.contains_key(sequence))
            .collect();
        if let Some(&sequence) = missing.first() {
            let not_archived = HyperlaneDuskError::Other(format!(
                "Finalized Dusk event {}/{topic} sequence {sequence} matching contract state is not archived yet",
                hex::encode(contract_id)
            ));
            // An archive that has passed the block holds every row of it it
            // ever will, so a missing record may never arrive.
            return Err(self
                .diagnose_missing_rows(
                    contract_id,
                    topic,
                    block_height,
                    &missing,
                    later_row.as_ref(),
                )
                .await
                .unwrap_or(not_archived));
        }
        let mut ordered = Vec::with_capacity(sequences.len());
        for sequence in sequences {
            if let Some(matched) = matches.remove(sequence) {
                ordered.push(matched);
            }
        }
        Ok(ordered)
    }

    /// Contract state holds records of finalized block `block_height`,
    /// `missing`, that the archive has no matching rows for. Rusk archives a
    /// finalized block's rows in one transaction, and blocks in height
    /// order, so an archive that has finalized a block at or above this one
    /// holds every row of it it ever will. But its write of a block can fail,
    /// after which it never holds that block's rows (a Rusk node bug): the
    /// lookup would then fail for good with nothing saying why.
    ///
    /// The evidence that the archive has passed the block is `later`, a row
    /// of the contract in a later block, when the scan read one, and
    /// otherwise the node's last final block, at or above this one, once
    /// `checkBlock(onlyFinalized)` accepts it: a contract that has emitted
    /// nothing since its last record has no later row. Returns an error
    /// naming the block and contract when `checkBlock` confirms the
    /// evidence, and says whether the archive finalized this block, and
    /// reports it as a critical error, at most once every
    /// `ARCHIVE_GAP_REPORT_INTERVAL` for a block. `None` when there is no
    /// such evidence; the lookup then reports the records as not archived
    /// yet. Either way the lookup fails: no record is accepted without its
    /// row.
    async fn diagnose_missing_rows(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        missing: &[usize],
        later: Option<&FinalizedContractEvent>,
    ) -> Option<HyperlaneDuskError> {
        // A block at or above this one that the archive has finalized, and
        // this block's hash when that block is this one.
        let (passed, own_hash) = match later {
            Some(later) => (
                self.check_finalized_block(later.block_height, strip_hex_prefix(&later.block_hash))
                    .await
                    .ok()?
                    .then_some(later.block_height)?,
                None,
            ),
            None => {
                let pair = self.block_pair().await.ok()?;
                if pair.finalized < block_height {
                    return None;
                }
                let finalized = self
                    .check_finalized_block(pair.finalized, &hex::encode(pair.finalized_hash))
                    .await
                    .ok()?;
                (
                    finalized.then_some(pair.finalized)?,
                    (pair.finalized == block_height).then_some(pair.finalized_hash),
                )
            }
        };
        let (ledger_hash, finalized) = match own_hash {
            // checkBlock has just accepted it.
            Some(hash) => (hash, true),
            None => {
                let ledger_hash = self.ledger_block_hash(block_height).await.ok()??;
                let finalized = self
                    .check_finalized_block(block_height, &hex::encode(ledger_hash))
                    .await
                    .ok()?;
                (ledger_hash, finalized)
            }
        };
        let contract = hex::encode(contract_id);
        let sequences = bounded_text(&format!("{missing:?}"));
        let detail = if finalized {
            format!(
                "Dusk archive finalized block {block_height} ({ledger_hash:?}) without the {topic} rows of contract {contract} that its finalized state records for sequences {sequences}"
            )
        } else {
            format!(
                "Dusk archive skipped finalized block {block_height} ({ledger_hash:?}): it has finalized block {passed} but not this one, and holds none of the {topic} rows of contract {contract} that its finalized state records there for sequences {sequences}. The node failed to archive the block and will not add its rows; repair or rebuild its archive, or use another endpoint"
            )
        };
        if critical_report_due(contract_id, topic, block_height) {
            error!(
                contract = %contract,
                topic,
                block_height,
                block_hash = ?ledger_hash,
                %detail,
                "CRITICAL: Dusk archive is missing finalized rows that contract state proves exist"
            );
        }
        Some(HyperlaneDuskError::ArchiveMissingRows(detail))
    }

    /// The event data of every row of `topic` in finalized block
    /// `block_height` that transaction `transaction` emitted and that can be
    /// a record: not reverted, with well-formed provenance and data. Comes
    /// from the block's cached rows, or from one pass over the block, which
    /// is then cached. Either way `checkBlock(onlyFinalized)` must accept
    /// the block's hash for this read.
    ///
    /// `None` when the block cannot be read whole: the archive holds no row
    /// of it for this topic yet, its rows are over the cache bound, name
    /// different block hashes or include one that does not parse, or
    /// `checkBlock(onlyFinalized)` rejects their hash. Callers then look the
    /// transaction up by sequence range.
    ///
    /// The rows only pick which sequences to look up. Every record still
    /// comes from `finalized_contract_events_at_block`, and an empty
    /// selection does not show that the transaction has no record: callers
    /// check that against contract state.
    pub(crate) async fn finalized_rows_of_transaction(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        transaction: &[u8; 32],
    ) -> Result<Option<Vec<Vec<u8>>>, HyperlaneDuskError> {
        let key = BlockKey::new(contract_id, topic, block_height);
        let cached = self.block_cache().get(&key);
        let (block, read) = match cached {
            Some(block) => (block, false),
            None => {
                let scan = self
                    .scan_block_rows(
                        contract_id,
                        topic,
                        block_height,
                        &HashSet::new(),
                        &|_: &[u8]| Vec::new(),
                    )
                    .await?;
                let Some(block) = scan.block else {
                    return Ok(None);
                };
                (Arc::new(block), true)
            }
        };
        if !self
            .check_finalized_block(block_height, &hex::encode(block.block_hash))
            .await?
        {
            self.block_cache().remove(&key);
            return Ok(None);
        }
        if read {
            self.block_cache().insert(key, block.clone());
        }
        Ok(Some(
            block
                .rows
                .iter()
                .filter(|row| row.origin == *transaction)
                .map(|row| row.data.clone())
                .collect(),
        ))
    }

    /// Scan the contract's archive rows once for the rows of `block_height`
    /// that `matching_sequences` assigns to the `wanted` sequences, starting
    /// at the closest persisted anchor below the block. Also returns the
    /// block's rows when the scan read all of them within the cache bound.
    async fn scan_block_rows<F>(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        wanted: &HashSet<usize>,
        matching_sequences: &F,
    ) -> Result<BlockScan, HyperlaneDuskError>
    where
        F: Fn(&[u8]) -> Vec<usize> + Sync,
    {
        let start = self.scan_anchor_at_or_below(contract_id, block_height)?;
        match self
            .scan_from(
                contract_id,
                topic,
                block_height,
                start,
                wanted,
                matching_sequences,
            )
            .await?
        {
            ScanOutcome::Done(scan) => return Ok(scan),
            // The anchors stay: the archive reaches them once it catches up.
            ScanOutcome::ArchiveBehind(anchor) => {
                return Err(HyperlaneDuskError::Other(format!(
                    "Dusk archive of contract {} has not reached the saved scan position after row {} of block {} yet; retrying later",
                    hex::encode(contract_id),
                    anchor.fence_id,
                    anchor.fence_block
                )))
            }
            ScanOutcome::WrongAnchor => {}
        }
        // The endpoint's archive is not the one the anchors were taken from.
        warn!(
            contract = %hex::encode(contract_id),
            "Dusk archive rows were renumbered; rescanning the contract's events from the start"
        );
        self.forget_scan_anchors(contract_id)?;
        match self
            .scan_from(
                contract_id,
                topic,
                block_height,
                None,
                wanted,
                matching_sequences,
            )
            .await?
        {
            ScanOutcome::Done(scan) => Ok(scan),
            ScanOutcome::WrongAnchor | ScanOutcome::ArchiveBehind(_) => {
                Err(HyperlaneDuskError::Other(
                    "Dusk archive scan without an anchor found a stale anchor".into(),
                ))
            }
        }
    }

    /// One pass over the contract's archive rows from `start`, or from its
    /// first row, for the rows of `block_height`. Reads to the end of that
    /// block, so the block's rows can be cached, and stops before the first
    /// row of a later block once every wanted sequence has a row. A block
    /// over the cache bound is read only until every wanted sequence has a
    /// row. Without a row for every wanted sequence the scan reads to the
    /// end of the archive. A resumed scan first checks `start`'s fence row,
    /// as `ScanAnchor` describes.
    ///
    /// A row the archive has not inserted yet is not an error here: the
    /// caller reports it, and the next lookup resumes from the anchors this
    /// pass records instead of from the first row.
    async fn scan_from<F>(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        start: Option<ScanAnchor>,
        wanted: &HashSet<usize>,
        matching_sequences: &F,
    ) -> Result<ScanOutcome, HyperlaneDuskError>
    where
        F: Fn(&[u8]) -> Vec<usize> + Sync,
    {
        let mut found = ScannedRows::new();
        let start_before = start.map_or(0, |anchor| anchor.before_block);
        let mut rank = start.map_or(0, |anchor| anchor.rank);
        let mut highest_block = None;
        let mut last_row = None;
        // The cursor just before the fence row, so the first page starts
        // with it.
        let mut previous_id =
            start.and_then(|anchor| anchor.fence_id.checked_sub(1).filter(|id| *id >= 0));
        let mut fence = start;
        let mut size = PageSize::largest();
        let mut capture = BlockCapture::new(self.block_cache().limits.block_bytes);
        let mut later = None;
        loop {
            let cursor = previous_id.map(canonical_event_cursor);
            let (page, bytes) = self
                .finalized_event_page(contract_id, cursor.as_deref(), &mut size)
                .await?;
            validate_finalized_event_page(
                contract_id,
                cursor.as_deref(),
                previous_id,
                size.rows(),
                &page,
            )?;
            size.after_page(page.events.len(), bytes);
            let has_next_page = page.has_next_page;
            let mut events = page.events.into_iter();
            if let Some(anchor) = fence.take() {
                match events.next() {
                    Some(first) if first.id == anchor.fence_id => {
                        if !anchor.is_fence(&first)? {
                            return Ok(ScanOutcome::WrongAnchor);
                        }
                        previous_id = Some(first.id);
                    }
                    // Later rows without the fence row: an archive that only
                    // lagged would return the fence row first.
                    Some(_) => return Ok(ScanOutcome::WrongAnchor),
                    // No row from the fence's ID on: the archive may be
                    // behind, or may never return the fence row.
                    None => return self.at_missing_fence(contract_id, anchor).await,
                }
            }

            let mut stop = false;
            for event in events {
                if event.block_height > block_height {
                    // The scanned block's rows all come before this one.
                    capture.complete = true;
                    if later.is_none() {
                        later = Some(event.clone());
                    }
                    if found.len() == wanted.len() {
                        // Stop before it: the position recorded below then
                        // ends the scanned block, where a lookup of a later
                        // block can start.
                        stop = true;
                        break;
                    }
                }
                previous_id = Some(event.id);
                rank = rank.saturating_add(1);
                let highest = highest_block.map_or(event.block_height, |height: u64| {
                    height.max(event.block_height)
                });
                highest_block = Some(highest);
                if rank % SCAN_ANCHOR_SPACING == 0 {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(&event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                if event.block_height == block_height {
                    capture.seen = true;
                    if !event.reverted && event.topic == topic {
                        match BlockRow::parse(&event) {
                            Ok(row) => {
                                assign_row(
                                    &row,
                                    contract_id,
                                    topic,
                                    block_height,
                                    wanted,
                                    matching_sequences,
                                    &mut found,
                                )?;
                                capture.push(row);
                            }
                            // Rusk writes well-formed rows. This one is no
                            // record, and the block's other rows are not all
                            // of its records: nothing of the block is cached.
                            Err(error) => {
                                if !capture.malformed {
                                    warn!(
                                        contract = %hex::encode(contract_id),
                                        topic,
                                        block_height,
                                        row = event.id,
                                        %error,
                                        "Dusk archive row does not parse; its block is read again on each lookup"
                                    );
                                }
                                capture.reject_malformed();
                            }
                        }
                    }
                }
                last_row = Some(event);
                // A block too large to cache is read only as far as the
                // lookup needs.
                if found.len() == wanted.len() && !capture.collecting() {
                    stop = true;
                    break;
                }
            }

            if stop || !has_next_page {
                if !has_next_page && capture.seen {
                    // Rusk archives a finalized block's rows together.
                    capture.complete = true;
                }
                if let (Some(event), Some(highest)) = (last_row, highest_block) {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(&event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                return Ok(ScanOutcome::Done(BlockScan {
                    found,
                    block: capture.finish(),
                    later,
                }));
            }
        }
    }

    /// What a scan does when the archive returns no row from `anchor`'s
    /// fence row on.
    ///
    /// The archive may be behind the one the anchor came from, and returns
    /// the fence row once it catches up; then the anchor is kept and the
    /// lookup fails until it does. But the archive may also never return the
    /// row: it has finalized the fence row's block without it, the anchor
    /// names a block the chain does not have, or the archive has passed the
    /// block without finalizing it. Waiting would then stall every lookup
    /// from the anchor for good, so the anchor is kept only on positive
    /// evidence that the archive is behind, and dropped otherwise.
    async fn at_missing_fence(
        &self,
        contract_id: &[u8; 32],
        anchor: ScanAnchor,
    ) -> Result<ScanOutcome, HyperlaneDuskError> {
        let fence_hash = H256::from(anchor.fence_block_hash);
        if self
            .check_finalized_block(anchor.fence_block, &hex::encode(fence_hash))
            .await?
        {
            // Finalized without the row: the rows are numbered differently.
            return Ok(ScanOutcome::WrongAnchor);
        }
        match self.archive_lag(anchor.fence_block, fence_hash).await? {
            None => Ok(ScanOutcome::ArchiveBehind(anchor)),
            Some(reason) => {
                warn!(
                    contract = %hex::encode(contract_id),
                    fence_block = anchor.fence_block,
                    %reason,
                    "Dusk archive will not reach a saved scan position; dropping it"
                );
                Ok(ScanOutcome::WrongAnchor)
            }
        }
    }

    /// Whether the archive has not finalized block `height` with `hash` only
    /// because it is behind: `None` if so, and otherwise why it will not.
    ///
    /// The archive is behind only if the node's ledger holds that block, and
    /// either the node has not finalized it yet, or the archive has not
    /// finalized the node's last final block either. A ledger without the
    /// block, or with another block at that height, or an archive that has
    /// finalized a later block than this one without it, never finalizes it.
    async fn archive_lag(
        &self,
        height: u64,
        hash: H256,
    ) -> Result<Option<String>, HyperlaneDuskError> {
        match self.ledger_block_hash(height).await? {
            None => return Ok(Some(format!("the node's ledger has no block {height}"))),
            Some(ledger) if ledger != hash => {
                return Ok(Some(format!(
                    "the ledger's block {height} is {ledger:?}, not {hash:?}"
                )))
            }
            Some(_) => {}
        }
        let pair = self.block_pair().await?;
        if height > pair.finalized {
            return Ok(None);
        }
        if self
            .check_finalized_block(pair.finalized, &hex::encode(pair.finalized_hash))
            .await?
        {
            return Ok(Some(format!(
                "the archive has finalized block {}, the node's last final block, but not block {height}",
                pair.finalized
            )));
        }
        Ok(None)
    }

    /// The hash of the node's ledger block at `height`, or `None` when the
    /// ledger has no block there.
    async fn ledger_block_hash(&self, height: u64) -> Result<Option<H256>, HyperlaneDuskError> {
        let data = self
            .graphql_query(&format!(
                "query {{ block(height: {height}) {{ header {{ hash }} }} }}"
            ))
            .await?;
        let block = data.get("block").ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk block response is missing block: {}",
                bounded_json(&data)
            ))
        })?;
        if block.is_null() {
            return Ok(None);
        }
        let hash = block
            .get("header")
            .and_then(|header| header.get("hash"))
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk block {height} is missing its header hash: {}",
                    bounded_json(block)
                ))
            })?;
        parse_h256_hex(hash, "ledger block hash").map(Some)
    }

    /// The anchor a scan for `block_height` should start from: the one with
    /// the highest `before_block` at or below the height.
    fn scan_anchor_at_or_below(
        &self,
        contract_id: &[u8; 32],
        block_height: u64,
    ) -> Result<Option<ScanAnchor>, HyperlaneDuskError> {
        let store = self.event_store()?;
        let prefix = scan_anchor_prefix(contract_id);
        let seek = scan_anchor_key(contract_id, block_height);
        for entry in store.iterator(IteratorMode::From(&seek, Direction::Reverse)) {
            let (key, value) = entry.map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read Dusk archive scan anchors: {error}"
                ))
            })?;
            if !key.starts_with(&prefix) {
                break;
            }
            match ScanAnchor::decode(&value) {
                Some(anchor) if *key == *scan_anchor_key(contract_id, anchor.before_block) => {
                    return Ok(Some(anchor));
                }
                // A hint that does not decode is dropped, not trusted.
                _ => store.delete(&key).map_err(|error| {
                    HyperlaneDuskError::Other(format!(
                        "Failed to drop an unreadable Dusk archive scan anchor: {error}"
                    ))
                })?,
            }
        }
        Ok(None)
    }

    /// Persist `anchor` unless an existing anchor serves the same lookups
    /// from at least as far along. Keeps about one anchor per
    /// `SCAN_ANCHOR_SPACING` rows: the furthest one recorded in that span.
    fn offer_scan_anchor(
        &self,
        contract_id: &[u8; 32],
        anchor: ScanAnchor,
    ) -> Result<(), HyperlaneDuskError> {
        let _guard = self
            .scan_anchors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut batch = WriteBatch::default();
        if let Some(previous) = self.scan_anchor_at_or_below(contract_id, anchor.before_block)? {
            if previous.rank >= anchor.rank {
                return Ok(());
            }
            let span = |rank: u64| rank.saturating_sub(1) / SCAN_ANCHOR_SPACING;
            if span(previous.rank) == span(anchor.rank) {
                batch.delete(scan_anchor_key(contract_id, previous.before_block));
            }
        }
        batch.put(
            scan_anchor_key(contract_id, anchor.before_block),
            anchor.encode(),
        );
        self.event_store()?.write(batch).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to persist a Dusk archive scan anchor: {error}"
            ))
        })
    }

    /// Drop every scan anchor and cached block of a contract. The next scan
    /// starts from the contract's first archive row.
    fn forget_scan_anchors(&self, contract_id: &[u8; 32]) -> Result<(), HyperlaneDuskError> {
        self.block_cache().forget_contract(contract_id);
        let _guard = self
            .scan_anchors
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let store = self.event_store()?;
        let prefix = scan_anchor_prefix(contract_id);
        let mut batch = WriteBatch::default();
        for entry in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, _) = entry.map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read Dusk archive scan anchors: {error}"
                ))
            })?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete(key);
        }
        store.write(batch).map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to drop Dusk archive scan anchors: {error}"))
        })
    }

    fn block_cache(&self) -> std::sync::MutexGuard<'_, BlockCache> {
        self.block_cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn event_store(&self) -> Result<&DB, HyperlaneDuskError> {
        self.event_store.as_deref().ok_or_else(|| {
            HyperlaneDuskError::Other(
                "Dusk finalized-event indexing requires a durable eventCursorDir".into(),
            )
        })
    }

    fn load_finalized_event(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        sequence: usize,
    ) -> Result<Option<FinalizedContractEvent>, HyperlaneDuskError> {
        let Some(bytes) = self
            .event_store()?
            .get(finalized_event_row_key(contract_id, topic, sequence)?)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read persisted Dusk finalized event: {error}"
                ))
            })?
        else {
            return Ok(None);
        };
        if bytes.len() > MAX_RUES_RESPONSE_BYTES {
            return Err(HyperlaneDuskError::Other(
                "Persisted Dusk finalized event exceeds the transport bound".into(),
            ));
        }
        let mut event: FinalizedContractEvent =
            serde_json::from_slice(&bytes).map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to decode persisted Dusk finalized event: {}",
                    bounded_error(error)
                ))
            })?;
        // Archive row IDs are pagination hints, not durable provenance. Use
        // the locally owned topic sequence.
        event.id = i64::try_from(sequence).map_err(|_| {
            HyperlaneDuskError::Other(
                "Dusk finalized-event sequence exceeds i64 provenance range".into(),
            )
        })?;
        validate_persisted_event(contract_id, topic, &event)?;
        Ok(Some(event))
    }

    /// Durably persist authenticated rows in one synced write.
    fn persist_finalized_events(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        rows: &[(usize, &FinalizedContractEvent)],
    ) -> Result<(), HyperlaneDuskError> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        for (sequence, event) in rows {
            validate_persisted_event(contract_id, topic, event)?;
            let bytes = serde_json::to_vec(event).map_err(|error| {
                HyperlaneDuskError::Other(format!("Failed to encode Dusk finalized event: {error}"))
            })?;
            if bytes.len() > MAX_RUES_RESPONSE_BYTES {
                return Err(HyperlaneDuskError::Other(
                    "Dusk finalized event exceeds the transport bound".into(),
                ));
            }
            batch.put(
                finalized_event_row_key(contract_id, topic, *sequence)?,
                bytes,
            );
        }
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.event_store()?
            .write_opt(batch, &write_options)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to durably persist authenticated Dusk finalized event: {error}"
                ))
            })
    }

    /// Drop rows that no longer match contract state or finality, together
    /// with the contract's scan anchors and cached blocks: an endpoint that
    /// served them may also have served the rows the anchors and blocks were
    /// taken from.
    fn invalidate_finalized_events(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        sequences: &[usize],
    ) -> Result<(), HyperlaneDuskError> {
        self.forget_scan_anchors(contract_id)?;
        let mut batch = WriteBatch::default();
        for sequence in sequences {
            batch.delete(finalized_event_row_key(contract_id, topic, *sequence)?);
        }
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.event_store()?
            .write_opt(batch, &write_options)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to invalidate {} Dusk finalized events {}/{topic}: {error}",
                    sequences.len(),
                    hex::encode(contract_id)
                ))
            })
    }

    /// The page of the contract's archive rows after `cursor`, asking for
    /// `size` rows, and for fewer at the same cursor while the reply is too
    /// large to read. Returns the page and the size of its reply in bytes.
    async fn finalized_event_page(
        &self,
        contract_id: &[u8; 32],
        cursor: Option<&str>,
        size: &mut PageSize,
    ) -> Result<(FinalizedEventPage, usize), HyperlaneDuskError> {
        loop {
            let (reason, by_agent) = match self
                .finalized_event_page_reply(contract_id, cursor, size.rows())
                .await?
            {
                PageReply::Page(page, bytes) => return Ok((page, bytes)),
                PageReply::TooLarge { reason, by_agent } => (reason, by_agent),
            };
            if !size.shrink(by_agent) {
                return Err(HyperlaneDuskError::Other(format!(
                    "One Dusk archive row of contract {} after finalizedEvents cursor {} does not fit the {MAX_RUES_RESPONSE_BYTES}-byte response limit. Rusk events carry at most 64 KiB of data, so the endpoint does not serve its archive as Rusk does: {reason}",
                    hex::encode(contract_id),
                    cursor.unwrap_or("<first row>")
                )));
            }
            debug!(
                contract = %hex::encode(contract_id),
                rows = size.rows(),
                "Dusk finalizedEvents reply was too large; asking for fewer rows"
            );
        }
    }

    /// One `finalizedEvents` request for `limit` rows after `cursor`.
    async fn finalized_event_page_reply(
        &self,
        contract_id: &[u8; 32],
        cursor: Option<&str>,
        limit: usize,
    ) -> Result<PageReply, HyperlaneDuskError> {
        let unusable = |error: &dyn fmt::Display| {
            HyperlaneDuskError::Other(format!(
                "Dusk endpoint does not provide usable archive finalizedEvents pagination: {error}"
            ))
        };
        let cursor_argument = match cursor {
            Some(cursor) => format!(
                ", cursor: {}",
                serde_json::to_string(cursor).map_err(|error| {
                    HyperlaneDuskError::Other(format!(
                        "Failed to encode finalizedEvents cursor: {error}"
                    ))
                })?
            ),
            None => String::new(),
        };
        let query = format!(
            "query {{ finalizedEvents(contractId: \"{}\", limit: {limit}{cursor_argument}) {{ json }} }}",
            hex::encode(contract_id)
        );
        let (payload, bytes) = match self.graphql_reply(&query).await {
            Ok(reply) => reply,
            Err(error) => {
                return match reply_too_large(&error) {
                    Some(by_agent) => Ok(PageReply::TooLarge {
                        reason: error.to_string(),
                        by_agent,
                    }),
                    None => Err(unusable(&error)),
                }
            }
        };
        if let Some(errors) = graphql_errors(&payload) {
            let errors = format!("GraphQL query returned errors: {}", bounded_json(errors));
            if reports_oversized_reply(&errors) {
                return Ok(PageReply::TooLarge {
                    reason: errors,
                    by_agent: false,
                });
            }
            return Err(unusable(&errors));
        }
        let data = graphql_data(payload)?;
        let page = data
            .get("finalizedEvents")
            .and_then(|events| events.get("json"))
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive response is missing finalizedEvents.json: {}",
                    bounded_json(&data)
                ))
            })?;
        let page = serde_json::from_value(page.clone()).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to decode Rusk finalizedEvents page: {}",
                bounded_error(error)
            ))
        })?;
        Ok(PageReply::Page(page, bytes))
    }

    /// `checkBlock(onlyFinalized: true)` for a block by its parsed hash.
    pub(crate) async fn check_finalized_block_hash(
        &self,
        block_height: u64,
        block_hash: H256,
    ) -> Result<bool, HyperlaneDuskError> {
        self.check_finalized_block(block_height, &hex::encode(block_hash))
            .await
    }

    /// `checkBlock(onlyFinalized: true)` for one block.
    async fn check_finalized_block(
        &self,
        block_height: u64,
        block_hash: &str,
    ) -> Result<bool, HyperlaneDuskError> {
        let encoded_block_hash = serde_json::to_string(block_hash).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to encode finalized event block hash: {error}"
            ))
        })?;
        let check_query = format!(
            "query {{ checkBlock(height: {block_height}, hash: {encoded_block_hash}, onlyFinalized: true) }}"
        );
        self.graphql_query(&check_query)
            .await?
            .get("checkBlock")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(
                    "Rusk checkBlock response is missing a boolean result".into(),
                )
            })
    }

    /// Return the latest block height finalized by the node's consensus view.
    pub(crate) async fn finalized_block_height(&self) -> Result<u64, HyperlaneDuskError> {
        self.checkpoint_block_height(&ReorgPeriod::None).await
    }

    /// Apply the configured block delay while retaining consensus finality as
    /// an upper bound. Both heights come from one archive snapshot.
    pub(crate) async fn checkpoint_block_height(
        &self,
        reorg_period: &ReorgPeriod,
    ) -> Result<u64, HyperlaneDuskError> {
        let delay = match reorg_period {
            ReorgPeriod::None => 0,
            ReorgPeriod::Blocks(blocks) => u64::from(blocks.get()),
            ReorgPeriod::Tag(tag) if tag == "finalized" => 0,
            ReorgPeriod::Tag(tag) => {
                return Err(HyperlaneDuskError::Other(format!(
                    "Dusk supports a numeric reorgPeriod or the finalized tag, got {tag:?}"
                )));
            }
        };
        let data = self
            .graphql_query("query { lastBlockPair { json } }")
            .await?;
        let (latest, finalized) = parse_block_heights(&data)?;
        Ok(finalized.min(latest.saturating_sub(delay)))
    }

    /// Return the finalized height in Hyperlane's shared u32 cursor range.
    pub(crate) async fn finalized_block_number(&self) -> Result<u32, HyperlaneDuskError> {
        let height = self.finalized_block_height().await?;
        u32::try_from(height).map_err(|_| {
            HyperlaneDuskError::Other(format!(
                "Dusk finalized height {height} exceeds the shared u32 cursor range"
            ))
        })
    }

    /// The node's tip and last finalized block. A finality check reads them
    /// before the contract state it checks; see `non_final_events`.
    pub(crate) async fn block_pair(&self) -> Result<BlockPair, HyperlaneDuskError> {
        parse_block_pair(
            &self
                .graphql_query("query { lastBlockPair { json } }")
                .await?,
        )
    }

    /// The non-reverted `topic` events of `contract_id` in the blocks above
    /// `pair`'s last finalized block, oldest block first.
    ///
    /// Contract queries read the node's tip, and Rusk can roll a block back
    /// until it is final. To check that a record the tip holds is final, read
    /// `pair` with `block_pair`, then the state, then this: when none of the
    /// blocks this returns emitted the record's event, the record is in a
    /// final block.
    ///
    /// The blocks are the ledger's chain from the child of `pair`'s last
    /// finalized block to two blocks past `pair`'s tip, where the ledger has
    /// them: the state may hold a block accepted after `pair` was read, and
    /// Rusk updates the state a query reads just before it stores a block in
    /// the ledger. The chain must still hold `pair`'s tip, so that no block
    /// the state could hold was rolled back in between. Their events come
    /// from the archive, which stores all of a block's events at once. Every
    /// block emits at least the stake contract's reward event, so a block
    /// without events is one the archive has not stored yet.
    ///
    /// Fails, and the caller checks again later, when finality is more than
    /// `MAX_NON_FINAL_BLOCKS` blocks behind the tip, the chain changed
    /// since `pair` was read, or the archive lacks one of the blocks.
    pub(crate) async fn non_final_events(
        &self,
        pair: &BlockPair,
        contract_id: &[u8; 32],
        topic: &str,
    ) -> Result<Vec<NonFinalEvent>, HyperlaneDuskError> {
        let behind = pair.latest.saturating_sub(pair.finalized);
        if behind > MAX_NON_FINAL_BLOCKS {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk finality is {behind} blocks behind the tip (last final block {}); waiting for it to catch up instead of reading more than {MAX_NON_FINAL_BLOCKS} non-final blocks",
                pair.finalized
            )));
        }
        let mut events = Vec::new();
        for block in self.non_final_blocks(pair).await? {
            events.extend(
                self.non_final_block_events(block, contract_id, topic)
                    .await?
                    .iter()
                    .cloned(),
            );
        }
        Ok(events)
    }

    /// The ledger's chain above `pair`'s last finalized block, which must
    /// still hold `pair`'s tip, up to two blocks past it.
    async fn non_final_blocks(
        &self,
        pair: &BlockPair,
    ) -> Result<Vec<NonFinalBlock>, HyperlaneDuskError> {
        let first = pair.finalized + 1;
        let last = pair.latest + 2;
        let query = format!(
            "query {{ blocks(range: [{first}, {last}]) {{ header {{ height hash prevBlockHash }} }} }}"
        );
        let data = self.graphql_query(&query).await?;
        let headers = data
            .get("blocks")
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk blocks response is not a list: {}",
                    bounded_json(&data)
                ))
            })?;
        let changed = |detail: String| {
            HyperlaneDuskError::Other(format!(
                "Dusk chain changed since its tip was block {} ({detail}); retrying later",
                pair.latest
            ))
        };
        if headers.len() as u64 > (last + 1).saturating_sub(first) {
            return Err(changed(format!("{} blocks returned", headers.len())));
        }
        let mut parent = pair.finalized_hash;
        let mut blocks = Vec::with_capacity(headers.len());
        for (expected_height, block) in (first..).zip(headers) {
            let header = block.get("header").unwrap_or(&JsonValue::Null);
            let field = |name: &str| {
                header.get(name).ok_or_else(|| {
                    HyperlaneDuskError::Other(format!(
                        "Rusk block header is missing {name}: {}",
                        bounded_json(block)
                    ))
                })
            };
            let height = field("height")?.as_u64().ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk block header has a non-numeric height: {}",
                    bounded_json(block)
                ))
            })?;
            let hash = parse_h256_hex(
                field("hash")?.as_str().unwrap_or_default(),
                "block header hash",
            )?;
            let prev = parse_h256_hex(
                field("prevBlockHash")?.as_str().unwrap_or_default(),
                "block header prevBlockHash",
            )?;
            if height != expected_height || prev != parent {
                return Err(changed(format!(
                    "block {height} does not follow block {}",
                    expected_height - 1
                )));
            }
            if height == pair.latest && hash != pair.latest_hash {
                return Err(changed(format!("block {height} was replaced")));
            }
            parent = hash;
            blocks.push(NonFinalBlock { height, hash });
        }
        if blocks.last().map_or(pair.finalized, |block| block.height) < pair.latest {
            return Err(changed("it was rolled back".to_owned()));
        }
        Ok(blocks)
    }

    /// The non-reverted `topic` events of `contract_id` in `block`, from the
    /// archive.
    async fn non_final_block_events(
        &self,
        block: NonFinalBlock,
        contract_id: &[u8; 32],
        topic: &str,
    ) -> Result<Arc<Vec<NonFinalEvent>>, HyperlaneDuskError> {
        let key = (block.hash, *contract_id, topic.to_owned());
        if let Some(events) = self.non_final_cache().get(&key) {
            return Ok(events);
        }
        let query = format!(
            "query {{ contractEvents(hash: \"{}\") {{ json }} }}",
            hex::encode(block.hash)
        );
        let data = self.graphql_query(&query).await?;
        let rows = data
            .get("contractEvents")
            .and_then(|events| events.get("json"))
            .and_then(JsonValue::as_array)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive response is missing contractEvents.json: {}",
                    bounded_json(&data)
                ))
            })?;
        if rows.is_empty() {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive has not stored the events of non-final block {} ({:?}) yet; retrying later",
                block.height, block.hash
            )));
        }
        let source = hex::encode(contract_id);
        let mut events = Vec::new();
        for row in rows {
            let row: ArchivedBlockEvent = serde_json::from_value(row.clone()).map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to decode a Rusk archive event of block {}: {}",
                    block.height,
                    bounded_error(error)
                ))
            })?;
            if row.reverted
                || row.topic != topic
                || !strip_hex_prefix(&row.source).eq_ignore_ascii_case(&source)
            {
                continue;
            }
            let data = hex::decode(strip_hex_prefix(&row.data)).map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive event of block {} has invalid data: {}",
                    block.height,
                    bounded_error(error)
                ))
            })?;
            events.push(NonFinalEvent {
                block_height: block.height,
                block_hash: block.hash,
                origin: row.origin,
                data,
            });
        }
        let events = Arc::new(events);
        self.non_final_cache().insert(key, events.clone());
        Ok(events)
    }

    fn non_final_cache(&self) -> std::sync::MutexGuard<'_, NonFinalEventCache> {
        self.non_final_cache
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Resolve the canonical block height containing a transaction.
    pub(crate) async fn transaction_block_height(
        &self,
        tx_id: &str,
    ) -> Result<Option<u64>, HyperlaneDuskError> {
        validate_transaction_id(tx_id)?;
        let query = format!(r#"query {{ tx(hash: "{tx_id}") {{ blockHeight }} }}"#);
        let data = self.graphql_query(&query).await?;
        let Some(transaction) = data.get("tx").filter(|value| !value.is_null()) else {
            return Ok(None);
        };
        let height = transaction
            .get("blockHeight")
            .and_then(JsonValue::as_u64)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Ledger transaction {tx_id} is missing numeric blockHeight: {}",
                    bounded_json(transaction)
                ))
            })?;
        Ok(Some(height))
    }

    /// Wait for a propagated transaction to be included in the ledger.
    ///
    /// Propagation only acknowledges admission to the mempool. Hyperlane must not
    /// report a transaction as executed until Rusk exposes its spent transaction
    /// record, which also contains the actual gas use and any execution error.
    pub async fn wait_for_tx(
        &self,
        tx_id: &str,
        timeout: Duration,
    ) -> Result<ConfirmedTransaction, HyperlaneDuskError> {
        self.wait_for_tx_with_poll_interval(tx_id, timeout, TX_POLL_INTERVAL)
            .await
    }

    async fn wait_for_tx_with_poll_interval(
        &self,
        tx_id: &str,
        timeout: Duration,
        poll_interval: Duration,
    ) -> Result<ConfirmedTransaction, HyperlaneDuskError> {
        validate_transaction_id(tx_id)?;

        let query = transaction_query(tx_id);
        let deadline = tokio::time::Instant::now() + timeout;
        let mut last_observation_error = None;

        loop {
            if tokio::time::Instant::now() >= deadline {
                return Err(tx_confirmation_timeout_error(
                    tx_id,
                    timeout,
                    last_observation_error.as_deref(),
                ));
            }

            match tokio::time::timeout_at(deadline, self.graphql_query(&query)).await {
                Ok(Ok(data)) => {
                    if let Some(tx) = data.get("tx").filter(|tx| !tx.is_null()) {
                        // A non-null ledger record is an authoritative
                        // observation. Missing or malformed execution fields
                        // are corruption/schema drift, not a pending state that
                        // a later response may silently overwrite.
                        return parse_confirmed_transaction(tx_id, tx);
                    } else {
                        last_observation_error = None;
                    }
                }
                Ok(Err(error)) => last_observation_error = Some(error.to_string()),
                Err(_) => {
                    return Err(tx_confirmation_timeout_error(
                        tx_id,
                        timeout,
                        last_observation_error.as_deref(),
                    ))
                }
            }

            tokio::time::sleep_until((tokio::time::Instant::now() + poll_interval).min(deadline))
                .await;
        }
    }

    /// Query a Moonlight account status by bs58-encoded BLS public key.
    pub async fn account_status(&self, bs58_pk: &str) -> Result<AccountStatus, HyperlaneDuskError> {
        self.ensure_identity().await?;
        let url = self.endpoint(&format!("/on/account:{bs58_pk}/status"));

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(Vec::new())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "account status query").await?;
        if !status.is_success() {
            return Err(rues_response_error(status, &body));
        }

        serde_json::from_slice::<AccountStatus>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse account status JSON: {}. Body: {}",
                bounded_error(e),
                bounded_body(&body)
            ))
        })
    }

    /// Query a contract's balance by hex-encoded ContractId (64 hex chars, no 0x prefix).
    pub async fn contract_status(
        &self,
        contract_id_hex: &str,
    ) -> Result<ContractStatus, HyperlaneDuskError> {
        let bytes = hex::decode(contract_id_hex).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Invalid contract ID hex '{}': {}",
                bounded_text(contract_id_hex),
                bounded_error(error)
            ))
        })?;
        let contract_id: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
            HyperlaneDuskError::Other(format!("Contract ID must be 32 bytes, got {}", bytes.len()))
        })?;

        // The transfer contract is reserved contract ID 0x01 followed by zeros.
        // Querying it directly avoids Rusk's deprecated contract status route.
        let mut transfer_contract = [0u8; 32];
        transfer_contract[0] = 1;
        let balance = self
            .contract_query(&transfer_contract, "contract_balance", &contract_id)
            .await?;
        Ok(ContractStatus { balance })
    }

    /// Query node gas price statistics from the mempool.
    pub async fn gas_price_stats(
        &self,
        max_transactions: usize,
    ) -> Result<GasPriceStats, HyperlaneDuskError> {
        self.ensure_identity().await?;
        let url = self.endpoint("/on/blocks/gas-price");

        let response = self
            .client
            .post(url)
            .headers(self.default_headers())
            .body(max_transactions.to_string())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "gas price query").await?;
        if !status.is_success() {
            return Err(rues_response_error(status, &body));
        }

        serde_json::from_slice::<GasPriceStats>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse gas price stats JSON: {}. Body: {}",
                bounded_error(e),
                bounded_body(&body)
            ))
        })
    }

    fn default_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        );
        headers
    }
}

/// The key of the shared client of an endpoint and an event store: the
/// endpoint without a trailing slash or fragment.
fn shared_client_key(url: &Url, event_cursor_dir: &Path) -> (String, PathBuf) {
    let mut canonical_url = url.clone();
    canonical_url.set_path(url.path().trim_end_matches('/'));
    canonical_url.set_fragment(None);
    (canonical_url.to_string(), event_cursor_dir.to_path_buf())
}

fn validate_transaction_id(tx_id: &str) -> Result<(), HyperlaneDuskError> {
    if tx_id.len() != 64 || !tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HyperlaneDuskError::Other(format!(
            "Invalid Dusk transaction ID '{}'",
            bounded_text(tx_id)
        )));
    }
    Ok(())
}

fn parse_confirmed_transaction(
    tx_id: &str,
    transaction: &JsonValue,
) -> Result<ConfirmedTransaction, HyperlaneDuskError> {
    let gas_spent = transaction
        .get("gasSpent")
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Ledger transaction {tx_id} is missing numeric gasSpent: {}",
                bounded_json(transaction)
            ))
        })?;
    let error = match transaction.get("err") {
        Some(JsonValue::Null) => None,
        Some(JsonValue::String(error)) => Some(error.clone()),
        None => {
            return Err(HyperlaneDuskError::Other(format!(
                "Ledger transaction {tx_id} is missing err status: {}",
                bounded_json(transaction)
            )))
        }
        Some(other) => {
            return Err(HyperlaneDuskError::Other(format!(
                "Ledger transaction {tx_id} has invalid err field: {}",
                bounded_json(other)
            )))
        }
    };
    Ok(ConfirmedTransaction { gas_spent, error })
}

fn parse_block_heights(data: &JsonValue) -> Result<(u64, u64), HyperlaneDuskError> {
    let pair = data
        .get("lastBlockPair")
        .and_then(|value| value.get("json"))
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing lastBlockPair.json: {}",
                bounded_json(data)
            ))
        })?;
    let latest = pair
        .get("last_block")
        .and_then(JsonValue::as_array)
        .and_then(|value| value.first())
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing numeric last_block height: {}",
                bounded_json(pair)
            ))
        })?;
    let finalized = pair
        .get("last_finalized_block")
        .and_then(JsonValue::as_array)
        .and_then(|value| value.first())
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing numeric last_finalized_block height: {}",
                bounded_json(pair)
            ))
        })?;
    if finalized > latest {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalized height {finalized} exceeds latest height {latest}"
        )));
    }
    Ok((latest, finalized))
}

/// The node's tip and last finalized block, from one `lastBlockPair` reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockPair {
    latest: u64,
    latest_hash: H256,
    finalized: u64,
    finalized_hash: H256,
}

fn parse_block_pair(data: &JsonValue) -> Result<BlockPair, HyperlaneDuskError> {
    let (latest, finalized) = parse_block_heights(data)?;
    let pair = &data["lastBlockPair"]["json"];
    let hash = |field: &str| {
        let hash = pair
            .get(field)
            .and_then(JsonValue::as_array)
            .and_then(|block| block.get(1))
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive response is missing the {field} hash: {}",
                    bounded_json(pair)
                ))
            })?;
        parse_h256_hex(hash, field)
    };
    Ok(BlockPair {
        latest,
        latest_hash: hash("last_block")?,
        finalized,
        finalized_hash: hash("last_finalized_block")?,
    })
}

fn validate_finalized_event_page(
    contract_id: &[u8; 32],
    requested_cursor: Option<&str>,
    previous_id: Option<i64>,
    requested_limit: usize,
    page: &FinalizedEventPage,
) -> Result<(), HyperlaneDuskError> {
    let source = hex::encode(contract_id);
    match (requested_cursor, previous_id) {
        (None, None) => {}
        (Some(cursor), Some(id)) if decode_event_cursor(cursor)? == id => {}
        (Some(_), Some(id)) => {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk transient event cursor does not match last event ID {id} for {source}"
            )));
        }
        _ => {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk transient event cursor and last event ID are inconsistent for {source}"
            )));
        }
    }
    if page.events.len() > requested_limit {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalizedEvents returned {} rows above requested limit {requested_limit}",
            page.events.len()
        )));
    }
    if page.events.is_empty() {
        if page.has_next_page || page.start_cursor.is_some() || page.end_cursor.is_some() {
            return Err(HyperlaneDuskError::Other(format!(
                "Rusk finalizedEvents returned an inconsistent empty page for {source}"
            )));
        }
        return Ok(());
    }
    if page.start_cursor.is_none() || page.end_cursor.is_none() {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalizedEvents omitted page cursors for {source}"
        )));
    }
    let (Some(first_event), Some(last_event)) = (page.events.first(), page.events.last()) else {
        return Err(HyperlaneDuskError::Other(
            "Rusk finalizedEvents page unexpectedly became empty".into(),
        ));
    };
    let start_cursor_id = decode_event_cursor(page.start_cursor.as_deref().unwrap_or_default())?;
    let end_cursor_id = decode_event_cursor(page.end_cursor.as_deref().unwrap_or_default())?;
    if start_cursor_id != first_event.id || end_cursor_id != last_event.id {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalizedEvents cursors do not match page row IDs for {source}: start={start_cursor_id}/{} end={end_cursor_id}/{}",
            first_event.id, last_event.id
        )));
    }
    if requested_cursor == page.end_cursor.as_deref() {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalizedEvents did not advance the cursor for {source}"
        )));
    }

    let mut last_id = previous_id;
    for event in &page.events {
        if event.topic.is_empty() || event.topic.len() > MAX_EVENT_TOPIC_BYTES {
            return Err(HyperlaneDuskError::Other(format!(
                "Rusk finalizedEvents returned an empty or oversized topic for {source}"
            )));
        }
        if event.id < 0 || last_id.is_some_and(|last_id| event.id <= last_id) {
            return Err(HyperlaneDuskError::Other(format!(
                "Rusk finalizedEvents returned a non-monotonic event ID {} for {source}",
                event.id
            )));
        }
        if !strip_hex_prefix(&event.source).eq_ignore_ascii_case(&source) {
            return Err(HyperlaneDuskError::Other(format!(
                "Rusk finalizedEvents returned source {} while querying {source}",
                bounded_text(&event.source)
            )));
        }
        last_id = Some(event.id);
    }
    Ok(())
}

fn canonical_event_cursor(id: i64) -> String {
    BASE64_STANDARD.encode(format!("v1:{id}"))
}

fn decode_event_cursor(cursor: &str) -> Result<i64, HyperlaneDuskError> {
    let decoded = BASE64_STANDARD.decode(cursor).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Invalid Dusk finalized-event cursor encoding: {}",
            bounded_error(error)
        ))
    })?;
    let decoded = std::str::from_utf8(&decoded).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Invalid Dusk finalized-event cursor text: {}",
            bounded_error(error)
        ))
    })?;
    let id = decoded
        .strip_prefix("v1:")
        .ok_or_else(|| {
            HyperlaneDuskError::Other("Unsupported Dusk finalized-event cursor version".into())
        })?
        .parse::<i64>()
        .map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Invalid Dusk finalized-event cursor ID: {}",
                bounded_error(error)
            ))
        })?;
    if id < 0 || canonical_event_cursor(id) != cursor {
        return Err(HyperlaneDuskError::Other(
            "Dusk finalized-event cursor is negative or non-canonical".into(),
        ));
    }
    Ok(id)
}

fn validate_persisted_event(
    contract_id: &[u8; 32],
    topic: &str,
    event: &FinalizedContractEvent,
) -> Result<(), HyperlaneDuskError> {
    let source = hex::encode(contract_id);
    if event.reverted
        || event.topic != topic
        || !strip_hex_prefix(&event.source).eq_ignore_ascii_case(&source)
        || event.id < 0
    {
        return Err(HyperlaneDuskError::Other(format!(
            "Persisted Dusk finalized event has invalid {source}/{topic} provenance"
        )));
    }
    parse_h256_hex(&event.block_hash, "persisted finalized event block hash")?;
    parse_h256_hex(&event.origin, "persisted finalized event origin")?;
    hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Persisted Dusk finalized event contains invalid data: {}",
            bounded_error(error)
        ))
    })?;
    Ok(())
}

fn finalized_event_row_key(
    contract_id: &[u8; 32],
    topic: &str,
    sequence: usize,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    if topic.is_empty() || topic.len() > MAX_EVENT_TOPIC_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk event topic must contain 1..={MAX_EVENT_TOPIC_BYTES} bytes"
        )));
    }
    let sequence = u64::try_from(sequence)
        .map_err(|_| HyperlaneDuskError::Other("Dusk event sequence does not fit in u64".into()))?;
    let topic_hash = hyperlane_dusk_types::message::keccak256(topic.as_bytes());
    // v2 stores only independently state/checkBlock-authenticated rows. It
    // deliberately ignores v1 page-derived cache entries during migration.
    let mut key = b"dusk-finalized-row-v2:".to_vec();
    key.extend_from_slice(contract_id);
    key.extend_from_slice(&topic_hash);
    key.extend_from_slice(&sequence.to_be_bytes());
    Ok(key)
}

fn scan_anchor_prefix(contract_id: &[u8; 32]) -> Vec<u8> {
    let mut key = b"dusk-scan-anchor-v1:".to_vec();
    key.extend_from_slice(contract_id);
    key
}

/// Big-endian heights keep a contract's anchors in height order.
fn scan_anchor_key(contract_id: &[u8; 32], before_block: u64) -> Vec<u8> {
    let mut key = scan_anchor_prefix(contract_id);
    key.extend_from_slice(&before_block.to_be_bytes());
    key
}

/// Identify an archive row as the endpoint returned it, ID included.
fn archive_row_digest(event: &FinalizedContractEvent) -> Result<[u8; 32], HyperlaneDuskError> {
    let bytes = serde_json::to_vec(event).map_err(|error| {
        HyperlaneDuskError::Other(format!("Failed to encode a Dusk archive row: {error}"))
    })?;
    Ok(hyperlane_dusk_types::message::keccak256(&bytes))
}

/// Route archive rows to the sequences whose contract state implies exactly
/// their event bytes. Sequences with identical expected bytes share a row.
pub(crate) fn exact_event_data(
    expected: Vec<(usize, Vec<u8>)>,
) -> impl Fn(&[u8]) -> Vec<usize> + Send + Sync {
    let mut by_data: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (sequence, data) in expected {
        by_data.entry(data).or_default().push(sequence);
    }
    move |data: &[u8]| by_data.get(data).cloned().unwrap_or_default()
}

/// The checks a row must pass against contract state before it can be a
/// sequence's record, whether it comes from the archive or the local store.
fn check_event_against_state<F>(
    contract_id: &[u8; 32],
    topic: &str,
    sequence: usize,
    expected_block_height: u64,
    matching_sequences: &F,
    event: &FinalizedContractEvent,
) -> Result<(), HyperlaneDuskError>
where
    F: Fn(&[u8]) -> Vec<usize>,
{
    if event.reverted || event.topic != topic {
        return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
            "Finalized Dusk event {}/{topic} sequence {sequence} is reverted or has the wrong topic",
            hex::encode(contract_id)
        )));
    }
    if event.block_height != expected_block_height {
        return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
            "Finalized Dusk event {}/{topic} sequence {sequence} has block height {}, contract state says {expected_block_height}",
            hex::encode(contract_id),
            event.block_height
        )));
    }
    let data = hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
        HyperlaneDuskError::FinalizedEventMismatch(format!(
            "Finalized Dusk event {}/{topic} sequence {sequence} has invalid data: {}",
            hex::encode(contract_id),
            bounded_error(error)
        ))
    })?;
    if !matching_sequences(data.as_slice()).contains(&sequence) {
        return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
            "Finalized Dusk event {}/{topic} sequence {sequence} does not match contract state",
            hex::encode(contract_id)
        )));
    }
    Ok(())
}

/// The provenance and data of a row that passed every check.
fn finalized_event_match(
    event: &FinalizedContractEvent,
    block_hash: H256,
) -> Result<FinalizedEventMatch, HyperlaneDuskError> {
    let data = hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Finalized Dusk event has invalid data: {}",
            bounded_error(error)
        ))
    })?;
    let origin = parse_h256_hex(&event.origin, "finalized event origin")?;
    let mut transaction_id = [0u8; 64];
    transaction_id[32..].copy_from_slice(origin.as_bytes());
    Ok(FinalizedEventMatch {
        provenance: FinalizedEventProvenance {
            block_height: event.block_height,
            block_hash,
            transaction_id: H512::from(transaction_id),
            event_id: event.id,
        },
        data,
    })
}

fn rues_response_error(status: StatusCode, body: &[u8]) -> HyperlaneDuskError {
    HyperlaneDuskError::RuesResponse {
        status: status.as_u16(),
        body: bounded_body(body),
    }
}

/// At most `MAX_ERROR_BODY_BYTES` of a response body, for an error or a log
/// line. Bodies can be megabytes, and an endpoint chooses their content.
pub(crate) fn bounded_body(body: &[u8]) -> String {
    let shown = &body[..body.len().min(MAX_ERROR_BODY_BYTES)];
    truncated_text(&String::from_utf8_lossy(shown), body.len())
}

/// At most `MAX_ERROR_BODY_BYTES` of endpoint-supplied text.
pub(crate) fn bounded_text(text: &str) -> String {
    truncated_text(text, text.len())
}

/// At most `MAX_ERROR_BODY_BYTES` of an endpoint-supplied JSON value.
pub(crate) fn bounded_json(value: &JsonValue) -> String {
    bounded_text(&value.to_string())
}

/// At most `MAX_ERROR_BODY_BYTES` of an error's text. Parse errors quote the
/// input they fail on, such as a string where a number belongs, and an
/// endpoint or a helper process chooses that input.
pub(crate) fn bounded_error(error: impl fmt::Display) -> String {
    bounded_text(&error.to_string())
}

/// Cut `text` to `MAX_ERROR_BODY_BYTES` on a character boundary, and say how
/// long the original was when anything was cut.
fn truncated_text(text: &str, original_len: usize) -> String {
    let mut end = text.len().min(MAX_ERROR_BODY_BYTES);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    if end == text.len() && original_len <= MAX_ERROR_BODY_BYTES {
        return text.to_owned();
    }
    format!("{}... [truncated, {original_len} bytes]", &text[..end])
}

fn transaction_query(tx_id: &str) -> String {
    format!(r#"query {{ tx(hash: "{tx_id}") {{ gasSpent err }} }}"#)
}

fn graphql_errors_present(errors: &JsonValue) -> bool {
    match errors {
        JsonValue::Null => false,
        JsonValue::Array(errors) => !errors.is_empty(),
        _ => true,
    }
}

/// The GraphQL errors a reply carries, if any.
fn graphql_errors(payload: &JsonValue) -> Option<&JsonValue> {
    payload
        .get("errors")
        .filter(|errors| graphql_errors_present(errors))
}

/// The data of a GraphQL reply, or its errors as an error.
fn graphql_data(payload: JsonValue) -> Result<JsonValue, HyperlaneDuskError> {
    if let Some(errors) = graphql_errors(&payload) {
        return Err(HyperlaneDuskError::Other(format!(
            "GraphQL query returned errors: {}",
            bounded_json(errors)
        )));
    }
    // The legacy `/on/graphql/query` route returns the data object directly
    // on success. Accept the canonical `/graphql` envelope too so callers do
    // not accidentally depend on the deprecated response shape.
    Ok(payload.get("data").cloned().unwrap_or(payload))
}

/// Whether a failed `finalizedEvents` request shows that its reply was too
/// large to read: `Some(true)` when the agent's own transport limit refused
/// it, `Some(false)` when the endpoint or a proxy in front of it says so.
fn reply_too_large(error: &HyperlaneDuskError) -> Option<bool> {
    match error {
        HyperlaneDuskError::ResponseTooLarge { .. } => Some(true),
        HyperlaneDuskError::RuesResponse { status, body }
            if *status == StatusCode::PAYLOAD_TOO_LARGE.as_u16()
                || reports_oversized_reply(body) =>
        {
            Some(false)
        }
        _ => None,
    }
}

/// Whether endpoint-supplied error text says a reply was too large, as in
/// "GraphQL query response exceeds 4194304 bytes" or "response too large".
/// Taking an unrelated error for one only costs smaller pages: a scan that
/// gets down to one row reports the error.
fn reports_oversized_reply(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    ["response", "payload", "body"]
        .iter()
        .any(|subject| text.contains(subject))
        && ["exceed", "too large", "too big"]
            .iter()
            .any(|verb| text.contains(verb))
}

fn tx_confirmation_timeout_error(
    tx_id: &str,
    timeout: Duration,
    last_observation_error: Option<&str>,
) -> HyperlaneDuskError {
    let suffix = last_observation_error
        .map(|error| format!("; last observation error: {error}"))
        .unwrap_or_default();
    HyperlaneDuskError::Other(format!(
        "Timed out after {}s waiting for Dusk transaction {tx_id} to be included{suffix}",
        timeout.as_secs()
    ))
}

fn parse_h256_hex(value: &str, field: &str) -> Result<H256, HyperlaneDuskError> {
    let bytes = hex::decode(strip_hex_prefix(value)).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Invalid {field} hex '{}': {}",
            bounded_text(value),
            bounded_error(error)
        ))
    })?;
    if bytes.len() != 32 {
        return Err(HyperlaneDuskError::Other(format!(
            "{field} must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(H256::from_slice(&bytes))
}

fn strip_hex_prefix(value: &str) -> &str {
    value
        .strip_prefix("0x")
        .or_else(|| value.strip_prefix("0X"))
        .unwrap_or(value)
}

async fn read_bounded_body(
    mut response: reqwest::Response,
    context: &'static str,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let too_large = || HyperlaneDuskError::ResponseTooLarge {
        context,
        limit: MAX_RUES_RESPONSE_BYTES,
    };
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RUES_RESPONSE_BYTES as u64)
    {
        return Err(too_large());
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MAX_RUES_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_RUES_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(too_large());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Serialize a value using rkyv.
pub fn rkyv_serialize<T>(value: &T) -> Result<Vec<u8>, HyperlaneDuskError>
where
    T: Serialize<AllocSerializer<256>>,
{
    let mut serializer = AllocSerializer::<256>::default();
    serializer
        .serialize_value(value)
        .map_err(|e| HyperlaneDuskError::Other(format!("rkyv serialization error: {e:?}")))?;
    Ok(serializer.into_serializer().into_inner().to_vec())
}

/// Deserialize a value from rkyv bytes.
pub fn rkyv_deserialize<T>(bytes: &[u8]) -> Result<T, HyperlaneDuskError>
where
    T: Archive,
    T::Archived: Deserialize<T, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
{
    let archived = check_archived_root::<T>(bytes)
        .map_err(|e| HyperlaneDuskError::RkyvDeserialize(bounded_error(e)))?;
    archived
        .deserialize(&mut Infallible)
        .map_err(|e| HyperlaneDuskError::RkyvDeserialize(bounded_text(&format!("{e:?}"))))
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn request_routes_preserve_path_query_and_ignore_fragment() {
        use std::io::{BufRead, BufReader, Read, Write};
        use std::net::TcpListener;
        for suffix in [
            "/rpc",
            "/rpc/",
            "/rpc?token=fixture%2Fvalue",
            "/rpc#label",
            "/rpc/?token=fixture%2Fvalue#label",
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let worker = std::thread::spawn(move || {
                let mut paths = Vec::new();
                for body in [
                    vec![7],
                    br#"{"tx":{"gasSpent":1,"err":null}}"#.to_vec(),
                    br#"{"contract_owner":"fixture-owner"}"#.to_vec(),
                    vec![],
                    vec![],
                    br#"{"balance":5,"nonce":1,"next_nonce":2}"#.to_vec(),
                    br#"{"average":1,"max":2,"median":1,"min":0}"#.to_vec(),
                ] {
                    let (mut stream, _) = listener.accept().unwrap();
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_secs(3)))
                        .unwrap();
                    let mut reader = BufReader::new(&mut stream);
                    let mut first = String::new();
                    reader.read_line(&mut first).unwrap();
                    paths.push(first.split_whitespace().nth(1).unwrap().to_owned());
                    let mut length = 0;
                    loop {
                        let mut header = String::new();
                        assert_ne!(reader.read_line(&mut header).unwrap(), 0);
                        if header == "\r\n" {
                            break;
                        }
                        if let Some(value) =
                            header.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse::<usize>().unwrap();
                        }
                    }
                    let mut input = vec![0; length];
                    reader.read_exact(&mut input).unwrap();
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    )
                    .unwrap();
                    stream.write_all(&body).unwrap();
                }
                paths
            });
            let client =
                RuesClient::new(Url::parse(&format!("http://{address}{suffix}")).unwrap()).unwrap();
            assert_eq!(client.chain_id().await.unwrap(), 7);
            assert_eq!(
                client
                    .wait_for_tx(&"ab".repeat(32), Duration::from_secs(1))
                    .await
                    .unwrap()
                    .gas_spent,
                1
            );
            assert_eq!(
                client
                    .contract_metadata(&[2; 32])
                    .await
                    .unwrap()
                    .contract_owner,
                "fixture-owner"
            );
            client.propagate_tx(&[1]).await.unwrap();
            assert_eq!(client.account_status("11111").await.unwrap().balance, 5);
            assert_eq!(client.gas_price_stats(1).await.unwrap().max, 2);
            let query = if suffix.contains('?') {
                "?token=fixture%2Fvalue"
            } else {
                ""
            };
            assert_eq!(worker.join().unwrap(), vec![
                format!("/rpc/on/contracts:0100000000000000000000000000000000000000000000000000000000000000/chain_id{query}"),
                format!("/rpc/on/graphql/query{query}"),
                format!("/rpc/on/contract:{}/metadata{query}", "02".repeat(32)),
                format!("/rpc/on/transactions/preverify{query}"),
                format!("/rpc/on/transactions/propagate{query}"),
                format!("/rpc/on/account:11111/status{query}"),
                format!("/rpc/on/blocks/gas-price{query}"),
            ], "base suffix: {suffix}");
        }
    }

    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    #[test]
    fn client_debug_never_contains_url_credentials_or_private_components() {
        let client = RuesClient::new(
            Url::parse(
                "https://debug-user:debug-password@rpc.example/private-path?token=query-sentinel",
            )
            .unwrap(),
        )
        .unwrap();
        let rendered = format!("{client:?}");

        assert!(rendered.contains("<redacted>"));
        for secret in [
            "debug-user",
            "debug-password",
            "private-path",
            "query-sentinel",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    fn test_server<T: AsRef<[u8]> + Send + 'static>(responses: Vec<(u16, T)>) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            for (status, body) in responses {
                let body = body.as_ref();
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = Vec::new();
                let mut buffer = [0u8; 4096];
                loop {
                    let count = stream.read(&mut buffer).unwrap();
                    request.extend_from_slice(&buffer[..count]);
                    let Some(header_end) = request.windows(4).position(|v| v == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..header_end]);
                    let content_length = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .and_then(|value| value.trim().parse::<usize>().ok())
                        })
                        .unwrap_or_default();
                    if request.len() >= header_end + 4 + content_length {
                        break;
                    }
                }

                let reason = if status == 200 {
                    "OK"
                } else {
                    "Service Unavailable"
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(body).unwrap();
            }
        });
        Url::parse(&format!("http://{address}")).unwrap()
    }

    pub(super) fn test_client_with_event_store(url: Url, path: &std::path::Path) -> RuesClient {
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut client = RuesClient::new(url).unwrap();
        client.event_store = Some(Arc::new(DB::open(&options, path).unwrap()));
        client
    }

    fn oversized_response_server() -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = Vec::new();
            let mut buffer = [0u8; 4096];
            while !request.windows(4).any(|value| value == b"\r\n\r\n") {
                let count = stream.read(&mut buffer).unwrap();
                request.extend_from_slice(&buffer[..count]);
            }
            write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{{}}",
                MAX_RUES_RESPONSE_BYTES + 1
            )
            .unwrap();
        });
        Url::parse(&format!("http://{address}")).unwrap()
    }

    #[test]
    fn transaction_query_uses_graphql_quotes_without_literal_backslashes() {
        assert_eq!(
            transaction_query("aabbcc"),
            r#"query { tx(hash: "aabbcc") { gasSpent err } }"#
        );
    }

    #[test]
    fn confirmed_transaction_requires_explicit_execution_status() {
        let tx_id = "ab".repeat(32);
        assert!(
            parse_confirmed_transaction(&tx_id, &serde_json::json!({"gasSpent": 7}))
                .unwrap_err()
                .to_string()
                .contains("missing err status")
        );
        assert_eq!(
            parse_confirmed_transaction(&tx_id, &serde_json::json!({"gasSpent": 7, "err": null}))
                .unwrap(),
            ConfirmedTransaction {
                gas_spent: 7,
                error: None,
            }
        );
    }

    #[test]
    fn finalized_height_is_parsed_fail_closed() {
        assert_eq!(
            parse_block_heights(&serde_json::json!({
                "lastBlockPair": {
                    "json": {
                        "last_block": [46, "latest"],
                        "last_finalized_block": [45, "finalized"]
                    }
                }
            }))
            .unwrap(),
            (46, 45)
        );
        assert!(parse_block_heights(&serde_json::json!({
            "lastBlockPair": {
                "json": {
                    "last_block": [45, "latest"],
                    "last_finalized_block": [46, "finalized"]
                }
            }
        }))
        .is_err());
        assert!(parse_block_heights(&serde_json::json!({})).is_err());
    }

    #[test]
    fn block_pairs_carry_both_block_hashes() {
        let reply = |last: &str, finalized: &str| {
            serde_json::json!({"lastBlockPair": {"json": {
                "last_block": [46, last],
                "last_finalized_block": [45, finalized],
            }}})
        };
        assert_eq!(
            parse_block_pair(&reply(&"ab".repeat(32), &"cd".repeat(32))).unwrap(),
            BlockPair {
                latest: 46,
                latest_hash: H256::repeat_byte(0xab),
                finalized: 45,
                finalized_hash: H256::repeat_byte(0xcd),
            }
        );
        assert!(parse_block_pair(&reply("latest", &"cd".repeat(32))).is_err());
        assert!(parse_block_pair(&reply(&"ab".repeat(32), "finalized")).is_err());
    }

    #[tokio::test]
    async fn non_final_blocks_must_extend_the_last_final_block_through_the_tip() {
        let pair = BlockPair {
            latest: 11,
            latest_hash: H256::repeat_byte(11),
            finalized: 10,
            finalized_hash: H256::repeat_byte(10),
        };
        let header = |height: u64, hash: u8, prev: u8| {
            serde_json::json!({"header": {
                "height": height,
                "hash": hex::encode([hash; 32]),
                "prevBlockHash": hex::encode([prev; 32]),
            }})
        };
        for (blocks, expected) in [
            (vec![header(11, 11, 9)], "block 11 does not follow block 10"),
            (
                vec![header(12, 12, 11)],
                "block 12 does not follow block 10",
            ),
            (vec![header(11, 12, 10)], "block 11 was replaced"),
            (vec![], "it was rolled back"),
            (
                (11..=14)
                    .map(|height| header(height, height as u8, height as u8 - 1))
                    .collect(),
                "4 blocks returned",
            ),
        ] {
            let url = test_server(vec![(
                200,
                serde_json::json!({ "blocks": blocks }).to_string(),
            )]);
            let error = RuesClient::new(url)
                .unwrap()
                .non_final_blocks(&pair)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn non_final_block_events_reject_malformed_archive_replies() {
        let block = NonFinalBlock {
            height: 11,
            hash: H256::repeat_byte(11),
        };
        let row = |data: &str| {
            serde_json::json!({
                "origin": "33".repeat(32),
                "topic": "process_id",
                "source": hex::encode([2u8; 32]),
                "data": data,
                "reverted": false,
            })
        };
        for (reply, expected) in [
            (
                serde_json::json!({"contractEvents": null}),
                "missing contractEvents.json",
            ),
            (
                serde_json::json!({"contractEvents": {"json": [{"topic": "process_id"}]}}),
                "Failed to decode",
            ),
            (
                serde_json::json!({"contractEvents": {"json": [row("not hex")]}}),
                "invalid data",
            ),
        ] {
            let url = test_server(vec![(200, reply.to_string())]);
            let error = RuesClient::new(url)
                .unwrap()
                .non_final_block_events(block, &[2; 32], "process_id")
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn the_non_final_event_cache_keeps_its_bound() {
        let mut cache = NonFinalEventCache::default();
        let key = |block: usize| {
            let mut hash = [0u8; 32];
            hash[..8].copy_from_slice(&(block as u64).to_be_bytes());
            (H256::from(hash), [2; 32], "process_id".to_owned())
        };
        for block in 0..=NON_FINAL_EVENT_CACHE_BLOCKS {
            cache.insert(key(block), Arc::new(Vec::new()));
        }
        // Inserting a block again does not grow the cache.
        cache.insert(key(NON_FINAL_EVENT_CACHE_BLOCKS), Arc::new(Vec::new()));
        assert_eq!(cache.entries.len(), NON_FINAL_EVENT_CACHE_BLOCKS);
        assert_eq!(cache.order.len(), NON_FINAL_EVENT_CACHE_BLOCKS);
        assert!(cache.get(&key(0)).is_none());
        assert!(cache.get(&key(1)).is_some());
        assert!(cache.get(&key(NON_FINAL_EVENT_CACHE_BLOCKS)).is_some());
    }

    #[tokio::test]
    async fn confirmation_retries_transport_errors_for_the_same_transaction() {
        let url = test_server(vec![
            (503, "temporarily unavailable"),
            (200, r#"{"errors":[{"message":"archive catching up"}]}"#),
            (200, r#"{"tx":{"gasSpent":42,"err":null}}"#),
        ]);
        let client = RuesClient::new(url).unwrap();
        let tx_id = "ab".repeat(32);
        let confirmed = client
            .wait_for_tx_with_poll_interval(
                &tx_id,
                Duration::from_secs(2),
                Duration::from_millis(1),
            )
            .await
            .unwrap();
        assert_eq!(confirmed.gas_spent, 42);
        assert_eq!(confirmed.error, None);
    }

    #[tokio::test]
    async fn confirmation_rejects_a_malformed_included_transaction() {
        let url = test_server(vec![
            (200, r#"{"tx":{"gasSpent":"not-a-number","err":null}}"#),
            (200, r#"{"tx":{"gasSpent":42,"err":null}}"#),
        ]);
        let client = RuesClient::new(url).unwrap();
        let tx_id = "ab".repeat(32);
        let error = client
            .wait_for_tx_with_poll_interval(
                &tx_id,
                Duration::from_secs(2),
                Duration::from_millis(1),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("missing numeric gasSpent"));
    }

    #[tokio::test]
    async fn graphql_rejects_declared_oversized_responses_before_buffering() {
        let client = RuesClient::new(oversized_response_server()).unwrap();
        let error = client.graphql_query("query { ping }").await.unwrap_err();
        assert!(error.to_string().contains("response exceeds"));
    }

    #[tokio::test]
    async fn finalized_events_use_row_owned_provenance_and_durable_semantic_cache() {
        let contract_id = [7u8; 32];
        let expected_data = vec![1, 2, 3];
        let origin = [9u8; 32];
        let block_hash = [8u8; 32];
        let page = serde_json::json!({
            "finalizedEvents": {
                "json": {
                    "events": [{
                        "id": 11,
                        "block_height": 7,
                        "block_hash": hex::encode(block_hash),
                        "origin": hex::encode(origin),
                        "topic": "dispatch",
                        "source": hex::encode(contract_id),
                        "data": hex::encode(&expected_data),
                        "reverted": false
                    }],
                    "startCursor": "djE6MTE=",
                    "endCursor": "djE6MTE=",
                    "hasNextPage": false
                }
            }
        })
        .to_string();
        let page: &'static str = Box::leak(page.into_boxed_str());
        let url = test_server(vec![(200, page), (200, r#"{"checkBlock":true}"#)]);
        let cursor_dir = tempfile::tempdir().unwrap();
        let store_path = cursor_dir.path().join("event-store");
        let client = test_client_with_event_store(url, &store_path);
        let provenance = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &expected_data)
            .await
            .unwrap();
        assert_eq!(provenance.block_hash, H256::from(block_hash));
        assert_eq!(&provenance.transaction_id.as_bytes()[32..], &origin);
        assert_eq!(provenance.event_id, 0);
        assert!(store_path.join("CURRENT").is_file());
        drop(client);

        // A fresh client can serve an already indexed row without trusting or
        // loading any endpoint-owned pagination cursor. It performs only the
        // row's finalized checkBlock validation.
        let url = test_server(vec![(200, r#"{"checkBlock":true}"#)]);
        let restarted = test_client_with_event_store(url, &store_path);
        let replayed = restarted
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &expected_data)
            .await
            .unwrap();
        assert_eq!(replayed, provenance);
    }

    #[tokio::test]
    async fn reverse_archive_rows_keep_exact_provenance_after_store_reopen() {
        let contract_id = [7u8; 32];
        let page = serde_json::json!({
            "finalizedEvents": { "json": {
                "events": ([2u8, 1u8].into_iter().enumerate().map(|(id, value)| {
                    serde_json::json!({
                        "id": id as i64, "block_height": 7,
                        "block_hash": hex::encode([8u8; 32]),
                        "origin": hex::encode([value; 32]),
                        "topic": "dispatch", "source": hex::encode(contract_id),
                        "data": hex::encode([value]), "reverted": false
                    })
                }).collect::<Vec<_>>()),
                "startCursor": canonical_event_cursor(0),
                "endCursor": canonical_event_cursor(1),
                "hasNextPage": false
            }}
        })
        .to_string();
        let page: &'static str = Box::leak(page.into_boxed_str());
        let directory = tempfile::tempdir().unwrap();
        let store = directory.path().join("event-store");
        // The second lookup matches the block's rows that the first one
        // read, so it only checks the block.
        let client = test_client_with_event_store(
            test_server(vec![
                (200, page),
                (200, r#"{"checkBlock":true}"#),
                (200, r#"{"checkBlock":true}"#),
            ]),
            &store,
        );
        for (sequence, value) in [1u8, 2].into_iter().enumerate() {
            let row = client
                .finalized_contract_event(&contract_id, "dispatch", sequence, 7, &[value])
                .await
                .unwrap();
            assert_eq!(row.event_id, sequence as i64);
            assert_eq!(&row.transaction_id.as_bytes()[32..], &[value; 32]);
            if sequence == 0 {
                assert!(client
                    .load_finalized_event(&contract_id, "dispatch", 1)
                    .unwrap()
                    .is_none());
            }
        }
        drop(client);
        // Reopen the actual store with no archive pages available. Each row
        // still requires its own finalized-block validation on cache replay.
        let reopened = test_client_with_event_store(
            test_server(vec![
                (200, r#"{"checkBlock":true}"#),
                (200, r#"{"checkBlock":true}"#),
            ]),
            &store,
        );
        for (sequence, value) in [(1usize, 2u8), (0, 1)] {
            let row = reopened
                .finalized_contract_event(&contract_id, "dispatch", sequence, 7, &[value])
                .await
                .unwrap();
            assert_eq!(row.event_id, sequence as i64);
            assert_eq!(&row.transaction_id.as_bytes()[32..], &[value; 32]);
        }
    }

    impl RuesClient {
        /// Match one sequence whose contract state implies exactly
        /// `expected_data`.
        async fn finalized_contract_event(
            &self,
            contract_id: &[u8; 32],
            topic: &str,
            sequence: usize,
            expected_block_height: u64,
            expected_data: &[u8],
        ) -> Result<FinalizedEventProvenance, HyperlaneDuskError> {
            let matches = self
                .finalized_contract_events_at_block(
                    contract_id,
                    topic,
                    expected_block_height,
                    &[sequence],
                    exact_event_data(vec![(sequence, expected_data.to_vec())]),
                )
                .await?;
            Ok(matches.into_iter().next().unwrap().provenance)
        }

        fn scan_anchors_of(&self, contract_id: &[u8; 32]) -> Vec<ScanAnchor> {
            let prefix = scan_anchor_prefix(contract_id);
            self.event_store()
                .unwrap()
                .iterator(IteratorMode::From(&prefix, Direction::Forward))
                .map(Result::unwrap)
                .take_while(|(key, _)| key.starts_with(&prefix))
                .map(|(_, value)| ScanAnchor::decode(&value).unwrap())
                .collect()
        }
    }

    pub(super) fn archive_row(
        id: i64,
        height: u64,
        origin: u8,
        topic: &str,
        contract_id: [u8; 32],
        data: &[u8],
    ) -> JsonValue {
        serde_json::json!({
            "id": id, "block_height": height, "block_hash": hex::encode([height as u8; 32]),
            "origin": hex::encode([origin; 32]), "topic": topic,
            "source": hex::encode(contract_id), "data": hex::encode(data), "reverted": false
        })
    }

    /// Read one HTTP request and return its body.
    fn read_request_body(stream: &mut std::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let count = stream.read(&mut buffer).unwrap();
            assert!(count > 0, "request ended early");
            request.extend_from_slice(&buffer[..count]);
            let Some(header_end) = request.windows(4).position(|v| v == b"\r\n\r\n") else {
                continue;
            };
            let length = String::from_utf8_lossy(&request[..header_end])
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .unwrap_or_default();
            if request.len() >= header_end + 4 + length {
                return request[header_end + 4..header_end + 4 + length].to_vec();
            }
        }
    }

    /// An archive endpoint that pages `finalizedEvents` by cursor as Rusk
    /// 1.7.3 does and answers `checkBlock` with `finalized`.
    pub(super) struct Archive {
        pub(super) url: Url,
        pub(super) rows: Arc<StdMutex<Vec<JsonValue>>>,
        /// The cursor ID of each `finalizedEvents` request; `None` asks for
        /// the contract's first rows.
        pages: Arc<StdMutex<Vec<Option<i64>>>>,
        /// The `limit` of each `finalizedEvents` request.
        limits: Arc<StdMutex<Vec<usize>>>,
        /// When set to `(rows, status, body)`, a `finalizedEvents` request
        /// for more than `rows` rows gets this reply instead, as from a node
        /// or proxy that refuses large replies.
        refusal: Arc<StdMutex<Option<(usize, u16, String)>>>,
        /// The block of each `checkBlock` request. The archive has finalized
        /// a block when it holds a row of it and its height is not listed in
        /// `unfinalized`.
        block_checks: Arc<StdMutex<Vec<(u64, String)>>>,
        unfinalized: Arc<StdMutex<HashSet<u64>>>,
        /// The node's last finalized block, which `lastBlockPair` reports
        /// as both its tip and its last final block. The ledger holds every
        /// block up to it, block `h` with hash `[h as u8; 32]` as in
        /// `archive_row`, except the heights in `pruned`.
        pub(super) ledger_tip: Arc<StdMutex<u64>>,
        pruned: Arc<StdMutex<HashSet<u64>>>,
        stop: Arc<std::sync::atomic::AtomicBool>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl Archive {
        pub(super) fn start(rows: Vec<JsonValue>) -> Self {
            use std::sync::atomic::{AtomicBool, Ordering};
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
            let rows = Arc::new(StdMutex::new(rows));
            let pages = Arc::new(StdMutex::new(Vec::new()));
            let limits = Arc::new(StdMutex::new(Vec::new()));
            let refusal = Arc::new(StdMutex::new(None));
            let block_checks = Arc::new(StdMutex::new(Vec::new()));
            let unfinalized = Arc::new(StdMutex::new(HashSet::new()));
            let ledger_tip = Arc::new(StdMutex::new(1000));
            let pruned = Arc::new(StdMutex::new(HashSet::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let worker_rows = rows.clone();
            let worker_pages = pages.clone();
            let worker_limits = limits.clone();
            let worker_refusal = refusal.clone();
            let worker_block_checks = block_checks.clone();
            let worker_unfinalized = unfinalized.clone();
            let worker_ledger_tip = ledger_tip.clone();
            let worker_pruned = pruned.clone();
            let worker_stop = stop.clone();
            let worker = thread::spawn(move || {
                while !worker_stop.load(Ordering::SeqCst) {
                    let Ok((mut stream, _)) = listener.accept() else {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    };
                    stream.set_nonblocking(false).unwrap();
                    // The client may stop reading a reply it refuses.
                    stream
                        .set_write_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let query = String::from_utf8(read_request_body(&mut stream)).unwrap();
                    let (status, reply) = if query.contains("finalizedEvents(") {
                        let limit = Self::limit(&query);
                        worker_limits.lock().unwrap().push(limit);
                        match worker_refusal.lock().unwrap().clone() {
                            Some((rows, status, body)) if limit > rows => (status, body),
                            _ => {
                                let rows = worker_rows.lock().unwrap();
                                let page =
                                    Self::page(&rows, &query, &mut worker_pages.lock().unwrap());
                                (200, page.to_string())
                            }
                        }
                    } else if query.contains("checkBlock(") {
                        let height = query
                            .split_once("height: ")
                            .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
                            .and_then(|height| height.parse::<u64>().ok())
                            .unwrap();
                        let hash = query
                            .split_once("hash: \"")
                            .and_then(|(_, rest)| rest.split('"').next())
                            .unwrap()
                            .to_owned();
                        let finalized = !worker_unfinalized.lock().unwrap().contains(&height)
                            && worker_rows.lock().unwrap().iter().any(|row| {
                                row["block_height"] == height && row["block_hash"] == hash.as_str()
                            });
                        worker_block_checks.lock().unwrap().push((height, hash));
                        (
                            200,
                            serde_json::json!({ "checkBlock": finalized }).to_string(),
                        )
                    } else if let Some(rest) = query.strip_prefix("query { block(height: ") {
                        let height = rest
                            .split(|c: char| !c.is_ascii_digit())
                            .next()
                            .and_then(|height| height.parse::<u64>().ok())
                            .unwrap();
                        let block = (height <= *worker_ledger_tip.lock().unwrap()
                            && !worker_pruned.lock().unwrap().contains(&height))
                        .then(|| {
                            serde_json::json!({"header": {"hash": hex::encode([height as u8; 32])}})
                        });
                        (200, serde_json::json!({ "block": block }).to_string())
                    } else if query == "query { lastBlockPair { json } }" {
                        let tip = *worker_ledger_tip.lock().unwrap();
                        let block = serde_json::json!([tip, hex::encode([tip as u8; 32])]);
                        (
                            200,
                            serde_json::json!({"lastBlockPair": {"json": {
                                "last_block": block,
                                "last_finalized_block": block,
                            }}})
                            .to_string(),
                        )
                    } else {
                        (
                            200,
                            serde_json::json!({"errors": [{"message": "unexpected query"}]})
                                .to_string(),
                        )
                    };
                    let _ = write!(
                        stream,
                        "HTTP/1.1 {status} Reply\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{reply}",
                        reply.len()
                    );
                }
            });
            Self {
                url,
                rows,
                pages,
                limits,
                refusal,
                block_checks,
                unfinalized,
                ledger_tip,
                pruned,
                stop,
                worker: Some(worker),
            }
        }

        /// The `limit` a `finalizedEvents` query asks for.
        fn limit(query: &str) -> usize {
            query
                .split_once("limit: ")
                .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|limit| limit.parse::<usize>().ok())
                .unwrap_or(50)
        }

        fn page(rows: &[JsonValue], query: &str, pages: &mut Vec<Option<i64>>) -> JsonValue {
            let source = query
                .split_once("contractId: \"")
                .and_then(|(_, rest)| rest.split('"').next())
                .unwrap();
            let limit = Self::limit(query).clamp(1, 200);
            let after = query
                .split_once("cursor: \"")
                .and_then(|(_, rest)| rest.split('"').next())
                .map(|cursor| decode_event_cursor(cursor).unwrap());
            pages.push(after);
            let mut events: Vec<JsonValue> = rows
                .iter()
                .filter(|row| row["source"] == source)
                .filter(|row| row["id"].as_i64().unwrap() > after.unwrap_or(-1))
                .take(limit + 1)
                .cloned()
                .collect();
            let has_next_page = events.len() > limit;
            events.truncate(limit);
            let cursor = |row: Option<&JsonValue>| {
                row.map(|row| canonical_event_cursor(row["id"].as_i64().unwrap()))
            };
            serde_json::json!({"finalizedEvents": {"json": {
                "startCursor": cursor(events.first()),
                "endCursor": cursor(events.last()),
                "hasNextPage": has_next_page,
                "events": events,
            }}})
        }

        pub(super) fn pages(&self) -> Vec<Option<i64>> {
            self.pages.lock().unwrap().clone()
        }

        fn limits(&self) -> Vec<usize> {
            self.limits.lock().unwrap().clone()
        }

        pub(super) fn checked_heights(&self) -> Vec<u64> {
            self.block_checks
                .lock()
                .unwrap()
                .iter()
                .map(|(height, _)| *height)
                .collect()
        }
    }

    impl Drop for Archive {
        fn drop(&mut self) {
            self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
            if let Some(worker) = self.worker.take() {
                let _ = worker.join();
            }
        }
    }

    #[tokio::test]
    async fn a_caught_up_scan_resumes_after_its_last_row() {
        let contract_id = [7u8; 32];
        let archive = Archive::start(vec![archive_row(11, 7, 9, "dispatch", contract_id, &[1])]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();

        archive
            .rows
            .lock()
            .unwrap()
            .push(archive_row(12, 8, 11, "dispatch", contract_id, &[2]));
        let later = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 8, &[2])
            .await
            .unwrap();
        assert_eq!(later.event_id, 1);
        // The second scan starts with the first scan's last row, which it
        // checks is unchanged, instead of at the contract's first row.
        assert_eq!(archive.pages(), vec![None, Some(10)]);
    }

    #[tokio::test]
    async fn scan_positions_survive_a_restart_and_stay_sparse() {
        let contract_id = [7u8; 32];
        let mut rows: Vec<JsonValue> = (0..450)
            .map(|id| archive_row(id, 1 + id as u64 / 10, 1, "other", contract_id, &[0]))
            .collect();
        rows.push(archive_row(450, 100, 2, "dispatch", contract_id, &[1]));
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 100, &[1])
            .await
            .unwrap();
        assert_eq!(archive.pages(), vec![None, Some(199), Some(399)]);
        // About one anchor per page of rows, plus the scan's end.
        assert_eq!(
            client
                .scan_anchors_of(&contract_id)
                .iter()
                .map(|anchor| (anchor.rank, anchor.before_block))
                .collect::<Vec<_>>(),
            vec![(200, 21), (400, 41), (451, 101)]
        );
        drop(client);

        archive
            .rows
            .lock()
            .unwrap()
            .push(archive_row(451, 101, 3, "dispatch", contract_id, &[2]));
        let restarted = test_client_with_event_store(archive.url.clone(), directory.path());
        restarted
            .finalized_contract_event(&contract_id, "dispatch", 1, 101, &[2])
            .await
            .unwrap();
        assert_eq!(archive.pages()[3..], [Some(449)]);

        // A lookup for an earlier block starts from the closest anchor below
        // it, not from the contract's first row.
        restarted
            .finalized_contract_event(&contract_id, "other", 7, 42, &[0])
            .await
            .unwrap();
        assert_eq!(archive.pages()[4..], [Some(398)]);
    }

    #[tokio::test]
    async fn tip_following_keeps_one_anchor_per_page_of_rows() {
        let contract_id = [8u8; 32];
        let archive = Archive::start(Vec::new());
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        for sequence in 0..450u64 {
            archive.rows.lock().unwrap().push(archive_row(
                sequence as i64,
                10 + sequence,
                1,
                "dispatch",
                contract_id,
                &sequence.to_be_bytes(),
            ));
            client
                .finalized_contract_event(
                    &contract_id,
                    "dispatch",
                    sequence as usize,
                    10 + sequence,
                    &sequence.to_be_bytes(),
                )
                .await
                .unwrap();
        }
        // Each lookup resumed at the previous one's last row.
        let pages = archive.pages();
        assert_eq!(pages.len(), 450);
        assert!(pages[1..]
            .iter()
            .enumerate()
            .all(|(previous, cursor)| *cursor == previous.checked_sub(1).map(|id| id as i64)));
        assert_eq!(
            client
                .scan_anchors_of(&contract_id)
                .iter()
                .map(|anchor| anchor.rank)
                .collect::<Vec<_>>(),
            vec![200, 400, 450]
        );
    }

    #[test]
    fn finalized_event_pages_reject_cursor_rollback_and_wrong_source() {
        let contract_id = [7u8; 32];
        let event = FinalizedContractEvent {
            id: 10,
            block_height: 7,
            block_hash: hex::encode([8u8; 32]),
            origin: hex::encode([9u8; 32]),
            topic: "dispatch".into(),
            source: hex::encode([6u8; 32]),
            data: String::new(),
            reverted: false,
        };
        let page = FinalizedEventPage {
            events: vec![event],
            start_cursor: Some("same".into()),
            end_cursor: Some("same".into()),
            has_next_page: true,
        };
        assert!(validate_finalized_event_page(
            &contract_id,
            Some("same"),
            Some(10),
            FINALIZED_EVENT_PAGE_SIZE,
            &page
        )
        .is_err());
    }

    #[tokio::test]
    async fn a_poisoned_scan_position_cannot_hide_rows_after_restart() {
        let contract_id = [7u8; 32];
        let archive = Archive::start(vec![archive_row(
            i64::MAX,
            7,
            9,
            "dispatch",
            contract_id,
            &[1],
        )]);
        let directory = tempfile::tempdir().unwrap();
        let poisoned = test_client_with_event_store(archive.url.clone(), directory.path());
        let first = poisoned
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert_eq!(
            first.event_id, 0,
            "provenance uses the local topic sequence"
        );
        drop(poisoned);

        *archive.rows.lock().unwrap() = vec![
            archive_row(11, 7, 9, "dispatch", contract_id, &[1]),
            archive_row(12, 8, 11, "dispatch", contract_id, &[2]),
        ];
        let repaired = test_client_with_event_store(archive.url.clone(), directory.path());
        let later = repaired
            .finalized_contract_event(&contract_id, "dispatch", 1, 8, &[2])
            .await
            .unwrap();
        assert_eq!(later.event_id, 1);
        // The persisted position names a row the repaired archive does not
        // have, and the archive has finalized that row's block 7, so the
        // scan starts over instead of skipping its rows.
        assert_eq!(archive.pages(), vec![None, Some(i64::MAX - 1), None]);
        assert_eq!(archive.checked_heights(), [7, 7, 8]);
    }

    #[tokio::test]
    async fn unrequested_page_peer_cannot_poison_durable_semantic_rows() {
        let contract_id = [17u8; 32];
        let archive = Archive::start(vec![
            archive_row(11, 7, 9, "dispatch", contract_id, &[1]),
            archive_row(12, 7, 9, "dispatch", contract_id, &[1]),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let poisoned = test_client_with_event_store(archive.url.clone(), directory.path());
        poisoned
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert!(poisoned
            .load_finalized_event(&contract_id, "dispatch", 1)
            .unwrap()
            .is_none());
        drop(poisoned);

        // The archive is rebuilt with other row IDs.
        *archive.rows.lock().unwrap() = vec![
            archive_row(21, 7, 9, "dispatch", contract_id, &[1]),
            archive_row(22, 8, 11, "dispatch", contract_id, &[2]),
        ];
        let repaired = test_client_with_event_store(archive.url.clone(), directory.path());
        let later = repaired
            .finalized_contract_event(&contract_id, "dispatch", 1, 8, &[2])
            .await
            .unwrap();
        assert_eq!(later.event_id, 1);
        // The first scan read all of block 7, so its position is row 12.
        assert_eq!(archive.pages(), vec![None, Some(11), None]);
    }

    #[tokio::test]
    async fn repaired_endpoint_can_replace_an_exact_poisoned_cached_row() {
        let contract_id = [37u8; 32];
        let archive = Archive::start(vec![
            archive_row(1, 9, 44, "dispatch", contract_id, &[41]),
            archive_row(2, 10, 46, "dispatch", contract_id, &[42]),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let compromised = test_client_with_event_store(archive.url.clone(), directory.path());
        compromised
            .finalized_contract_event(&contract_id, "dispatch", 1, 10, &[42])
            .await
            .unwrap();
        drop(compromised);

        *archive.rows.lock().unwrap() = vec![
            archive_row(11, 19, 54, "dispatch", contract_id, &[51]),
            archive_row(12, 20, 56, "dispatch", contract_id, &[52]),
        ];
        let repaired = test_client_with_event_store(archive.url.clone(), directory.path());
        let error = repaired
            .finalized_contract_event(&contract_id, "dispatch", 1, 20, &[52])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("contract state says 20"));
        // The mismatch dropped the row and the endpoint's scan positions.
        assert!(repaired.scan_anchors_of(&contract_id).is_empty());

        let replaced = repaired
            .finalized_contract_event(&contract_id, "dispatch", 1, 20, &[52])
            .await
            .unwrap();
        assert_eq!(replaced.event_id, 1);
        assert_eq!(replaced.block_height, 20);
        assert_eq!(archive.pages(), vec![None, None]);
    }

    #[tokio::test]
    async fn one_block_resolves_every_sequence_in_one_pass() {
        let contract_id = [57u8; 32];
        // 300 rows of one block in hash order, with reverted and foreign-topic
        // peers, after a page of earlier rows.
        let mut rows: Vec<JsonValue> = (0..200)
            .map(|id| archive_row(id, 5, 1, "other", contract_id, &[0]))
            .collect();
        for position in 0..300u16 {
            let sequence = (position * 7) % 300;
            let mut row = archive_row(
                200 + i64::from(position),
                6,
                (sequence % 251) as u8,
                if position % 10 == 3 {
                    "other"
                } else {
                    "dispatch"
                },
                contract_id,
                &sequence.to_be_bytes(),
            );
            row["reverted"] = serde_json::json!(position % 10 == 7);
            rows.push(row);
        }
        let wanted: Vec<usize> = (0..300usize)
            .filter(|sequence| {
                let position = (0..300u16)
                    .find(|position| usize::from((position * 7) % 300) == *sequence)
                    .unwrap();
                position % 10 != 3 && position % 10 != 7
            })
            .collect();
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let expected = wanted
            .iter()
            .map(|sequence| (*sequence, (*sequence as u16).to_be_bytes().to_vec()))
            .collect();
        let matches = client
            .finalized_contract_events_at_block(
                &contract_id,
                "dispatch",
                6,
                &wanted,
                exact_event_data(expected),
            )
            .await
            .unwrap();
        assert_eq!(matches.len(), wanted.len());
        for (sequence, matched) in wanted.iter().zip(&matches) {
            assert_eq!(matched.data, (*sequence as u16).to_be_bytes());
            assert_eq!(matched.provenance.event_id, *sequence as i64);
        }
        assert_eq!(archive.pages(), vec![None, Some(199), Some(399)]);
        for sequence in &wanted {
            assert!(client
                .load_finalized_event(&contract_id, "dispatch", *sequence)
                .unwrap()
                .is_some());
        }

        // A reverted row's sequence stays unmatched, and the error names it.
        let reverted = (0..300usize)
            .find(|sequence| !wanted.contains(sequence))
            .unwrap();
        let error = client
            .finalized_contract_events_at_block(
                &contract_id,
                "dispatch",
                6,
                &[reverted],
                exact_event_data(vec![(reverted, (reverted as u16).to_be_bytes().to_vec())]),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not archived yet"), "{error}");
    }

    #[tokio::test]
    async fn identical_records_in_one_block_share_the_first_matching_row() {
        let contract_id = [67u8; 32];
        let archive = Archive::start(vec![
            archive_row(1, 9, 1, "gas_payment", contract_id, &[5]),
            archive_row(2, 9, 2, "gas_payment", contract_id, &[5]),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let matches = client
            .finalized_contract_events_at_block(
                &contract_id,
                "gas_payment",
                9,
                &[3, 4],
                exact_event_data(vec![(3, vec![5]), (4, vec![5])]),
            )
            .await
            .unwrap();
        for (sequence, matched) in [3, 4].into_iter().zip(matches) {
            assert_eq!(matched.provenance.event_id, sequence);
            assert_eq!(
                &matched.provenance.transaction_id.as_bytes()[32..],
                &[1u8; 32]
            );
        }
    }

    #[tokio::test]
    async fn one_topic_scan_cannot_skip_an_earlier_unrequested_topic() {
        let contract_id = [27u8; 32];
        let process_data = vec![3u8];
        let dispatch_data = vec![4u8];
        let page = serde_json::json!({
            "finalizedEvents": { "json": {
                "events": [{
                    "id": 1,
                    "block_height": 9,
                    "block_hash": hex::encode([28u8; 32]),
                    "origin": hex::encode([29u8; 32]),
                    "topic": "process",
                    "source": hex::encode(contract_id),
                    "data": hex::encode(&process_data),
                    "reverted": false
                }, {
                    "id": 2,
                    "block_height": 10,
                    "block_hash": hex::encode([30u8; 32]),
                    "origin": hex::encode([31u8; 32]),
                    "topic": "dispatch",
                    "source": hex::encode(contract_id),
                    "data": hex::encode(&dispatch_data),
                    "reverted": false
                }],
                "startCursor": canonical_event_cursor(1),
                "endCursor": canonical_event_cursor(2),
                "hasNextPage": false
            }}
        })
        .to_string();
        let first_page: &'static str = Box::leak(page.clone().into_boxed_str());
        let second_page: &'static str = Box::leak(page.into_boxed_str());
        let cursor_dir = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(
            test_server(vec![
                (200, first_page),
                (200, r#"{"checkBlock":true}"#),
                (200, second_page),
                (200, r#"{"checkBlock":true}"#),
            ]),
            cursor_dir.path(),
        );

        let dispatch = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 10, &dispatch_data)
            .await
            .unwrap();
        let process = client
            .finalized_contract_event(&contract_id, "process", 0, 9, &process_data)
            .await
            .unwrap();
        assert_eq!(dispatch.event_id, 0);
        assert_eq!(process.event_id, 0);
    }

    #[tokio::test]
    async fn invalid_transaction_ids_fail_before_network_access() {
        let client = RuesClient::new(Url::parse("http://127.0.0.1:1").unwrap()).unwrap();
        let error = client
            .wait_for_tx("not-a-hash", Duration::from_millis(1))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("Invalid Dusk transaction ID"));
    }

    #[tokio::test]
    async fn transient_block_checks_keep_durable_rows_until_revalidation() {
        let contract = [3u8; 32];
        let event = FinalizedContractEvent {
            id: 80,
            block_height: 90,
            block_hash: hex::encode([4; 32]),
            origin: hex::encode([5; 32]),
            topic: "dispatch".into(),
            source: hex::encode(contract),
            data: hex::encode([6]),
            reverted: false,
        };
        let anchor = ScanAnchor {
            before_block: 90,
            fence_id: 79,
            fence_digest: [1; 32],
            rank: 1,
            fence_block: 89,
            fence_block_hash: [2; 32],
        };
        for response in [
            (503, "temporarily unavailable"),
            (200, "{}"),
            (200, "not-json"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let client = test_client_with_event_store(
                test_server(vec![response, (200, r#"{"checkBlock":true}"#)]),
                directory.path(),
            );
            client
                .persist_finalized_events(&contract, "dispatch", &[(80, &event)])
                .unwrap();
            client.offer_scan_anchor(&contract, anchor).unwrap();
            assert!(client
                .finalized_contract_event(&contract, "dispatch", 80, 90, &[6])
                .await
                .is_err());
            // A failed check is not evidence against the row or the anchors.
            assert!(client
                .load_finalized_event(&contract, "dispatch", 80)
                .unwrap()
                .is_some());
            assert_eq!(client.scan_anchors_of(&contract), vec![anchor]);
            // This server has only a checkBlock response left: no archive replay.
            let recovered = client
                .finalized_contract_event(&contract, "dispatch", 80, 90, &[6])
                .await
                .unwrap();
            assert_eq!(recovered.event_id, 80);
            assert_eq!(recovered.block_height, 90);
        }
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(
            test_server(vec![(200, r#"{"checkBlock":false}"#)]),
            directory.path(),
        );
        client
            .persist_finalized_events(&contract, "dispatch", &[(80, &event)])
            .unwrap();
        client.offer_scan_anchor(&contract, anchor).unwrap();
        assert!(matches!(
            client
                .finalized_contract_event(&contract, "dispatch", 80, 90, &[6])
                .await,
            Err(HyperlaneDuskError::FinalizedEventMismatch(_))
        ));
        assert!(client
            .load_finalized_event(&contract, "dispatch", 80)
            .unwrap()
            .is_none());
        assert!(client.scan_anchors_of(&contract).is_empty());
    }

    #[tokio::test]
    async fn predicate_rows_return_their_data_and_are_dropped_once_they_stop_matching() {
        let contract_id = [47u8; 32];
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[1, 0]),
            archive_row(2, 7, 2, "dispatch", contract_id, &[2, 0]),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let tagged = |tag: u8| {
            move |data: &[u8]| {
                if data.first() == Some(&tag) {
                    vec![0]
                } else {
                    Vec::new()
                }
            }
        };
        let lookup = |tag| {
            client.finalized_contract_events_at_block(
                &contract_id,
                "dispatch",
                7,
                &[0],
                tagged(tag),
            )
        };

        // The first row the predicate accepts becomes the record, data included.
        let first = lookup(2).await.unwrap().remove(0);
        assert_eq!(first.data, vec![2, 0]);
        assert_eq!(
            &first.provenance.transaction_id.as_bytes()[32..],
            &[2u8; 32]
        );
        assert_eq!(first.provenance.event_id, 0);

        // The persisted row is served again only after passing the predicate
        // and a fresh checkBlock, without reading the archive.
        let replayed = lookup(2).await.unwrap().remove(0);
        assert_eq!(replayed, first);
        assert_eq!(archive.pages(), vec![None]);

        // Contract state now implies other data. The persisted row is dropped
        // without being served, together with the contract's scan anchors.
        let error = lookup(1).await.unwrap_err();
        assert!(matches!(
            error,
            HyperlaneDuskError::FinalizedEventMismatch(_)
        ));
        assert!(client
            .load_finalized_event(&contract_id, "dispatch", 0)
            .unwrap()
            .is_none());
        assert!(client.scan_anchors_of(&contract_id).is_empty());

        // The retry rebuilds the record from the archive.
        let rebuilt = lookup(1).await.unwrap().remove(0);
        assert_eq!(rebuilt.data, vec![1, 0]);
        assert_eq!(
            &rebuilt.provenance.transaction_id.as_bytes()[32..],
            &[1u8; 32]
        );
        assert_eq!(archive.pages(), vec![None, None]);
    }

    #[tokio::test]
    async fn error_bodies_are_cut_before_they_reach_errors() {
        let body = "x".repeat(64 * 1024);
        let client = RuesClient::new(test_server(vec![
            (503, body.clone()),
            (200, body.clone()),
            (200, format!(r#"{{"errors":[{{"message":"{body}"}}]}}"#)),
        ]))
        .unwrap();
        let errors = [
            client
                .contract_query_raw(&[1; 32], "nonce", &[])
                .await
                .unwrap_err(),
            client.graphql_query("query { ping }").await.unwrap_err(),
            client.graphql_query("query { ping }").await.unwrap_err(),
        ];
        for error in errors {
            let error = error.to_string();
            assert!(
                error.len() < 2 * MAX_ERROR_BODY_BYTES,
                "{} bytes",
                error.len()
            );
            assert!(error.contains("[truncated, "), "{error}");
        }
    }

    #[tokio::test]
    async fn oversized_strings_in_typed_replies_stay_out_of_errors() {
        // A string where a number belongs: parse errors quote it.
        let huge = "x".repeat(1024 * 1024);
        let contract_id = [7u8; 32];
        let page = serde_json::json!({"finalizedEvents": {"json": {
            "events": [{
                "id": huge, "block_height": 7, "block_hash": hex::encode([8u8; 32]),
                "origin": hex::encode([9u8; 32]), "topic": "dispatch",
                "source": hex::encode(contract_id), "data": "01", "reverted": false
            }],
            "startCursor": canonical_event_cursor(11),
            "endCursor": canonical_event_cursor(11),
            "hasNextPage": false
        }}})
        .to_string();
        let account = serde_json::json!({"balance": huge, "nonce": 1, "next_nonce": 2}).to_string();
        let gas = serde_json::json!({"average": huge, "max": 2, "median": 1, "min": 0}).to_string();
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(
            test_server(vec![(200, page), (200, account), (200, gas)]),
            directory.path(),
        );
        let errors = [
            client
                .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
                .await
                .unwrap_err(),
            client.account_status("11111").await.unwrap_err(),
            client.gas_price_stats(1).await.unwrap_err(),
        ];
        for error in errors {
            let error = error.to_string();
            // The parse error and, for account and gas replies, the body,
            // each cut to the limit.
            assert!(
                error.len() < 3 * MAX_ERROR_BODY_BYTES,
                "{} bytes",
                error.len()
            );
            assert!(error.contains("invalid type: string"), "{error}");
            assert!(error.contains("[truncated, "), "{error}");
        }

        // A damaged row in the local event store is reported the same way.
        client
            .event_store()
            .unwrap()
            .put(
                finalized_event_row_key(&contract_id, "dispatch", 3).unwrap(),
                serde_json::json!({ "id": huge }).to_string(),
            )
            .unwrap();
        let error = client
            .load_finalized_event(&contract_id, "dispatch", 3)
            .unwrap_err()
            .to_string();
        assert!(
            error.len() < 2 * MAX_ERROR_BODY_BYTES,
            "{} bytes",
            error.len()
        );
        assert!(error.contains("[truncated, "), "{error}");
    }

    #[test]
    fn bounded_text_cuts_on_a_character_boundary() {
        assert_eq!(bounded_text("short"), "short");
        let text = "\u{20ac}".repeat(MAX_ERROR_BODY_BYTES);
        let bounded = bounded_text(&text);
        assert!(bounded.starts_with(&"\u{20ac}".repeat(MAX_ERROR_BODY_BYTES / 3)));
        assert!(bounded.ends_with(&format!("... [truncated, {} bytes]", text.len())));
        // Bytes that are not UTF-8 decode to wider replacement characters;
        // the result is still bounded.
        let bounded = bounded_body(&vec![0xff; 4 * MAX_ERROR_BODY_BYTES]);
        assert!(
            bounded.len() <= MAX_ERROR_BODY_BYTES + 32,
            "{}",
            bounded.len()
        );
        assert!(bounded.ends_with(&format!("[truncated, {} bytes]", 4 * MAX_ERROR_BODY_BYTES)));
    }

    #[test]
    fn scan_anchors_round_trip_and_reject_malformed_values() {
        let anchor = ScanAnchor {
            before_block: 9,
            fence_id: 7,
            fence_digest: [3; 32],
            rank: 5,
            fence_block: 8,
            fence_block_hash: [4; 32],
        };
        assert_eq!(ScanAnchor::decode(&anchor.encode()), Some(anchor));
        assert_eq!(ScanAnchor::decode(&anchor.encode()[1..]), None);
        for malformed in [
            ScanAnchor {
                fence_id: -1,
                ..anchor
            },
            ScanAnchor { rank: 0, ..anchor },
            // Its fence row would not be below the position it names.
            ScanAnchor {
                fence_block: 9,
                ..anchor
            },
        ] {
            assert_eq!(ScanAnchor::decode(&malformed.encode()), None);
        }
    }

    #[tokio::test]
    async fn identity_binds_the_required_hook_topology_and_announce_to_one_mailbox() {
        let mailbox = [3u8; 32];
        let announce = [4u8; 32];
        let merkle = [5u8; 32];
        let aggregate = [6u8; 32];
        let foreign = [7u8; 32];
        let prefix = || {
            vec![
                (200, vec![1]),
                (200, rkyv_serialize(&4242u32).unwrap()),
                (200, rkyv_serialize(&4242u32).unwrap()),
                (200, rkyv_serialize(&MAILBOX_STATE_VERSION).unwrap()),
                (
                    200,
                    rkyv_serialize(&VALIDATOR_ANNOUNCE_STATE_VERSION).unwrap(),
                ),
                (200, rkyv_serialize(&mailbox).unwrap()),
            ]
        };
        let mut direct = prefix();
        direct.extend([
            (200, rkyv_serialize(&merkle).unwrap()),
            (200, vec![3]),
            (
                200,
                rkyv_serialize(&MERKLE_TREE_HOOK_STATE_VERSION).unwrap(),
            ),
            (200, rkyv_serialize(&mailbox).unwrap()),
        ]);
        RuesClient::new(test_server(direct))
            .unwrap()
            .validate_chain_identity(1, 4242, &mailbox, &announce, &merkle)
            .await
            .unwrap();
        let aggregated = || {
            let mut responses = prefix();
            responses.extend([
                (200, rkyv_serialize(&aggregate).unwrap()),
                (200, vec![3]),
                (
                    200,
                    rkyv_serialize(&MERKLE_TREE_HOOK_STATE_VERSION).unwrap(),
                ),
                (200, vec![2]),
                (200, rkyv_serialize(&mailbox).unwrap()),
                (200, rkyv_serialize(&vec![merkle, foreign]).unwrap()),
                (200, rkyv_serialize(&aggregate).unwrap()),
            ]);
            responses
        };
        RuesClient::new(test_server(aggregated()))
            .unwrap()
            .validate_chain_identity(1, 4242, &mailbox, &announce, &merkle)
            .await
            .unwrap();
        for (position, bytes, diagnostic) in [
            (
                3,
                rkyv_serialize(&4u32).unwrap(),
                "Dusk Mailbox state_version 4 is not supported: this agent needs state_version 5",
            ),
            (
                3,
                rkyv_serialize(&6u32).unwrap(),
                "Dusk Mailbox state_version 6 is not supported",
            ),
            (
                4,
                rkyv_serialize(&1u32).unwrap(),
                "Dusk ValidatorAnnounce state_version 1 is not supported: this agent needs state_version 2",
            ),
            (
                5,
                rkyv_serialize(&foreign).unwrap(),
                "ValidatorAnnounce mailbox",
            ),
            (7, vec![2], "MerkleTree hook_type"),
            (
                8,
                rkyv_serialize(&1u32).unwrap(),
                "Dusk MerkleTreeHook state_version 1 is not supported: this agent needs state_version 2",
            ),
            (9, vec![1], "required_hook"),
            (
                10,
                rkyv_serialize(&foreign).unwrap(),
                "required aggregation hook mailbox",
            ),
            (11, rkyv_serialize(&vec![foreign]).unwrap(), "exactly once"),
            (
                11,
                rkyv_serialize(&vec![merkle, merkle]).unwrap(),
                "exactly once",
            ),
            (
                12,
                rkyv_serialize(&mailbox).unwrap(),
                "MerkleTreeHook mailbox",
            ),
        ] {
            let mut responses = aggregated();
            responses[position].1 = bytes;
            responses.truncate(position + 1);
            let error = RuesClient::new(test_server(responses))
                .unwrap()
                .validate_chain_identity(1, 4242, &mailbox, &announce, &merkle)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(diagnostic), "{error}");
        }
    }

    #[tokio::test]
    async fn contracts_of_another_state_version_are_refused() {
        let mailbox = [3u8; 32];
        let identity = |state_version: (u16, Vec<u8>)| {
            RuesClient::new(test_server(vec![
                (200, vec![1]),
                (200, rkyv_serialize(&4242u32).unwrap()),
                (200, rkyv_serialize(&4242u32).unwrap()),
                state_version,
            ]))
            .unwrap()
        };
        // Version 4 stored the processed-message log and root_at that this
        // agent no longer reads, and the Mailbox dropped them in version 5.
        for version in [0u32, 1, 3, 4, 6] {
            let error = identity((200, rkyv_serialize(&version).unwrap()))
                .validate_chain_identity(1, 4242, &mailbox, &[4u8; 32], &[5u8; 32])
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("state_version {version} is not supported"))
                    && error.contains("needs state_version 5"),
                "{error}"
            );
        }
        // A Mailbox from before state_version existed has no such query.
        let error = identity((500, br#"{"error":"function not found"}"#.to_vec()))
            .validate_chain_identity(1, 4242, &mailbox, &[4u8; 32], &[5u8; 32])
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("Failed to query Dusk Mailbox state_version")
                && error.contains("needs state_version 5"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn requests_wait_until_the_endpoint_serves_the_configured_deployment() {
        let identity = DeploymentIdentity {
            chain_id: 1,
            domain: 4242,
            mailbox: [3; 32],
            validator_announce: [4; 32],
            merkle_tree_hook: [5; 32],
        };
        // The node is unavailable for the first check, then serves the
        // deployment with its Merkle hook attached directly.
        let mut responses = vec![(503, b"unavailable".to_vec())];
        responses.extend([
            (200, vec![1]),
            (200, rkyv_serialize(&4242u32).unwrap()),
            (200, rkyv_serialize(&4242u32).unwrap()),
            (200, rkyv_serialize(&MAILBOX_STATE_VERSION).unwrap()),
            (
                200,
                rkyv_serialize(&VALIDATOR_ANNOUNCE_STATE_VERSION).unwrap(),
            ),
            (200, rkyv_serialize(&[3u8; 32]).unwrap()),
            (200, rkyv_serialize(&[5u8; 32]).unwrap()),
            (200, vec![3]),
            (
                200,
                rkyv_serialize(&MERKLE_TREE_HOOK_STATE_VERSION).unwrap(),
            ),
            (200, rkyv_serialize(&[3u8; 32]).unwrap()),
        ]);
        responses.extend([(200, vec![42]), (200, vec![43])]);
        // Building the client sends nothing.
        let client = RuesClient::new(test_server(responses))
            .unwrap()
            .with_identity(identity);
        // The query is not sent while the check fails.
        let error = client
            .contract_query_raw(&[9; 32], "nonce", &[])
            .await
            .unwrap_err();
        assert!(
            matches!(error, HyperlaneDuskError::IdentityUnverified(_)),
            "{error}"
        );
        // The next request checks again, then is sent.
        assert_eq!(
            client
                .contract_query_raw(&[9; 32], "nonce", &[])
                .await
                .unwrap(),
            vec![42]
        );
        // Clones, and clients that require the same deployment, share the
        // outcome: no check precedes this query.
        assert_eq!(
            client
                .with_identity(identity)
                .contract_query_raw(&[9; 32], "nonce", &[])
                .await
                .unwrap(),
            vec![43]
        );
    }

    #[tokio::test]
    async fn endpoint_chain_id_mismatch_fails_before_contract_queries() {
        let client = RuesClient::new(test_server(vec![(200, "\u{2}")])).unwrap();
        let error = client
            .validate_chain_identity(1, 4242, &[3u8; 32], &[4u8; 32], &[5u8; 32])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match endpoint chain ID 2"));
    }

    #[test]
    fn page_size_halves_to_one_row_and_grows_back_after_small_full_pages() {
        let mut size = PageSize::largest();
        let mut sizes = vec![size.rows()];
        while size.shrink(true) {
            sizes.push(size.rows());
        }
        assert_eq!(sizes, [200, 100, 50, 25, 12, 6, 3, 1]);
        // A full page that used at most a quarter of the limit doubles the
        // size; a larger reply or a short page does not.
        size.after_page(1, MAX_RUES_RESPONSE_BYTES / 4);
        assert_eq!(size.rows(), 2);
        size.after_page(2, MAX_RUES_RESPONSE_BYTES / 4 + 1);
        size.after_page(1, 10);
        assert_eq!(size.rows(), 2);
        for _ in 0..10 {
            size.after_page(size.rows(), 10);
        }
        assert_eq!(size.rows(), FINALIZED_EVENT_PAGE_SIZE);
        // An endpoint that refuses replies does not say how much it accepts,
        // so the size no longer grows.
        assert!(size.shrink(false));
        size.after_page(size.rows(), 10);
        assert_eq!(size.rows(), FINALIZED_EVENT_PAGE_SIZE / 2);
    }

    #[test]
    fn oversized_replies_are_told_apart_from_other_failures() {
        let ours = HyperlaneDuskError::ResponseTooLarge {
            context: "GraphQL query",
            limit: MAX_RUES_RESPONSE_BYTES,
        };
        assert_eq!(
            ours.to_string(),
            "GraphQL query response exceeds 4194304 bytes"
        );
        assert_eq!(reply_too_large(&ours), Some(true));
        for (status, body) in [
            (413, "Payload Too Large"),
            (500, "GraphQL query response exceeds 4194304 bytes"),
            (502, "upstream response too large"),
        ] {
            let error = HyperlaneDuskError::RuesResponse {
                status,
                body: body.into(),
            };
            assert_eq!(reply_too_large(&error), Some(false), "{error}");
        }
        for error in [
            HyperlaneDuskError::RuesResponse {
                status: 500,
                body: "Cannot fetch events: database is locked".into(),
            },
            HyperlaneDuskError::RuesResponse {
                status: 503,
                body: "temporarily unavailable".into(),
            },
            HyperlaneDuskError::RuesHttp {
                kind: "timeout",
                status: None,
            },
            HyperlaneDuskError::Other("GraphQL query returned errors: Invalid cursor".into()),
        ] {
            assert_eq!(reply_too_large(&error), None, "{error}");
        }
    }

    #[tokio::test]
    async fn an_endpoint_that_refuses_large_pages_is_asked_for_fewer_rows() {
        let contract_id = [77u8; 32];
        let rows: Vec<JsonValue> = (0..120)
            .map(|id| {
                let data = (id as u16).to_be_bytes();
                archive_row(id, 5 + id as u64 / 40, 1, "dispatch", contract_id, &data)
            })
            .collect();
        for (status, body) in [
            (
                200,
                r#"{"errors":[{"message":"GraphQL query response exceeds 4194304 bytes"}]}"#,
            ),
            (413, "Payload Too Large"),
        ] {
            let archive = Archive::start(rows.clone());
            *archive.refusal.lock().unwrap() = Some((30, status, body.to_owned()));
            let directory = tempfile::tempdir().unwrap();
            let client = test_client_with_event_store(archive.url.clone(), directory.path());
            let matched = client
                .finalized_contract_event(&contract_id, "dispatch", 0, 7, &100u16.to_be_bytes())
                .await
                .unwrap();
            assert_eq!(matched.block_height, 7);
            // Halved at the first cursor until the endpoint accepted, then
            // kept, since the endpoint did not say how much it accepts.
            assert_eq!(archive.limits(), [200, 100, 50, 25, 25, 25, 25, 25]);
            assert_eq!(
                archive.pages(),
                [None, Some(24), Some(49), Some(74), Some(99)]
            );
        }
    }

    #[tokio::test]
    async fn pages_grow_back_after_a_block_of_large_rows() {
        let contract_id = [78u8; 32];
        // 40 rows of 100 KiB, 200 KiB as hex, then 400 small rows.
        let mut rows: Vec<JsonValue> = (0..40)
            .map(|id| archive_row(id, 5, 1, "other", contract_id, &[id as u8; 100 * 1024]))
            .collect();
        rows.extend((40..440).map(|id| {
            let data = (id as u16).to_be_bytes();
            archive_row(id, 6 + id as u64 / 100, 1, "dispatch", contract_id, &data)
        }));
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 10, &439u16.to_be_bytes())
            .await
            .unwrap();
        assert_eq!(matched.block_height, 10);
        // 25 large rows are over the limit and 12 fit. The page that ends
        // the large rows used under a quarter of the limit, so the size
        // doubles back to 200.
        assert_eq!(
            archive.limits(),
            [200, 100, 50, 25, 12, 12, 12, 12, 24, 48, 96, 192, 200]
        );
    }

    fn cached_block(height: u64, rows: usize, data_len: usize) -> Arc<CachedBlock> {
        let rows: Vec<BlockRow> = (0..rows)
            .map(|row| BlockRow {
                block_hash: H256::from([height as u8; 32]),
                origin: [row as u8; 32],
                data: vec![row as u8; data_len],
            })
            .collect();
        let bytes = rows.iter().map(BlockRow::bytes).sum();
        Arc::new(CachedBlock {
            block_hash: H256::from([height as u8; 32]),
            rows,
            bytes,
        })
    }

    #[test]
    fn block_cache_keeps_its_bounds_and_drops_the_least_recently_used_block() {
        let one_row = cached_block(1, 1, 1000).bytes;
        let mut cache = BlockCache::new(BlockCacheLimits {
            blocks: 3,
            bytes: 4 * one_row,
            block_bytes: 2 * one_row,
        });
        let key = |height| BlockKey::new(&[1; 32], "dispatch", height);
        for height in 1..=3 {
            cache.insert(key(height), cached_block(height, 1, 1000));
        }
        // Using block 1 leaves block 2 the least recently used.
        assert!(cache.get(&key(1)).is_some());
        cache.insert(key(4), cached_block(4, 1, 1000));
        assert!(cache.get(&key(2)).is_none());
        for height in [1, 3, 4] {
            assert!(cache.get(&key(height)).is_some(), "block {height}");
        }
        // A two-row block needs room for two: block 1, now the least
        // recently used, goes.
        cache.insert(key(5), cached_block(5, 2, 1000));
        assert!(cache.get(&key(1)).is_none());
        assert_eq!((cache.blocks.len(), cache.bytes), (3, 4 * one_row));
        // A block over the per-block bound is not cached.
        cache.insert(key(6), cached_block(6, 3, 1000));
        assert!(cache.get(&key(6)).is_none());
        // Forgetting a contract keeps other contracts' blocks.
        let other = BlockKey::new(&[2; 32], "dispatch", 1);
        cache.insert(other.clone(), cached_block(1, 1, 1000));
        cache.forget_contract(&[1; 32]);
        assert!(cache.get(&other).is_some());
        assert_eq!((cache.blocks.len(), cache.bytes), (1, one_row));
    }

    #[tokio::test]
    async fn a_block_over_the_cache_bound_is_read_per_lookup() {
        let contract_id = [97u8; 32];
        // 200 rows of block 5, then 400 dispatch rows of block 6.
        let mut rows: Vec<JsonValue> = (0..200)
            .map(|id| archive_row(id, 5, 1, "other", contract_id, &[0]))
            .collect();
        rows.extend((0..400u16).map(|n| {
            archive_row(
                200 + i64::from(n),
                6,
                2,
                "dispatch",
                contract_id,
                &n.to_be_bytes(),
            )
        }));
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        // Room for 100 of block 6's rows.
        let row = BlockRow {
            block_hash: H256::from([6u8; 32]),
            origin: [2u8; 32],
            data: vec![0; 2],
        };
        *client.block_cache.lock().unwrap() = BlockCache::new(BlockCacheLimits {
            blocks: 4,
            bytes: 1 << 20,
            block_bytes: 100 * row.bytes(),
        });
        for n in [0u16, 1, 399] {
            let matched = client
                .finalized_contract_event(
                    &contract_id,
                    "dispatch",
                    usize::from(n),
                    6,
                    &n.to_be_bytes(),
                )
                .await
                .unwrap();
            assert_eq!(matched.block_height, 6);
        }
        // Each lookup read the block only as far as its row, or 100 rows
        // further, and the next one started at the position before the
        // block, as without the cache.
        assert_eq!(
            archive.pages(),
            [None, Some(199), Some(198), Some(198), Some(398), Some(598)]
        );
        assert!(client.block_cache().blocks.is_empty());
    }

    #[tokio::test]
    async fn only_blocks_that_checkblock_accepts_are_cached() {
        let contract_id = [98u8; 32];
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[1]),
            archive_row(2, 7, 2, "dispatch", contract_id, &[2]),
        ]);
        archive.unfinalized.lock().unwrap().insert(7);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HyperlaneDuskError::FinalizedEventMismatch(_)
        ));
        assert_eq!(
            client
                .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[1; 32])
                .await
                .unwrap(),
            None
        );
        assert!(client.block_cache().blocks.is_empty());

        // Once the archive has finalized the block, the next lookup reads it
        // again and caches it; the block's other record and the
        // transaction's rows then need no page.
        archive.unfinalized.lock().unwrap().clear();
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        let other = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap();
        assert_eq!(&other.transaction_id.as_bytes()[32..], &[2u8; 32]);
        assert_eq!(
            client
                .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[2; 32])
                .await
                .unwrap(),
            Some(vec![vec![2]])
        );
        assert_eq!(archive.pages(), [None, None, None]);
    }

    #[tokio::test]
    async fn a_block_whose_rows_name_two_hashes_is_not_cached() {
        let contract_id = [99u8; 32];
        let mut forked = archive_row(2, 7, 2, "dispatch", contract_id, &[2]);
        forked["block_hash"] = serde_json::json!(hex::encode([0xaa; 32]));
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[1]),
            forked,
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert!(client.block_cache().blocks.is_empty());
        client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap();
        assert_eq!(archive.pages(), [None, None]);
    }

    #[tokio::test]
    async fn a_cached_block_that_lacks_a_record_is_read_again() {
        let contract_id = [100u8; 32];
        let archive = Archive::start(vec![archive_row(1, 7, 1, "dispatch", contract_id, &[1])]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert_eq!(client.block_cache().blocks.len(), 1);
        // Contract state names a second record the cached rows lack: the
        // lookup fails without a page and drops the block.
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not archived yet"), "{error}");
        assert!(client.block_cache().blocks.is_empty());
        assert_eq!(archive.pages(), [None]);
        // A repaired archive serves the row, and the retry reads it.
        archive
            .rows
            .lock()
            .unwrap()
            .push(archive_row(2, 7, 2, "dispatch", contract_id, &[2]));
        client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap();
        assert_eq!(archive.pages(), [None, None]);
    }

    #[tokio::test]
    async fn a_block_with_a_malformed_row_is_not_cached_and_recovers_once_repaired() {
        let contract_id = [101u8; 32];
        // A peer row of the block whose origin is one byte instead of a
        // transaction hash.
        let mut malformed = archive_row(2, 7, 2, "dispatch", contract_id, &[2]);
        malformed["origin"] = serde_json::json!("ab");
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[1]),
            malformed,
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        // The block cannot be served from memory: a lookup of the malformed
        // row's record would fail on it forever.
        assert!(client.block_cache().blocks.is_empty());
        assert_eq!(
            client
                .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[1; 32])
                .await
                .unwrap(),
            None
        );
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not archived yet"), "{error}");

        // Once the endpoint serves the row correctly, the next lookup reads
        // it from the archive.
        archive.rows.lock().unwrap()[1] = archive_row(2, 7, 2, "dispatch", contract_id, &[2]);
        let repaired = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 7, &[2])
            .await
            .unwrap();
        assert_eq!(&repaired.transaction_id.as_bytes()[32..], &[2u8; 32]);
        assert_eq!(client.block_cache().blocks.len(), 1);
    }

    #[tokio::test]
    async fn a_block_with_undecodable_row_data_is_not_cached() {
        let contract_id = [102u8; 32];
        let mut undecodable = archive_row(2, 7, 2, "dispatch", contract_id, &[2]);
        undecodable["data"] = serde_json::json!("not hex");
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[1]),
            undecodable,
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        // The block's rows are not whole: transaction 2 emitted a row that
        // cannot be read, so its lookup must not come back empty.
        assert!(client.block_cache().blocks.is_empty());
        assert_eq!(
            client
                .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[2; 32])
                .await
                .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn a_cached_block_is_checked_again_before_a_transaction_lookup_uses_it() {
        let contract_id = [103u8; 32];
        let archive = Archive::start(vec![archive_row(1, 7, 1, "dispatch", contract_id, &[1])]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert_eq!(client.block_cache().blocks.len(), 1);
        assert_eq!(archive.checked_heights(), [7]);

        // The endpoint no longer reports the block final under that hash.
        archive.unfinalized.lock().unwrap().insert(7);
        assert_eq!(
            client
                .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[1; 32])
                .await
                .unwrap(),
            None
        );
        assert_eq!(archive.checked_heights(), [7, 7]);
        assert!(client.block_cache().blocks.is_empty());
        // A miss reads the block again and checks it; the hit after it
        // checks it too.
        archive.unfinalized.lock().unwrap().clear();
        for _ in 0..2 {
            assert_eq!(
                client
                    .finalized_rows_of_transaction(&contract_id, "dispatch", 7, &[1; 32])
                    .await
                    .unwrap(),
                Some(vec![vec![1]])
            );
        }
        assert_eq!(archive.checked_heights(), [7, 7, 7, 7]);
        assert_eq!(archive.pages(), [None, None]);
    }

    #[tokio::test]
    async fn records_of_a_block_the_archive_skipped_are_reported_as_missing() {
        let contract_id = [104u8; 32];
        let archive = Archive::start(vec![archive_row(1, 6, 1, "dispatch", contract_id, &[6])]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        // Contract state records a dispatch at block 7, which the archive has
        // not reached: it may only be behind.
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not archived yet"), "{error}");
        // The archive has not finalized the node's last final block either.
        assert_eq!(archive.checked_heights(), [1000]);

        // The archive failed to write block 7, as Rusk does when its archive
        // write fails, and has finalized blocks 8 and 9 since.
        archive.rows.lock().unwrap().extend([
            archive_row(2, 8, 2, "dispatch", contract_id, &[8]),
            archive_row(3, 9, 3, "dispatch", contract_id, &[9]),
        ]);
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            matches!(error, HyperlaneDuskError::ArchiveMissingRows(_)),
            "{text}"
        );
        assert!(
            text.contains("skipped finalized block 7")
                && text.contains("finalized block 8")
                && text.contains(&hex::encode(contract_id))
                && text.contains("sequences [0]"),
            "{text}"
        );
        // checkBlock confirmed block 8 and found block 7, under the ledger's
        // hash, not finalized.
        assert_eq!(archive.checked_heights(), [1000, 8, 7]);
        // The critical report was made and is not due again yet; the lookup
        // keeps failing on every retry.
        assert!(!critical_report_due(&contract_id, "dispatch", 7));
        assert!(matches!(
            client
                .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
                .await,
            Err(HyperlaneDuskError::ArchiveMissingRows(_))
        ));
    }

    #[tokio::test]
    async fn a_finalized_block_without_the_records_state_holds_is_reported() {
        let contract_id = [105u8; 32];
        // Block 7's row is not the record contract state implies.
        let archive = Archive::start(vec![
            archive_row(1, 7, 1, "dispatch", contract_id, &[70]),
            archive_row(2, 8, 2, "dispatch", contract_id, &[8]),
        ]);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            matches!(error, HyperlaneDuskError::ArchiveMissingRows(_))
                && text.contains("finalized block 7")
                && text.contains("without the dispatch rows"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn an_idle_contracts_records_in_a_block_the_archive_passed_are_reported_as_missing() {
        let contract_id = [108u8; 32];
        let other = [109u8; 32];
        // The contract's last row is in block 6, and contract state records
        // a dispatch at block 7, the contract's last: no later row of it can
        // show that the archive has passed block 7. Another contract's row
        // shows that the archive has finalized block 9, the node's last final
        // block, so it skipped block 7.
        let archive = Archive::start(vec![
            archive_row(1, 6, 1, "dispatch", contract_id, &[6]),
            archive_row(2, 9, 2, "transfer", other, &[9]),
        ]);
        *archive.ledger_tip.lock().unwrap() = 9;
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            matches!(error, HyperlaneDuskError::ArchiveMissingRows(_))
                && text.contains("skipped finalized block 7")
                && text.contains("finalized block 9")
                && text.contains(&hex::encode(contract_id)),
            "{text}"
        );

        // The node's last final block is the record's own, which the archive
        // finalized without the contract's rows.
        let contract_id = [110u8; 32];
        let archive = Archive::start(vec![
            archive_row(1, 6, 1, "dispatch", contract_id, &[6]),
            archive_row(2, 7, 2, "transfer", other, &[7]),
        ]);
        *archive.ledger_tip.lock().unwrap() = 7;
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[7])
            .await
            .unwrap_err();
        let text = error.to_string();
        assert!(
            matches!(error, HyperlaneDuskError::ArchiveMissingRows(_))
                && text.contains("finalized block 7")
                && text.contains("without the dispatch rows"),
            "{text}"
        );
    }

    #[test]
    fn archive_gap_reports_are_rate_limited_per_block() {
        let contract_id = [106u8; 32];
        assert!(critical_report_due(&contract_id, "dispatch", 7));
        assert!(!critical_report_due(&contract_id, "dispatch", 7));
        // Another block, topic or contract is reported on its own.
        assert!(critical_report_due(&contract_id, "dispatch", 8));
        assert!(critical_report_due(&contract_id, "process_id", 7));
        assert!(critical_report_due(&[107u8; 32], "dispatch", 7));
    }

    /// Ten dispatch rows, one per block from 5 to 14, with IDs 0 to 9.
    fn one_row_per_block(contract_id: [u8; 32]) -> Vec<JsonValue> {
        (0..10)
            .map(|id| archive_row(id, 5 + id as u64, 1, "dispatch", contract_id, &[id as u8]))
            .collect()
    }

    #[tokio::test]
    async fn a_resumed_anchor_whose_fence_row_is_not_below_it_is_dropped() {
        let contract_id = [87u8; 32];
        let rows = vec![
            archive_row(11, 7, 9, "dispatch", contract_id, &[1]),
            archive_row(12, 8, 11, "dispatch", contract_id, &[2]),
        ];
        let fence: FinalizedContractEvent = serde_json::from_value(rows[1].clone()).unwrap();
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        // Well formed, and its fence row is the archive's row 12, unchanged.
        // But row 12 is in block 8, not below the block-8 position the anchor
        // names, so resuming after it would skip block 8's only dispatch.
        let poisoned = ScanAnchor {
            before_block: 8,
            fence_id: 12,
            fence_digest: archive_row_digest(&fence).unwrap(),
            rank: 2,
            fence_block: 7,
            fence_block_hash: [7; 32],
        };
        client.offer_scan_anchor(&contract_id, poisoned).unwrap();
        assert_eq!(client.scan_anchors_of(&contract_id), vec![poisoned]);

        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 0, 8, &[2])
            .await
            .unwrap();
        assert_eq!(&matched.transaction_id.as_bytes()[32..], &[11u8; 32]);
        // The scan read row 12 at the anchor, dropped the anchor and started
        // over from the first row.
        assert_eq!(archive.pages(), vec![Some(11), None]);
        assert!(!client.scan_anchors_of(&contract_id).contains(&poisoned));
    }

    #[tokio::test]
    async fn an_archive_behind_its_saved_position_keeps_it_until_it_catches_up() {
        let contract_id = [88u8; 32];
        let rows = one_row_per_block(contract_id);
        let archive = Archive::start(rows.clone());
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 14, &[9])
            .await
            .unwrap();
        let anchors = client.scan_anchors_of(&contract_id);
        assert_eq!(
            anchors
                .iter()
                .map(|anchor| (anchor.fence_id, anchor.before_block, anchor.fence_block))
                .collect::<Vec<_>>(),
            vec![(9, 15, 14)]
        );

        // The node's archive is restored from an older copy and has not
        // inserted blocks 10 to 14 again yet. The contract records a
        // dispatch at block 15, the node's last final block.
        archive.rows.lock().unwrap().truncate(5);
        *archive.ledger_tip.lock().unwrap() = 15;
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 15, &[10])
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("has not reached the saved scan position"),
            "{error}"
        );
        // The page at the position was empty, the archive has not finalized
        // block 14, the fence row's block, which the ledger holds under the
        // position's hash, and it has not finalized block 15, the node's last
        // final block, either: it is behind. The position is kept and the
        // scan does not start over from the first row.
        assert_eq!(archive.checked_heights(), [14, 14, 15]);
        assert_eq!(client.scan_anchors_of(&contract_id), anchors);
        assert_eq!(archive.pages(), vec![None, Some(8)]);

        // Once the archive has caught up, the scan resumes at the position.
        {
            let mut archived = archive.rows.lock().unwrap();
            *archived = rows;
            archived.push(archive_row(10, 15, 1, "dispatch", contract_id, &[10]));
        }
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 15, &[10])
            .await
            .unwrap();
        assert_eq!(matched.block_height, 15);
        assert_eq!(archive.pages(), vec![None, Some(8), Some(8)]);
    }

    #[tokio::test]
    async fn a_page_that_skips_the_fence_row_starts_the_scan_over() {
        let contract_id = [89u8; 32];
        let archive = Archive::start(one_row_per_block(contract_id));
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 14, &[9])
            .await
            .unwrap();

        // The endpoint now serves another node's archive. It numbers the same
        // rows from 20 and has archived blocks 5 to 9 so far, so it has rows
        // after the saved position (row 9) but not row 9. A copy of the old
        // archive that only lagged would return row 9 first, so this one
        // numbers its rows differently, even though it has not finalized
        // block 14 yet: the scan starts over instead of waiting for a row it
        // may never have.
        let renumbered =
            |n: i64| archive_row(20 + n, 5 + n as u64, 1, "dispatch", contract_id, &[n as u8]);
        *archive.rows.lock().unwrap() = (0..5).map(renumbered).collect();
        let error = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 15, &[10])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not archived yet"), "{error}");
        assert_eq!(archive.pages(), vec![None, Some(8), None]);
        assert_eq!(
            client
                .scan_anchors_of(&contract_id)
                .iter()
                .map(|anchor| (anchor.fence_id, anchor.before_block))
                .collect::<Vec<_>>(),
            vec![(24, 10)]
        );

        // Once it catches up, the scan resumes at its own position.
        archive.rows.lock().unwrap().extend((5..11).map(renumbered));
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 15, &[10])
            .await
            .unwrap();
        assert_eq!(matched.block_height, 15);
        assert_eq!(archive.pages(), vec![None, Some(8), None, Some(23)]);
        // Only the matched rows' blocks were checked, and, while block 15
        // was missing, the node's last final block, which the archive had not
        // finalized yet either.
        assert_eq!(archive.checked_heights(), [14, 1000, 15]);
    }

    #[tokio::test]
    async fn a_scan_position_whose_block_hash_is_not_the_ledgers_is_dropped() {
        let contract_id = [90u8; 32];
        // An endpoint serves a row of block 7 under another block's hash.
        // The row passes checkBlock there, and the scan keeps a position
        // after it.
        let mut forged = archive_row(i64::MAX, 7, 9, "dispatch", contract_id, &[1]);
        forged["block_hash"] = serde_json::json!(hex::encode([0xee; 32]));
        let archive = Archive::start(vec![forged]);
        let directory = tempfile::tempdir().unwrap();
        let poisoned = test_client_with_event_store(archive.url.clone(), directory.path());
        poisoned
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &[1])
            .await
            .unwrap();
        assert_eq!(
            poisoned.scan_anchors_of(&contract_id)[0].fence_block_hash,
            [0xee; 32]
        );
        drop(poisoned);

        // The endpoint now serves the real archive, which has no row from
        // the position on and has not finalized block 7 under that hash.
        *archive.rows.lock().unwrap() = vec![
            archive_row(11, 7, 9, "dispatch", contract_id, &[1]),
            archive_row(12, 8, 11, "dispatch", contract_id, &[2]),
        ];
        let repaired = test_client_with_event_store(archive.url.clone(), directory.path());
        let later = repaired
            .finalized_contract_event(&contract_id, "dispatch", 1, 8, &[2])
            .await
            .unwrap();
        assert_eq!(later.block_height, 8);
        // The ledger's block 7 has another hash: the position is not this
        // chain's, so it is dropped and the scan starts over instead of
        // waiting for an archive that will never reach it.
        assert_eq!(archive.pages(), vec![None, Some(i64::MAX - 1), None]);
        assert!(repaired
            .scan_anchors_of(&contract_id)
            .iter()
            .all(|anchor| anchor.fence_block_hash != [0xee; 32]));
    }

    #[tokio::test]
    async fn a_scan_position_in_a_block_the_archive_skipped_is_dropped() {
        let contract_id = [91u8; 32];
        let archive = Archive::start(one_row_per_block(contract_id));
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 14, &[9])
            .await
            .unwrap();
        let saved = client.scan_anchors_of(&contract_id);
        assert_eq!(saved[0].fence_block, 14);

        // Another node's archive behind the endpoint failed to finalize
        // blocks 13 and 14, as Rusk does when its archive write fails, and
        // has finalized block 16, the node's last final block, since. It
        // numbers its rows like the first archive did up to block 12.
        {
            let mut rows = archive.rows.lock().unwrap();
            rows.truncate(8);
            rows.push(archive_row(8, 16, 1, "dispatch", contract_id, &[11]));
        }
        *archive.ledger_tip.lock().unwrap() = 16;
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 16, &[11])
            .await
            .unwrap();
        assert_eq!(matched.block_height, 16);
        // The archive has finalized block 16 without block 14, so it will
        // never return the position's row: the scan started over.
        assert_eq!(archive.pages(), vec![None, Some(8), None]);
        assert!(!client.scan_anchors_of(&contract_id).contains(&saved[0]));
    }

    #[tokio::test]
    async fn a_scan_position_in_a_block_the_node_does_not_hold_is_dropped() {
        let contract_id = [92u8; 32];
        let archive = Archive::start(one_row_per_block(contract_id));
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 14, &[9])
            .await
            .unwrap();

        // The endpoint now serves a node started from a snapshot after block
        // 14: neither its ledger nor its archive holds the position's block,
        // and its archive has numbered its rows from 0 since the snapshot.
        *archive.rows.lock().unwrap() = vec![archive_row(0, 20, 1, "dispatch", contract_id, &[11])];
        archive.pruned.lock().unwrap().extend(0..=19);
        *archive.ledger_tip.lock().unwrap() = 30;
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 20, &[11])
            .await
            .unwrap();
        assert_eq!(matched.block_height, 20);
        assert_eq!(archive.pages(), vec![None, Some(8), None]);
    }
}
