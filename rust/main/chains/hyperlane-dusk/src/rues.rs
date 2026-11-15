use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
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
use tracing::{debug, warn};
use url::Url;

use hyperlane_core::{ReorgPeriod, H256, H512};

use crate::HyperlaneDuskError;

const MAX_RUES_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
/// Rows per `finalizedEvents` page: the most Rusk 1.7.3 returns
/// (`MAX_LIMIT` in its GraphQL archive module).
const FINALIZED_EVENT_PAGE_SIZE: usize = 200;
const MAX_EVENT_TOPIC_BYTES: usize = 256;
/// Archive rows of one contract between two persisted scan anchors.
const SCAN_ANCHOR_SPACING: u64 = FINALIZED_EVENT_PAGE_SIZE as u64;
/// Most bytes of a response body that go into an error or a log line.
const MAX_ERROR_BODY_BYTES: usize = 1024;
/// First Mailbox state version that stores only dispatched message IDs
/// (`dispatched_id`), which dispatch indexing reads.
const MIN_MAILBOX_STATE_VERSION: u32 = 4;
const TX_POLL_INTERVAL: Duration = Duration::from_secs(2);

type SharedRuesClients = HashMap<(String, PathBuf), RuesClient>;
static SHARED_RUES_CLIENTS: OnceLock<StdMutex<SharedRuesClients>> = OnceLock::new();

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
/// When the archive returns another row there, the anchor is wrong for this
/// archive, the contract's anchors are dropped and the scan starts over. When
/// the fence row is missing, the archive may only be behind: the scan asks
/// whether the archive has finalized the fence row's block, which Rusk does
/// in the same transaction that inserts the block's rows. If it has, its rows
/// are numbered differently and the scan starts over; if not, the lookup
/// fails until the archive catches up, and the anchors are kept.
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

/// How a scan from an anchor, or from the contract's first row, ended.
enum ScanOutcome {
    /// The scan ran; the rows it matched.
    Done(ScannedRows),
    /// The archive returned another row where the anchor's fence row was,
    /// or has finalized the fence row's block without that row, so the
    /// anchor does not belong to this archive.
    WrongAnchor,
    /// The archive has not reached the anchor's fence row yet.
    ArchiveBehind(ScanAnchor),
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
        })
    }

    /// Create a RUES client whose validated contract event rows and archive
    /// scan positions survive restart. A scan position is checked against
    /// the archive before it is reused. The configured directory must be
    /// owned by one agent process.
    pub fn new_with_event_cursor_dir(
        url: Url,
        event_cursor_dir: PathBuf,
    ) -> Result<Self, HyperlaneDuskError> {
        let mut canonical_url = url.clone();
        canonical_url.set_path(url.path().trim_end_matches('/'));
        canonical_url.set_fragment(None);
        let key = (canonical_url.to_string(), event_cursor_dir.clone());
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

    fn endpoint(&self, path: &str) -> Url {
        let mut url = self.base_url.clone();
        url.set_path(&format!("{}{path}", url.path().trim_end_matches('/')));
        url.set_fragment(None);
        url
    }

    /// Query the native Dusk chain ID from the transfer contract.
    pub async fn chain_id(&self) -> Result<u8, HyperlaneDuskError> {
        let mut transfer_contract = [0u8; 32];
        transfer_contract[0] = 1;
        let bytes = self
            .contract_query_raw(&transfer_contract, "chain_id", &[])
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
    /// indexes or submits anything.
    pub async fn validate_chain_identity(
        &self,
        expected_chain_id: u8,
        expected_domain: u32,
        mailbox_id: &[u8; 32],
        validator_announce_id: &[u8; 32],
        merkle_tree_hook_id: &[u8; 32],
    ) -> Result<(), HyperlaneDuskError> {
        let observed_chain_id = self.chain_id().await?;
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
                .contract_query(contract_id, "local_domain", &())
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
        self.validate_mailbox_state_version(mailbox_id).await?;
        self.validate_mailbox_binding(validator_announce_id, mailbox_id, "ValidatorAnnounce")
            .await?;

        // The supported deployments attach the Merkle hook directly, or as a
        // child of the required static aggregation. A matching local domain
        // alone cannot identify the deployment whose checkpoints we sign.
        let required: [u8; 32] = self
            .contract_query(mailbox_id, "required_hook", &())
            .await?;
        let hook_type: u8 = self
            .contract_query(merkle_tree_hook_id, "hook_type", &())
            .await?;
        if hook_type != 3 {
            return Err(HyperlaneDuskError::Other(
                "Configured Dusk MerkleTreeHook does not have MerkleTree hook_type 3".into(),
            ));
        }
        if required == *merkle_tree_hook_id {
            self.validate_mailbox_binding(merkle_tree_hook_id, mailbox_id, "MerkleTreeHook")
                .await?;
        } else {
            let required_type: u8 = self.contract_query(&required, "hook_type", &()).await?;
            if required_type != 2 {
                return Err(HyperlaneDuskError::Other(
                    "Dusk Mailbox required_hook must be the configured MerkleTreeHook or its static aggregation".into(),
                ));
            }
            self.validate_mailbox_binding(&required, mailbox_id, "required aggregation hook")
                .await?;
            let hooks: Vec<[u8; 32]> = self.contract_query(&required, "hooks", &()).await?;
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
        let mailbox: [u8; 32] = self.contract_query(contract, "mailbox", &()).await?;
        if mailbox != *expected_mailbox {
            return Err(HyperlaneDuskError::Other(format!(
                "Configured Dusk {label} mailbox does not match the configured deployment"
            )));
        }
        Ok(())
    }

    /// Dispatch indexing reads `dispatched_id`, which older Mailboxes lack:
    /// every dispatch fetch would fail with an unexplained node error.
    async fn validate_mailbox_state_version(
        &self,
        mailbox_id: &[u8; 32],
    ) -> Result<(), HyperlaneDuskError> {
        let state_version: u32 = self
            .contract_query(mailbox_id, "state_version", &())
            .await
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to query Dusk Mailbox state_version (this agent needs state_version {MIN_MAILBOX_STATE_VERSION} or later): {error}"
                ))
            })?;
        if state_version < MIN_MAILBOX_STATE_VERSION {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk Mailbox state_version {state_version} is not supported: this agent needs state_version {MIN_MAILBOX_STATE_VERSION} or later, which stores dispatched message IDs (dispatched_id). Deploy a Mailbox that matches this agent's hyperlane-dusk-types pin"
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

    /// Propagate a serialized transaction to the network.
    pub async fn propagate_tx(&self, tx_bytes: &[u8]) -> Result<(), HyperlaneDuskError> {
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
        if !wanted.is_empty() {
            let found = self
                .scan_block_rows(
                    contract_id,
                    topic,
                    block_height,
                    &wanted,
                    &matching_sequences,
                )
                .await?;
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
            let block_hash = parse_h256_hex(&event.block_hash, "finalized event block hash")?;
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
            matches.insert(sequence, finalized_event_match(event, block_hash)?);
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

        let mut ordered = Vec::with_capacity(sequences.len());
        for sequence in sequences {
            let Some(matched) = matches.remove(sequence) else {
                return Err(HyperlaneDuskError::Other(format!(
                    "Finalized Dusk event {}/{topic} sequence {sequence} matching contract state is not archived yet",
                    hex::encode(contract_id)
                )));
            };
            ordered.push(matched);
        }
        Ok(ordered)
    }

    /// Scan the contract's archive rows once for the rows of `block_height`
    /// that `matching_sequences` assigns to the `wanted` sequences, starting
    /// at the closest persisted anchor below the block.
    async fn scan_block_rows<F>(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        block_height: u64,
        wanted: &HashSet<usize>,
        matching_sequences: &F,
    ) -> Result<ScannedRows, HyperlaneDuskError>
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
            ScanOutcome::Done(rows) => return Ok(rows),
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
            ScanOutcome::Done(rows) => Ok(rows),
            ScanOutcome::WrongAnchor | ScanOutcome::ArchiveBehind(_) => {
                Err(HyperlaneDuskError::Other(
                    "Dusk archive scan without an anchor found a stale anchor".into(),
                ))
            }
        }
    }

    /// One pass over the contract's archive rows from `start`, or from its
    /// first row. Stops once every wanted sequence has a row, or at the end
    /// of the archive. A resumed scan first checks `start`'s fence row, as
    /// `ScanAnchor` describes.
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
                    // The fence row is missing: the page is empty or starts
                    // after it.
                    _ => {
                        let fence_block_hash = hex::encode(anchor.fence_block_hash);
                        return Ok(
                            if self
                                .check_finalized_block(anchor.fence_block, &fence_block_hash)
                                .await?
                            {
                                ScanOutcome::WrongAnchor
                            } else {
                                ScanOutcome::ArchiveBehind(anchor)
                            },
                        );
                    }
                }
            }

            let mut complete = false;
            for event in events {
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
                if !event.reverted && event.topic == topic && event.block_height == block_height {
                    if let Ok(data) = hex::decode(strip_hex_prefix(&event.data)) {
                        for sequence in matching_sequences(&data) {
                            if wanted.contains(&sequence) && !found.contains_key(&sequence) {
                                let mut row = event.clone();
                                row.id = i64::try_from(sequence).map_err(|_| {
                                    HyperlaneDuskError::Other(
                                        "Dusk finalized-event sequence exceeds i64 provenance range"
                                            .into(),
                                    )
                                })?;
                                found.insert(sequence, row);
                            }
                        }
                    }
                }
                last_row = Some(event);
                if found.len() == wanted.len() {
                    complete = true;
                    break;
                }
            }

            if complete || !has_next_page {
                if let (Some(event), Some(highest)) = (last_row, highest_block) {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(&event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                return Ok(ScanOutcome::Done(found));
            }
        }
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

    /// Drop every scan anchor of a contract. The next scan starts from the
    /// contract's first archive row.
    fn forget_scan_anchors(&self, contract_id: &[u8; 32]) -> Result<(), HyperlaneDuskError> {
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
    /// with the contract's scan anchors: an endpoint that served them may
    /// also have served the rows the anchors were taken from.
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

    fn test_client_with_event_store(url: Url, path: &std::path::Path) -> RuesClient {
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
        let client = test_client_with_event_store(
            test_server(vec![
                (200, page),
                (200, r#"{"checkBlock":true}"#),
                (200, page),
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

    fn archive_row(
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
    struct Archive {
        url: Url,
        rows: Arc<StdMutex<Vec<JsonValue>>>,
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
        /// a block when it holds a row of it.
        block_checks: Arc<StdMutex<Vec<(u64, String)>>>,
        stop: Arc<std::sync::atomic::AtomicBool>,
        worker: Option<thread::JoinHandle<()>>,
    }

    impl Archive {
        fn start(rows: Vec<JsonValue>) -> Self {
            use std::sync::atomic::{AtomicBool, Ordering};
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
            let rows = Arc::new(StdMutex::new(rows));
            let pages = Arc::new(StdMutex::new(Vec::new()));
            let limits = Arc::new(StdMutex::new(Vec::new()));
            let refusal = Arc::new(StdMutex::new(None));
            let block_checks = Arc::new(StdMutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let worker_rows = rows.clone();
            let worker_pages = pages.clone();
            let worker_limits = limits.clone();
            let worker_refusal = refusal.clone();
            let worker_block_checks = block_checks.clone();
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
                        let finalized = worker_rows.lock().unwrap().iter().any(|row| {
                            row["block_height"] == height && row["block_hash"] == hash.as_str()
                        });
                        worker_block_checks.lock().unwrap().push((height, hash));
                        (
                            200,
                            serde_json::json!({ "checkBlock": finalized }).to_string(),
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

        fn pages(&self) -> Vec<Option<i64>> {
            self.pages.lock().unwrap().clone()
        }

        fn limits(&self) -> Vec<usize> {
            self.limits.lock().unwrap().clone()
        }

        fn checked_heights(&self) -> Vec<u64> {
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
        // have, so the scan starts over instead of skipping its rows.
        assert_eq!(archive.pages(), vec![None, Some(i64::MAX - 1), None]);
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
        assert_eq!(archive.pages(), vec![None, Some(10), None]);
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
                (200, rkyv_serialize(&MIN_MAILBOX_STATE_VERSION).unwrap()),
                (200, rkyv_serialize(&mailbox).unwrap()),
            ]
        };
        let mut direct = prefix();
        direct.extend([
            (200, rkyv_serialize(&merkle).unwrap()),
            (200, vec![3]),
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
                rkyv_serialize(&(MIN_MAILBOX_STATE_VERSION - 1)).unwrap(),
                "Dusk Mailbox state_version 3 is not supported",
            ),
            (
                4,
                rkyv_serialize(&foreign).unwrap(),
                "ValidatorAnnounce mailbox",
            ),
            (6, vec![2], "MerkleTree hook_type"),
            (7, vec![1], "required_hook"),
            (
                8,
                rkyv_serialize(&foreign).unwrap(),
                "required aggregation hook mailbox",
            ),
            (9, rkyv_serialize(&vec![foreign]).unwrap(), "exactly once"),
            (
                9,
                rkyv_serialize(&vec![merkle, merkle]).unwrap(),
                "exactly once",
            ),
            (
                10,
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
    async fn mailboxes_older_than_state_version_4_are_refused() {
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
        for version in [0u32, 1, 3] {
            let error = identity((200, rkyv_serialize(&version).unwrap()))
                .validate_chain_identity(1, 4242, &mailbox, &[4u8; 32], &[5u8; 32])
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains(&format!("state_version {version} is not supported"))
                    && error.contains("state_version 4 or later"),
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
                && error.contains("state_version 4 or later"),
            "{error}"
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
        // dispatch at block 15.
        archive.rows.lock().unwrap().truncate(5);
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
        // The page at the position was empty and the archive has not
        // finalized block 14, the fence row's block: the position is kept
        // and the scan does not start over from the first row.
        assert_eq!(archive.checked_heights(), [14, 14]);
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
    async fn a_missing_fence_row_is_waited_for_until_its_block_is_finalized() {
        let contract_id = [89u8; 32];
        let archive = Archive::start(one_row_per_block(contract_id));
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 14, &[9])
            .await
            .unwrap();

        // The endpoint now serves another node's archive. It numbers the same
        // rows from 20 and has archived blocks 5 to 9 so far.
        let renumbered =
            |n: i64| archive_row(20 + n, 5 + n as u64, 1, "dispatch", contract_id, &[n as u8]);
        *archive.rows.lock().unwrap() = (0..5).map(renumbered).collect();
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
        assert_eq!(client.scan_anchors_of(&contract_id).len(), 1);

        // It catches up. It has now finalized block 14 but still has no row
        // 9, which proves it numbers its rows differently: the scan starts
        // over from the first row.
        archive.rows.lock().unwrap().extend((5..11).map(renumbered));
        let matched = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 15, &[10])
            .await
            .unwrap();
        assert_eq!(matched.block_height, 15);
        assert_eq!(archive.pages(), vec![None, Some(8), Some(8), None]);
        assert_eq!(archive.checked_heights(), [14, 14, 14, 15]);
    }
}
