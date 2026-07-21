use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::{Mutex as StdMutex, OnceLock};
use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{check_archived_root, Archive, Deserialize, Infallible, Serialize};
use rocksdb::{Options, WriteBatch, WriteOptions, DB};
use serde::{Deserialize as SerdeDeserialize, Serialize as SerdeSerialize};
use serde_json::Value as JsonValue;
use url::Url;

use hyperlane_core::{H256, H512};

use crate::HyperlaneDuskError;

const MAX_RUES_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const FINALIZED_EVENT_PAGE_SIZE: usize = 16;
const MAX_FINALIZED_EVENT_STATE_BYTES: usize = 256 * 1024;
const MAX_TRACKED_EVENT_TOPICS: usize = 256;
const MAX_EVENT_TOPIC_BYTES: usize = 256;
const TX_POLL_INTERVAL: Duration = Duration::from_secs(2);

type SharedRuesClients = HashMap<(String, PathBuf), RuesClient>;
static SHARED_RUES_CLIENTS: OnceLock<StdMutex<SharedRuesClients>> = OnceLock::new();

/// RUES (Rusk Unified Event System) HTTP client.
///
/// Communicates with Dusk nodes using the RUES protocol:
/// - Contract queries: `POST /on/contracts:{hex_id}/{method}`
/// - Transaction propagation: `POST /on/transactions/propagate`
#[derive(Debug, Clone)]
pub struct RuesClient {
    client: reqwest::Client,
    base_url: String,
    finalized_event_caches: Arc<tokio::sync::Mutex<HashMap<[u8; 32], FinalizedEventCache>>>,
    event_store: Option<Arc<DB>>,
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

#[derive(Debug, Clone, Default, SerdeDeserialize, SerdeSerialize)]
struct FinalizedEventCache {
    cursor: Option<String>,
    last_id: Option<i64>,
    topic_lengths: HashMap<String, usize>,
}

/// Row-owned provenance for one finalized, state-matched contract event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FinalizedEventProvenance {
    pub block_height: u64,
    pub block_hash: H256,
    pub transaction_id: H512,
    pub event_id: i64,
}

impl RuesClient {
    /// Create a new RUES client.
    pub fn new(url: Url) -> Result<Self, HyperlaneDuskError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        // Normalize to avoid footguns around missing/extra trailing slashes.
        let base_url = url.to_string();
        let base_url = base_url.trim_end_matches('/').to_string();
        Ok(Self {
            client,
            base_url,
            finalized_event_caches: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            event_store: None,
        })
    }

    /// Create a RUES client whose contract event cursors survive process
    /// restart. The configured directory must be owned by one agent process.
    pub fn new_with_event_cursor_dir(
        url: Url,
        event_cursor_dir: PathBuf,
    ) -> Result<Self, HyperlaneDuskError> {
        let key = (
            url.as_str().trim_end_matches('/').to_owned(),
            event_cursor_dir.clone(),
        );
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
        let url = format!("{}/on/contracts:{}/{}", self.base_url, contract_hex, method);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(body.to_vec())
            .send()
            .await?;

        let status = response.status();
        let response_body = read_bounded_body(response, "contract query").await?;
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&response_body).into_owned(),
            });
        }

        Ok(response_body)
    }

    /// Query VM metadata for a contract without invoking contract code.
    pub async fn contract_metadata(
        &self,
        contract_id: &[u8; 32],
    ) -> Result<ContractMetadata, HyperlaneDuskError> {
        let contract_hex = hex::encode(contract_id);
        let url = format!("{}/on/contract:{}/metadata", self.base_url, contract_hex);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(Vec::new())
            .send()
            .await?;

        let status = response.status();
        let response_body = read_bounded_body(response, "contract metadata query").await?;
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&response_body).into_owned(),
            });
        }

        let body = String::from_utf8_lossy(&response_body);
        serde_json::from_str::<ContractMetadata>(&body).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse contract metadata JSON: {error}. Body: {body}"
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
        let preverify_url = format!("{}/on/transactions/preverify", self.base_url);
        let response = self
            .client
            .post(&preverify_url)
            .headers(self.default_headers())
            .body(tx_bytes.to_vec())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = read_bounded_body(response, "transaction preverification").await?;
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        let url = format!("{}/on/transactions/propagate", self.base_url);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(tx_bytes.to_vec())
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            let body = read_bounded_body(response, "transaction propagation").await?;
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        Ok(())
    }

    /// Execute a GraphQL query against the node's `/on/graphql/query` endpoint.
    pub async fn graphql_query(&self, query: &str) -> Result<JsonValue, HyperlaneDuskError> {
        let url = format!("{}/on/graphql/query", self.base_url);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(query.as_bytes().to_vec())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "GraphQL query").await?;
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        let payload = serde_json::from_slice::<JsonValue>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse GraphQL JSON response: {e}. Body: {}",
                String::from_utf8_lossy(&body)
            ))
        })?;

        if payload.get("errors").is_some_and(graphql_errors_present) {
            return Err(HyperlaneDuskError::Other(format!(
                "GraphQL query returned errors: {}",
                payload["errors"]
            )));
        }

        // The legacy `/on/graphql/query` route returns the data object
        // directly on success. Accept the canonical `/graphql` envelope too so
        // callers do not accidentally depend on the deprecated response shape.
        Ok(payload.get("data").cloned().unwrap_or(payload))
    }

    /// Return one finalized contract event by its topic-local sequence and
    /// prove its row-owned provenance against finalized block state.
    pub(crate) async fn finalized_contract_event(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        sequence: usize,
        expected_block_height: u64,
        expected_data: &[u8],
    ) -> Result<FinalizedEventProvenance, HyperlaneDuskError> {
        loop {
            let (cached, cursor, last_id) = {
                let mut caches = self.finalized_event_caches.lock().await;
                if !caches.contains_key(contract_id) {
                    let cache = self.load_finalized_event_cache(contract_id)?;
                    caches.insert(*contract_id, cache);
                }
                let cache = caches.get(contract_id);
                let cached = match cache
                    .and_then(|cache| cache.topic_lengths.get(topic))
                    .is_some_and(|length| sequence < *length)
                {
                    true => self.load_finalized_event(contract_id, topic, sequence)?,
                    false => None,
                };
                (
                    cached,
                    cache.and_then(|cache| cache.cursor.clone()),
                    cache.and_then(|cache| cache.last_id),
                )
            };

            if let Some(event) = cached {
                return self
                    .validate_finalized_event(
                        contract_id,
                        topic,
                        sequence,
                        expected_block_height,
                        expected_data,
                        event,
                    )
                    .await;
            }

            let page = self
                .finalized_event_page(contract_id, cursor.as_deref())
                .await?;
            validate_finalized_event_page(contract_id, cursor.as_deref(), last_id, &page)?;
            let has_next_page = page.has_next_page;

            // Only one concurrent fetch may advance a contract cursor. If
            // another task won the race, discard this page and retry from the
            // now-current cache rather than duplicating rows.
            let mut caches = self.finalized_event_caches.lock().await;
            let cache = caches.entry(*contract_id).or_default();
            if cache.cursor != cursor || cache.last_id != last_id {
                continue;
            }
            let mut candidate = cache.clone();
            let mut new_rows = Vec::new();
            let mut cached = None;
            for event in page.events {
                candidate.last_id = Some(event.id);
                if !event.reverted {
                    let topic_sequence = *candidate.topic_lengths.get(&event.topic).unwrap_or(&0);
                    if event.topic == topic && topic_sequence == sequence {
                        cached = Some(event.clone());
                    }
                    let next_topic_sequence = topic_sequence.checked_add(1).ok_or_else(|| {
                        HyperlaneDuskError::Other(
                            "Dusk finalized-event topic sequence overflow".into(),
                        )
                    })?;
                    candidate
                        .topic_lengths
                        .insert(event.topic.clone(), next_topic_sequence);
                    new_rows.push((event.topic.clone(), topic_sequence, event));
                }
            }
            // An empty caught-up page has no end cursor. Preserve the last
            // opaque cursor so the next poll asks only for newer rows.
            candidate.cursor = page.end_cursor.or(cursor);
            self.persist_finalized_event_page(contract_id, &candidate, &new_rows)?;
            *cache = candidate;

            if let Some(event) = cached {
                drop(caches);
                return self
                    .validate_finalized_event(
                        contract_id,
                        topic,
                        sequence,
                        expected_block_height,
                        expected_data,
                        event,
                    )
                    .await;
            }
            if !has_next_page {
                return Err(HyperlaneDuskError::Other(format!(
                    "Finalized Dusk event {}/{topic} sequence {sequence} is not archived yet",
                    hex::encode(contract_id)
                )));
            }
        }
    }

    fn event_store(&self) -> Result<&DB, HyperlaneDuskError> {
        self.event_store.as_deref().ok_or_else(|| {
            HyperlaneDuskError::Other(
                "Dusk finalized-event indexing requires a durable eventCursorDir".into(),
            )
        })
    }

    fn load_finalized_event_cache(
        &self,
        contract_id: &[u8; 32],
    ) -> Result<FinalizedEventCache, HyperlaneDuskError> {
        let Some(bytes) = self
            .event_store()?
            .get(finalized_event_state_key(contract_id))
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read Dusk finalized-event cursor state: {error}"
                ))
            })?
        else {
            return Ok(FinalizedEventCache::default());
        };
        if bytes.len() > MAX_FINALIZED_EVENT_STATE_BYTES {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk finalized-event cursor state exceeds {MAX_FINALIZED_EVENT_STATE_BYTES} bytes"
            )));
        }
        let cache: FinalizedEventCache = serde_json::from_slice(&bytes).map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to decode Dusk event cursor: {error}"))
        })?;
        validate_loaded_event_cache(&cache)?;
        Ok(cache)
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
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk finalized-event cursor references missing row {}/{topic}/{sequence}",
                hex::encode(contract_id)
            )));
        };
        if bytes.len() > MAX_RUES_RESPONSE_BYTES {
            return Err(HyperlaneDuskError::Other(
                "Persisted Dusk finalized event exceeds the transport bound".into(),
            ));
        }
        let event: FinalizedContractEvent = serde_json::from_slice(&bytes).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to decode persisted Dusk finalized event: {error}"
            ))
        })?;
        validate_persisted_event(contract_id, topic, &event)?;
        Ok(Some(event))
    }

    fn persist_finalized_event_page(
        &self,
        contract_id: &[u8; 32],
        cache: &FinalizedEventCache,
        rows: &[(String, usize, FinalizedContractEvent)],
    ) -> Result<(), HyperlaneDuskError> {
        validate_loaded_event_cache(cache)?;
        let state = serde_json::to_vec(cache).map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to encode Dusk event cursor: {error}"))
        })?;
        if state.len() > MAX_FINALIZED_EVENT_STATE_BYTES {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk finalized-event cursor state exceeds {MAX_FINALIZED_EVENT_STATE_BYTES} bytes"
            )));
        }

        let mut batch = WriteBatch::default();
        for (topic, sequence, event) in rows {
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
        batch.put(finalized_event_state_key(contract_id), state);
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.event_store()?
            .write_opt(batch, &write_options)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to atomically persist Dusk finalized-event page: {error}"
                ))
            })
    }

    async fn finalized_event_page(
        &self,
        contract_id: &[u8; 32],
        cursor: Option<&str>,
    ) -> Result<FinalizedEventPage, HyperlaneDuskError> {
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
            "query {{ finalizedEvents(contractId: \"{}\", limit: {FINALIZED_EVENT_PAGE_SIZE}{cursor_argument}) {{ json }} }}",
            hex::encode(contract_id)
        );
        let data = self.graphql_query(&query).await.map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Dusk endpoint does not provide usable archive finalizedEvents pagination: {error}"
            ))
        })?;
        let page = data
            .get("finalizedEvents")
            .and_then(|events| events.get("json"))
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive response is missing finalizedEvents.json: {data}"
                ))
            })?;
        serde_json::from_value(page.clone()).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to decode Rusk finalizedEvents page: {error}"
            ))
        })
    }

    async fn validate_finalized_event(
        &self,
        contract_id: &[u8; 32],
        topic: &str,
        sequence: usize,
        expected_block_height: u64,
        expected_data: &[u8],
        event: FinalizedContractEvent,
    ) -> Result<FinalizedEventProvenance, HyperlaneDuskError> {
        if event.reverted || event.topic != topic {
            return Err(HyperlaneDuskError::Other(format!(
                "Finalized Dusk event {}/{topic} sequence {sequence} is reverted or has the wrong topic",
                hex::encode(contract_id)
            )));
        }
        if event.block_height != expected_block_height {
            return Err(HyperlaneDuskError::Other(format!(
                "Finalized Dusk event {}/{topic} sequence {sequence} has block height {}, contract state says {expected_block_height}",
                hex::encode(contract_id),
                event.block_height
            )));
        }
        let data = hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Finalized Dusk event {}/{topic} sequence {sequence} has invalid data: {error}",
                hex::encode(contract_id)
            ))
        })?;
        if data != expected_data {
            return Err(HyperlaneDuskError::Other(format!(
                "Finalized Dusk event {}/{topic} sequence {sequence} does not match contract state",
                hex::encode(contract_id)
            )));
        }

        let block_hash = parse_h256_hex(&event.block_hash, "finalized event block hash")?;
        let encoded_block_hash = serde_json::to_string(strip_hex_prefix(&event.block_hash))
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to encode finalized event block hash: {error}"
                ))
            })?;
        let check_query = format!(
            "query {{ checkBlock(height: {}, hash: {encoded_block_hash}, onlyFinalized: true) }}",
            event.block_height
        );
        let checked = self
            .graphql_query(&check_query)
            .await?
            .get("checkBlock")
            .and_then(JsonValue::as_bool)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(
                    "Rusk checkBlock response is missing a boolean result".into(),
                )
            })?;
        if !checked {
            return Err(HyperlaneDuskError::Other(format!(
                "Finalized Dusk event block {}/{} failed checkBlock",
                event.block_height, event.block_hash
            )));
        }

        let origin = parse_h256_hex(&event.origin, "finalized event origin")?;
        let mut transaction_id = [0u8; 64];
        transaction_id[32..].copy_from_slice(origin.as_bytes());
        Ok(FinalizedEventProvenance {
            block_height: event.block_height,
            block_hash,
            transaction_id: H512::from(transaction_id),
            event_id: event.id,
        })
    }

    /// Return the latest block height finalized by the node's consensus view.
    pub(crate) async fn finalized_block_height(&self) -> Result<u64, HyperlaneDuskError> {
        let data = self
            .graphql_query("query { lastBlockPair { json } }")
            .await?;
        parse_finalized_block_height(&data)
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
                    "Ledger transaction {tx_id} is missing numeric blockHeight: {transaction}"
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
        let url = format!("{}/on/account:{}/status", self.base_url, bs58_pk);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(Vec::new())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "account status query").await?;
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        serde_json::from_slice::<AccountStatus>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse account status JSON: {e}. Body: {}",
                String::from_utf8_lossy(&body)
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
                "Invalid contract ID hex '{contract_id_hex}': {error}"
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
        let url = format!("{}/on/blocks/gas-price", self.base_url);

        let response = self
            .client
            .post(&url)
            .headers(self.default_headers())
            .body(max_transactions.to_string())
            .send()
            .await?;

        let status = response.status();
        let body = read_bounded_body(response, "gas price query").await?;
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body: String::from_utf8_lossy(&body).into_owned(),
            });
        }

        serde_json::from_slice::<GasPriceStats>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse gas price stats JSON: {e}. Body: {}",
                String::from_utf8_lossy(&body)
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
            "Invalid Dusk transaction ID '{tx_id}'"
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
                "Ledger transaction {tx_id} is missing numeric gasSpent: {transaction}"
            ))
        })?;
    let error = match transaction.get("err") {
        Some(JsonValue::Null) => None,
        Some(JsonValue::String(error)) => Some(error.clone()),
        None => {
            return Err(HyperlaneDuskError::Other(format!(
                "Ledger transaction {tx_id} is missing err status: {transaction}"
            )))
        }
        Some(other) => {
            return Err(HyperlaneDuskError::Other(format!(
                "Ledger transaction {tx_id} has invalid err field: {other}"
            )))
        }
    };
    Ok(ConfirmedTransaction { gas_spent, error })
}

fn parse_finalized_block_height(data: &JsonValue) -> Result<u64, HyperlaneDuskError> {
    let pair = data
        .get("lastBlockPair")
        .and_then(|value| value.get("json"))
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing lastBlockPair.json: {data}"
            ))
        })?;
    let latest = pair
        .get("last_block")
        .and_then(JsonValue::as_array)
        .and_then(|value| value.first())
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing numeric last_block height: {pair}"
            ))
        })?;
    let finalized = pair
        .get("last_finalized_block")
        .and_then(JsonValue::as_array)
        .and_then(|value| value.first())
        .and_then(JsonValue::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Rusk archive response is missing numeric last_finalized_block height: {pair}"
            ))
        })?;
    if finalized > latest {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalized height {finalized} exceeds latest height {latest}"
        )));
    }
    Ok(finalized)
}

fn validate_finalized_event_page(
    contract_id: &[u8; 32],
    requested_cursor: Option<&str>,
    previous_id: Option<i64>,
    page: &FinalizedEventPage,
) -> Result<(), HyperlaneDuskError> {
    let source = hex::encode(contract_id);
    if page.events.len() > FINALIZED_EVENT_PAGE_SIZE {
        return Err(HyperlaneDuskError::Other(format!(
            "Rusk finalizedEvents returned {} rows above requested limit {FINALIZED_EVENT_PAGE_SIZE}",
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
                event.source
            )));
        }
        last_id = Some(event.id);
    }
    Ok(())
}

fn validate_loaded_event_cache(cache: &FinalizedEventCache) -> Result<(), HyperlaneDuskError> {
    if cache.last_id.is_some() && cache.cursor.is_none() {
        return Err(HyperlaneDuskError::Other(
            "Dusk event cursor has rows but no opaque archive cursor".into(),
        ));
    }
    if cache.topic_lengths.len() > MAX_TRACKED_EVENT_TOPICS {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk event cursor tracks more than {MAX_TRACKED_EVENT_TOPICS} topics"
        )));
    }
    for topic in cache.topic_lengths.keys() {
        if topic.is_empty() || topic.len() > MAX_EVENT_TOPIC_BYTES {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk event cursor contains an empty or oversized topic (maximum {MAX_EVENT_TOPIC_BYTES} bytes)"
            )));
        }
    }
    Ok(())
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
            "Persisted Dusk finalized event contains invalid data: {error}"
        ))
    })?;
    Ok(())
}

fn finalized_event_state_key(contract_id: &[u8; 32]) -> Vec<u8> {
    let mut key = b"dusk-finalized-state-v1:".to_vec();
    key.extend_from_slice(contract_id);
    key
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
    let mut key = b"dusk-finalized-row-v1:".to_vec();
    key.extend_from_slice(contract_id);
    key.extend_from_slice(&topic_hash);
    key.extend_from_slice(&sequence.to_be_bytes());
    Ok(key)
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
        HyperlaneDuskError::Other(format!("Invalid {field} hex '{value}': {error}"))
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
    context: &str,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_RUES_RESPONSE_BYTES as u64)
    {
        return Err(HyperlaneDuskError::Other(format!(
            "{context} response exceeds {MAX_RUES_RESPONSE_BYTES} bytes"
        )));
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .unwrap_or_default()
            .min(MAX_RUES_RESPONSE_BYTES as u64) as usize,
    );
    while let Some(chunk) = response.chunk().await? {
        if chunk.len() > MAX_RUES_RESPONSE_BYTES.saturating_sub(body.len()) {
            return Err(HyperlaneDuskError::Other(format!(
                "{context} response exceeds {MAX_RUES_RESPONSE_BYTES} bytes"
            )));
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
        .map_err(|e| HyperlaneDuskError::RkyvDeserialize(format!("{e}")))?;
    archived
        .deserialize(&mut Infallible)
        .map_err(|e| HyperlaneDuskError::RkyvDeserialize(format!("{e:?}")))
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::thread;

    use super::*;

    fn test_server(responses: Vec<(u16, &'static str)>) -> Url {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        thread::spawn(move || {
            for (status, body) in responses {
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
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
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
            parse_finalized_block_height(&serde_json::json!({
                "lastBlockPair": {
                    "json": {
                        "last_block": [46, "latest"],
                        "last_finalized_block": [45, "finalized"]
                    }
                }
            }))
            .unwrap(),
            45
        );
        assert!(parse_finalized_block_height(&serde_json::json!({
            "lastBlockPair": {
                "json": {
                    "last_block": [45, "latest"],
                    "last_finalized_block": [46, "finalized"]
                }
            }
        }))
        .is_err());
        assert!(parse_finalized_block_height(&serde_json::json!({})).is_err());
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
    async fn finalized_events_use_row_owned_provenance_and_durable_cursor() {
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
        assert_eq!(provenance.event_id, 11);
        assert!(store_path.join("CURRENT").is_file());
        drop(client);

        // A fresh client loads the opaque cursor and row mapping rather than
        // requesting finalizedEvents from genesis again. It performs only the
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
    async fn caught_up_finalized_cursor_can_index_later_events() {
        let contract_id = [7u8; 32];
        let source = hex::encode(contract_id);
        let first_data = vec![1u8];
        let second_data = vec![2u8];
        let first_page = serde_json::json!({
            "finalizedEvents": { "json": {
                "events": [{
                    "id": 11,
                    "block_height": 7,
                    "block_hash": hex::encode([8u8; 32]),
                    "origin": hex::encode([9u8; 32]),
                    "topic": "dispatch",
                    "source": source,
                    "data": hex::encode(&first_data),
                    "reverted": false
                }],
                "startCursor": "djE6MTE=",
                "endCursor": "djE6MTE=",
                "hasNextPage": false
            }}
        })
        .to_string();
        let second_page = serde_json::json!({
            "finalizedEvents": { "json": {
                "events": [{
                    "id": 12,
                    "block_height": 8,
                    "block_hash": hex::encode([10u8; 32]),
                    "origin": hex::encode([11u8; 32]),
                    "topic": "dispatch",
                    "source": hex::encode(contract_id),
                    "data": hex::encode(&second_data),
                    "reverted": false
                }],
                "startCursor": "djE6MTI=",
                "endCursor": "djE6MTI=",
                "hasNextPage": false
            }}
        })
        .to_string();
        let first_page: &'static str = Box::leak(first_page.into_boxed_str());
        let second_page: &'static str = Box::leak(second_page.into_boxed_str());
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

        client
            .finalized_contract_event(&contract_id, "dispatch", 0, 7, &first_data)
            .await
            .unwrap();
        let later = client
            .finalized_contract_event(&contract_id, "dispatch", 1, 8, &second_data)
            .await
            .unwrap();
        assert_eq!(later.event_id, 12);
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
        assert!(
            validate_finalized_event_page(&contract_id, Some("same"), Some(10), &page).is_err()
        );
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
    async fn endpoint_chain_id_mismatch_fails_before_contract_queries() {
        let client = RuesClient::new(test_server(vec![(200, "\u{2}")])).unwrap();
        let error = client
            .validate_chain_identity(1, 4242, &[3u8; 32], &[4u8; 32])
            .await
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("does not match endpoint chain ID 2"));
    }
}
