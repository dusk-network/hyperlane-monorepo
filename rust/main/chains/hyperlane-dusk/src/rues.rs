use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{check_archived_root, Archive, Deserialize, Infallible, Serialize};
use serde::Deserialize as SerdeDeserialize;
use serde_json::Value as JsonValue;
use url::Url;

use hyperlane_core::{H256, H512};

use crate::HyperlaneDuskError;

const MAX_RUES_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
const TX_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// RUES (Rusk Unified Event System) HTTP client.
///
/// Communicates with Dusk nodes using the RUES protocol:
/// - Contract queries: `POST /on/contracts:{hex_id}/{method}`
/// - Transaction propagation: `POST /on/transactions/propagate`
#[derive(Debug, Clone)]
pub struct RuesClient {
    client: reqwest::Client,
    base_url: String,
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

/// Raw contract event returned by Rusk's archive GraphQL API.
#[derive(Debug, Clone, PartialEq, Eq, SerdeDeserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ArchivedContractEvent {
    pub origin: String,
    pub topic: String,
    pub source: String,
    pub data: String,
    pub reverted: bool,
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
        Ok(Self { client, base_url })
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

    /// Return archived contract events emitted in one block.
    pub(crate) async fn contract_events_at(
        &self,
        block_height: u64,
    ) -> Result<Vec<ArchivedContractEvent>, HyperlaneDuskError> {
        let query = format!("query {{ contractEvents(height: {block_height}) {{ json }} }}");
        let data = self.graphql_query(&query).await?;
        let events = data
            .get("contractEvents")
            .and_then(|events| events.get("json"))
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Rusk archive response is missing contractEvents.json at block {block_height}: {data}"
                ))
            })?;
        serde_json::from_value(events.clone()).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to decode archived contract events at block {block_height}: {error}"
            ))
        })
    }

    /// Resolve a canonical block hash by height.
    pub(crate) async fn block_hash_at(
        &self,
        block_height: u64,
    ) -> Result<H256, HyperlaneDuskError> {
        let query = format!("query {{ block(height: {block_height}) {{ header {{ hash }} }} }}");
        let data = self.graphql_query(&query).await?;
        let hash = data
            .get("block")
            .and_then(|block| block.get("header"))
            .and_then(|header| header.get("hash"))
            .and_then(JsonValue::as_str)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL block response is missing header.hash at height {block_height}: {data}"
                ))
            })?;
        parse_h256_hex(hash, "block hash")
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
        if tx_id.len() != 64 || !tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(HyperlaneDuskError::Other(format!(
                "Invalid Dusk transaction ID '{tx_id}'"
            )));
        }

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
                        let gas_spent =
                            tx.get("gasSpent")
                                .and_then(JsonValue::as_u64)
                                .ok_or_else(|| {
                                    HyperlaneDuskError::Other(format!(
                                    "Ledger transaction {tx_id} is missing numeric gasSpent: {tx}"
                                ))
                                })?;
                        let error = match tx.get("err") {
                            Some(JsonValue::Null) | None => None,
                            Some(JsonValue::String(error)) => Some(error.clone()),
                            Some(other) => {
                                return Err(HyperlaneDuskError::Other(format!(
                                    "Ledger transaction {tx_id} has invalid err field: {other}"
                                )))
                            }
                        };
                        return Ok(ConfirmedTransaction { gas_spent, error });
                    }
                    last_observation_error = None;
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

pub(crate) fn contract_event_transaction_id(
    events: &[ArchivedContractEvent],
    contract_id: &[u8; 32],
    topic: &str,
    ordinal: usize,
    expected_data: &[u8],
) -> Result<H512, HyperlaneDuskError> {
    let source = hex::encode(contract_id);
    let event = events
        .iter()
        .filter(|event| {
            !event.reverted
                && event.topic == topic
                && strip_hex_prefix(&event.source).eq_ignore_ascii_case(&source)
        })
        .nth(ordinal)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Archived event {source}/{topic} ordinal {ordinal} was not found"
            ))
        })?;

    let data = hex::decode(strip_hex_prefix(&event.data)).map_err(|error| {
        HyperlaneDuskError::Other(format!(
            "Archived event {source}/{topic} has invalid hex data: {error}"
        ))
    })?;
    if data != expected_data {
        return Err(HyperlaneDuskError::Other(format!(
            "Archived event {source}/{topic} ordinal {ordinal} does not match contract state"
        )));
    }

    let origin = parse_h256_hex(&event.origin, "contract event origin")?;
    let mut transaction_id = [0u8; 64];
    transaction_id[32..].copy_from_slice(origin.as_bytes());
    Ok(H512::from(transaction_id))
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

    #[tokio::test]
    async fn confirmation_retries_observation_errors_for_the_same_transaction() {
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
    async fn graphql_rejects_declared_oversized_responses_before_buffering() {
        let client = RuesClient::new(oversized_response_server()).unwrap();
        let error = client.graphql_query("query { ping }").await.unwrap_err();
        assert!(error.to_string().contains("response exceeds"));
    }

    #[test]
    fn archived_event_selection_preserves_real_transaction_origin_and_order() {
        let contract_id = [7u8; 32];
        let expected_data = vec![1, 2, 3];
        let first_origin = [8u8; 32];
        let second_origin = [9u8; 32];
        let events = vec![
            ArchivedContractEvent {
                origin: format!("0x{}", hex::encode(first_origin)),
                topic: "dispatch".into(),
                source: format!("0x{}", hex::encode(contract_id)),
                data: format!("0x{}", hex::encode(&expected_data)),
                reverted: false,
            },
            ArchivedContractEvent {
                origin: hex::encode(second_origin),
                topic: "dispatch".into(),
                source: hex::encode(contract_id),
                data: hex::encode(&expected_data),
                reverted: false,
            },
        ];

        let transaction_id =
            contract_event_transaction_id(&events, &contract_id, "dispatch", 1, &expected_data)
                .unwrap();
        assert_eq!(&transaction_id.as_bytes()[32..], &second_origin);
        assert!(
            contract_event_transaction_id(&events, &contract_id, "dispatch", 0, b"wrong").is_err()
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
}
