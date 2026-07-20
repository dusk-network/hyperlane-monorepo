use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rkyv::ser::serializers::AllocSerializer;
use rkyv::ser::Serializer;
use rkyv::validation::validators::DefaultValidator;
use rkyv::{check_archived_root, Archive, Deserialize, Infallible, Serialize};
use serde::Deserialize as SerdeDeserialize;
use serde_json::Value as JsonValue;
use url::Url;

use crate::HyperlaneDuskError;

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
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
            });
        }

        Ok(response.bytes().await?.to_vec())
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
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
            });
        }

        let body = response.text().await.unwrap_or_default();
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
            let body = response.text().await.unwrap_or_default();
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
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
            let body = response.text().await.unwrap_or_default();
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
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
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
            });
        }

        serde_json::from_str::<JsonValue>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse GraphQL JSON response: {e}. Body: {body}"
            ))
        })
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
        if tx_id.len() != 64 || !tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(HyperlaneDuskError::Other(format!(
                "Invalid Dusk transaction ID '{tx_id}'"
            )));
        }

        let query = format!(r#"query {{ tx(hash: \"{tx_id}\") {{ gasSpent err }} }}"#);
        let deadline = tokio::time::Instant::now() + timeout;

        loop {
            let data = self.graphql_query(&query).await?;
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

            if tokio::time::Instant::now() >= deadline {
                return Err(HyperlaneDuskError::Other(format!(
                    "Timed out after {}s waiting for Dusk transaction {tx_id} to be included",
                    timeout.as_secs()
                )));
            }

            tokio::time::sleep(Duration::from_secs(2)).await;
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
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
            });
        }

        serde_json::from_str::<AccountStatus>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse account status JSON: {e}. Body: {body}"
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
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(HyperlaneDuskError::RuesResponse {
                status: status.as_u16(),
                body,
            });
        }

        serde_json::from_str::<GasPriceStats>(&body).map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to parse gas price stats JSON: {e}. Body: {body}"
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
