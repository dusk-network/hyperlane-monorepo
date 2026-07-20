use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use hyperlane_core::{
    BlockInfo, ChainCommunicationError, ChainInfo, ChainResult, HyperlaneChain, HyperlaneDomain,
    HyperlaneProvider, HyperlaneProviderError, TxnInfo, H256, H512, U256,
};

use crate::tx_sender::h512_to_dusk_tx_id;
use crate::{HyperlaneDuskError, RuesClient};

/// Dusk provider implementing `HyperlaneProvider`.
#[derive(Debug, Clone)]
pub struct DuskProvider {
    domain: HyperlaneDomain,
    rues: Arc<RuesClient>,
}

impl DuskProvider {
    /// Create a new Dusk provider.
    pub fn new(domain: HyperlaneDomain, rues: Arc<RuesClient>) -> Self {
        Self { domain, rues }
    }

    /// Get a reference to the RUES client.
    pub fn rues(&self) -> &Arc<RuesClient> {
        &self.rues
    }
}

impl HyperlaneChain for DuskProvider {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl HyperlaneProvider for DuskProvider {
    async fn get_block_by_height(&self, height: u64) -> ChainResult<BlockInfo> {
        let query =
            format!("query {{ block(height: {height}) {{ header {{ height hash timestamp }} }} }}");
        let data = self.rues.graphql_query(&query).await?;

        let block = data
            .get("block")
            .ok_or(HyperlaneDuskError::BlockNotFound(height))?;
        if block.is_null() {
            return Err(HyperlaneDuskError::BlockNotFound(height).into());
        }

        let header = block
            .get("header")
            .and_then(|v| v.as_object())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!("GraphQL block response missing header: {data}"))
            })?;

        let returned_height = required_u64(header, "height", "block header")?;
        if returned_height != height {
            return Err(
                HyperlaneProviderError::IncorrectBlockByHeight(height, returned_height).into(),
            );
        }

        let hash_hex = header.get("hash").and_then(|v| v.as_str()).ok_or_else(|| {
            HyperlaneDuskError::Other(format!("GraphQL block header missing hash: {data}"))
        })?;
        let hash_bytes = hex::decode(hash_hex).map_err(|e| {
            HyperlaneDuskError::Other(format!("Invalid hex block hash '{hash_hex}': {e}"))
        })?;
        if hash_bytes.len() != 32 {
            return Err(HyperlaneDuskError::Other(format!(
                "Block hash is not 32 bytes (got {}): {hash_hex}",
                hash_bytes.len()
            ))
            .into());
        }

        let timestamp = header
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!("GraphQL block header missing timestamp: {data}"))
            })?;

        Ok(BlockInfo {
            hash: H256::from_slice(&hash_bytes),
            timestamp,
            number: height,
        })
    }

    async fn get_txn_by_hash(&self, hash: &H512) -> ChainResult<TxnInfo> {
        // Reject non-canonical H512 values instead of silently aliasing any
        // upper 32 bytes onto the same Dusk transaction ID.
        let dusk_tx_hex = h512_to_dusk_tx_id(hash)?;

        let query = format!(
            "query {{ tx(hash: \"{dusk_tx_hex}\") {{ gasSpent tx {{ gasLimit gasPrice raw json callData {{ contractId }} }} }} }}"
        );
        let data = self.rues.graphql_query(&query).await?;

        let spent = data
            .get("tx")
            .ok_or_else(|| HyperlaneDuskError::TransactionNotFound(*hash))?;
        if spent.is_null() {
            return Err(ChainCommunicationError::from_other(
                HyperlaneDuskError::TransactionNotFound(*hash),
            ));
        }

        let inner_tx = spent.get("tx").and_then(|v| v.as_object()).ok_or_else(|| {
            HyperlaneDuskError::Other(format!("GraphQL tx response missing tx field: {data}"))
        })?;

        let gas_limit = required_u64(inner_tx, "gasLimit", "transaction")?;
        let gas_price = required_u64(inner_tx, "gasPrice", "transaction")?;
        let transaction_json = inner_tx
            .get("json")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL transaction is missing its JSON identity data: {data}"
                ))
            })?;
        let (sender, nonce) = moonlight_identity_from_json(transaction_json)?;

        let raw_input_data = inner_tx
            .get("raw")
            .and_then(|v| v.as_str())
            .and_then(|hex_str| hex::decode(hex_str).ok());

        let recipient = inner_tx
            .get("callData")
            .and_then(|v| v.as_object())
            .and_then(|cd| cd.get("contractId").and_then(|v| v.as_str()))
            .and_then(|contract_hex| {
                let bytes = hex::decode(contract_hex).ok()?;
                (bytes.len() == 32).then(|| H256::from_slice(&bytes))
            });

        let gas_used = spent
            .get("gasSpent")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL spent transaction missing numeric gasSpent: {data}"
                ))
            })?;

        Ok(TxnInfo {
            hash: *hash,
            gas_limit: U256::from(gas_limit),
            max_priority_fee_per_gas: None,
            max_fee_per_gas: None,
            gas_price: Some(U256::from(gas_price)),
            nonce,
            sender,
            recipient,
            receipt: Some(hyperlane_core::TxnReceiptInfo {
                gas_used: U256::from(gas_used),
                cumulative_gas_used: U256::from(gas_used),
                effective_gas_price: Some(U256::from(gas_price)),
            }),
            raw_input_data,
        })
    }

    async fn is_contract(&self, address: &H256) -> ChainResult<bool> {
        // Query VM metadata for the contract owner. This avoids relying on
        // contract-specific methods (many contracts do not expose `owner()`).
        let contract_id: [u8; 32] = (*address).into();
        let metadata = self.rues.contract_metadata(&contract_id).await?;
        Ok(!metadata.contract_owner.is_empty())
    }

    async fn get_balance(&self, address: String) -> ChainResult<U256> {
        let addr = address.trim();
        let hex_addr = addr.strip_prefix("0x").unwrap_or(addr);
        let is_hex_contract_id =
            hex_addr.len() == 64 && hex_addr.chars().all(|c| c.is_ascii_hexdigit());

        if is_hex_contract_id {
            let status = self.rues.contract_status(hex_addr).await?;
            return Ok(U256::from(status.balance));
        }

        // Otherwise treat as bs58-encoded BLS public key (Moonlight account).
        let status = self.rues.account_status(addr).await?;
        Ok(U256::from(status.balance))
    }

    async fn get_chain_metrics(&self) -> ChainResult<Option<ChainInfo>> {
        let query = "query { blocks(last: 1) { header { height hash timestamp } } }";
        let data = self.rues.graphql_query(query).await?;

        let first = data
            .get("blocks")
            .and_then(|v| v.as_array())
            .and_then(|ary| ary.first())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!("GraphQL blocks response missing blocks: {data}"))
            })?;

        let header = first
            .get("header")
            .and_then(|v| v.as_object())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!("GraphQL blocks response missing header: {data}"))
            })?;

        let height = required_u64(header, "height", "latest block header")?;
        let timestamp = required_u64(header, "timestamp", "latest block header")?;
        let hash_hex = header.get("hash").and_then(|v| v.as_str()).ok_or_else(|| {
            HyperlaneDuskError::Other(format!("GraphQL latest block header missing hash: {data}"))
        })?;
        let hash_bytes = hex::decode(hash_hex).map_err(|error| {
            HyperlaneDuskError::Other(format!("Invalid latest block hash '{hash_hex}': {error}"))
        })?;
        if hash_bytes.len() != 32 {
            return Err(HyperlaneDuskError::Other(format!(
                "Latest block hash is not 32 bytes (got {}): {hash_hex}",
                hash_bytes.len()
            ))
            .into());
        }
        let hash = H256::from_slice(&hash_bytes);

        let gas_price = self.rues.gas_price_stats(200).await?.average;

        Ok(Some(ChainInfo::new(
            BlockInfo {
                hash,
                timestamp,
                number: height,
            },
            Some(U256::from(gas_price)),
        )))
    }
}

fn moonlight_identity_from_json(value: &str) -> Result<(H256, u64), HyperlaneDuskError> {
    let transaction: serde_json::Value = serde_json::from_str(value).map_err(|error| {
        HyperlaneDuskError::Other(format!("Invalid ledger transaction JSON: {error}"))
    })?;
    if transaction.get("type").and_then(serde_json::Value::as_str) != Some("moonlight") {
        return Err(HyperlaneDuskError::Other(
            "Dusk transaction identity is only defined here for Moonlight transactions".into(),
        ));
    }
    let nonce = transaction
        .get("nonce")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Moonlight transaction JSON is missing numeric nonce: {transaction}"
            ))
        })?;
    let sender_bs58 = transaction
        .get("sender")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Moonlight transaction JSON is missing sender: {transaction}"
            ))
        })?;
    let sender_bytes = bs58::decode(sender_bs58).into_vec().map_err(|error| {
        HyperlaneDuskError::Other(format!("Invalid Moonlight sender encoding: {error}"))
    })?;
    if sender_bytes.len() != 96 {
        return Err(HyperlaneDuskError::Other(format!(
            "Moonlight sender must decode to 96 bytes, got {}",
            sender_bytes.len()
        )));
    }
    let sender = hyperlane_dusk_types::message::keccak256(&sender_bytes);
    Ok((H256::from_slice(&sender), nonce))
}

fn required_u64(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &str,
    context: &str,
) -> Result<u64, HyperlaneDuskError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "GraphQL {context} missing numeric {field}: {}",
                serde_json::Value::Object(object.clone())
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::{moonlight_identity_from_json, required_u64};
    use hyperlane_core::H256;
    use serde_json::{json, Map, Value};

    fn object(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn moonlight_transaction_identity_uses_the_real_sender_and_nonce() {
        let sender = [7u8; 96];
        let encoded = bs58::encode(sender).into_string();
        let json = serde_json::json!({
            "type": "moonlight",
            "sender": encoded,
            "nonce": 42,
        })
        .to_string();
        let (identity, nonce) = moonlight_identity_from_json(&json).unwrap();
        assert_eq!(
            identity,
            H256::from_slice(&hyperlane_dusk_types::message::keccak256(&sender))
        );
        assert_eq!(nonce, 42);
        assert!(moonlight_identity_from_json(r#"{"type":"phoenix","nonce":1}"#).is_err());
    }

    #[test]
    fn mandatory_ledger_numbers_fail_closed() {
        assert_eq!(
            required_u64(&object(json!({ "height": 42 })), "height", "block").unwrap(),
            42
        );
        assert!(required_u64(&object(json!({})), "height", "block").is_err());
        assert!(required_u64(&object(json!({ "height": "42" })), "height", "block").is_err());
    }
}
