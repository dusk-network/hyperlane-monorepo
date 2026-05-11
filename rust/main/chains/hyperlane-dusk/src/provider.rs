use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use hyperlane_core::{
    BlockInfo, ChainCommunicationError, ChainInfo, ChainResult, HyperlaneChain, HyperlaneDomain,
    HyperlaneProvider, TxnInfo, H256, H512, U256,
};

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
        let query = format!(
            "query {{ block(height: {height}) {{ header {{ height hash timestamp }} }} }}"
        );
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
                HyperlaneDuskError::Other(format!(
                    "GraphQL block response missing header: {data}"
                ))
            })?;

        let hash_hex = header
            .get("hash")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL block header missing hash: {data}"
                ))
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
                HyperlaneDuskError::Other(format!(
                    "GraphQL block header missing timestamp: {data}"
                ))
            })?;

        Ok(BlockInfo {
            hash: H256::from_slice(&hash_bytes),
            timestamp,
            number: height,
        })
    }

    async fn get_txn_by_hash(&self, hash: &H512) -> ChainResult<TxnInfo> {
        // Dusk transaction IDs are 32 bytes. We embed them into `H512` by
        // left-padding with zeros, so the ID lives in the lower 32 bytes.
        let dusk_tx_id = &hash.as_bytes()[32..64];
        let dusk_tx_hex = hex::encode(dusk_tx_id);

        let query = format!(
            "query {{ tx(hash: \"{dusk_tx_hex}\") {{ gasSpent blockHeight err tx {{ id gasLimit gasPrice raw callData {{ contractId fnName data }} }} }} }}"
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

        let inner_tx = spent
            .get("tx")
            .and_then(|v| v.as_object())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL tx response missing tx field: {data}"
                ))
            })?;

        let gas_limit = inner_tx
            .get("gasLimit")
            .and_then(|v| v.as_u64())
            .unwrap_or_default();
        let gas_price = inner_tx
            .get("gasPrice")
            .and_then(|v| v.as_u64())
            .unwrap_or_default();

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
            .unwrap_or_default();

        Ok(TxnInfo {
            hash: *hash,
            gas_limit: U256::from(gas_limit),
            max_priority_fee_per_gas: None,
            max_fee_per_gas: None,
            gas_price: Some(U256::from(gas_price)),
            nonce: 0,
            sender: H256::zero(),
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
        match self.rues.contract_owner_raw(&contract_id).await {
            Ok(_) => Ok(true),
            Err(crate::HyperlaneDuskError::RuesResponse { status, body })
                if status == 500 && {
                    let body_lc = body.to_lowercase();
                    body.contains("ContractDoesNotExist")
                        || body_lc.contains("contract does not exist")
                        || body_lc.contains("contract owner not found")
                } =>
            {
                Ok(false)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn get_balance(&self, address: String) -> ChainResult<U256> {
        let addr = address.trim();
        let hex_addr = addr.strip_prefix("0x").unwrap_or(addr);
        let is_hex_contract_id = hex_addr.len() == 64 && hex_addr.chars().all(|c| c.is_ascii_hexdigit());

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
                HyperlaneDuskError::Other(format!(
                    "GraphQL blocks response missing blocks: {data}"
                ))
            })?;

        let header = first
            .get("header")
            .and_then(|v| v.as_object())
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "GraphQL blocks response missing header: {data}"
                ))
            })?;

        let height = header
            .get("height")
            .and_then(|v| v.as_u64())
            .unwrap_or_default();
        let timestamp = header
            .get("timestamp")
            .and_then(|v| v.as_u64())
            .unwrap_or_default();
        let hash_hex = header
            .get("hash")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let hash_bytes = hex::decode(hash_hex).unwrap_or_default();
        let hash = if hash_bytes.len() == 32 {
            H256::from_slice(&hash_bytes)
        } else {
            H256::zero()
        };

        let gas_price = self.rues.gas_price_stats(200).await.ok().map(|s| s.average);

        Ok(Some(ChainInfo::new(
            BlockInfo {
                hash,
                timestamp,
                number: height,
            },
            gas_price.map(U256::from),
        )))
    }
}
