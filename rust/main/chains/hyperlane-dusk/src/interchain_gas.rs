use std::fmt::Debug;
use std::ops::RangeInclusive;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use hyperlane_core::{
    ChainResult, HyperlaneChain, HyperlaneContract, HyperlaneDomain, HyperlaneProvider, Indexed,
    Indexer, InterchainGasPaymaster, InterchainGasPayment, LogMeta, SequenceAwareIndexer, H256,
    H512, U256,
};

use hyperlane_dusk_types::GasPaymentRecord;

use crate::{DuskProvider, RuesClient};

/// Dusk InterchainGasPaymaster implementation (marker contract).
#[derive(Debug, Clone)]
pub struct DuskInterchainGasPaymaster {
    provider: Arc<DuskProvider>,
    igp_id: [u8; 32],
    domain: HyperlaneDomain,
}

impl DuskInterchainGasPaymaster {
    /// Create a new DuskInterchainGasPaymaster.
    pub fn new(provider: Arc<DuskProvider>, igp_id: H256, domain: HyperlaneDomain) -> Self {
        Self {
            provider,
            igp_id: igp_id.into(),
            domain,
        }
    }
}

impl HyperlaneChain for DuskInterchainGasPaymaster {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new((*self.provider).clone())
    }
}

impl HyperlaneContract for DuskInterchainGasPaymaster {
    fn address(&self) -> H256 {
        H256::from_slice(&self.igp_id)
    }
}

impl InterchainGasPaymaster for DuskInterchainGasPaymaster {}

/// Dusk IGP indexer — fetches stored gas payment records by sequence.
#[derive(Debug, Clone)]
pub struct DuskInterchainGasPaymasterIndexer {
    rues: Arc<RuesClient>,
    igp_id: [u8; 32],
    igp_address: H256,
}

impl DuskInterchainGasPaymasterIndexer {
    /// Create a new indexer.
    pub fn new(rues: Arc<RuesClient>, igp_id: H256) -> Self {
        Self {
            rues,
            igp_id: igp_id.into(),
            igp_address: igp_id,
        }
    }
}

#[async_trait]
impl Indexer<InterchainGasPayment> for DuskInterchainGasPaymasterIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        let mut results = Vec::new();

        let payment_count: u32 = self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?;

        for index in range {
            if index >= payment_count {
                break;
            }

            let record: GasPaymentRecord = self
                .rues
                .contract_query(&self.igp_id, "gas_payment_at", &(index,))
                .await?;

            let payment = InterchainGasPayment {
                message_id: H256::from_slice(&record.message_id),
                destination: record.destination,
                payment: U256::from(record.payment),
                gas_amount: U256::from(record.gas_limit),
            };

            let indexed = Indexed::from(payment).with_sequence(index);
            let meta = LogMeta {
                address: self.igp_address,
                block_number: record.block_height,
                block_hash: H256::zero(),
                transaction_id: H512::zero(),
                transaction_index: 0,
                log_index: U256::from(index),
            };

            results.push((indexed, meta));
        }

        debug!(count = results.len(), "Fetched interchain gas payment logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(0)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        _tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        Ok(vec![])
    }
}

#[async_trait]
impl SequenceAwareIndexer<InterchainGasPayment> for DuskInterchainGasPaymasterIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let count: u32 = self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?;
        Ok((Some(count), 0))
    }
}
