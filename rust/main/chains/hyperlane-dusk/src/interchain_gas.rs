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

use hyperlane_dusk_types::{events, GasPaymentRecord};

use crate::rues::{contract_event_transaction_id, rkyv_serialize, ArchivedContractEvent};
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
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

    async fn payment_ordinal_in_block(
        &self,
        sequence: u32,
        block_height: u64,
    ) -> ChainResult<usize> {
        let first = self
            .first_payment_at_or_after(sequence + 1, block_height)
            .await?;
        Ok((sequence - first) as usize)
    }

    async fn payment_record(&self, sequence: u32) -> ChainResult<GasPaymentRecord> {
        Ok(self
            .rues
            .contract_query(&self.igp_id, "gas_payment_at", &(sequence,))
            .await?)
    }

    async fn first_payment_at_or_after(&self, count: u32, block_height: u64) -> ChainResult<u32> {
        let mut low = 0u32;
        let mut high = count;
        while low < high {
            let middle = low + (high - low) / 2;
            if self.payment_record(middle).await?.block_height < block_height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    async fn payment_range_at_block(
        &self,
        count: u32,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = self.first_payment_at_or_after(count, block_height).await?;
        if first == count || self.payment_record(first).await?.block_height != block_height {
            return Ok(None);
        }
        let after = self
            .first_payment_at_or_after(count, block_height.saturating_add(1))
            .await?;
        Ok(Some(first..=after - 1))
    }

    async fn finalized_payment_count(&self, current_count: u32) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self
            .first_payment_at_or_after(current_count, u64::from(finalized_tip) + 1)
            .await?;
        Ok((finalized_count, finalized_tip))
    }
}

#[async_trait]
impl Indexer<InterchainGasPayment> for DuskInterchainGasPaymasterIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        let mut results = Vec::new();
        let mut archive_height = None;
        let mut archive_events = Vec::<ArchivedContractEvent>::new();
        let mut archive_block_hash = H256::zero();
        let mut payment_ordinal = 0usize;

        let payment_count: u32 = self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?;
        let (finalized_count, _) = self.finalized_payment_count(payment_count).await?;

        for index in range {
            if index >= finalized_count {
                break;
            }

            let record: GasPaymentRecord = self
                .rues
                .contract_query(&self.igp_id, "gas_payment_at", &(index,))
                .await?;
            ensure_cursor_height(record.block_height)?;

            let payment = InterchainGasPayment {
                message_id: H256::from_slice(&record.message_id),
                destination: record.destination,
                payment: U256::from(record.payment),
                gas_amount: U256::from(record.gas_limit),
            };

            if archive_height != Some(record.block_height) {
                archive_events = self.rues.contract_events_at(record.block_height).await?;
                archive_block_hash = self.rues.block_hash_at(record.block_height).await?;
                payment_ordinal = self
                    .payment_ordinal_in_block(index, record.block_height)
                    .await?;
                archive_height = Some(record.block_height);
            }

            let expected_event = rkyv_serialize(&events::GasPayment {
                message_id: record.message_id,
                gas_limit: record.gas_limit,
                payment: record.payment,
            })?;
            let transaction_id = contract_event_transaction_id(
                &archive_events,
                &self.igp_id,
                events::GasPayment::TOPIC,
                payment_ordinal,
                &expected_event,
            )?;
            payment_ordinal += 1;

            let indexed = Indexed::from(payment).with_sequence(index);
            let meta = LogMeta {
                address: self.igp_address,
                block_number: record.block_height,
                block_hash: archive_block_hash,
                transaction_id,
                transaction_index: 0,
                log_index: U256::from(index),
            };

            results.push((indexed, meta));
        }

        debug!(count = results.len(), "Fetched interchain gas payment logs");
        Ok(results)
    }

    async fn get_finalized_block_number(&self) -> ChainResult<u32> {
        Ok(self.rues.finalized_block_number().await?)
    }

    async fn fetch_logs_by_tx_hash(
        &self,
        tx_hash: H512,
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        let tx_id = h512_to_dusk_tx_id(&tx_hash)?;
        let Some(block_height) = self.rues.transaction_block_height(&tx_id).await? else {
            return Ok(vec![]);
        };
        ensure_cursor_height(block_height)?;
        if block_height > self.rues.finalized_block_height().await? {
            return Ok(vec![]);
        }
        let count: u32 = self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?;
        let Some(range) = self.payment_range_at_block(count, block_height).await? else {
            return Ok(vec![]);
        };
        let mut logs = self.fetch_logs_in_range(range).await?;
        logs.retain(|(_, meta)| meta.transaction_id == tx_hash);
        Ok(logs)
    }

    fn parse_tx_hash(&self, tx_hash: &str) -> ChainResult<H512> {
        Ok(dusk_tx_id_to_h512(tx_hash)?)
    }
}

fn ensure_cursor_height(block_height: u64) -> ChainResult<()> {
    if block_height > u64::from(u32::MAX) {
        return Err(crate::HyperlaneDuskError::Other(format!(
            "Dusk block height {block_height} exceeds the shared u32 cursor range"
        ))
        .into());
    }
    Ok(())
}

#[async_trait]
impl SequenceAwareIndexer<InterchainGasPayment> for DuskInterchainGasPaymasterIndexer {
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let count: u32 = self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?;
        let (finalized_count, finalized_tip) = self.finalized_payment_count(count).await?;
        Ok((Some(finalized_count), finalized_tip))
    }
}
