use std::collections::HashSet;
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

use crate::final_records::FinalRecords;
use crate::rues::{exact_event_data, rkyv_serialize};
use crate::tx_sender::{dusk_tx_id_to_h512, h512_to_dusk_tx_id};
use crate::{DuskProvider, RuesClient};

const GAS_PAYMENT_PAGE_SIZE: u32 = 256;

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
    /// Where the payments last crossed a final height.
    payments: Arc<FinalRecords>,
}

impl DuskInterchainGasPaymasterIndexer {
    /// Create a new indexer.
    pub fn new(rues: Arc<RuesClient>, igp_id: H256) -> Self {
        Self {
            igp_id: igp_id.into(),
            igp_address: igp_id,
            payments: rues.final_records(igp_id.into(), "gas_payment_at"),
            rues,
        }
    }

    async fn payment_record(&self, sequence: u32) -> ChainResult<GasPaymentRecord> {
        Ok(self
            .rues
            .contract_query(&self.igp_id, "gas_payment_at", &(sequence,))
            .await?)
    }

    async fn payment_count(&self) -> ChainResult<u32> {
        Ok(self
            .rues
            .contract_query(&self.igp_id, "gas_payment_count", &())
            .await?)
    }

    /// How many payments are at or below final block `block_height`.
    async fn payments_at_or_below(&self, block_height: u64) -> ChainResult<u32> {
        self.payments
            .count_at_or_below(
                block_height,
                || self.payment_count(),
                |sequence| async move { Ok(self.payment_record(sequence).await?.block_height) },
            )
            .await
    }

    /// The payments of final block `block_height`.
    async fn payment_range_at_block(
        &self,
        block_height: u64,
    ) -> ChainResult<Option<RangeInclusive<u32>>> {
        let first = match block_height.checked_sub(1) {
            Some(below) => self.payments_at_or_below(below).await?,
            None => 0,
        };
        let after = self.payments_at_or_below(block_height).await?;
        if after == first {
            return Ok(None);
        }
        Ok(Some(crate::bounded_block_range(
            first,
            after,
            "IGP payment",
        )?))
    }

    /// The final payments and the last final block.
    async fn finalized_payment_count(&self) -> ChainResult<(u32, u32)> {
        let finalized_tip = self.rues.finalized_block_number().await?;
        let finalized_count = self.payments_at_or_below(u64::from(finalized_tip)).await?;
        Ok((finalized_count, finalized_tip))
    }

    /// The stored payment records in `range` below `count`, read a page of
    /// records at a time.
    async fn payment_records(
        &self,
        range: RangeInclusive<u32>,
        count: u32,
    ) -> ChainResult<Vec<(u32, GasPaymentRecord)>> {
        let mut stored = Vec::new();
        let mut next = *range.start();
        let requested_end = *range.end();
        while next < count && next <= requested_end {
            let limit = (count - next)
                .min(requested_end.saturating_sub(next).saturating_add(1))
                .min(GAS_PAYMENT_PAGE_SIZE);
            let records: Vec<GasPaymentRecord> = self
                .rues
                .contract_query(&self.igp_id, "gas_payments", &(next, limit))
                .await?;
            if records.len() != limit as usize {
                return Err(crate::HyperlaneDuskError::Other(format!(
                    "IGP returned {} payment records for page start={next} limit={limit}",
                    records.len()
                ))
                .into());
            }
            for (offset, record) in records.into_iter().enumerate() {
                ensure_cursor_height(record.block_height)?;
                stored.push((next + offset as u32, record));
            }
            next += limit;
        }
        Ok(stored)
    }

    /// Match finalized stored payment records to their archive rows, one
    /// archive pass per block, however many payments it holds. Identical
    /// payments in one block share the first matching row.
    async fn payment_logs(
        &self,
        stored: &[(u32, GasPaymentRecord)],
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        let mut results = Vec::with_capacity(stored.len());
        for block in stored.chunk_by(|a, b| a.1.block_height == b.1.block_height) {
            let Some((_, first)) = block.first() else {
                continue;
            };
            let mut expected = Vec::with_capacity(block.len());
            for (index, record) in block {
                expected.push((*index as usize, payment_event(record)?));
            }
            let sequences: Vec<usize> = expected.iter().map(|(index, _)| *index).collect();
            let matches = self
                .rues
                .finalized_contract_events_at_block(
                    &self.igp_id,
                    events::GasPayment::TOPIC,
                    first.block_height,
                    &sequences,
                    exact_event_data(expected),
                )
                .await?;

            for ((index, record), matched) in block.iter().zip(matches) {
                let payment = InterchainGasPayment {
                    message_id: H256::from_slice(&record.message_id),
                    destination: record.destination,
                    payment: U256::from(record.payment),
                    gas_amount: U256::from(record.gas_limit),
                };
                let provenance = matched.provenance;
                let indexed = Indexed::from(payment).with_sequence(*index);
                let meta = LogMeta {
                    address: self.igp_address,
                    block_number: provenance.block_height,
                    block_hash: provenance.block_hash,
                    transaction_id: provenance.transaction_id,
                    transaction_index: 0,
                    log_index: U256::from(*index),
                };
                results.push((indexed, meta));
            }
        }
        Ok(results)
    }
}

/// The `GasPayment` event the IGP emits for a stored record.
fn payment_event(record: &GasPaymentRecord) -> ChainResult<Vec<u8>> {
    Ok(rkyv_serialize(&events::GasPayment {
        message_id: record.message_id,
        gas_limit: record.gas_limit,
        payment: record.payment,
    })?)
}

#[async_trait]
impl Indexer<InterchainGasPayment> for DuskInterchainGasPaymasterIndexer {
    async fn fetch_logs_in_range(
        &self,
        range: RangeInclusive<u32>,
    ) -> ChainResult<Vec<(Indexed<InterchainGasPayment>, LogMeta)>> {
        let (finalized_count, _) = self.finalized_payment_count().await?;
        let stored = self.payment_records(range, finalized_count).await?;
        let results = self.payment_logs(&stored).await?;
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
        let rows = self
            .rues
            .finalized_rows_of_transaction(
                &self.igp_id,
                events::GasPayment::TOPIC,
                block_height,
                &crate::dusk_transaction_bytes(&tx_hash),
            )
            .await?;
        // Contract state decides whether the block holds any payment, also
        // when the transaction's rows are empty: an empty selection is not
        // evidence that it paid nothing.
        let Some(range) = self.payment_range_at_block(block_height).await? else {
            return Ok(vec![]);
        };
        crate::ensure_tx_hash_lookup_budget(&range, "IGP payment")?;
        if let Some(rows) = rows.filter(|rows| !rows.is_empty()) {
            // Only a record whose event bytes the transaction emitted can
            // have it as provenance, so only those are matched, against the
            // block's rows read once. The block is at or below the finalized
            // height, so all of its records are finalized.
            let emitted: HashSet<Vec<u8>> = rows.into_iter().collect();
            let mut candidates = Vec::new();
            let after = range.end().saturating_add(1);
            for (index, record) in self.payment_records(range, after).await? {
                if emitted.contains(&payment_event(&record)?) {
                    candidates.push((index, record));
                }
            }
            return Ok(self
                .payment_logs(&candidates)
                .await?
                .into_iter()
                .filter(|(_, meta)| {
                    meta.transaction_id == tx_hash && meta.block_number == block_height
                })
                .collect());
        }
        let mut logs = Vec::new();
        for chunk in crate::block_range_chunks(range) {
            logs.extend(
                self.fetch_logs_in_range(chunk)
                    .await?
                    .into_iter()
                    .filter(|(_, meta)| meta.transaction_id == tx_hash),
            );
        }
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
    /// The final payments and the last final block. While the node has
    /// finalized no new block, this reads only the shared `lastBlockPair`.
    async fn latest_sequence_count_and_tip(&self) -> ChainResult<(Option<u32>, u32)> {
        let (finalized_count, finalized_tip) = self.finalized_payment_count().await?;
        Ok((Some(finalized_count), finalized_tip))
    }
}
