// SPDX-License-Identifier: MIT OR Apache-2.0
//
//! Hyperlane agent crate for Dusk Network.
//!
//! Implements Hyperlane core traits (Mailbox, MerkleTreeHook, ISM, etc.)
//! for the Dusk blockchain, communicating via the RUES protocol.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use hyperlane_core::{ChainResult, Indexed, Indexer, LogMeta, H512};

pub use application::*;
pub use error::*;
pub use interchain_gas::*;
pub use interchain_security_module::*;
pub use mailbox::*;
pub use mailbox_indexer::*;
pub use merkle_tree_hook::*;
pub use multisig_ism::*;
pub use provider::*;
pub use rues::*;
pub use signer::*;
pub use trait_builder::*;
pub use validator_announce::*;

mod application;
mod error;
mod interchain_gas;
mod interchain_security_module;
mod mailbox;
mod mailbox_indexer;
mod merkle_tree_hook;
mod multisig_ism;
mod provider;
mod rues;
mod signer;
mod trait_builder;
pub(crate) mod tx_sender;
mod validator_announce;

const DUSK_LOG_LOOKUP_CHUNK_SIZE: u32 = 256;
const DUSK_TX_HASH_LOOKUP_MAX_RECORDS: u64 = 4_096;

/// Convert binary-search boundaries into a checked inclusive range.
///
/// The boundaries come from repeated remote reads. Treat inconsistent answers
/// as endpoint equivocation instead of allowing an unsigned underflow. Callers
/// process the resulting range in bounded chunks.
pub(crate) fn bounded_block_range(
    first: u32,
    after: u32,
    context: &str,
) -> ChainResult<RangeInclusive<u32>> {
    let count = after.checked_sub(first).ok_or_else(|| {
        HyperlaneDuskError::Other(format!(
            "Dusk {context} range boundaries are inconsistent: first={first} after={after}"
        ))
    })?;
    if count == 0 {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk {context} range is empty after a matching first record: first={first} after={after}"
        ))
        .into());
    }
    let end = after.checked_sub(1).ok_or_else(|| {
        HyperlaneDuskError::Other(format!("Dusk {context} range end underflow: after={after}"))
    })?;
    Ok(first..=end)
}

/// Iterate a valid block-local range in bounded-memory lookup chunks.
pub(crate) fn block_range_chunks(
    range: RangeInclusive<u32>,
) -> impl Iterator<Item = RangeInclusive<u32>> {
    let mut next = *range.start();
    let end = *range.end();
    let mut finished = next > end;
    std::iter::from_fn(move || {
        if finished {
            return None;
        }
        let start = next;
        let chunk_end = start
            .saturating_add(DUSK_LOG_LOOKUP_CHUNK_SIZE - 1)
            .min(end);
        if chunk_end == end {
            finished = true;
        } else {
            next = chunk_end + 1;
        }
        Some(start..=chunk_end)
    })
}

/// Bound the total work of an endpoint-derived transaction/block lookup.
///
/// Chunking bounds allocation per call, but does not bound aggregate work when
/// a hostile endpoint claims billions of records share one block. Background
/// sequence indexing remains independently chunkable; only ad-hoc tx-hash
/// reconciliation uses this fail-closed aggregate budget.
pub(crate) fn ensure_tx_hash_lookup_budget(
    range: &RangeInclusive<u32>,
    context: &str,
) -> ChainResult<()> {
    let first = u64::from(*range.start());
    let last = u64::from(*range.end());
    let count = last
        .checked_sub(first)
        .and_then(|span| span.checked_add(1))
        .ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Dusk {context} transaction lookup range is inconsistent: {first}..={last}"
            ))
        })?;
    ensure_tx_hash_record_budget(count, context)
}

/// The same fail-closed budget, for a lookup of `count` named records.
fn ensure_tx_hash_record_budget(count: u64, context: &str) -> ChainResult<()> {
    if count > DUSK_TX_HASH_LOOKUP_MAX_RECORDS {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk {context} transaction lookup spans {count} records, exceeding the fail-closed budget of {DUSK_TX_HASH_LOOKUP_MAX_RECORDS}"
        ))
        .into());
    }
    Ok(())
}

/// Index the `sequences` that a transaction's archive rows name, in runs of
/// consecutive sequences, and keep the logs that transaction emitted in
/// `block_height`. Each sequence is indexed like any other, so contract
/// state and the archive decide every log: the rows only say which
/// sequences to look at, instead of every record of the block.
pub(crate) async fn transaction_logs<T, I>(
    indexer: &I,
    sequences: BTreeSet<u32>,
    transaction_id: H512,
    block_height: u64,
    context: &str,
) -> ChainResult<Vec<(Indexed<T>, LogMeta)>>
where
    T: Send,
    I: Indexer<T>,
{
    ensure_tx_hash_record_budget(sequences.len() as u64, context)?;
    let mut logs = Vec::new();
    for run in sequence_runs(&sequences) {
        for chunk in block_range_chunks(run) {
            logs.extend(
                indexer
                    .fetch_logs_in_range(chunk)
                    .await?
                    .into_iter()
                    .filter(|(_, meta)| {
                        meta.transaction_id == transaction_id && meta.block_number == block_height
                    }),
            );
        }
    }
    Ok(logs)
}

/// The runs of consecutive sequences in `sequences`.
fn sequence_runs(sequences: &BTreeSet<u32>) -> Vec<RangeInclusive<u32>> {
    let mut runs: Vec<RangeInclusive<u32>> = Vec::new();
    for &sequence in sequences {
        match runs.last_mut() {
            Some(run) if run.end().checked_add(1) == Some(sequence) => {
                *run = *run.start()..=sequence;
            }
            _ => runs.push(sequence..=sequence),
        }
    }
    runs
}

/// The Dusk transaction ID inside a left-padded transaction hash, which
/// `h512_to_dusk_tx_id` has checked.
pub(crate) fn dusk_transaction_bytes(transaction_id: &H512) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(&transaction_id.as_bytes()[32..]);
    bytes
}

#[cfg(test)]
mod range_tests {
    use std::collections::BTreeSet;

    use super::{
        block_range_chunks, bounded_block_range, ensure_tx_hash_lookup_budget,
        ensure_tx_hash_record_budget, sequence_runs, DUSK_LOG_LOOKUP_CHUNK_SIZE,
        DUSK_TX_HASH_LOOKUP_MAX_RECORDS,
    };

    #[test]
    fn remote_block_ranges_are_checked_and_chunked() {
        assert_eq!(bounded_block_range(7, 10, "test").unwrap(), 7..=9);
        assert!(bounded_block_range(1, 0, "test").is_err());
        assert!(bounded_block_range(0, 0, "test").is_err());
        let large = bounded_block_range(0, DUSK_LOG_LOOKUP_CHUNK_SIZE + 1, "test").unwrap();
        assert_eq!(
            block_range_chunks(large).collect::<Vec<_>>(),
            vec![0..=255, 256..=256]
        );
        assert_eq!(
            block_range_chunks((u32::MAX - 1)..=u32::MAX).collect::<Vec<_>>(),
            vec![(u32::MAX - 1)..=u32::MAX]
        );
        let maximum = u32::try_from(DUSK_TX_HASH_LOOKUP_MAX_RECORDS).unwrap();
        ensure_tx_hash_lookup_budget(&(0..=(maximum - 1)), "test").unwrap();
        assert!(ensure_tx_hash_lookup_budget(&(0..=maximum), "test").is_err());
        ensure_tx_hash_lookup_budget(&((u32::MAX - 1)..=u32::MAX), "test").unwrap();
        ensure_tx_hash_record_budget(DUSK_TX_HASH_LOOKUP_MAX_RECORDS, "test").unwrap();
        assert!(ensure_tx_hash_record_budget(DUSK_TX_HASH_LOOKUP_MAX_RECORDS + 1, "test").is_err());
    }

    #[test]
    fn transaction_sequences_are_looked_up_in_consecutive_runs() {
        let sequences = BTreeSet::from([0, 1, 2, 5, 7, 8, u32::MAX - 1, u32::MAX]);
        assert_eq!(
            sequence_runs(&sequences),
            vec![0..=2, 5..=5, 7..=8, (u32::MAX - 1)..=u32::MAX]
        );
        assert!(sequence_runs(&BTreeSet::new()).is_empty());
    }
}
