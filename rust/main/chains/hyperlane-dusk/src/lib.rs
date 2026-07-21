// SPDX-License-Identifier: MIT OR Apache-2.0
//
//! Hyperlane agent crate for Dusk Network.
//!
//! Implements Hyperlane core traits (Mailbox, MerkleTreeHook, ISM, etc.)
//! for the Dusk blockchain, communicating via the RUES protocol.

use std::ops::RangeInclusive;

use hyperlane_core::ChainResult;

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
    if count > DUSK_TX_HASH_LOOKUP_MAX_RECORDS {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk {context} transaction lookup spans {count} records, exceeding the fail-closed budget of {DUSK_TX_HASH_LOOKUP_MAX_RECORDS}"
        ))
        .into());
    }
    Ok(())
}

#[cfg(test)]
mod range_tests {
    use super::{
        block_range_chunks, bounded_block_range, ensure_tx_hash_lookup_budget,
        DUSK_LOG_LOOKUP_CHUNK_SIZE, DUSK_TX_HASH_LOOKUP_MAX_RECORDS,
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
    }
}
