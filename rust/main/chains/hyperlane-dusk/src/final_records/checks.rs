//! `FinalRecords::count_at_or_below` against a model: a contract whose
//! records only grow, each in a block no lower than the one before, whose
//! records above the last final block may be rolled back and replaced, and
//! whose final records never change.
//!
//! Against an honest endpoint every search returns the number of records at
//! or below the height it asks for, whatever searches came before, with any
//! number of probes per read, and reads only records that exist. Against an
//! endpoint that answers anything at all, searches return or fail, but never
//! panic, loop or read past the count the endpoint reported.

use std::cell::{Cell, RefCell};

use hyperlane_core::{ChainCommunicationError, ChainResult};

use super::FinalRecords;
use crate::checks::{block_on, Input};

/// The contract's record heights, in record order.
struct Contract {
    heights: Vec<u64>,
    /// The last final block.
    final_height: u64,
}

impl Contract {
    fn expected(&self, height: u64) -> u32 {
        self.heights.partition_point(|record| *record <= height) as u32
    }

    /// Records added above the last final block.
    fn grow(&mut self, input: &mut Input) {
        let mut height = self
            .heights
            .last()
            .copied()
            .unwrap_or(0)
            .max(self.final_height.saturating_add(1));
        for _ in 0..input.below(300) {
            if input.u8() % 3 == 0 {
                height = height.saturating_add(1 + u64::from(input.u8() % 4));
            }
            self.heights.push(height);
        }
    }

    /// A rollback: the records above the last final block go, and others
    /// may take their place.
    fn roll_back(&mut self, input: &mut Input) {
        let keep = self.expected(self.final_height) as usize;
        self.heights.truncate(keep);
        if input.bool() {
            self.grow(input);
        }
    }

    /// A height a search may ask for: at or below the last final block,
    /// often one that records sit at, or just below one.
    fn height_to_search(&self, input: &mut Input) -> u64 {
        let final_records = self.expected(self.final_height) as usize;
        let at = match final_records {
            0 => 0,
            n => self.heights[input.below(n)],
        };
        let height = match input.u8() % 6 {
            0 => self.final_height,
            1 => 0,
            2 => at,
            3 => at.saturating_sub(1),
            _ => u64::from(input.u32()) % self.final_height.saturating_add(1),
        };
        height.min(self.final_height)
    }
}

/// One search, answered honestly from `contract`.
fn honest_search(records: &FinalRecords, contract: &Contract, height: u64, probes: usize) {
    let count = contract.heights.len() as u32;
    let result = block_on(records.count_at_or_below(
        height,
        || async { Ok::<_, ChainCommunicationError>(count) },
        probes,
        |indices: Vec<u32>| {
            assert!(!indices.is_empty() && indices.len() <= probes.max(1));
            assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
            assert!(
                indices.iter().all(|index| *index < count),
                "{indices:?} of {count}"
            );
            let heights: Vec<u64> = indices
                .iter()
                .map(|index| contract.heights[*index as usize])
                .collect();
            async move { Ok::<_, ChainCommunicationError>(heights) }
        },
    ));
    assert_eq!(
        result.ok(),
        Some(contract.expected(height)),
        "height {height}, probes {probes}, records {:?}",
        contract.heights
    );
}

/// One search answered with anything at all: no panic, no read past the
/// count reported, and a bounded number of reads.
fn hostile_search(records: &FinalRecords, input: &mut Input, height: u64, probes: usize) {
    let answers: Vec<u64> = (0..64).map(|_| input.edgy_u64() % 1_000).collect();
    let count = match input.u8() % 4 {
        0 => 0,
        1 => u32::MAX,
        _ => u32::from(input.u16()),
    };
    let fail_at = input.u8();
    let reads = Cell::new(0usize);
    let next = RefCell::new(0usize);
    let _ = block_on(records.count_at_or_below(
        height,
        || async { Ok::<_, ChainCommunicationError>(count) },
        probes,
        |indices: Vec<u32>| {
            reads.set(reads.get() + 1);
            assert!(reads.get() <= 4 * 64 + 8, "too many reads");
            assert!(indices.iter().all(|index| *index < count));
            let heights: ChainResult<Vec<u64>> = if reads.get() == usize::from(fail_at) {
                Err(ChainCommunicationError::from_other_str("lost"))
            } else {
                Ok(indices
                    .iter()
                    .map(|_| {
                        let mut next = next.borrow_mut();
                        *next += 1;
                        answers[*next % answers.len()]
                    })
                    .collect())
            };
            async move { heights }
        },
    ));
}

/// Runs the operations `data` describes: growth, finality, rollbacks and
/// searches, honest and hostile.
pub fn search(data: &[u8]) {
    let mut input = Input::new(data);
    let capacity = 1 + input.below(6);
    let mut records = FinalRecords::with_capacity(capacity);
    let mut contract = Contract {
        heights: Vec::new(),
        final_height: u64::from(input.u8()),
    };
    contract.grow(&mut input);
    for _ in 0..48 {
        if input.is_empty() {
            break;
        }
        match input.u8() % 8 {
            0 => contract.grow(&mut input),
            // Finality moves on; records at or below it never change again.
            1 => {
                contract.final_height = contract
                    .final_height
                    .saturating_add(u64::from(input.u8() % 8))
            }
            2 => contract.roll_back(&mut input),
            3 => {
                let height = contract.height_to_search(&mut input);
                let probes = 1 + input.below(4);
                hostile_search(&records, &mut input, height, probes);
                // What a hostile endpoint said may stay in the boundary; a
                // restart starts over.
                records = FinalRecords::with_capacity(capacity);
            }
            _ => {
                let height = contract.height_to_search(&mut input);
                let probes = match input.u8() % 4 {
                    0 => 0,
                    1 => 1,
                    2 => 256,
                    _ => 1 + input.below(300),
                };
                honest_search(&records, &contract, height, probes);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::checks::run_cases;

    #[test]
    fn searches_match_a_model_of_growing_finalizing_and_rolled_back_records() {
        run_cases(51, 3_000, 400, super::search);
    }
}
