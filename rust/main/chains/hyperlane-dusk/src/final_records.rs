//! Where a contract's records cross a block height.
//!
//! The Mailbox, the Merkle hook and the IGP each store their records in
//! order, with the height of the block that added each, so heights never
//! decrease along the records. How many of them a final block holds is the
//! index of the first record above it, which a binary search over all of
//! them finds in about log2(count) reads of a record's height. Indexers ask
//! every few seconds and validators at each checkpoint, so those reads grew
//! with the contract's history and made most of an idle agent's requests.
//!
//! A search for a height at or below the last final block shows two things
//! that no rollback can change: the records before the index it found are
//! at or below that height, and every later record is above it. So the
//! answer for a final height is kept, and asked again it costs no read at
//! all, not even of the record count. The highest one is the boundary the
//! next search starts from: it reads the count, and gallops (1, 2, 4, ...
//! records) from the boundary towards the answer, which costs no further
//! read while no new record is final, and about 2·log2(k) reads for k
//! records past the boundary. Without a usable boundary (after a restart, or
//! when the endpoint reports fewer records than the boundary counts, which
//! only an inconsistent endpoint does) the search covers all records again.

use std::collections::VecDeque;
use std::future::Future;

use tracing::warn;

use hyperlane_core::ChainResult;

/// Answers kept for heights other than the boundary's, such as the blocks
/// of transactions looked up by hash.
const KEPT_ANSWERS: usize = 16;

/// The first `count` records are at or below block `height`, which was at
/// or below the last final block when it was found, and every later record
/// is above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Boundary {
    count: u32,
    height: u64,
}

#[derive(Debug, Default)]
struct Known {
    /// The answer for the highest height searched.
    boundary: Option<Boundary>,
    /// Recent answers for other heights.
    answers: VecDeque<Boundary>,
}

impl Known {
    fn answer(&self, height: u64) -> Option<u32> {
        self.boundary
            .iter()
            .chain(&self.answers)
            .find(|known| known.height == height)
            .map(|known| known.count)
    }

    fn remember(&mut self, found: Boundary) {
        match self.boundary {
            Some(boundary) if found.height < boundary.height => {
                if self.answers.len() == KEPT_ANSWERS {
                    self.answers.pop_front();
                }
                self.answers.push_back(found);
            }
            _ => self.boundary = Some(found),
        }
    }
}

/// What searches found of one contract's records, shared by every reader of
/// that contract on one endpoint. Searches run one at a time, so readers that
/// ask for the same height together read once.
#[derive(Debug, Default)]
pub(crate) struct FinalRecords {
    known: tokio::sync::Mutex<Known>,
}

impl FinalRecords {
    /// How many of the contract's records are at or below block `height`:
    /// the index of the first record above it. `height` must be at or below
    /// the last final block, read before any record. `count` reads how many
    /// records there are, and `height_of(index)` a record's height; neither
    /// is called for a height asked before.
    pub(crate) async fn count_at_or_below<C, CF, F, Fut>(
        &self,
        height: u64,
        count: C,
        mut height_of: F,
    ) -> ChainResult<u32>
    where
        C: FnOnce() -> CF,
        CF: Future<Output = ChainResult<u32>>,
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = ChainResult<u64>>,
    {
        let mut known = self.known.lock().await;
        if let Some(found) = known.answer(height) {
            return Ok(found);
        }
        let count = count().await?;
        let found = match known.boundary {
            Some(boundary) if boundary.count > count => {
                warn!(
                    records = count,
                    final_records = boundary.count,
                    final_height = boundary.height,
                    "Dusk endpoint reports fewer records than were final before; searching them all again"
                );
                *known = Known::default();
                first_above(0, count, height, &mut height_of).await?
            }
            // Every record before the boundary is at or below `height`.
            Some(boundary) if height >= boundary.height => {
                gallop_up(boundary.count, count, height, &mut height_of).await?
            }
            // Every record from the boundary on is above `height`.
            Some(boundary) => gallop_down(boundary.count, height, &mut height_of).await?,
            None => first_above(0, count, height, &mut height_of).await?,
        };
        known.remember(Boundary {
            count: found,
            height,
        });
        Ok(found)
    }
}

/// The first record in `low..high` above `height`, or `high`, when every
/// record before `low` is at or below it: probe `low`, `low + 1`,
/// `low + 3`, `low + 7`, ... until one is above it or past `high`, then
/// search the last step.
async fn gallop_up<F, Fut>(low: u32, high: u32, height: u64, height_of: &mut F) -> ChainResult<u32>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = ChainResult<u64>>,
{
    let mut low = low;
    let mut step = 1u32;
    let high = loop {
        let probe = low.saturating_add(step - 1);
        if probe >= high {
            break high;
        }
        if height_of(probe).await? > height {
            break probe;
        }
        low = probe + 1;
        step = step.saturating_mul(2);
    };
    first_above(low, high, height, height_of).await
}

/// The first record before `high` above `height`, when every record from
/// `high` on is above it: probe `high - 1`, `high - 2`, `high - 4`, ...
/// until one is at or below it or the first record is passed, then search
/// the last step.
async fn gallop_down<F, Fut>(high: u32, height: u64, height_of: &mut F) -> ChainResult<u32>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = ChainResult<u64>>,
{
    let mut high = high;
    let mut step = 1u32;
    let low = loop {
        let Some(probe) = high.checked_sub(step) else {
            break 0;
        };
        if height_of(probe).await? <= height {
            break probe + 1;
        }
        high = probe;
        step = step.saturating_mul(2);
    };
    first_above(low, high, height, height_of).await
}

/// The first record in `low..high` above `height`, or `high`, by binary
/// search.
async fn first_above<F, Fut>(
    low: u32,
    high: u32,
    height: u64,
    height_of: &mut F,
) -> ChainResult<u32>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = ChainResult<u64>>,
{
    let (mut low, mut high) = (low, high);
    while low < high {
        let middle = low + (high - low) / 2;
        if height_of(middle).await? > height {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    /// A contract whose record `i` is at `heights[i]`, counting reads of
    /// its record count and of record heights.
    struct Contract {
        heights: Vec<u64>,
        counts: Cell<usize>,
        reads: Cell<usize>,
    }

    impl Contract {
        fn new(heights: Vec<u64>) -> Self {
            Self {
                heights,
                counts: Cell::new(0),
                reads: Cell::new(0),
            }
        }

        fn count(&self) -> u32 {
            self.heights.len() as u32
        }

        async fn search(&self, records: &FinalRecords, height: u64) -> u32 {
            self.counts.set(0);
            self.reads.set(0);
            records
                .count_at_or_below(
                    height,
                    || async {
                        self.counts.set(self.counts.get() + 1);
                        Ok(self.count())
                    },
                    |index| {
                        self.reads.set(self.reads.get() + 1);
                        let height = self.heights[index as usize];
                        async move { Ok(height) }
                    },
                )
                .await
                .unwrap()
        }

        fn expected(&self, height: u64) -> u32 {
            self.heights.iter().filter(|h| **h <= height).count() as u32
        }
    }

    #[tokio::test]
    async fn every_search_finds_what_a_full_search_finds() {
        // Several records per block, gaps, and heights below the first.
        let heights: Vec<u64> = (0..300u64).map(|i| 10 + i / 3 * 2).collect();
        let contract = Contract::new(heights);
        let top = *contract.heights.last().unwrap() + 3;
        // Upwards, downwards and back, from every boundary.
        let order: Vec<u64> = (0..=top)
            .chain((0..=top).rev())
            .chain((0..=top).step_by(7))
            .collect();
        let records = FinalRecords::default();
        for height in order {
            assert_eq!(
                contract.search(&records, height).await,
                contract.expected(height),
                "height {height}"
            );
        }
    }

    #[tokio::test]
    async fn an_idle_search_reads_nothing_whatever_the_history() {
        for history in [10u32, 1_000, 100_000] {
            let contract = Contract::new((0..u64::from(history)).collect());
            let records = FinalRecords::default();
            let final_height = u64::from(history) + 5;
            assert_eq!(contract.search(&records, final_height).await, history);
            let full = contract.reads.get();
            assert!(full >= history.ilog2() as usize, "{history}: {full}");
            // The same final block: no read at all, not even of the count.
            assert_eq!(contract.search(&records, final_height).await, history);
            assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
            // A new final block without new records: the count only.
            assert_eq!(contract.search(&records, final_height + 10).await, history);
            assert_eq!((contract.counts.get(), contract.reads.get()), (1, 0));
        }
    }

    #[tokio::test]
    async fn a_new_final_record_costs_a_few_reads() {
        let mut heights: Vec<u64> = (0..100_000).collect();
        let records = FinalRecords::default();
        let contract = Contract::new(heights.clone());
        contract.search(&records, 200_000).await;
        // One record more, final; then three more, one of them not final.
        heights.push(200_001);
        let contract = Contract::new(heights.clone());
        assert_eq!(contract.search(&records, 200_001).await, 100_001);
        assert_eq!(contract.reads.get(), 1);
        heights.extend([200_002, 200_003, 200_010]);
        let contract = Contract::new(heights);
        assert_eq!(contract.search(&records, 200_005).await, 100_003);
        assert!(contract.reads.get() <= 4, "{}", contract.reads.get());
    }

    #[tokio::test]
    async fn fewer_records_than_were_final_start_the_search_over() {
        let records = FinalRecords::default();
        let long = Contract::new((0..1000).collect());
        assert_eq!(long.search(&records, 2000).await, 1000);
        let short = Contract::new((0..500).collect());
        assert_eq!(short.search(&records, 2001).await, 500);
        assert!(
            short.reads.get() >= short.count().ilog2() as usize,
            "{}",
            short.reads.get()
        );
        // The search started over, so the boundary is the new one.
        assert_eq!(short.search(&records, 2002).await, 500);
        assert_eq!(short.reads.get(), 0);
    }

    #[tokio::test]
    async fn a_lower_height_searches_below_the_boundary_and_keeps_it() {
        let contract = Contract::new((0..10_000).map(|i| i * 2).collect());
        let records = FinalRecords::default();
        assert_eq!(contract.search(&records, 30_000).await, 10_000);
        // A transaction lookup in an older block.
        assert_eq!(contract.search(&records, 19_990).await, 9_996);
        assert!(contract.reads.get() <= 6, "{}", contract.reads.get());
        // The idle search still starts at the highest boundary.
        assert_eq!(contract.search(&records, 30_000).await, 10_000);
        assert_eq!(contract.reads.get(), 0);
    }

    #[tokio::test]
    async fn read_errors_fail_the_search_and_keep_the_boundary() {
        let records = FinalRecords::default();
        let contract = Contract::new((0..100).collect());
        assert_eq!(contract.search(&records, 99).await, 100);
        let error = records
            .count_at_or_below(
                150,
                || async { Ok(200) },
                |_| async {
                    Err(hyperlane_core::ChainCommunicationError::from_other_str(
                        "unreachable",
                    ))
                },
            )
            .await;
        assert!(error.is_err());
        assert_eq!(contract.search(&records, 99).await, 100);
        assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
        // A failed search leaves no answer behind.
        assert_eq!(contract.search(&records, 150).await, 100);
        assert_eq!(contract.counts.get(), 1);
    }
}
