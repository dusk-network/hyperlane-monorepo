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
//! at or below that height, and every later record is above it. The answer
//! for the highest height searched is the boundary the next search starts
//! from: it reads the count, and gallops (1, 2, 4, ... records) from the
//! boundary towards the answer, which costs no further read while no new
//! record is final, and about 2·log2(k) reads for k records past the
//! boundary. Asked again for the boundary's height, it costs no read at
//! all, not even of the record count. Without a usable boundary (after a
//! restart, or when the endpoint reports fewer records than the boundary
//! counts, which only an inconsistent endpoint does) the search covers all
//! records again.
//!
//! A record a search read at or below a final height is final, and so is
//! its height, so the heights searches read are kept: for each block, the
//! first and the last of its records read, for the most recent
//! `KEPT_BLOCKS` blocks. A height below the boundary's, such as the block
//! of a transaction looked up by hash, is searched only between the closest
//! kept records on either side of it. That costs no read at all when the
//! two are neighbours, as they are for every record a search read whole,
//! and otherwise reads only records no search has read, whatever the
//! contract's history. Without a kept record below the height, its search
//! starts at the first record; without one above it, at the boundary.
//!
//! Only an endpoint that answers inconsistently shows heights that
//! decrease along the records, or a search whose reads disagree with what
//! it found or with the boundary. Such a search may rest on a wrong reply,
//! its own or one a kept height came from, so its answer is not used: the
//! kept heights are forgotten, and a search of every record, reading each
//! height again, decides. When that one contradicts itself too, the search
//! fails rather than guess, and nothing it read is kept.
//!
//! Through a multicall, one read takes the heights of up to 256 records at
//! once: a search then reads the heights of every record in its range when
//! there are that few, and otherwise of 256 spread evenly over it, each
//! read narrowing the range 257-fold. A restart's search over 10,000
//! records takes two reads instead of thirteen, and a final block with up
//! to 256 new records one, which keeps the height of every one of them.

use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Bound;

use tracing::warn;

use hyperlane_core::{ChainCommunicationError, ChainResult};

/// Blocks whose records' heights a contract keeps, the most recent ones a
/// search read: 4,096 blocks are 11 hours of a 10 s chain that adds records
/// to every block, and more of a quieter one. Each takes a few dozen bytes.
const KEPT_BLOCKS: usize = 4_096;

/// The first `count` records are at or below block `height`, which was at
/// or below the last final block when it was found, and every later record
/// is above it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Boundary {
    count: u32,
    height: u64,
}

#[derive(Debug)]
struct Known {
    /// The answer for the highest height searched.
    boundary: Option<Boundary>,
    /// The heights of final records searches read: for each block, the
    /// first and the last of its records read. All of them are at or below
    /// the boundary's height, and before its count.
    heights: BTreeMap<u64, (u32, u32)>,
    /// Most blocks `heights` keeps.
    capacity: usize,
}

impl Default for Known {
    fn default() -> Self {
        Self {
            boundary: None,
            heights: BTreeMap::new(),
            capacity: KEPT_BLOCKS,
        }
    }
}

impl Known {
    /// Where the first record above `height`, below the boundary's height,
    /// is as far as the kept heights show: in `low..=high`, every record
    /// before `low` at or below `height`, and every one from `high` on
    /// above it. `low` is past `high` only when the endpoint answered
    /// inconsistently.
    fn bracket(&self, height: u64, boundary: Boundary) -> (u32, u32) {
        let low = self
            .heights
            .range(..=height)
            .next_back()
            .map_or(0, |(_, &(_, last))| last.saturating_add(1));
        let high = self
            .heights
            .range((Bound::Excluded(height), Bound::Unbounded))
            .next()
            .map_or(boundary.count, |(_, &(first, _))| first.min(boundary.count));
        (low, high)
    }

    /// Keep that record `index`, which is final, is in block `height`.
    /// Keeps nothing, and returns false, when that contradicts a kept
    /// height: heights never decrease along the records.
    fn keep(&mut self, index: u32, height: u64) -> bool {
        let after_lower = self
            .heights
            .range(..height)
            .next_back()
            .is_none_or(|(_, &(_, last))| last < index);
        let before_higher = self
            .heights
            .range((Bound::Excluded(height), Bound::Unbounded))
            .next()
            .is_none_or(|(_, &(first, _))| index < first);
        if !(after_lower && before_higher) {
            return false;
        }
        let (first, last) = self.heights.entry(height).or_insert((index, index));
        *first = (*first).min(index);
        *last = (*last).max(index);
        true
    }

    /// Keep what a search for `height`, which started from boundary
    /// `before`, found: `found` records at or below it, having read the
    /// heights in `reads`. Every one of them at or below a final height is
    /// kept. Keeps nothing, forgets every kept height and returns false when
    /// the search contradicts itself, the boundary or what is kept: a read
    /// that disagrees with what the search found, a count on the wrong side
    /// of the boundary's, or heights that decrease along the records. What
    /// the search found may then rest on a wrong reply, now or before.
    fn remember(
        &mut self,
        height: u64,
        found: u32,
        before: Option<Boundary>,
        reads: Vec<(u32, u64)>,
    ) -> bool {
        let final_height = before.map_or(height, |before| before.height.max(height));
        let mut consistent = before.is_none_or(|before| match height.cmp(&before.height) {
            std::cmp::Ordering::Less => found <= before.count,
            std::cmp::Ordering::Equal => found == before.count,
            std::cmp::Ordering::Greater => found >= before.count,
        });
        for (index, record_height) in reads {
            if (index < found) != (record_height <= height) {
                consistent = false;
            } else if record_height <= final_height {
                consistent &= self.keep(index, record_height);
            }
        }
        if !consistent {
            warn!(
                final_height,
                "Dusk endpoint reports record heights that contradict each other or ones it reported before; forgetting the kept heights"
            );
            self.heights.clear();
            return false;
        }
        match self.boundary {
            Some(boundary) if height < boundary.height => {}
            _ => {
                self.boundary = Some(Boundary {
                    count: found,
                    height,
                })
            }
        }
        while self.heights.len() > self.capacity {
            self.heights.pop_first();
        }
        true
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
    /// One that keeps the heights of at most `capacity` blocks.
    #[cfg(any(test, feature = "fuzzing"))]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            known: tokio::sync::Mutex::new(Known {
                capacity,
                ..Known::default()
            }),
        }
    }

    /// How many of the contract's records are at or below block `height`:
    /// the index of the first record above it. `height` must be at or below
    /// the last final block, read before any record. `count` reads how many
    /// records there are, and `heights_of(indices)` the heights of records,
    /// at most `probes` of them per read; neither is called for the
    /// boundary's height, nor for a height between two neighbouring kept
    /// records. With `probes` of 1 the search reads one record at a time,
    /// galloping from the boundary, or from the closest kept record above
    /// the height.
    pub(crate) async fn count_at_or_below<C, CF, F, Fut>(
        &self,
        height: u64,
        count: C,
        probes: usize,
        mut heights_of: F,
    ) -> ChainResult<u32>
    where
        C: FnOnce() -> CF,
        CF: Future<Output = ChainResult<u32>>,
        F: FnMut(Vec<u32>) -> Fut,
        Fut: Future<Output = ChainResult<Vec<u64>>>,
    {
        let mut known = self.known.lock().await;
        // Below the boundary, the kept heights may answer without a read.
        let below = match known.boundary {
            Some(boundary) if height == boundary.height => return Ok(boundary.count),
            Some(boundary) if height < boundary.height => {
                let (low, high) = known.bracket(height, boundary);
                if low == high {
                    return Ok(low);
                }
                Some((low, high))
            }
            _ => None,
        };
        let count = count().await?;
        let search = match (known.boundary, below) {
            (Some(boundary), _) if boundary.count > count => {
                warn!(
                    records = count,
                    final_records = boundary.count,
                    final_height = boundary.height,
                    "Dusk endpoint reports fewer records than were final before; searching them all again"
                );
                *known = Known {
                    capacity: known.capacity,
                    ..Known::default()
                };
                Search::All { high: count }
            }
            // Every record before the boundary is at or below `height`.
            (Some(boundary), _) if height >= boundary.height => Search::Up {
                low: boundary.count,
                high: count,
            },
            (Some(_), Some((low, high))) if low <= high => Search::Down { low, high },
            // Every record from the boundary on is above `height`.
            (Some(boundary), _) => {
                warn!(
                    height,
                    "Dusk endpoint's kept record heights contradict each other; searching below the boundary again"
                );
                known.heights.clear();
                Search::Down {
                    low: 0,
                    high: boundary.count,
                }
            }
            (None, _) => Search::All { high: count },
        };
        let before = known.boundary;
        let mut reads = Reads {
            heights_of: &mut heights_of,
            seen: Vec::new(),
        };
        let found = match (search, probes) {
            (Search::Up { low, high }, 0 | 1) => gallop_up(low, high, height, &mut reads).await?,
            (Search::Down { low, high }, 0 | 1) => {
                gallop_down(low, high, height, &mut reads).await?
            }
            (Search::All { high }, 0 | 1) => first_above(0, high, height, &mut reads).await?,
            (Search::Up { low, high } | Search::Down { low, high }, _) => {
                first_above_batched(low, high, height, probes, &mut reads).await?
            }
            (Search::All { high }, _) => {
                first_above_batched(0, high, height, probes, &mut reads).await?
            }
        };
        if known.remember(height, found, before, reads.seen) {
            return Ok(found);
        }
        // The endpoint contradicted itself, so what this search found may
        // rest on a wrong reply, its own or one a kept height came from. It
        // is not used: the kept heights are gone, and a search of every
        // record, reading each height again, decides.
        let mut reads = Reads {
            heights_of: &mut heights_of,
            seen: Vec::new(),
        };
        let found = match probes {
            0 | 1 => first_above(0, count, height, &mut reads).await?,
            _ => first_above_batched(0, count, height, probes, &mut reads).await?,
        };
        if known.remember(height, found, before, reads.seen) {
            return Ok(found);
        }
        // Read again, the endpoint still contradicts itself, or the
        // boundary: nothing it reported is kept, and the next search reads
        // every record.
        *known = Known {
            capacity: known.capacity,
            ..Known::default()
        };
        Err(ChainCommunicationError::from_other_str(&format!(
            "Dusk endpoint reports heights of records at or below block {height} that contradict each other, also when read again; the next search reads every record again"
        )))
    }
}

/// Where a search looks for the first record above a height.
#[derive(Debug, Clone, Copy)]
enum Search {
    /// Above the boundary: `low..high`, every record before `low` at or
    /// below the height.
    Up { low: u32, high: u32 },
    /// Below the boundary: `low..high`, every record before `low` at or
    /// below the height, and every record from `high` on above it.
    Down { low: u32, high: u32 },
    /// Every record, `0..high`.
    All { high: u32 },
}

/// The record heights a search reads, and every one it read.
struct Reads<'a, F> {
    heights_of: &'a mut F,
    seen: Vec<(u32, u64)>,
}

impl<F, Fut> Reads<'_, F>
where
    F: FnMut(Vec<u32>) -> Fut,
    Fut: Future<Output = ChainResult<Vec<u64>>>,
{
    /// The heights of the records at `indices`, in one read.
    async fn heights(&mut self, indices: Vec<u32>) -> ChainResult<Vec<u64>> {
        let heights = (self.heights_of)(indices.clone()).await?;
        if heights.len() != indices.len() {
            return Err(ChainCommunicationError::from_other_str(&format!(
                "Dusk endpoint returned {} heights for {} records",
                heights.len(),
                indices.len()
            )));
        }
        self.seen
            .extend(indices.into_iter().zip(heights.iter().copied()));
        Ok(heights)
    }

    /// The height of the record at `index`, read on its own.
    async fn height(&mut self, index: u32) -> ChainResult<u64> {
        self.heights(vec![index])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                ChainCommunicationError::from_other_str("Dusk endpoint returned no height")
            })
    }
}

/// The first record in `low..high` above `height`, or `high`, when every
/// record before `low` is at or below it: probe `low`, `low + 1`,
/// `low + 3`, `low + 7`, ... until one is above it or past `high`, then
/// search the last step.
async fn gallop_up<F, Fut>(
    low: u32,
    high: u32,
    height: u64,
    reads: &mut Reads<'_, F>,
) -> ChainResult<u32>
where
    F: FnMut(Vec<u32>) -> Fut,
    Fut: Future<Output = ChainResult<Vec<u64>>>,
{
    let mut low = low;
    let mut step = 1u32;
    let high = loop {
        let probe = low.saturating_add(step - 1);
        if probe >= high {
            break high;
        }
        if reads.height(probe).await? > height {
            break probe;
        }
        low = probe + 1;
        step = step.saturating_mul(2);
    };
    first_above(low, high, height, reads).await
}

/// The first record in `floor..high` above `height`, or `high`, when every
/// record before `floor` is at or below it and every record from `high` on
/// above it: probe `high - 1`, `high - 2`, `high - 4`, ... until one is at
/// or below it or `floor` is passed, then search the last step.
async fn gallop_down<F, Fut>(
    floor: u32,
    high: u32,
    height: u64,
    reads: &mut Reads<'_, F>,
) -> ChainResult<u32>
where
    F: FnMut(Vec<u32>) -> Fut,
    Fut: Future<Output = ChainResult<Vec<u64>>>,
{
    let mut high = high;
    let mut step = 1u32;
    let low = loop {
        let Some(probe) = high.checked_sub(step).filter(|probe| *probe >= floor) else {
            break floor;
        };
        if reads.height(probe).await? <= height {
            break probe + 1;
        }
        high = probe;
        step = step.saturating_mul(2);
    };
    first_above(low, high, height, reads).await
}

/// The first record in `low..high` above `height`, or `high`, by binary
/// search.
async fn first_above<F, Fut>(
    low: u32,
    high: u32,
    height: u64,
    reads: &mut Reads<'_, F>,
) -> ChainResult<u32>
where
    F: FnMut(Vec<u32>) -> Fut,
    Fut: Future<Output = ChainResult<Vec<u64>>>,
{
    let (mut low, mut high) = (low, high);
    while low < high {
        let middle = low + (high - low) / 2;
        if reads.height(middle).await? > height {
            high = middle;
        } else {
            low = middle + 1;
        }
    }
    Ok(low)
}

/// The first record in `low..high` above `height`, or `high`, reading the
/// heights of up to `probes` records at once: every record of the range
/// when it holds that few, otherwise `probes` spread evenly over it, which
/// leaves a range between two of them.
async fn first_above_batched<F, Fut>(
    low: u32,
    high: u32,
    height: u64,
    probes: usize,
    reads: &mut Reads<'_, F>,
) -> ChainResult<u32>
where
    F: FnMut(Vec<u32>) -> Fut,
    Fut: Future<Output = ChainResult<Vec<u64>>>,
{
    let probes = probes.max(1) as u64;
    let (mut low, mut high) = (low, high);
    while low < high {
        let span = u64::from(high - low);
        let mut indices: Vec<u32> = if span <= probes {
            (low..high).collect()
        } else {
            // Strictly between `low` and `high`, since `span > probes`.
            (1..=probes)
                .map(|step| low + (span * step / (probes + 1)) as u32)
                .collect()
        };
        indices.dedup();
        let heights = reads.heights(indices.clone()).await?;
        for (index, record_height) in indices.into_iter().zip(heights) {
            if record_height > height {
                high = index;
                break;
            }
            low = index + 1;
        }
    }
    Ok(low)
}

#[cfg(any(test, feature = "fuzzing"))]
pub(crate) mod checks;

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::collections::HashSet;

    use super::*;

    /// A contract whose record `i` is at `heights[i]`, counting reads of
    /// its record count, reads of record heights, records read, and
    /// records read again.
    struct Contract {
        heights: Vec<u64>,
        counts: Cell<usize>,
        reads: Cell<usize>,
        records: Cell<usize>,
        /// Every record read, ever.
        read: RefCell<HashSet<u32>>,
        /// Records read that a search had read before.
        repeated: Cell<usize>,
    }

    impl Contract {
        fn new(heights: Vec<u64>) -> Self {
            Self {
                heights,
                counts: Cell::new(0),
                reads: Cell::new(0),
                records: Cell::new(0),
                read: RefCell::new(HashSet::new()),
                repeated: Cell::new(0),
            }
        }

        fn count(&self) -> u32 {
            self.heights.len() as u32
        }

        /// A search reading one record at a time.
        async fn search(&self, records: &FinalRecords, height: u64) -> u32 {
            self.search_with(records, height, 1).await
        }

        /// A search reading up to `probes` records at a time.
        async fn search_with(&self, records: &FinalRecords, height: u64, probes: usize) -> u32 {
            self.counts.set(0);
            self.reads.set(0);
            self.records.set(0);
            records
                .count_at_or_below(
                    height,
                    || async {
                        self.counts.set(self.counts.get() + 1);
                        Ok(self.count())
                    },
                    probes,
                    |indices: Vec<u32>| {
                        assert!(!indices.is_empty() && indices.len() <= probes.max(1));
                        assert!(indices.windows(2).all(|pair| pair[0] < pair[1]));
                        self.reads.set(self.reads.get() + 1);
                        self.records.set(self.records.get() + indices.len());
                        let mut read = self.read.borrow_mut();
                        for index in &indices {
                            if !read.insert(*index) {
                                self.repeated.set(self.repeated.get() + 1);
                            }
                        }
                        let heights: Vec<u64> = indices
                            .iter()
                            .map(|index| self.heights[*index as usize])
                            .collect();
                        async move { Ok(heights) }
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
        for probes in [1, 2, 3, 16, 256] {
            // Keeping the heights of every block, or of a few only.
            for records in [FinalRecords::default(), FinalRecords::with_capacity(3)] {
                for &height in &order {
                    assert_eq!(
                        contract.search_with(&records, height, probes).await,
                        contract.expected(height),
                        "height {height}, {probes} probes"
                    );
                }
            }
        }
    }

    #[tokio::test]
    async fn an_idle_search_reads_nothing_whatever_the_history() {
        for probes in [1, 256] {
            for history in [10u32, 1_000, 100_000] {
                let contract = Contract::new((0..u64::from(history)).collect());
                let records = FinalRecords::default();
                let final_height = u64::from(history) + 5;
                assert_eq!(
                    contract.search_with(&records, final_height, probes).await,
                    history
                );
                let full = contract.reads.get();
                if probes == 1 {
                    assert!(full >= history.ilog2() as usize, "{history}: {full}");
                }
                // The same final block: no read at all, not even of the count.
                assert_eq!(
                    contract.search_with(&records, final_height, probes).await,
                    history
                );
                assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
                // A new final block without new records: the count only.
                assert_eq!(
                    contract
                        .search_with(&records, final_height + 10, probes)
                        .await,
                    history
                );
                assert_eq!((contract.counts.get(), contract.reads.get()), (1, 0));
            }
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
    async fn batched_searches_take_a_read_or_two() {
        // A restart over 10,000 records: two reads instead of thirteen.
        let contract = Contract::new((0..10_000).collect());
        let single = FinalRecords::default();
        assert_eq!(contract.search(&single, 20_000).await, 10_000);
        assert_eq!(contract.reads.get(), 13);
        let batched = FinalRecords::default();
        assert_eq!(contract.search_with(&batched, 20_000, 256).await, 10_000);
        assert_eq!(contract.reads.get(), 2);
        // Up to 256 new records, some not final yet: one read of all of
        // them.
        let mut heights: Vec<u64> = (0..10_000).collect();
        heights.extend((0..200).map(|i| 30_000 + i));
        let contract = Contract::new(heights);
        assert_eq!(contract.search_with(&batched, 30_150, 256).await, 10_151);
        assert_eq!((contract.reads.get(), contract.records.get()), (1, 200));
        // A transaction's older block, below the boundary: one read, of the
        // records between two the restart's search read.
        assert_eq!(contract.search_with(&batched, 5_000, 256).await, 5_001);
        assert_eq!(contract.reads.get(), 1);
        assert!(contract.records.get() <= 40, "{}", contract.records.get());
        // The block of a record the new block's search read: no read.
        assert_eq!(contract.search_with(&batched, 30_100, 256).await, 10_101);
        assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
        // The boundary is still the highest one.
        assert_eq!(contract.search_with(&batched, 30_150, 256).await, 10_151);
        assert_eq!(contract.reads.get(), 0);
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
        // And the heights the long history's search kept are gone: its
        // records past 500 are not this endpoint's.
        assert!(records
            .known
            .lock()
            .await
            .heights
            .values()
            .all(|(_, last)| *last < 500));
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

    /// The block of each record a search read is known for good: a search
    /// below the boundary reads only records no search read before, and none
    /// at all between two neighbours that were read.
    #[tokio::test]
    async fn searches_below_the_boundary_read_only_records_none_read_before() {
        // Two records to a block, some blocks without any.
        let heights: Vec<u64> = (0..2_000u64).map(|i| 100 + i / 2 * 3).collect();
        let top = *heights.last().unwrap();
        for probes in [1, 256] {
            let contract = Contract::new(heights.clone());
            let records = FinalRecords::default();
            assert_eq!(contract.search_with(&records, top + 5, probes).await, 2_000);
            // Every block from the top down, and each twice, as a lookup
            // asks for a block and the one below it.
            for height in (90..=top).rev() {
                for height in [height, height.saturating_sub(1)] {
                    assert_eq!(
                        contract.search_with(&records, height, probes).await,
                        contract.expected(height),
                        "height {height}, {probes} probes"
                    );
                }
            }
            assert_eq!(contract.repeated.get(), 0, "{probes} probes");
            // Asked again, every one of them is answered without a read.
            let reads_before = contract.read.borrow().len();
            for height in (90..=top).rev() {
                assert_eq!(
                    contract.search_with(&records, height, probes).await,
                    contract.expected(height)
                );
                assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
            }
            assert_eq!(contract.read.borrow().len(), reads_before);
        }
    }

    /// The relayer's lookups of each new message's transaction, as
    /// `txIdIndexingEnabled` makes them: the message's block and the one
    /// below it, once its block is final and the indexer's tick has read
    /// it. They cost the same reads after 100 records as after 10,000.
    #[tokio::test]
    async fn a_message_costs_the_same_reads_whatever_the_history() {
        /// Reads of record heights for `messages` messages after `history`
        /// records, one a block, and the count reads among them.
        async fn per_message(history: u64, probes: usize) -> (usize, usize, usize) {
            const MESSAGES: u64 = 20;
            let mut heights: Vec<u64> = (0..history).map(|i| 1_000 + i).collect();
            let records = FinalRecords::default();
            let mut final_height = 1_000 + history + 5;
            let contract = Contract::new(heights.clone());
            contract.search_with(&records, final_height, probes).await;
            let (mut reads, mut records_read, mut counts) = (0, 0, 0);
            for _ in 0..MESSAGES {
                // The message's block is the tip: the tick reads nothing new.
                heights.push(final_height + 1);
                let contract = Contract::new(heights.clone());
                contract.search_with(&records, final_height, probes).await;
                // Its block is final: the tick reads it.
                final_height += 1;
                contract.search_with(&records, final_height, probes).await;
                let tick = (
                    contract.reads.get(),
                    contract.records.get(),
                    contract.counts.get(),
                );
                // The lookup of its transaction.
                let below = contract
                    .search_with(&records, final_height - 1, probes)
                    .await;
                let lookup_below = (
                    contract.reads.get(),
                    contract.records.get(),
                    contract.counts.get(),
                );
                let at = contract.search_with(&records, final_height, probes).await;
                assert_eq!(at - below, 1);
                reads += tick.0 + lookup_below.0 + contract.reads.get();
                records_read += tick.1 + lookup_below.1 + contract.records.get();
                counts += tick.2 + lookup_below.2 + contract.counts.get();
            }
            (reads, records_read, counts)
        }
        for probes in [1, 256] {
            let short = per_message(100, probes).await;
            let long = per_message(10_000, probes).await;
            assert_eq!(short, long, "{probes} probes");
            // The tick reads the new record and the count; the lookup
            // nothing.
            assert_eq!(long, (20, 20, 20), "{probes} probes");
        }
    }

    #[tokio::test]
    async fn the_kept_heights_are_bounded() {
        // A record in each of 10,000 blocks, every one read.
        let contract = Contract::new((0..10_000).map(|i| 100 + i).collect());
        let records = FinalRecords::default();
        assert_eq!(contract.search_with(&records, 20_000, 256).await, 10_000);
        for height in (100..10_100).step_by(50) {
            assert_eq!(
                contract.search_with(&records, height, 256).await,
                contract.expected(height)
            );
        }
        let kept = records.known.lock().await.heights.len();
        assert!(kept <= KEPT_BLOCKS, "{kept}");
        // The most recent are kept: a block among them costs no read, and
        // an older one is still found.
        assert_eq!(contract.search_with(&records, 10_000, 256).await, 9_901);
        assert_eq!(contract.reads.get(), 0);
        assert_eq!(contract.search_with(&records, 150, 256).await, 51);
        assert!(contract.reads.get() <= 2, "{}", contract.reads.get());
    }

    #[tokio::test]
    async fn heights_that_contradict_the_kept_ones_are_not_kept() {
        let mut known = Known::default();
        assert!(known.keep(10, 100));
        assert!(known.keep(12, 100));
        assert!(known.keep(20, 110));
        // A record before a kept one in a later block, or after a kept one
        // in an earlier block.
        assert!(!known.keep(15, 120));
        assert!(!known.keep(11, 90));
        assert!(!known.keep(25, 105));
        // Between two kept records of one block, any block but that one
        // contradicts them.
        assert!(!known.keep(11, 101));
        assert!(known.keep(11, 100));
        assert_eq!(
            known
                .heights
                .iter()
                .map(|(h, r)| (*h, *r))
                .collect::<Vec<_>>(),
            [(100, (10, 12)), (110, (20, 20))]
        );
        // A search whose reads contradict the kept heights forgets them all.
        known.boundary = Some(Boundary {
            count: 30,
            height: 120,
        });
        assert!(!known.remember(105, 13, known.boundary, vec![(13, 101), (12, 104)]));
        assert!(known.heights.is_empty());
        assert_eq!(known.boundary.map(|boundary| boundary.height), Some(120));
    }

    /// The search of `contract` through `height`, reading up to `probes`
    /// heights at once, to which the endpoint answers `wrong` for the next
    /// reads of record `index`, and then the record's height.
    async fn search_with_wrong_replies(
        contract: &Contract,
        records: &FinalRecords,
        height: u64,
        probes: usize,
        index: u32,
        wrong: Vec<u64>,
    ) -> ChainResult<u32> {
        let wrong = RefCell::new(std::collections::VecDeque::from(wrong));
        records
            .count_at_or_below(
                height,
                || async { Ok(contract.count()) },
                probes,
                |indices: Vec<u32>| {
                    let heights: Vec<u64> = indices
                        .iter()
                        .map(|read| {
                            let reply = if *read == index {
                                wrong.borrow_mut().pop_front()
                            } else {
                                None
                            };
                            reply.unwrap_or(contract.heights[*read as usize])
                        })
                        .collect();
                    async move { Ok(heights) }
                },
            )
            .await
    }

    /// A search that a wrong reply contradicts is not used: the kept heights
    /// go, and a search of every record, reading each height again,
    /// decides. The review's counterexample: records at heights 10, 20, ...,
    /// 80; a search through 80 keeps those of records 4, 6 and 7; a search
    /// through 59, narrowed to record 5, gets 40 for its height, which
    /// contradicts record 4's 50. It used to answer 6 instead of 5, so that
    /// block 60 seemed to hold no record and its payment was skipped.
    #[tokio::test]
    async fn a_search_a_wrong_reply_contradicts_reads_every_record_again() {
        let contract = Contract::new((1..=8).map(|block| block * 10).collect());
        let records = FinalRecords::default();
        assert_eq!(contract.search(&records, 80).await, 8);
        let kept: Vec<(u64, (u32, u32))> = records
            .known
            .lock()
            .await
            .heights
            .iter()
            .map(|(height, indices)| (*height, *indices))
            .collect();
        assert_eq!(kept, [(50, (4, 4)), (70, (6, 6)), (80, (7, 7))]);
        let found = search_with_wrong_replies(&contract, &records, 59, 1, 5, vec![40])
            .await
            .unwrap();
        assert_eq!(found, 5);
        // Block 60 holds record 5, as `payment_range_at_block` sees it.
        assert_eq!(contract.search(&records, 60).await, 6);
        assert_eq!(contract.search(&records, 59).await, 5);
        // Batched alike, from a boundary through 80 that read every record.
        let records = FinalRecords::default();
        assert_eq!(contract.search_with(&records, 80, 256).await, 8);
        records
            .known
            .lock()
            .await
            .heights
            .retain(|height, _| *height != 60);
        let found = search_with_wrong_replies(&contract, &records, 59, 256, 5, vec![40])
            .await
            .unwrap();
        assert_eq!(found, 5);
    }

    /// A contradiction that persists when every height is read again fails
    /// the search instead of answering a guess, and leaves nothing kept: the
    /// next search reads every record again.
    #[tokio::test]
    async fn a_contradiction_read_again_fails_the_search() {
        let contract = Contract::new((1..=8).map(|block| block * 10).collect());
        let records = FinalRecords::default();
        assert_eq!(contract.search(&records, 80).await, 8);
        let error = search_with_wrong_replies(&contract, &records, 59, 1, 5, vec![40, 40])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("contradict"), "{error}");
        {
            let known = records.known.lock().await;
            assert!(known.heights.is_empty());
            assert_eq!(known.boundary, None);
        }
        assert_eq!(contract.search(&records, 59).await, 5);
        assert_eq!(contract.counts.get(), 1);
        assert!(contract.reads.get() >= 3, "{}", contract.reads.get());
    }

    #[tokio::test]
    async fn read_errors_fail_the_search_and_keep_the_boundary() {
        let records = FinalRecords::default();
        let contract = Contract::new((0..100).collect());
        assert_eq!(contract.search(&records, 99).await, 100);
        for probes in [1, 256] {
            let error = records
                .count_at_or_below(
                    150,
                    || async { Ok(200) },
                    probes,
                    |_| async {
                        Err(hyperlane_core::ChainCommunicationError::from_other_str(
                            "unreachable",
                        ))
                    },
                )
                .await;
            assert!(error.is_err());
            // An endpoint that answers fewer heights than asked fails it too.
            let error = records
                .count_at_or_below(
                    150,
                    || async { Ok(200) },
                    probes,
                    |_| async { Ok(Vec::new()) },
                )
                .await;
            assert!(error.is_err());
        }
        assert_eq!(contract.search(&records, 99).await, 100);
        assert_eq!((contract.counts.get(), contract.reads.get()), (0, 0));
        // A failed search leaves no answer behind.
        assert_eq!(contract.search(&records, 150).await, 100);
        assert_eq!(contract.counts.get(), 1);
    }
}
