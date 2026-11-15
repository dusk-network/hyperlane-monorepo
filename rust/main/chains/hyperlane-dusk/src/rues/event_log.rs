//! Finalized event logs.
//!
//! The Mailbox records deliveries, and ValidatorAnnounce announced
//! locations, only in events. An event log holds every non-reverted row of
//! one contract and topic that the endpoint's archive has finalized, in
//! archive order, numbered from zero, in the agent's event store, with an
//! index from a key each row's data names (a message ID, or a validator and
//! a location hash) to the rows that name it.
//!
//! A log reads forward from the closest scan position below the first block
//! it lacks, and takes a block only once it has read all of its rows, every
//! row names the same block hash, the block is at or below the node's last
//! final block, and `checkBlock(onlyFinalized)` accepted it: a block with no
//! row of the topic as well, since the log then holds that the block has
//! none. Rusk archives a finalized block's rows in one transaction, and
//! blocks in height order, so a block's rows end at a row of a later block
//! or at the end of the archive. A log never numbers rows of a block it
//! already holds again, so its numbering does not change when the
//! endpoint's archive only numbers its rows differently.
//!
//! A call reads at most the pages it is given. A block it has not read whole
//! by then is staged in the event store, with the position after the last
//! row read, and the next call reads on from there; its rows become entries
//! only once the block is whole and valid. A block of more rows than any
//! Rusk block holds fails the log with an error.
//!
//! The archive's history can change under a log: rows can come out of block
//! order, and a rebuilt archive can hold rows of a block that the old one
//! skipped, or lack rows it had. Every block a pass reads again that the log
//! holds must match the log's rows of it exactly, and every block the log
//! holds rows of must come again before a later block does. A row below a
//! block read before it, or a block that differs, drops the log's rows from
//! that block on, and the log reads them again. Consumers that index a log
//! by its sequences (the delivery indexer's contract sync) mark the
//! sequences they are told of or read as published: every sequence below a
//! count they get, and every row they read. A log the previous version
//! wrote has none published until then. Dropped rows of published sequences
//! are kept, unseen, as evidence: the rows the log takes again under those
//! sequences must be the
//! same, and where one is not, the log stops with a critical error instead
//! of numbering them differently, until the operator resets the agent's
//! database and event store.

use super::*;

/// Most rows one key may name in a lookup.
const MAX_ROWS_PER_KEY: usize = 64;
/// Bytes of an encoded row before its data: height, block hash, origin.
const ENTRY_HEADER_BYTES: usize = 8 + 32 + 32;
/// Bytes of a log's state as the previous version wrote it, without the
/// published sequences.
const STATE_V1_BYTES: usize = 8 + 9 + 9;
const STATE_BYTES: usize = STATE_V1_BYTES + 8;

/// The lock that serializes each log's advances, by contract and topic.
pub(super) type EventLogLocks = HashMap<([u8; 32], &'static str), Arc<tokio::sync::Mutex<()>>>;

/// Bounds of one block of a log's contract, which no block Rusk produces
/// reaches. A block past one fails the log with an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockLimits {
    /// Most archive rows of the contract in one block, of any topic.
    pub rows: u64,
    /// Most bytes of the topic's rows in one block, as the log keeps them.
    pub topic_bytes: usize,
}

/// A Rusk block holds at most 3,000,000,000 gas (Rusk's block gas limit),
/// and each row is an event the contract emitted. A Mailbox or
/// ValidatorAnnounce call that emits an event costs far more than the 3,000
/// gas per row a million rows would leave, and a `ProcessId` row or an
/// announcement takes a few hundred bytes as a log keeps it.
pub(crate) const BLOCK_LIMITS: BlockLimits = BlockLimits {
    rows: 1_000_000,
    topic_bytes: 32 * 1024 * 1024,
};

/// A finalized event log: a contract, a topic, and the lookup key a row's
/// data names. `key_of` returns `None` for data that is not a well-formed
/// event of the topic; such a row stops the log, which never leaves a
/// finalized row out.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventLog {
    pub contract_id: [u8; 32],
    pub topic: &'static str,
    pub key_of: fn(&[u8]) -> Option<Vec<u8>>,
    pub limits: BlockLimits,
}

/// One row of an event log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogEntry {
    /// The row's number in the log.
    pub sequence: u64,
    pub block_height: u64,
    pub block_hash: H256,
    /// Hash of the transaction that emitted the row.
    pub origin: [u8; 32],
    pub data: Vec<u8>,
}

/// How far an event log reaches.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct LogState {
    /// Rows in the log.
    pub count: u64,
    /// The last block whose rows the log has read whole.
    pub last_block: Option<u64>,
    /// The log holds every row of the topic in every block up to this
    /// height that the archive had finalized when the log read it: the last
    /// block read whole, or the node's last final block if the archive had
    /// finalized it before the log read past it.
    pub complete_through: Option<u64>,
    /// Consumers may have read every sequence below this one. It can exceed
    /// `count` after the log dropped rows: the dropped rows of those
    /// sequences are kept as evidence.
    pub published: u64,
}

/// What a lookup of one key found among an event log's finalized rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LogLookup {
    /// A row with the key, in a block `checkBlock(onlyFinalized)` accepts.
    Found(LogEntry),
    /// The log holds every finalized row up to `final_block`, the node's
    /// last final block, and none of them has the key.
    Absent { final_block: u64 },
    /// The log cannot tell yet, and why.
    Unknown(String),
}

/// How one pass over a contract's rows for an event log ended.
enum LogScan {
    /// It read every row up to the end of the archive or up to a row above
    /// the node's last final block (`passed_final`), or it used its pages.
    Done { passed_final: bool },
    /// The start position does not belong to this archive.
    WrongAnchor,
    /// The archive has not reached the start position yet.
    ArchiveBehind,
    /// A block the log holds differs in the archive now: the log dropped
    /// its rows from that block on, and reads them again.
    Rebuild,
}

/// Where a pass over a contract's rows starts.
enum PassStart {
    /// After an anchor's fence row, or at the contract's first row.
    Anchor(Option<ScanAnchor>),
    /// Where an earlier call stopped inside a block it staged.
    Staged(StagedBlock),
}

/// The node's tip and last final block when a log reads on.
#[derive(Debug, Clone, Copy)]
struct Frontier {
    latest: u64,
    finalized: u64,
}

/// A row of the log's topic in a block the log takes.
#[derive(Debug, Clone)]
struct TopicRow {
    origin: [u8; 32],
    data: Vec<u8>,
    key: Vec<u8>,
}

impl TopicRow {
    /// Memory the row takes, for the block bound.
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.len() + self.key.len()
    }
}

/// What a pass does with the rows of the block it is reading.
enum BlockKind {
    /// A block above the log's last one: its rows of the topic, to take.
    New {
        topic: Vec<TopicRow>,
        topic_bytes: usize,
        /// How many of `topic` are in the event store's staging already.
        staged: usize,
    },
    /// A block the log holds, read from its first row: its rows of the
    /// topic must be the log's entries from `first` on. `matched` of them
    /// have been.
    Held { first: u64, matched: u64 },
    /// The rest of the block of the anchor's fence row, which the pass did
    /// not read from its first row.
    Tail,
}

/// The block a pass is reading.
struct OpenBlock {
    height: u64,
    /// The block hash its first row named, which every row must name.
    hash: Option<H256>,
    /// Rows of the contract read in the block, of any topic.
    rows: u64,
    kind: BlockKind,
    /// Whether an earlier call staged the block.
    was_staged: bool,
}

impl OpenBlock {
    fn new(height: u64) -> Self {
        Self {
            height,
            hash: None,
            rows: 0,
            kind: BlockKind::New {
                topic: Vec::new(),
                topic_bytes: 0,
                staged: 0,
            },
            was_staged: false,
        }
    }

    fn held(height: u64, first: u64) -> Self {
        Self {
            kind: BlockKind::Held { first, matched: 0 },
            ..Self::new(height)
        }
    }

    fn tail(height: u64) -> Self {
        Self {
            kind: BlockKind::Tail,
            ..Self::new(height)
        }
    }
}

/// A block a call stopped inside, and the position after the last row read.
/// For a block above the log's last one, its rows of the topic read so far
/// are in the event store (`topic_rows` of them); for a block the log holds,
/// `topic_rows` of them matched its entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StagedBlock {
    height: u64,
    hash: H256,
    rows: u64,
    topic_rows: u64,
    topic_bytes: u64,
    /// After the last row read; `before_block` is `height + 1`.
    position: ScanAnchor,
}

impl StagedBlock {
    const ENCODED_LEN: usize = 8 + 32 + 8 + 8 + 8 + ScanAnchor::ENCODED_LEN;

    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::ENCODED_LEN);
        bytes.extend_from_slice(&self.height.to_be_bytes());
        bytes.extend_from_slice(self.hash.as_bytes());
        bytes.extend_from_slice(&self.rows.to_be_bytes());
        bytes.extend_from_slice(&self.topic_rows.to_be_bytes());
        bytes.extend_from_slice(&self.topic_bytes.to_be_bytes());
        bytes.extend_from_slice(&self.position.encode());
        bytes
    }

    fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != Self::ENCODED_LEN {
            return None;
        }
        let number = |offset: usize| -> Option<u64> {
            Some(u64::from_be_bytes(
                bytes.get(offset..offset + 8)?.try_into().ok()?,
            ))
        };
        let staged = Self {
            height: number(0)?,
            hash: H256::from_slice(bytes.get(8..40)?),
            rows: number(40)?,
            topic_rows: number(48)?,
            topic_bytes: number(56)?,
            position: ScanAnchor::decode(bytes.get(64..)?)?,
        };
        (staged.position.fence_block == staged.height
            && staged.position.before_block == staged.height.checked_add(1)?
            && staged.rows >= staged.topic_rows)
            .then_some(staged)
    }
}

/// The lower of `value` and `below`, where `None` is below everything.
fn lowered(value: Option<u64>, below: Option<u64>) -> Option<u64> {
    match (value, below) {
        (Some(value), Some(below)) => Some(value.min(below)),
        _ => None,
    }
}

impl RuesClient {
    fn event_log_lock(&self, log: &EventLog) -> Arc<tokio::sync::Mutex<()>> {
        self.event_logs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry((log.contract_id, log.topic))
            .or_default()
            .clone()
    }

    /// The node's tip and last final block, from `lastBlockPair`.
    async fn frontier(&self) -> Result<Frontier, HyperlaneDuskError> {
        let pair = self.block_pair().await?;
        Ok(Frontier {
            latest: pair.latest,
            finalized: pair.finalized,
        })
    }

    /// Read the archive rows of `log`'s contract after the last block the log
    /// holds, and add the rows of its topic of every block read whole. Reads
    /// at most `max_pages` pages (at least one); a block not read whole by
    /// then is staged, and the next call reads on from there. With
    /// `finality`, the node's tip and last final block read before, a log
    /// that reads past the rows of that last final block, which the archive
    /// had finalized before the read, holds every row up to it; without it,
    /// the call reads `lastBlockPair` itself. No block above the node's last
    /// final block is taken. Fails while the log is halted.
    pub(crate) async fn advance_event_log(
        &self,
        log: &EventLog,
        max_pages: usize,
        finality: Option<&BlockPair>,
    ) -> Result<LogState, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        self.advance_locked_event_log(log, max_pages, finality)
            .await
    }

    /// `advance_event_log` for a consumer that indexes the log by its
    /// sequences and is told the count: every sequence below it counts as
    /// published from now on.
    pub(crate) async fn advance_and_publish_event_log(
        &self,
        log: &EventLog,
        max_pages: usize,
        finality: Option<&BlockPair>,
    ) -> Result<LogState, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        let mut state = self
            .advance_locked_event_log(log, max_pages, finality)
            .await?;
        let count = state.count;
        self.publish_event_log(log, &mut state, count)?;
        Ok(state)
    }

    /// `advance_event_log`, with the log's lock held.
    async fn advance_locked_event_log(
        &self,
        log: &EventLog,
        max_pages: usize,
        finality: Option<&BlockPair>,
    ) -> Result<LogState, HyperlaneDuskError> {
        self.ensure_event_log_runs(log)?;
        let mut state = self.event_log_state(log)?;
        // A log that holds every finalized row up to the node's last final
        // block has nothing to read until the node finalizes another.
        if let Some(pair) = finality {
            if state
                .complete_through
                .is_some_and(|through| through >= pair.finalized)
            {
                return Ok(state);
            }
        }
        let frontier = match finality {
            Some(pair) => Frontier {
                latest: pair.latest,
                finalized: pair.finalized,
            },
            None => self.frontier().await?,
        };
        let archived_final = match finality {
            Some(pair)
                if state
                    .complete_through
                    .is_none_or(|through| through < pair.finalized) =>
            {
                self.check_finalized_block(pair.finalized, &hex::encode(pair.finalized_hash))
                    .await?
                    .then_some(pair.finalized)
            }
            _ => None,
        };
        let target = |state: &LogState| state.last_block.map_or(0, |block| block.saturating_add(1));
        let mut start = match self.staged_block(log, &state)? {
            Some(staged) => PassStart::Staged(staged),
            None => {
                PassStart::Anchor(self.scan_anchor_at_or_below(&log.contract_id, target(&state))?)
            }
        };
        let mut pages_left = max_pages.max(1);
        loop {
            let staged = matches!(start, PassStart::Staged(_));
            let from_anchor = matches!(start, PassStart::Anchor(Some(_)));
            match self
                .read_event_log(log, &mut state, start, &mut pages_left, frontier)
                .await?
            {
                LogScan::Done { passed_final } => {
                    // Blocks up to the last one read whole are complete, and
                    // with the archive read past the node's last final block,
                    // so is every block it had finalized before.
                    let archived_final = archived_final.filter(|_| passed_final);
                    let through = [state.complete_through, state.last_block, archived_final]
                        .into_iter()
                        .flatten()
                        .max();
                    if through != state.complete_through {
                        state.complete_through = through;
                        self.write_event_log(log, &state, WriteBatch::default())?;
                    }
                    return Ok(state);
                }
                LogScan::ArchiveBehind => {
                    debug!(
                        contract = %hex::encode(log.contract_id),
                        topic = log.topic,
                        "Dusk archive has not reached the event log's scan position yet"
                    );
                    return Ok(state);
                }
                LogScan::Rebuild => {
                    start = PassStart::Anchor(
                        self.scan_anchor_at_or_below(&log.contract_id, target(&state))?,
                    );
                }
                LogScan::WrongAnchor if staged => {
                    warn!(
                        contract = %hex::encode(log.contract_id),
                        topic = log.topic,
                        "Dusk event log cannot read on from its staged block; it reads the block again"
                    );
                    let mut batch = WriteBatch::default();
                    self.unstage(log, &mut batch)?;
                    self.write_event_log(log, &state, batch)?;
                    start = PassStart::Anchor(
                        self.scan_anchor_at_or_below(&log.contract_id, target(&state))?,
                    );
                }
                LogScan::WrongAnchor if from_anchor => {
                    warn!(
                        contract = %hex::encode(log.contract_id),
                        topic = log.topic,
                        "Dusk archive rows were renumbered; the event log reads its blocks again from the contract's first row"
                    );
                    self.forget_scan_anchors(&log.contract_id)?;
                    start = PassStart::Anchor(None);
                }
                LogScan::WrongAnchor => {
                    return Err(HyperlaneDuskError::Other(
                        "Dusk archive scan without an anchor found a stale anchor".into(),
                    ))
                }
            }
            if pages_left == 0 {
                return Ok(state);
            }
        }
    }

    /// One pass over the contract's rows from `start`, until it has used
    /// `pages_left`. It adds to `state` every block above the log's last one
    /// that it reads whole, and checks every block the log holds that it
    /// reads from its first row against the log's rows. Saves its progress
    /// after every page.
    async fn read_event_log(
        &self,
        log: &EventLog,
        state: &mut LogState,
        start: PassStart,
        pages_left: &mut usize,
        frontier: Frontier,
    ) -> Result<LogScan, HyperlaneDuskError> {
        let contract_id = &log.contract_id;
        // The block being read, how many of the log's rows the pass has
        // accounted for (those of blocks below the next block it reads from
        // its first row), and the highest block it has read a row of.
        let (anchor, mut open, mut next_sequence, mut max_height) = match start {
            PassStart::Anchor(None) => (None, None, 0, None),
            PassStart::Anchor(Some(anchor)) => {
                // Every row up to the fence row is below `before_block`, at
                // most one above the log's last block, so the fence row's
                // block is one the log holds.
                let fence_block = anchor.fence_block;
                let open = state
                    .last_block
                    .is_some_and(|last| fence_block <= last)
                    .then(|| OpenBlock::tail(fence_block));
                let next =
                    self.first_sequence_at_or_above(log, state, fence_block.saturating_add(1))?;
                (Some(anchor), open, next, Some(fence_block))
            }
            PassStart::Staged(staged) => {
                let block = match self.reopen(log, state, &staged) {
                    Ok(block) => block,
                    Err(error) => {
                        warn!(
                            contract = %hex::encode(contract_id),
                            topic = log.topic,
                            %error,
                            "Dusk event log cannot read its staged block back; it reads the block again"
                        );
                        return Ok(LogScan::WrongAnchor);
                    }
                };
                let next = match block.kind {
                    BlockKind::Held { first, .. } => first,
                    _ => state.count,
                };
                (
                    Some(staged.position),
                    Some(block),
                    next,
                    Some(staged.height),
                )
            }
        };
        let start_before = anchor.map_or(0, |anchor| anchor.before_block);
        let mut rank = anchor.map_or(0, |anchor| anchor.rank);
        let mut highest_block: Option<u64> = None;
        let mut last_row: Option<FinalizedContractEvent> = None;
        // The cursor just before the fence row, so the first page starts
        // with it.
        let mut previous_id =
            anchor.and_then(|anchor| anchor.fence_id.checked_sub(1).filter(|id| *id >= 0));
        let mut fence = anchor;
        let mut size = PageSize::largest();
        loop {
            let cursor = previous_id.map(canonical_event_cursor);
            let (page, bytes) = self
                .finalized_event_page(contract_id, cursor.as_deref(), &mut size)
                .await?;
            validate_finalized_event_page(
                contract_id,
                cursor.as_deref(),
                previous_id,
                size.rows(),
                &page,
            )?;
            size.after_page(page.events.len(), bytes);
            *pages_left = pages_left.saturating_sub(1);
            let has_next_page = page.has_next_page;
            let mut events = page.events.into_iter();
            if let Some(anchor) = fence.take() {
                match events.next() {
                    Some(first) if first.id == anchor.fence_id => {
                        if !anchor.is_fence(&first)? {
                            return Ok(LogScan::WrongAnchor);
                        }
                        previous_id = Some(first.id);
                    }
                    Some(_) => return Ok(LogScan::WrongAnchor),
                    None => {
                        return Ok(match self.at_missing_fence(contract_id, anchor).await? {
                            ScanOutcome::ArchiveBehind(_) => LogScan::ArchiveBehind,
                            _ => LogScan::WrongAnchor,
                        })
                    }
                }
            }

            let mut batch = WriteBatch::default();
            let mut passed_final = false;
            for event in events {
                // Rusk archives blocks in height order.
                if let Some(max) = max_height.filter(|max| event.block_height < *max) {
                    let height = event.block_height;
                    self.write_event_log(log, state, batch)?;
                    self.drop_event_log_from(log, state, height)?;
                    return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                        "Dusk archive row {} of contract {} is in block {height}, below block {max} before it: the archive does not serve its rows in block order. The {} event log dropped its rows from block {height} on and stops before that block until the endpoint serves its rows in order",
                        event.id,
                        hex::encode(contract_id),
                        log.topic
                    )));
                }
                max_height = Some(event.block_height);
                if open
                    .as_ref()
                    .is_some_and(|block| event.block_height > block.height)
                {
                    // The block being read ends before this row.
                    if let Some(block) = open.take() {
                        match self
                            .close_event_log_block(
                                log,
                                state,
                                block,
                                frontier,
                                &mut next_sequence,
                                &mut batch,
                            )
                            .await
                        {
                            Ok(None) => {}
                            Ok(Some(height)) => {
                                return self.event_log_diverged(log, state, batch, height)
                            }
                            Err(error) => {
                                // Keep the blocks taken before it.
                                self.write_event_log(log, state, batch)?;
                                return Err(error);
                            }
                        }
                    }
                }
                if open.is_none() {
                    let height = event.block_height;
                    if state.last_block.is_some_and(|last| height <= last) {
                        // A block the log holds, read again from its first
                        // row. The log's rows of blocks before it must have
                        // come again before it, so its own rows, if any,
                        // start at the next row the pass accounts for.
                        if next_sequence < state.count {
                            let skipped = self.entry_height(log, next_sequence)?;
                            if skipped < height {
                                return self.event_log_diverged(log, state, batch, skipped);
                            }
                        }
                        open = Some(OpenBlock::held(height, next_sequence));
                    } else {
                        if next_sequence < state.count {
                            // Blocks the log holds rows of did not come again.
                            let skipped = self.entry_height(log, next_sequence)?;
                            return self.event_log_diverged(log, state, batch, skipped);
                        }
                        // A block the log does not hold yet: only one the
                        // node has finalized.
                        if height > frontier.finalized {
                            if height > frontier.latest.saturating_add(MAX_NON_FINAL_BLOCKS) {
                                self.write_event_log(log, state, batch)?;
                                return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                                    "Dusk archive row {} of {}/{} is in block {height}, above the node's tip {}: the endpoint does not serve its archive as Rusk does, and the event log stops before it",
                                    event.id,
                                    hex::encode(contract_id),
                                    log.topic,
                                    frontier.latest
                                )));
                            }
                            // Finalized since `frontier` was read: the next
                            // call takes it.
                            passed_final = true;
                            break;
                        }
                        open = Some(OpenBlock::new(height));
                    }
                }
                if let Some(block) = &mut open {
                    match self.push_event_log_row(log, state, block, &event) {
                        Ok(true) => {}
                        Ok(false) => {
                            let height = block.height;
                            return self.event_log_diverged(log, state, batch, height);
                        }
                        Err(error) => {
                            self.write_event_log(log, state, batch)?;
                            return Err(error);
                        }
                    }
                }
                previous_id = Some(event.id);
                rank = rank.saturating_add(1);
                let highest = highest_block
                    .map_or(event.block_height, |height| height.max(event.block_height));
                highest_block = Some(highest);
                if rank % SCAN_ANCHOR_SPACING == 0 {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(&event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                last_row = Some(event);
            }
            let reached_end = !passed_final && !has_next_page;
            if reached_end {
                // Rusk archives a finalized block's rows at once, so the
                // archive's last block is whole.
                if let Some(block) = open.take() {
                    match self
                        .close_event_log_block(
                            log,
                            state,
                            block,
                            frontier,
                            &mut next_sequence,
                            &mut batch,
                        )
                        .await
                    {
                        Ok(None) => {}
                        Ok(Some(height)) => {
                            return self.event_log_diverged(log, state, batch, height)
                        }
                        Err(error) => {
                            self.write_event_log(log, state, batch)?;
                            return Err(error);
                        }
                    }
                }
            }
            let out_of_pages = !passed_final && has_next_page && *pages_left == 0;
            if out_of_pages {
                // The next call reads on inside the block from here.
                if let (Some(block), Some(row)) = (&mut open, &last_row) {
                    self.stage(log, block, row, rank, &mut batch)?;
                }
            }
            self.write_event_log(log, state, batch)?;
            if passed_final || reached_end || out_of_pages {
                if let (Some(event), Some(highest)) = (&last_row, highest_block) {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                return Ok(LogScan::Done {
                    passed_final: passed_final || reached_end,
                });
            }
        }
    }

    /// Add the next row of `block`. Every row must name one block hash; a
    /// row of the topic in a block above the log's last one must parse and
    /// name a key, and the block must stay within the log's bounds:
    /// otherwise the log stops before the block. `false` when a row of a
    /// block the log holds is not the log's next row of it.
    fn push_event_log_row(
        &self,
        log: &EventLog,
        state: &LogState,
        block: &mut OpenBlock,
        event: &FinalizedContractEvent,
    ) -> Result<bool, HyperlaneDuskError> {
        if matches!(block.kind, BlockKind::Tail) {
            return Ok(true);
        }
        let height = block.height;
        let contract = hex::encode(log.contract_id);
        let malformed = |reason: String| {
            HyperlaneDuskError::Other(format!(
                "Dusk archive row {} of {contract}/{} in block {height} is malformed ({reason}); the event log stops before its block until the endpoint serves it correctly",
                event.id, log.topic
            ))
        };
        let hash = parse_h256_hex(&event.block_hash, "archive row block hash")
            .map_err(|error| malformed(error.to_string()))?;
        if *block.hash.get_or_insert(hash) != hash {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive rows of contract {contract} in block {height} name more than one block hash; the event log stops before the block"
            )));
        }
        block.rows += 1;
        if block.rows > log.limits.rows {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive serves more than {} rows of contract {contract} in block {height}, more than any Rusk block holds: the endpoint does not serve its archive as Rusk does, and the {} event log stops before the block",
                log.limits.rows, log.topic
            )));
        }
        if event.reverted || event.topic != log.topic {
            return Ok(true);
        }
        match &mut block.kind {
            BlockKind::New {
                topic, topic_bytes, ..
            } => {
                let row = BlockRow::parse(event).map_err(|error| malformed(error.to_string()))?;
                let key = (log.key_of)(&row.data)
                    .ok_or_else(|| malformed(format!("not a well-formed {} event", log.topic)))?;
                let row = TopicRow {
                    origin: row.origin,
                    data: row.data,
                    key,
                };
                *topic_bytes = topic_bytes.saturating_add(row.bytes());
                if *topic_bytes > log.limits.topic_bytes {
                    return Err(HyperlaneDuskError::Other(format!(
                        "Dusk archive serves more than {} bytes of {} rows of contract {contract} in block {height}, more than any Rusk block holds: the endpoint does not serve its archive as Rusk does, and the event log stops before the block",
                        log.limits.topic_bytes, log.topic
                    )));
                }
                topic.push(row);
                Ok(true)
            }
            BlockKind::Held { first, matched } => {
                let sequence = first.saturating_add(*matched);
                if sequence >= state.count {
                    return Ok(false);
                }
                let (Ok(row), Some(entry)) =
                    (BlockRow::parse(event), self.event_log_entry(log, sequence)?)
                else {
                    return Ok(false);
                };
                if entry.block_height != height
                    || entry.block_hash != row.block_hash
                    || entry.origin != row.origin
                    || entry.data != row.data
                {
                    return Ok(false);
                }
                *matched += 1;
                Ok(true)
            }
            BlockKind::Tail => Ok(true),
        }
    }

    /// Finish a block the pass read whole: take a block above the log's last
    /// one, and check that one the log holds has no more rows in the log
    /// than the pass read. Moves `next_sequence` past the block's rows.
    /// `Some(height)` when the log's rows of the block differ.
    async fn close_event_log_block(
        &self,
        log: &EventLog,
        state: &mut LogState,
        block: OpenBlock,
        frontier: Frontier,
        next_sequence: &mut u64,
        batch: &mut WriteBatch,
    ) -> Result<Option<u64>, HyperlaneDuskError> {
        match block.kind {
            BlockKind::Tail => Ok(None),
            BlockKind::Held { first, matched } => {
                let next = first.saturating_add(matched);
                if next < state.count && self.entry_height(log, next)? == block.height {
                    return Ok(Some(block.height));
                }
                *next_sequence = next;
                if block.was_staged {
                    self.unstage(log, batch)?;
                }
                Ok(None)
            }
            BlockKind::New { .. } => {
                self.take_event_log_block(log, state, block, frontier, batch)
                    .await?;
                *next_sequence = state.count;
                Ok(None)
            }
        }
    }

    /// Add a block read whole to the log: its rows of the topic, numbered on
    /// from the log's count. The block must be at or below the node's last
    /// final block and `checkBlock(onlyFinalized)` must accept it, whether or
    /// not it has rows of the topic: otherwise the log stops before it. A
    /// row under a published sequence must be the row consumers read there:
    /// otherwise the log halts.
    async fn take_event_log_block(
        &self,
        log: &EventLog,
        state: &mut LogState,
        block: OpenBlock,
        frontier: Frontier,
        batch: &mut WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        let height = block.height;
        let (BlockKind::New { topic, .. }, Some(hash)) = (block.kind, block.hash) else {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk event log took block {height} without reading it"
            )));
        };
        if height > frontier.finalized {
            return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                "Dusk archive serves finalized {}/{} rows of block {height}, above the node's last final block {}; the event log stops before it",
                hex::encode(log.contract_id),
                log.topic,
                frontier.finalized
            )));
        }
        if !self
            .check_finalized_block(height, &hex::encode(hash))
            .await?
        {
            return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                "Dusk archive serves finalized {}/{} rows of block {height} ({hash:?}), which checkBlock(onlyFinalized) rejects; the event log stops before the block",
                hex::encode(log.contract_id),
                log.topic
            )));
        }
        let mut sequence = state.count;
        for row in &topic {
            if sequence < state.published {
                let read = self.event_log_entry(log, sequence)?;
                let same = read.as_ref().is_some_and(|read| {
                    read.block_height == height
                        && read.block_hash == hash
                        && read.origin == row.origin
                        && read.data == row.data
                });
                if !same {
                    let read = match read {
                        Some(read) => format!(
                            "the row of transaction {} in block {}",
                            hex::encode(read.origin),
                            read.block_height
                        ),
                        None => "a row the event store no longer holds".to_owned(),
                    };
                    return Err(self.halt_event_log(
                        log,
                        height,
                        format!(
                            "CRITICAL: the Dusk {} event log of contract {} would number its rows differently from what consumers have read: sequence {sequence}, read as {read}, would be the row of transaction {} in block {height}, so the endpoint's finalized archive history changed under it. The log stops instead of numbering them again. Check the endpoint's archive, then stop the agent, reset its database and its eventCursorDir, and start it again so that both index the archive afresh",
                            log.topic,
                            hex::encode(log.contract_id),
                            hex::encode(row.origin)
                        ),
                    )?);
                }
            }
            sequence = sequence.checked_add(1).ok_or_else(|| {
                HyperlaneDuskError::Other("Dusk event log row count overflows".into())
            })?;
        }
        for (sequence, row) in (state.count..).zip(&topic) {
            if sequence >= state.published {
                batch.put(
                    event_log_entry_key(log, sequence),
                    encode_entry(height, hash, &row.origin, &row.data),
                );
                batch.put(event_log_lookup_key(log, &row.key, sequence), []);
            }
        }
        if block.was_staged {
            self.unstage(log, batch)?;
        }
        state.count = sequence;
        state.last_block = Some(height);
        Ok(())
    }

    /// The log's rows of a block it holds differ from the archive's now:
    /// drop them from `height` on, after `batch`, and read them again.
    fn event_log_diverged(
        &self,
        log: &EventLog,
        state: &mut LogState,
        batch: WriteBatch,
        height: u64,
    ) -> Result<LogScan, HyperlaneDuskError> {
        self.write_event_log(log, state, batch)?;
        warn!(
            contract = %hex::encode(log.contract_id),
            topic = log.topic,
            block_height = height,
            "Dusk archive's rows of a block the event log holds differ from the log's; the log reads its rows again from that block on"
        );
        self.drop_event_log_from(log, state, height)?;
        Ok(LogScan::Rebuild)
    }

    /// Drop the log's rows of blocks from `height` on, so that it reads
    /// them again, with any staged block. The rows of published sequences
    /// stay in the event store, unseen, as evidence the rows taken again
    /// under them must match.
    fn drop_event_log_from(
        &self,
        log: &EventLog,
        state: &mut LogState,
        height: u64,
    ) -> Result<(), HyperlaneDuskError> {
        let first = self.first_sequence_at_or_above(log, state, height)?;
        let kept = state.published.clamp(first, state.count.max(first));
        let mut batch = WriteBatch::default();
        for sequence in kept..state.count {
            if let Some(entry) = self.event_log_entry(log, sequence)? {
                if let Some(key) = (log.key_of)(&entry.data) {
                    batch.delete(event_log_lookup_key(log, &key, sequence));
                }
            }
            batch.delete(event_log_entry_key(log, sequence));
        }
        self.unstage(log, &mut batch)?;
        let dropped = state.count.saturating_sub(first);
        let below = height.checked_sub(1);
        state.count = first;
        state.last_block = lowered(state.last_block, below);
        state.complete_through = lowered(state.complete_through, below);
        self.write_event_log(log, state, batch)?;
        warn!(
            contract = %hex::encode(log.contract_id),
            topic = log.topic,
            block_height = height,
            dropped,
            kept_as_evidence = kept.saturating_sub(first),
            "Dusk event log dropped its rows from a block on and reads them again"
        );
        Ok(())
    }

    /// Stop the log for good with `message`, and report it as a critical
    /// error. Every read of the log fails with it until the event store is
    /// reset.
    fn halt_event_log(
        &self,
        log: &EventLog,
        height: u64,
        message: String,
    ) -> Result<HyperlaneDuskError, HyperlaneDuskError> {
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.event_store()?
            .put_opt(event_log_halt_key(log), message.as_bytes(), &write_options)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to persist the halt of the Dusk {} event log: {error}",
                    log.topic
                ))
            })?;
        error!(
            contract = %hex::encode(log.contract_id),
            topic = log.topic,
            block_height = height,
            %message,
            "CRITICAL: Dusk event log halted"
        );
        Ok(HyperlaneDuskError::EventLogHalted(message))
    }

    /// Fail with the log's halt, if it is halted.
    fn ensure_event_log_runs(&self, log: &EventLog) -> Result<(), HyperlaneDuskError> {
        let Some(message) = self
            .event_store()?
            .get(event_log_halt_key(log))
            .map_err(|error| event_log_read_error(log, error))?
        else {
            return Ok(());
        };
        let message = String::from_utf8_lossy(&message).into_owned();
        if critical_report_due(&log.contract_id, &format!("halted {}", log.topic), 0) {
            error!(
                contract = %hex::encode(log.contract_id),
                topic = log.topic,
                %message,
                "CRITICAL: Dusk event log is halted"
            );
        }
        Err(HyperlaneDuskError::EventLogHalted(message))
    }

    /// Stage `block`, which the pass read up to `row`, the contract's `rank`th
    /// row, with the position after `row`: for a block above the log's last
    /// one, its rows of the topic not staged yet; for one the log holds, how
    /// many of its rows matched. The rest of a fence row's block is not
    /// staged: the next call starts after the position this one ends at.
    fn stage(
        &self,
        log: &EventLog,
        block: &mut OpenBlock,
        row: &FinalizedContractEvent,
        rank: u64,
        batch: &mut WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(hash) = block.hash else {
            return Ok(());
        };
        let Some(position) = ScanAnchor::after_row(row, block.height.saturating_add(1), rank)?
        else {
            return Ok(());
        };
        let (topic_rows, topic_bytes) = match &mut block.kind {
            BlockKind::Tail => return Ok(()),
            BlockKind::Held { matched, .. } => (*matched, 0),
            BlockKind::New {
                topic,
                topic_bytes,
                staged,
            } => {
                for (index, topic) in topic.iter().enumerate().skip(*staged) {
                    let mut value = topic.origin.to_vec();
                    value.extend_from_slice(&topic.data);
                    batch.put(event_log_staged_row_key(log, index as u64), value);
                }
                *staged = topic.len();
                (topic.len() as u64, *topic_bytes as u64)
            }
        };
        let staged = StagedBlock {
            height: block.height,
            hash,
            rows: block.rows,
            topic_rows,
            topic_bytes,
            position,
        };
        batch.put(event_log_staged_key(log), staged.encode());
        block.was_staged = true;
        Ok(())
    }

    /// The block an earlier call staged, if any. One that does not decode is
    /// dropped with its rows, and its block is read again.
    fn staged_block(
        &self,
        log: &EventLog,
        state: &LogState,
    ) -> Result<Option<StagedBlock>, HyperlaneDuskError> {
        let Some(bytes) = self
            .event_store()?
            .get(event_log_staged_key(log))
            .map_err(|error| event_log_read_error(log, error))?
        else {
            return Ok(None);
        };
        if let Some(staged) = StagedBlock::decode(&bytes) {
            return Ok(Some(staged));
        }
        let mut batch = WriteBatch::default();
        self.unstage(log, &mut batch)?;
        self.write_event_log(log, state, batch)?;
        Ok(None)
    }

    /// The block `staged` holds, to read on: above the log's last block, with
    /// its rows of the topic read so far; one the log holds, with how many
    /// of its rows matched.
    fn reopen(
        &self,
        log: &EventLog,
        state: &LogState,
        staged: &StagedBlock,
    ) -> Result<OpenBlock, HyperlaneDuskError> {
        let mut block = OpenBlock {
            hash: Some(staged.hash),
            rows: staged.rows,
            was_staged: true,
            ..OpenBlock::new(staged.height)
        };
        if state.last_block.is_some_and(|last| staged.height <= last) {
            block.kind = BlockKind::Held {
                first: self.first_sequence_at_or_above(log, state, staged.height)?,
                matched: staged.topic_rows,
            };
            return Ok(block);
        }
        let store = self.event_store()?;
        let corrupt = || {
            HyperlaneDuskError::Other(format!(
                "The staged block of the Dusk {} event log in eventCursorDir is corrupt",
                log.topic
            ))
        };
        let mut topic = Vec::with_capacity(staged.topic_rows.min(1024) as usize);
        let mut topic_bytes = 0usize;
        for index in 0..staged.topic_rows {
            let value = store
                .get(event_log_staged_row_key(log, index))
                .map_err(|error| event_log_read_error(log, error))?
                .ok_or_else(corrupt)?;
            let (origin, data) = value.split_at_checked(32).ok_or_else(corrupt)?;
            let key = (log.key_of)(data).ok_or_else(corrupt)?;
            let row = TopicRow {
                origin: origin.try_into().map_err(|_| corrupt())?,
                data: data.to_vec(),
                key,
            };
            topic_bytes = topic_bytes.saturating_add(row.bytes());
            topic.push(row);
        }
        block.kind = BlockKind::New {
            staged: topic.len(),
            topic,
            topic_bytes,
        };
        Ok(block)
    }

    /// Drop the staged block and its rows.
    fn unstage(&self, log: &EventLog, batch: &mut WriteBatch) -> Result<(), HyperlaneDuskError> {
        let prefix = event_log_staged_row_prefix(log);
        for item in self
            .event_store()?
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            let (key, _) = item.map_err(|error| event_log_read_error(log, error))?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete(key);
        }
        batch.delete(event_log_staged_key(log));
        Ok(())
    }

    /// Write `batch` with the log's new state, durably and at once.
    fn write_event_log(
        &self,
        log: &EventLog,
        state: &LogState,
        mut batch: WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        batch.put(event_log_state_key(log), encode_state(state));
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        self.event_store()?
            .write_opt(batch, &write_options)
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to persist the Dusk {} event log: {error}",
                    log.topic
                ))
            })
    }

    /// How far the log reaches.
    pub(crate) fn event_log_state(&self, log: &EventLog) -> Result<LogState, HyperlaneDuskError> {
        let Some(bytes) = self
            .event_store()?
            .get(event_log_state_key(log))
            .map_err(|error| event_log_read_error(log, error))?
        else {
            return Ok(LogState::default());
        };
        decode_state(&bytes).ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "The Dusk {} event log's state in eventCursorDir is corrupt",
                log.topic
            ))
        })
    }

    /// The log's rows `first` to `last`, those it has, as a consumer that
    /// indexes the log by its sequences reads them: they count as published
    /// from now on.
    pub(crate) async fn read_event_log_entries(
        &self,
        log: &EventLog,
        first: u64,
        last: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        self.ensure_event_log_runs(log)?;
        let mut state = self.event_log_state(log)?;
        let entries = self.entries_in(log, &state, first, last)?;
        if let Some(entry) = entries.last() {
            self.publish_event_log(log, &mut state, entry.sequence.saturating_add(1))?;
        }
        Ok(entries)
    }

    /// The log's rows of block `height`, as a consumer that indexes the log
    /// by its sequences reads them: they count as published from now on.
    pub(crate) async fn read_event_log_block(
        &self,
        log: &EventLog,
        height: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        self.ensure_event_log_runs(log)?;
        let mut state = self.event_log_state(log)?;
        let first = self.first_sequence_at_or_above(log, &state, height)?;
        let mut entries = Vec::new();
        for sequence in first..state.count {
            let Some(entry) = self.event_log_entry(log, sequence)? else {
                break;
            };
            if entry.block_height != height {
                break;
            }
            entries.push(entry);
        }
        if let Some(entry) = entries.last() {
            self.publish_event_log(log, &mut state, entry.sequence.saturating_add(1))?;
        }
        Ok(entries)
    }

    /// Record that consumers may have read every sequence below `through`,
    /// before they get the rows.
    fn publish_event_log(
        &self,
        log: &EventLog,
        state: &mut LogState,
        through: u64,
    ) -> Result<(), HyperlaneDuskError> {
        if through > state.published {
            state.published = through;
            self.write_event_log(log, state, WriteBatch::default())?;
        }
        Ok(())
    }

    /// The log's rows `first` to `last`, those it has, without publishing
    /// them.
    #[cfg(test)]
    pub(crate) fn event_log_entries(
        &self,
        log: &EventLog,
        first: u64,
        last: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let state = self.event_log_state(log)?;
        self.entries_in(log, &state, first, last)
    }

    /// The log's rows `first` to `last` below its count.
    fn entries_in(
        &self,
        log: &EventLog,
        state: &LogState,
        first: u64,
        last: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let mut entries = Vec::new();
        let Some(end) = state.count.checked_sub(1).map(|top| top.min(last)) else {
            return Ok(entries);
        };
        for sequence in first..=end {
            match self.event_log_entry(log, sequence)? {
                Some(entry) => entries.push(entry),
                None => break,
            }
        }
        Ok(entries)
    }

    fn event_log_entry(
        &self,
        log: &EventLog,
        sequence: u64,
    ) -> Result<Option<LogEntry>, HyperlaneDuskError> {
        let Some(bytes) = self
            .event_store()?
            .get(event_log_entry_key(log, sequence))
            .map_err(|error| event_log_read_error(log, error))?
        else {
            return Ok(None);
        };
        decode_entry(sequence, &bytes).map(Some).ok_or_else(|| {
            HyperlaneDuskError::Other(format!(
                "Row {sequence} of the Dusk {} event log in eventCursorDir is corrupt",
                log.topic
            ))
        })
    }

    /// The height of the block of the log's row `sequence`, which it holds.
    fn entry_height(&self, log: &EventLog, sequence: u64) -> Result<u64, HyperlaneDuskError> {
        self.event_log_entry(log, sequence)?
            .map(|entry| entry.block_height)
            .ok_or_else(|| {
                HyperlaneDuskError::Other(format!(
                    "Row {sequence} of the Dusk {} event log is missing from eventCursorDir",
                    log.topic
                ))
            })
    }

    /// The sequence of the log's first row in block `height` or above, or
    /// its count. The log is in block order, so a binary search finds it.
    fn first_sequence_at_or_above(
        &self,
        log: &EventLog,
        state: &LogState,
        height: u64,
    ) -> Result<u64, HyperlaneDuskError> {
        let (mut low, mut high) = (0u64, state.count);
        while low < high {
            let middle = low + (high - low) / 2;
            if self.entry_height(log, middle)? < height {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok(low)
    }

    /// The rows that name `key`, in log order, at most `MAX_ROWS_PER_KEY`.
    pub(crate) fn event_log_find(
        &self,
        log: &EventLog,
        key: &[u8],
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        self.ensure_event_log_runs(log)?;
        let count = self.event_log_state(log)?.count;
        let store = self.event_store()?;
        let prefix = event_log_lookup_prefix(log, key);
        let mut entries = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (stored, _) = item.map_err(|error| event_log_read_error(log, error))?;
            if !stored.starts_with(&prefix) || entries.len() >= MAX_ROWS_PER_KEY {
                break;
            }
            let sequence = stored
                .get(prefix.len()..)
                .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
                .map(u64::from_be_bytes)
                .ok_or_else(|| {
                    HyperlaneDuskError::Other(format!(
                        "The Dusk {} event log's index in eventCursorDir is corrupt",
                        log.topic
                    ))
                })?;
            // Rows kept as evidence are not the log's.
            if sequence >= count {
                break;
            }
            if let Some(entry) = self
                .event_log_entry(log, sequence)?
                .filter(|entry| (log.key_of)(&entry.data).as_deref() == Some(key))
            {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// Read the log again from the contract's first row: the endpoint no
    /// longer agrees with it. Its rows are dropped, but those of published
    /// sequences are kept as evidence the rows read again must match.
    pub(crate) async fn reset_event_log(&self, log: &EventLog) -> Result<(), HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        self.ensure_event_log_runs(log)?;
        let mut state = self.event_log_state(log)?;
        self.drop_event_log_from(log, &mut state, 0)
    }

    /// Whether a row with `key` is in a final block, read after advancing
    /// the log by at most `max_pages` pages. `pair` is the node's tip and
    /// last final block, read before the caller read the state it checks.
    pub(crate) async fn finalized_event_log_lookup(
        &self,
        log: &EventLog,
        key: &[u8],
        pair: &BlockPair,
        max_pages: usize,
    ) -> Result<LogLookup, HyperlaneDuskError> {
        if self.event_store.is_none() {
            return Ok(LogLookup::Unknown(
                "no eventCursorDir holds an event log".into(),
            ));
        }
        let state = self.advance_event_log(log, max_pages, Some(pair)).await?;
        if let Some(entry) = self.event_log_find(log, key)?.into_iter().next() {
            if self
                .check_finalized_block(entry.block_height, &hex::encode(entry.block_hash))
                .await?
            {
                return Ok(LogLookup::Found(entry));
            }
            self.reset_event_log(log).await?;
            return Ok(LogLookup::Unknown(format!(
                "checkBlock(onlyFinalized) no longer accepts block {} ({:?}) of the {} event log, which is read again",
                entry.block_height, entry.block_hash, log.topic
            )));
        }
        Ok(match state.complete_through {
            Some(through) if through >= pair.finalized => LogLookup::Absent {
                final_block: pair.finalized,
            },
            through => LogLookup::Unknown(format!(
                "the {} event log holds the archive's finalized rows up to block {through:?}, short of the last final block {}",
                log.topic, pair.finalized
            )),
        })
    }
}

fn event_log_prefix(log: &EventLog) -> Vec<u8> {
    let mut key = b"dusk-event-log-v1:".to_vec();
    key.extend_from_slice(&log.contract_id);
    key.extend_from_slice(&hyperlane_dusk_types::message::keccak256(
        log.topic.as_bytes(),
    ));
    key
}

fn event_log_state_key(log: &EventLog) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b's');
    key
}

/// Big-endian sequences keep a log's rows in order.
fn event_log_entry_key(log: &EventLog, sequence: u64) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b'e');
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// The key's length comes first, so no key's rows run into another's.
fn event_log_lookup_prefix(log: &EventLog, lookup: &[u8]) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b'k');
    key.extend_from_slice(&(lookup.len() as u32).to_be_bytes());
    key.extend_from_slice(lookup);
    key
}

fn event_log_lookup_key(log: &EventLog, lookup: &[u8], sequence: u64) -> Vec<u8> {
    let mut key = event_log_lookup_prefix(log, lookup);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

/// The block a call stopped inside.
fn event_log_staged_key(log: &EventLog) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b'g');
    key
}

fn event_log_staged_row_prefix(log: &EventLog) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b'r');
    key
}

/// The staged block's rows of the topic, in archive order.
fn event_log_staged_row_key(log: &EventLog, index: u64) -> Vec<u8> {
    let mut key = event_log_staged_row_prefix(log);
    key.extend_from_slice(&index.to_be_bytes());
    key
}

/// Why the log halted, once it has.
fn event_log_halt_key(log: &EventLog) -> Vec<u8> {
    let mut key = event_log_prefix(log);
    key.push(b'h');
    key
}

fn event_log_read_error(log: &EventLog, error: impl fmt::Display) -> HyperlaneDuskError {
    HyperlaneDuskError::Other(format!(
        "Failed to read the Dusk {} event log: {error}",
        log.topic
    ))
}

fn encode_entry(height: u64, block_hash: H256, origin: &[u8; 32], data: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ENTRY_HEADER_BYTES + data.len());
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes.extend_from_slice(block_hash.as_bytes());
    bytes.extend_from_slice(origin);
    bytes.extend_from_slice(data);
    bytes
}

fn decode_entry(sequence: u64, bytes: &[u8]) -> Option<LogEntry> {
    let (header, data) = bytes.split_at_checked(ENTRY_HEADER_BYTES)?;
    let (height, rest) = header.split_at(8);
    let (block_hash, origin) = rest.split_at(32);
    Some(LogEntry {
        sequence,
        block_height: u64::from_be_bytes(height.try_into().ok()?),
        block_hash: H256::from_slice(block_hash),
        origin: origin.try_into().ok()?,
        data: data.to_vec(),
    })
}

fn encode_state(state: &LogState) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(STATE_BYTES);
    bytes.extend_from_slice(&state.count.to_be_bytes());
    for height in [state.last_block, state.complete_through] {
        bytes.push(u8::from(height.is_some()));
        bytes.extend_from_slice(&height.unwrap_or_default().to_be_bytes());
    }
    bytes.extend_from_slice(&state.published.to_be_bytes());
    bytes
}

/// A state the previous version wrote records no published sequences.
fn decode_state(bytes: &[u8]) -> Option<LogState> {
    if bytes.len() != STATE_BYTES && bytes.len() != STATE_V1_BYTES {
        return None;
    }
    let number = |offset: usize| -> Option<u64> {
        Some(u64::from_be_bytes(
            bytes.get(offset..offset + 8)?.try_into().ok()?,
        ))
    };
    let height = |offset: usize| -> Option<Option<u64>> {
        let value = number(offset + 1)?;
        match bytes.get(offset)? {
            0 => Some(None),
            1 => Some(Some(value)),
            _ => None,
        }
    };
    let published = if bytes.len() == STATE_BYTES {
        number(STATE_V1_BYTES)?
    } else {
        0
    };
    Some(LogState {
        count: number(0)?,
        last_block: height(8)?,
        complete_through: height(17)?,
        published,
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{archive_row, test_client_with_event_store, Archive};
    use super::*;

    /// A log of `deliver` rows of `contract_id`, keyed by their data.
    fn test_log(contract_id: [u8; 32], limits: BlockLimits) -> EventLog {
        EventLog {
            contract_id,
            topic: "deliver",
            key_of: |data| Some(data.to_vec()),
            limits,
        }
    }

    #[tokio::test]
    async fn a_block_past_the_logs_bounds_fails_with_a_clear_error() {
        // Block 5 holds 1,000 rows of another topic; the log allows 500.
        let contract_id = [120u8; 32];
        let mut rows: Vec<_> = (0..1000)
            .map(|id| archive_row(id, 5, 1, "other", contract_id, &[1]))
            .collect();
        rows.push(archive_row(1000, 6, 2, "deliver", contract_id, &[2]));
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let log = test_log(
            contract_id,
            BlockLimits {
                rows: 500,
                ..BLOCK_LIMITS
            },
        );
        // A call of one page stages the block.
        for _ in 0..2 {
            let state = client.advance_event_log(&log, 1, None).await.unwrap();
            assert_eq!((state.count, state.last_block), (0, None));
        }
        // The third page takes the block past 500 rows, and so does every
        // call after it.
        for _ in 0..2 {
            let error = client
                .advance_event_log(&log, 1, None)
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("more than 500 rows of contract") && error.contains("in block 5"),
                "{error}"
            );
        }
        assert_eq!(client.event_log_state(&log).unwrap().count, 0);
        assert_eq!(archive.pages().len(), 4);

        // The bytes of the topic's rows are bounded the same way.
        let contract_id = [121u8; 32];
        let rows = (0..50)
            .map(|id| archive_row(id, 5, id as u8, "deliver", contract_id, &[id as u8; 32]))
            .collect();
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let log = test_log(
            contract_id,
            BlockLimits {
                topic_bytes: 1000,
                ..BLOCK_LIMITS
            },
        );
        let error = client
            .advance_event_log(&log, 16, None)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("more than 1000 bytes of deliver rows"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_staged_block_is_read_on_after_a_restart() {
        // Block 5 spans three pages, with a row of the topic on each.
        let contract_id = [122u8; 32];
        let rows = (0..450)
            .map(|id| {
                let topic = if id % 200 == 7 { "deliver" } else { "other" };
                archive_row(id, 5, 1, topic, contract_id, &[id as u8])
            })
            .chain([archive_row(450, 6, 2, "deliver", contract_id, &[0xee])])
            .collect();
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let log = test_log(contract_id, BLOCK_LIMITS);
        {
            let client = test_client_with_event_store(archive.url.clone(), directory.path());
            let state = client.advance_event_log(&log, 1, None).await.unwrap();
            assert_eq!(state.count, 0);
        }
        // The store is closed and opened again: the next call reads on
        // from the last row the first one read.
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        assert_eq!((state.count, state.last_block), (4, Some(6)));
        assert_eq!(archive.pages(), [None, Some(198), Some(398)]);
        let data: Vec<Vec<u8>> = client
            .event_log_entries(&log, 0, 3)
            .unwrap()
            .into_iter()
            .map(|entry| entry.data)
            .collect();
        assert_eq!(data, [vec![7], vec![207], vec![151], vec![0xee]]);
        // Nothing stays staged once the block is taken.
        assert!(client.staged_block(&log, &state).unwrap().is_none());
    }

    /// Renumber `rows` from 1,000, as a rebuilt archive would.
    fn renumbered(rows: Vec<JsonValue>) -> Vec<JsonValue> {
        rows.into_iter()
            .enumerate()
            .map(|(id, mut row)| {
                row["id"] = serde_json::json!(1000 + id as i64);
                row
            })
            .collect()
    }

    #[tokio::test]
    async fn blocks_the_log_holds_that_a_rebuilt_archive_lacks_are_read_again() {
        let contract_id = [125u8; 32];
        let deliver = |id: i64, height: u64, data: u8| {
            archive_row(id, height, data, "deliver", contract_id, &[data])
        };
        let rows = vec![
            deliver(0, 5, 0xa1),
            deliver(1, 5, 0xa2),
            deliver(2, 6, 0xb),
            deliver(3, 7, 0xc),
        ];
        let archive = Archive::start(rows.clone());
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let log = test_log(contract_id, BLOCK_LIMITS);
        let data = |client: &RuesClient| -> Vec<Vec<u8>> {
            client
                .event_log_entries(&log, 0, 9)
                .unwrap()
                .into_iter()
                .map(|entry| entry.data)
                .collect()
        };
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        assert_eq!((state.count, state.last_block), (4, Some(7)));

        // A rebuilt archive lacks block 6's row: the log, which holds it,
        // drops its rows from block 6 on and takes block 7 again.
        *archive.rows.lock().unwrap() =
            renumbered(vec![rows[0].clone(), rows[1].clone(), rows[3].clone()]);
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        assert_eq!((state.count, state.last_block), (3, Some(7)));
        assert_eq!(data(&client), [vec![0xa1], vec![0xa2], vec![0xc]]);

        // Another lacks the second row of block 5: the log reads block 5
        // again from that row on.
        *archive.rows.lock().unwrap() = renumbered(vec![rows[0].clone(), rows[3].clone()]);
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        assert_eq!((state.count, state.last_block), (2, Some(7)));
        assert_eq!(data(&client), [vec![0xa1], vec![0xc]]);
    }

    #[tokio::test]
    async fn a_staged_block_whose_rows_are_lost_is_read_again() {
        let contract_id = [124u8; 32];
        let rows: Vec<_> = (0..300)
            .map(|id| {
                let topic = if id % 100 == 3 { "deliver" } else { "other" };
                archive_row(id, 5, 1, topic, contract_id, &[id as u8])
            })
            .collect();
        let archive = Archive::start(rows);
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let log = test_log(contract_id, BLOCK_LIMITS);
        client.advance_event_log(&log, 1, None).await.unwrap();
        // The staged row of block 5 is lost from the event store.
        client
            .event_store()
            .unwrap()
            .delete(event_log_staged_row_key(&log, 1))
            .unwrap();
        // The log reads the block again instead of failing on every call.
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        assert_eq!((state.count, state.last_block), (3, Some(5)));
        assert_eq!(archive.pages(), [None, None, Some(199)]);
    }

    #[tokio::test]
    async fn a_staged_position_the_archive_renumbered_is_read_again_from_its_block() {
        let contract_id = [123u8; 32];
        let rows: Vec<_> = (0..300)
            .map(|id| {
                let topic = if id % 100 == 3 { "deliver" } else { "other" };
                archive_row(id, 5, 1, topic, contract_id, &[id as u8])
            })
            .collect();
        let archive = Archive::start(rows.clone());
        let directory = tempfile::tempdir().unwrap();
        let client = test_client_with_event_store(archive.url.clone(), directory.path());
        let log = test_log(contract_id, BLOCK_LIMITS);
        client.advance_event_log(&log, 1, None).await.unwrap();
        // The endpoint now numbers the same rows from 1,000.
        *archive.rows.lock().unwrap() = rows
            .into_iter()
            .map(|mut row| {
                row["id"] = serde_json::json!(row["id"].as_i64().unwrap() + 1000);
                row
            })
            .collect();
        let state = client.advance_event_log(&log, 16, None).await.unwrap();
        // The block was read again from its first row, and each of its rows
        // of the topic was taken once.
        assert_eq!((state.count, state.last_block), (3, Some(5)));
        assert_eq!(archive.pages(), [None, Some(198), None, Some(1199)]);
    }

    #[test]
    fn log_rows_and_states_round_trip_and_reject_corruption() {
        let encoded = encode_entry(9, H256::repeat_byte(4), &[5; 32], &[6, 7]);
        let entry = decode_entry(3, &encoded).unwrap();
        assert_eq!(
            entry,
            LogEntry {
                sequence: 3,
                block_height: 9,
                block_hash: H256::repeat_byte(4),
                origin: [5; 32],
                data: vec![6, 7],
            }
        );
        assert!(decode_entry(3, &encoded[..ENTRY_HEADER_BYTES - 1]).is_none());
        for state in [
            LogState::default(),
            LogState {
                count: 7,
                last_block: Some(11),
                complete_through: Some(12),
                published: 9,
            },
            LogState {
                count: 0,
                last_block: None,
                complete_through: Some(0),
                published: 0,
            },
        ] {
            assert_eq!(decode_state(&encode_state(&state)), Some(state));
        }
        let mut corrupt = encode_state(&LogState::default());
        corrupt[8] = 2;
        assert!(decode_state(&corrupt).is_none());
        assert!(decode_state(&corrupt[1..]).is_none());
        // A state the previous version wrote records no published sequences.
        let state = LogState {
            count: 7,
            last_block: Some(11),
            complete_through: Some(12),
            published: 0,
        };
        assert_eq!(
            decode_state(&encode_state(&state)[..STATE_V1_BYTES]),
            Some(state)
        );
    }

    #[test]
    fn lookup_keys_of_different_lengths_never_share_rows() {
        let log = EventLog {
            contract_id: [1; 32],
            topic: "topic",
            key_of: |_| None,
            limits: BLOCK_LIMITS,
        };
        let short = event_log_lookup_prefix(&log, &[1, 2]);
        let long = event_log_lookup_key(&log, &[1, 2, 3], 0);
        assert!(!long.starts_with(&short));
    }
}
