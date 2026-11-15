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
//! or at the end of the archive. A log never takes rows of a block it
//! already holds, so its numbering does not change when the endpoint's
//! archive numbers its rows differently.
//!
//! A call reads at most the pages it is given. A block it has not read whole
//! by then is staged in the event store, with the rows of the topic read so
//! far and the position after the last row read, and the next call reads on
//! from there; the rows become entries only once the block is whole and
//! valid. A block of more rows than any Rusk block holds fails the log with
//! an error instead.

use super::*;

/// Most rows one key may name in a lookup.
const MAX_ROWS_PER_KEY: usize = 64;
/// Bytes of an encoded row before its data: height, block hash, origin.
const ENTRY_HEADER_BYTES: usize = 8 + 32 + 32;

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

/// A row of the log's topic in the block being read.
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

/// The block a pass is reading, above the last block the log holds.
struct OpenBlock {
    height: u64,
    /// The block hash its first row names, which every row must name.
    hash: H256,
    /// Rows of the contract read in the block, of any topic.
    rows: u64,
    /// Rows of the topic read in the block, in archive order.
    topic: Vec<TopicRow>,
    topic_bytes: usize,
    /// How many of `topic` are in the event store's staging already.
    staged: usize,
    /// Whether an earlier call staged the block.
    was_staged: bool,
}

impl OpenBlock {
    fn new(height: u64) -> Self {
        Self {
            height,
            hash: H256::zero(),
            rows: 0,
            topic: Vec::new(),
            topic_bytes: 0,
            staged: 0,
            was_staged: false,
        }
    }

    /// Add the block's next row. Every row must name one block hash, and a
    /// row of the topic must parse and name a key: otherwise the log stops
    /// before the block. The block must stay within the log's bounds.
    fn push(
        &mut self,
        log: &EventLog,
        event: &FinalizedContractEvent,
    ) -> Result<(), HyperlaneDuskError> {
        let height = self.height;
        let contract = hex::encode(log.contract_id);
        let malformed = |reason: String| {
            HyperlaneDuskError::Other(format!(
                "Dusk archive row {} of {contract}/{} in block {height} is malformed ({reason}); the event log stops before its block until the endpoint serves it correctly",
                event.id, log.topic
            ))
        };
        let hash = parse_h256_hex(&event.block_hash, "archive row block hash")
            .map_err(|error| malformed(error.to_string()))?;
        if self.rows == 0 {
            self.hash = hash;
        } else if hash != self.hash {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive rows of contract {contract} in block {height} name more than one block hash; the event log stops before the block"
            )));
        }
        self.rows += 1;
        if self.rows > log.limits.rows {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive serves more than {} rows of contract {contract} in block {height}, more than any Rusk block holds: the endpoint does not serve its archive as Rusk does, and the {} event log stops before the block",
                log.limits.rows, log.topic
            )));
        }
        if event.reverted || event.topic != log.topic {
            return Ok(());
        }
        let row = BlockRow::parse(event).map_err(|error| malformed(error.to_string()))?;
        let key = (log.key_of)(&row.data)
            .ok_or_else(|| malformed(format!("not a well-formed {} event", log.topic)))?;
        let row = TopicRow {
            origin: row.origin,
            data: row.data,
            key,
        };
        self.topic_bytes = self.topic_bytes.saturating_add(row.bytes());
        if self.topic_bytes > log.limits.topic_bytes {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk archive serves more than {} bytes of {} rows of contract {contract} in block {height}, more than any Rusk block holds: the endpoint does not serve its archive as Rusk does, and the event log stops before the block",
                log.limits.topic_bytes, log.topic
            )));
        }
        self.topic.push(row);
        Ok(())
    }
}

/// A block a call stopped inside, with its rows of the topic in the event
/// store, and the position after the last row read.
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
        let data = self
            .graphql_query("query { lastBlockPair { json } }")
            .await?;
        let (latest, finalized) = parse_block_heights(&data)?;
        Ok(Frontier { latest, finalized })
    }

    /// Read the archive rows of `log`'s contract after the last block the log
    /// holds, and add the rows of its topic of every block read whole. Reads
    /// at most `max_pages` pages (at least one); a block not read whole by
    /// then is staged, and the next call reads on from there. With
    /// `finality`, the node's tip and last final block read before, a log
    /// that reads past the rows of that last final block, which the archive
    /// had finalized before the read, holds every row up to it; without it,
    /// the call reads `lastBlockPair` itself. No block above the node's last
    /// final block is taken.
    pub(crate) async fn advance_event_log(
        &self,
        log: &EventLog,
        max_pages: usize,
        finality: Option<&BlockPair>,
    ) -> Result<LogState, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        let mut state = self.event_log_state(log)?;
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
        let target = state.last_block.map_or(0, |block| block.saturating_add(1));
        let mut start = match self.staged_block(log, &state)? {
            Some(staged) => PassStart::Staged(staged),
            None => PassStart::Anchor(self.scan_anchor_at_or_below(&log.contract_id, target)?),
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
                LogScan::WrongAnchor if staged => {
                    warn!(
                        contract = %hex::encode(log.contract_id),
                        topic = log.topic,
                        "Dusk archive rows were renumbered; the event log reads its staged block again"
                    );
                    let mut batch = WriteBatch::default();
                    self.unstage(log, &mut batch)?;
                    self.write_event_log(log, &state, batch)?;
                    start =
                        PassStart::Anchor(self.scan_anchor_at_or_below(&log.contract_id, target)?);
                }
                LogScan::WrongAnchor if from_anchor => {
                    warn!(
                        contract = %hex::encode(log.contract_id),
                        topic = log.topic,
                        "Dusk archive rows were renumbered; the event log reads on from the contract's first row"
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

    /// One pass over the contract's rows from `start`, adding to `state`
    /// every block above its last one that it reads whole, until it has used
    /// `pages_left`. Saves its progress after every page.
    async fn read_event_log(
        &self,
        log: &EventLog,
        state: &mut LogState,
        start: PassStart,
        pages_left: &mut usize,
        frontier: Frontier,
    ) -> Result<LogScan, HyperlaneDuskError> {
        let contract_id = &log.contract_id;
        let (anchor, mut open) = match start {
            PassStart::Anchor(anchor) => (anchor, None),
            PassStart::Staged(staged) => (Some(staged.position), Some(self.reopen(log, &staged)?)),
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
                if let Some(block) = &open {
                    if event.block_height < block.height {
                        let error = HyperlaneDuskError::Other(format!(
                            "Dusk archive row {} of contract {} is in block {}, below block {} before it",
                            event.id,
                            hex::encode(contract_id),
                            event.block_height,
                            block.height
                        ));
                        // Keep the blocks taken before it.
                        self.write_event_log(log, state, batch)?;
                        return Err(error);
                    }
                    if event.block_height > block.height {
                        // The block being read ends before this row.
                        if let Some(block) = open.take() {
                            if let Err(error) = self
                                .take_event_log_block(log, state, block, frontier, &mut batch)
                                .await
                            {
                                // Keep the blocks taken before it.
                                self.write_event_log(log, state, batch)?;
                                return Err(error);
                            }
                        }
                    }
                }
                let held = state
                    .last_block
                    .is_some_and(|last| event.block_height <= last);
                if open.is_none() && !held {
                    // A block the log does not hold yet: only one the node
                    // has finalized.
                    if event.block_height > frontier.finalized {
                        if event.block_height > frontier.latest.saturating_add(MAX_NON_FINAL_BLOCKS)
                        {
                            self.write_event_log(log, state, batch)?;
                            return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                                "Dusk archive row {} of {}/{} is in block {}, above the node's tip {}: the endpoint does not serve its archive as Rusk does, and the event log stops before it",
                                event.id,
                                hex::encode(contract_id),
                                log.topic,
                                event.block_height,
                                frontier.latest
                            )));
                        }
                        // Finalized since `frontier` was read: the next call
                        // takes it.
                        passed_final = true;
                        break;
                    }
                    open = Some(OpenBlock::new(event.block_height));
                }
                if let Some(block) = &mut open {
                    if let Err(error) = block.push(log, &event) {
                        self.write_event_log(log, state, batch)?;
                        return Err(error);
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
                    if let Err(error) = self
                        .take_event_log_block(log, state, block, frontier, &mut batch)
                        .await
                    {
                        self.write_event_log(log, state, batch)?;
                        return Err(error);
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

    /// Add a block read whole to the log: its rows of the topic, numbered on
    /// from the log's count. The block must be at or below the node's last
    /// final block and `checkBlock(onlyFinalized)` must accept it, whether or
    /// not it has rows of the topic: otherwise the log stops before it.
    async fn take_event_log_block(
        &self,
        log: &EventLog,
        state: &mut LogState,
        block: OpenBlock,
        frontier: Frontier,
        batch: &mut WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        let height = block.height;
        if height > frontier.finalized {
            return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                "Dusk archive serves finalized {}/{} rows of block {height}, above the node's last final block {}; the event log stops before it",
                hex::encode(log.contract_id),
                log.topic,
                frontier.finalized
            )));
        }
        if !self
            .check_finalized_block(height, &hex::encode(block.hash))
            .await?
        {
            return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                "Dusk archive serves finalized {}/{} rows of block {height} ({:?}), which checkBlock(onlyFinalized) rejects; the event log stops before the block",
                hex::encode(log.contract_id),
                log.topic,
                block.hash
            )));
        }
        for row in block.topic {
            let sequence = state.count;
            batch.put(
                event_log_entry_key(log, sequence),
                encode_entry(height, block.hash, &row.origin, &row.data),
            );
            batch.put(event_log_lookup_key(log, &row.key, sequence), []);
            state.count = sequence.checked_add(1).ok_or_else(|| {
                HyperlaneDuskError::Other("Dusk event log row count overflows".into())
            })?;
        }
        if block.was_staged {
            self.unstage(log, batch)?;
        }
        state.last_block = Some(height);
        Ok(())
    }

    /// Stage `block`, which the pass read up to `row`, the contract's `rank`th
    /// row: its rows of the topic not staged yet, and the position after
    /// `row`.
    fn stage(
        &self,
        log: &EventLog,
        block: &mut OpenBlock,
        row: &FinalizedContractEvent,
        rank: u64,
        batch: &mut WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        let Some(position) = ScanAnchor::after_row(row, block.height.saturating_add(1), rank)?
        else {
            return Ok(());
        };
        for (index, topic) in block.topic.iter().enumerate().skip(block.staged) {
            let mut value = topic.origin.to_vec();
            value.extend_from_slice(&topic.data);
            batch.put(event_log_staged_row_key(log, index as u64), value);
        }
        block.staged = block.topic.len();
        let staged = StagedBlock {
            height: block.height,
            hash: block.hash,
            rows: block.rows,
            topic_rows: block.topic.len() as u64,
            topic_bytes: block.topic_bytes as u64,
            position,
        };
        batch.put(event_log_staged_key(log), staged.encode());
        Ok(())
    }

    /// The block an earlier call staged above the log's last block, if any.
    /// A staged block that does not decode, or that the log holds already,
    /// is dropped with its rows, and its rows are read again.
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
        if let Some(staged) = StagedBlock::decode(&bytes)
            .filter(|staged| state.last_block.is_none_or(|last| staged.height > last))
        {
            return Ok(Some(staged));
        }
        let mut batch = WriteBatch::default();
        self.unstage(log, &mut batch)?;
        self.write_event_log(log, state, batch)?;
        Ok(None)
    }

    /// The block `staged` holds, with its rows of the topic, to read on.
    fn reopen(
        &self,
        log: &EventLog,
        staged: &StagedBlock,
    ) -> Result<OpenBlock, HyperlaneDuskError> {
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
        Ok(OpenBlock {
            height: staged.height,
            hash: staged.hash,
            rows: staged.rows,
            staged: topic.len(),
            topic,
            topic_bytes,
            was_staged: true,
        })
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
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read the Dusk {} event log: {error}",
                    log.topic
                ))
            })?
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

    /// The log's rows `first` to `last`, those it has.
    pub(crate) fn event_log_entries(
        &self,
        log: &EventLog,
        first: u64,
        last: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let mut entries = Vec::new();
        for sequence in first..=last {
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
            .map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read the Dusk {} event log: {error}",
                    log.topic
                ))
            })?
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

    /// The rows that name `key`, in log order, at most `MAX_ROWS_PER_KEY`.
    pub(crate) fn event_log_find(
        &self,
        log: &EventLog,
        key: &[u8],
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let store = self.event_store()?;
        let prefix = event_log_lookup_prefix(log, key);
        let mut entries = Vec::new();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (stored, _) = item.map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read the Dusk {} event log: {error}",
                    log.topic
                ))
            })?;
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
            if let Some(entry) = self.event_log_entry(log, sequence)? {
                entries.push(entry);
            }
        }
        Ok(entries)
    }

    /// The log's rows of block `height`. The log is in block order, so a
    /// binary search finds the first.
    pub(crate) fn event_log_block(
        &self,
        log: &EventLog,
        height: u64,
    ) -> Result<Vec<LogEntry>, HyperlaneDuskError> {
        let count = self.event_log_state(log)?.count;
        let (mut low, mut high) = (0u64, count);
        while low < high {
            let middle = low + (high - low) / 2;
            let below = self
                .event_log_entry(log, middle)?
                .is_some_and(|entry| entry.block_height < height);
            if below {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let mut entries = Vec::new();
        let mut sequence = low;
        while let Some(entry) = self.event_log_entry(log, sequence)? {
            if entry.block_height != height {
                break;
            }
            entries.push(entry);
            sequence += 1;
        }
        Ok(entries)
    }

    /// Drop every row of the log, which then reads again from the
    /// contract's first row: the endpoint no longer agrees with it.
    pub(crate) async fn reset_event_log(&self, log: &EventLog) -> Result<(), HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        let store = self.event_store()?;
        let prefix = event_log_prefix(log);
        let mut batch = WriteBatch::default();
        for item in store.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
            let (key, _) = item.map_err(|error| {
                HyperlaneDuskError::Other(format!(
                    "Failed to read the Dusk {} event log: {error}",
                    log.topic
                ))
            })?;
            if !key.starts_with(&prefix) {
                break;
            }
            batch.delete(key);
        }
        let mut write_options = WriteOptions::default();
        write_options.set_sync(true);
        store.write_opt(batch, &write_options).map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to drop the Dusk {} event log: {error}",
                log.topic
            ))
        })
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
                "checkBlock(onlyFinalized) no longer accepts block {} ({:?}) of the {} event log, which is dropped",
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
    let mut bytes = Vec::with_capacity(26);
    bytes.extend_from_slice(&state.count.to_be_bytes());
    for height in [state.last_block, state.complete_through] {
        bytes.push(u8::from(height.is_some()));
        bytes.extend_from_slice(&height.unwrap_or_default().to_be_bytes());
    }
    bytes
}

fn decode_state(bytes: &[u8]) -> Option<LogState> {
    if bytes.len() != 26 {
        return None;
    }
    let height = |offset: usize| -> Option<Option<u64>> {
        let value = u64::from_be_bytes(bytes.get(offset + 1..offset + 9)?.try_into().ok()?);
        match bytes.get(offset)? {
            0 => Some(None),
            1 => Some(Some(value)),
            _ => None,
        }
    };
    Some(LogState {
        count: u64::from_be_bytes(bytes.get(..8)?.try_into().ok()?),
        last_block: height(8)?,
        complete_through: height(17)?,
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
            },
            LogState {
                count: 0,
                last_block: None,
                complete_through: Some(0),
            },
        ] {
            assert_eq!(decode_state(&encode_state(&state)), Some(state));
        }
        let mut corrupt = encode_state(&LogState::default());
        corrupt[8] = 2;
        assert!(decode_state(&corrupt).is_none());
        assert!(decode_state(&corrupt[1..]).is_none());
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
