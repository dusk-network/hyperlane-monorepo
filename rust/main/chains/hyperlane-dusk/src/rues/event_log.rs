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
//! it lacks, and takes a block's rows only once it has read all of them and
//! `checkBlock(onlyFinalized)` accepted their block hash. Rusk archives a
//! finalized block's rows in one transaction, and blocks in height order, so
//! a block's rows end at a row of a later block or at the end of the archive.
//! A log never takes rows of a block it already holds, so its numbering does
//! not change when the endpoint's archive numbers its rows differently.

use super::*;

/// Most rows one key may name in a lookup.
const MAX_ROWS_PER_KEY: usize = 64;
/// Bytes of an encoded row before its data: height, block hash, origin.
const ENTRY_HEADER_BYTES: usize = 8 + 32 + 32;

/// The lock that serializes each log's advances, by contract and topic.
pub(super) type EventLogLocks = HashMap<([u8; 32], &'static str), Arc<tokio::sync::Mutex<()>>>;

/// A finalized event log: a contract, a topic, and the lookup key a row's
/// data names. `key_of` returns `None` for data that is not a well-formed
/// event of the topic; such a row stops the log, which never leaves a
/// finalized row out.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EventLog {
    pub contract_id: [u8; 32],
    pub topic: &'static str,
    pub key_of: fn(&[u8]) -> Option<Vec<u8>>,
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
    /// finalized it before the log read to the archive's end.
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
    /// It read to the end of the archive (`reached_end`), or stopped at a
    /// block boundary once its pages were used.
    Done { reached_end: bool },
    /// The start position does not belong to this archive.
    WrongAnchor,
    /// The archive has not reached the start position yet.
    ArchiveBehind,
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

    /// Read the archive rows of `log`'s contract after the last block the log
    /// holds, and add the rows of its topic of every block read whole. Reads
    /// at most `max_pages` pages, and then on to the end of the block being
    /// read. With `finality`, a log that reads to the end of the archive,
    /// which had finalized `finality`'s last final block before the read,
    /// holds every row up to that block.
    pub(crate) async fn advance_event_log(
        &self,
        log: &EventLog,
        max_pages: usize,
        finality: Option<&BlockPair>,
    ) -> Result<LogState, HyperlaneDuskError> {
        let lock = self.event_log_lock(log);
        let _guard = lock.lock().await;
        let mut state = self.event_log_state(log)?;
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
        let start = self.scan_anchor_at_or_below(&log.contract_id, target)?;
        let mut outcome = self
            .read_event_log(log, &mut state, start, max_pages)
            .await?;
        if matches!(outcome, LogScan::WrongAnchor) {
            warn!(
                contract = %hex::encode(log.contract_id),
                topic = log.topic,
                "Dusk archive rows were renumbered; the event log reads on from the contract's first row"
            );
            self.forget_scan_anchors(&log.contract_id)?;
            outcome = self
                .read_event_log(log, &mut state, None, max_pages)
                .await?;
        }
        match outcome {
            LogScan::Done { reached_end } => {
                // Blocks up to the last one read whole are complete, and with
                // the archive read to its end, so is every block it had
                // finalized before.
                let archived_final = archived_final.filter(|_| reached_end);
                let through = [state.complete_through, state.last_block, archived_final]
                    .into_iter()
                    .flatten()
                    .max();
                if through != state.complete_through {
                    state.complete_through = through;
                    self.write_event_log(log, &state, WriteBatch::default())?;
                }
            }
            LogScan::ArchiveBehind => debug!(
                contract = %hex::encode(log.contract_id),
                topic = log.topic,
                "Dusk archive has not reached the event log's scan position yet"
            ),
            LogScan::WrongAnchor => {
                return Err(HyperlaneDuskError::Other(
                    "Dusk archive scan without an anchor found a stale anchor".into(),
                ))
            }
        }
        Ok(state)
    }

    /// One pass over the contract's rows from `start`, or from its first
    /// row, adding to `state` every block above its last one that it reads
    /// whole. Saves its progress after every page.
    async fn read_event_log(
        &self,
        log: &EventLog,
        state: &mut LogState,
        start: Option<ScanAnchor>,
        max_pages: usize,
    ) -> Result<LogScan, HyperlaneDuskError> {
        let contract_id = &log.contract_id;
        let start_before = start.map_or(0, |anchor| anchor.before_block);
        let mut rank = start.map_or(0, |anchor| anchor.rank);
        let mut highest_block: Option<u64> = None;
        let mut last_row: Option<FinalizedContractEvent> = None;
        // The cursor just before the fence row, so the first page starts
        // with it.
        let mut previous_id =
            start.and_then(|anchor| anchor.fence_id.checked_sub(1).filter(|id| *id >= 0));
        let mut fence = start;
        let mut size = PageSize::largest();
        let mut pages = 0usize;
        // The block being read, and its rows of the topic so far.
        let mut pending: Option<(u64, Vec<FinalizedContractEvent>)> = None;
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
            pages = pages.saturating_add(1);
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
            let mut stop = false;
            for event in events {
                if let Some((height, _)) = &pending {
                    if event.block_height < *height {
                        let error = HyperlaneDuskError::Other(format!(
                            "Dusk archive row {} of contract {} is in block {}, below block {height} before it",
                            event.id,
                            hex::encode(contract_id),
                            event.block_height
                        ));
                        // Keep the blocks taken before it.
                        self.write_event_log(log, state, batch)?;
                        return Err(error);
                    }
                    if event.block_height > *height {
                        // The block being read ends before this row.
                        if let Some((height, rows)) = pending.take() {
                            if let Err(error) = self
                                .take_event_log_block(log, state, height, rows, &mut batch)
                                .await
                            {
                                // Keep the blocks taken before it.
                                self.write_event_log(log, state, batch)?;
                                return Err(error);
                            }
                        }
                        if pages > max_pages {
                            // The budgeted pages are used: stop before this
                            // row, where the next pass starts.
                            stop = true;
                            break;
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
                if state
                    .last_block
                    .is_none_or(|last| event.block_height > last)
                {
                    let (_, rows) = pending.get_or_insert_with(|| (event.block_height, Vec::new()));
                    if !event.reverted && event.topic == log.topic {
                        rows.push(event.clone());
                    }
                }
                last_row = Some(event);
            }
            // A page of blocks the log already holds ends at a boundary too.
            stop |= pages >= max_pages && pending.is_none() && has_next_page;
            let reached_end = !stop && !has_next_page;
            if reached_end {
                // Rusk archives a finalized block's rows at once, so the
                // archive's last block is whole.
                if let Some((height, rows)) = pending.take() {
                    if let Err(error) = self
                        .take_event_log_block(log, state, height, rows, &mut batch)
                        .await
                    {
                        self.write_event_log(log, state, batch)?;
                        return Err(error);
                    }
                }
            }
            self.write_event_log(log, state, batch)?;
            if stop || reached_end {
                if let (Some(event), Some(highest)) = (&last_row, highest_block) {
                    let before_block = start_before.max(highest.saturating_add(1));
                    if let Some(anchor) = ScanAnchor::after_row(event, before_block, rank)? {
                        self.offer_scan_anchor(contract_id, anchor)?;
                    }
                }
                return Ok(LogScan::Done { reached_end });
            }
        }
    }

    /// Add the rows of one block read whole to the log. Every row must parse
    /// and name a key, all must name one block hash, and checkBlock must
    /// accept it: otherwise the log stops before the block.
    async fn take_event_log_block(
        &self,
        log: &EventLog,
        state: &mut LogState,
        height: u64,
        rows: Vec<FinalizedContractEvent>,
        batch: &mut WriteBatch,
    ) -> Result<(), HyperlaneDuskError> {
        let malformed = |event: &FinalizedContractEvent, reason: String| {
            HyperlaneDuskError::Other(format!(
                "Dusk archive row {} of {}/{} in block {height} is malformed ({reason}); the event log stops before its block until the endpoint serves it correctly",
                event.id,
                hex::encode(log.contract_id),
                log.topic
            ))
        };
        let mut parsed = Vec::with_capacity(rows.len());
        for event in &rows {
            let row =
                BlockRow::parse(event).map_err(|error| malformed(event, error.to_string()))?;
            let key = (log.key_of)(&row.data).ok_or_else(|| {
                malformed(event, format!("not a well-formed {} event", log.topic))
            })?;
            parsed.push((row, key));
        }
        if let Some((first, _)) = parsed.first() {
            let block_hash = first.block_hash;
            if parsed.iter().any(|(row, _)| row.block_hash != block_hash) {
                return Err(HyperlaneDuskError::Other(format!(
                    "Dusk archive rows of {}/{} in block {height} name more than one block hash",
                    hex::encode(log.contract_id),
                    log.topic
                )));
            }
            if !self
                .check_finalized_block(height, &hex::encode(block_hash))
                .await?
            {
                return Err(HyperlaneDuskError::FinalizedEventMismatch(format!(
                    "Dusk archive serves finalized {}/{} rows of block {height} ({block_hash:?}), which checkBlock(onlyFinalized) rejects",
                    hex::encode(log.contract_id),
                    log.topic
                )));
            }
        }
        for (row, key) in parsed {
            let sequence = state.count;
            batch.put(
                event_log_entry_key(log, sequence),
                encode_entry(height, &row),
            );
            batch.put(event_log_lookup_key(log, &key, sequence), []);
            state.count = sequence.checked_add(1).ok_or_else(|| {
                HyperlaneDuskError::Other("Dusk event log row count overflows".into())
            })?;
        }
        state.last_block = Some(height);
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

fn encode_entry(height: u64, row: &BlockRow) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ENTRY_HEADER_BYTES + row.data.len());
    bytes.extend_from_slice(&height.to_be_bytes());
    bytes.extend_from_slice(row.block_hash.as_bytes());
    bytes.extend_from_slice(&row.origin);
    bytes.extend_from_slice(&row.data);
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
    use super::*;

    #[test]
    fn log_rows_and_states_round_trip_and_reject_corruption() {
        let row = BlockRow {
            block_hash: H256::repeat_byte(4),
            origin: [5; 32],
            data: vec![6, 7],
        };
        let entry = decode_entry(3, &encode_entry(9, &row)).unwrap();
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
        assert!(decode_entry(3, &encode_entry(9, &row)[..ENTRY_HEADER_BYTES - 1]).is_none());
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
        };
        let short = event_log_lookup_prefix(&log, &[1, 2]);
        let long = event_log_lookup_key(&log, &[1, 2, 3], 0);
        assert!(!long.starts_with(&short));
    }
}
