//! Archive work per block: the indexers read each block's archive rows once,
//! however many sequences the block holds, and keep their scan position when
//! the archive lags behind finality.
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{Indexer, H256, H512};
use hyperlane_dusk::{DuskMailboxIndexer, DuskMerkleTreeHookIndexer, RuesClient};
use hyperlane_dusk_types::{events, message};
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;

const MAILBOX: [u8; 32] = [2; 32];
const HOOK: [u8; 32] = [5; 32];
const ORIGIN: u32 = 4242;
/// Rusk 1.7.3 clamps `finalizedEvents(limit)` to this many rows
/// (`rusk/src/lib/http/chain/graphql/archive/events.rs`, `MAX_LIMIT`).
const RUSK_MAX_PAGE: usize = 200;
/// The most bytes of a reply the agent reads (`MAX_RUES_RESPONSE_BYTES`).
/// Rusk itself sends a `finalizedEvents` page whole, however large.
const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;

/// One `finalizedEvents` call.
#[derive(Clone, Debug)]
struct PageRequest {
    contract: [u8; 32],
    after: Option<i64>,
    /// The `limit` the request asked for, before Rusk's clamp.
    limit: usize,
    /// Bytes of the reply body.
    reply_len: usize,
}

/// The node: Mailbox and MerkleTreeHook state plus an archive that behaves
/// like Rusk 1.7.3. Row IDs are global across contracts. A finalized block's
/// rows are inserted together, grouped by transaction hash, so archive order
/// within a block is not dispatch order.
#[derive(Default)]
struct Node {
    rows: Vec<Value>,
    /// Per nonce: dispatch height and stored message ID.
    dispatches: Vec<(u64, [u8; 32])>,
    /// Per tree index: insertion height and message ID.
    insertions: Vec<(u64, [u8; 32])>,
    /// Per nonce: the transaction that dispatched it.
    dispatch_tx: Vec<[u8; 32]>,
    /// Blocks whose rows the archive has not inserted yet.
    unarchived: BTreeMap<u64, Vec<Value>>,
    finalized: u64,
    pages: Vec<PageRequest>,
    block_checks: usize,
    /// Bytes in each dispatched message body; one byte when zero.
    body_len: usize,
    /// When set, every `finalizedEvents` reply to a request for `limit` rows
    /// is padded with JSON whitespace to `page_reply_len(limit)` bytes.
    page_reply_len: Option<fn(usize) -> usize>,
}

impl Node {
    /// Finalize a block with `dispatches` successful dispatches and `reverted`
    /// reverted dispatch transactions. Reverted rows stay in the archive.
    fn finalize_block(&mut self, height: u64, dispatches: usize, reverted: usize, archived: bool) {
        // BTreeMap<origin, events> mirrors Rusk's grouping before ID
        // assignment, so rows are in transaction-hash order.
        let mut by_origin: BTreeMap<[u8; 32], Vec<Emitted>> = BTreeMap::new();
        for _ in 0..dispatches {
            let nonce = self.dispatches.len() as u32;
            let body = vec![nonce as u8; self.body_len.max(1)];
            let message = message::encode(3, nonce, ORIGIN, [7; 32], 42, [8; 32], &body);
            let id = message::id(&message);
            let tx = message::keccak256(&[b"tx".as_slice(), &nonce.to_be_bytes()].concat());
            by_origin.insert(
                tx,
                vec![
                    (
                        MAILBOX,
                        events::Dispatch::TOPIC,
                        dispatch_event(message),
                        false,
                    ),
                    (MAILBOX, events::DispatchId::TOPIC, id.to_vec(), false),
                    (
                        HOOK,
                        events::InsertedIntoTree::TOPIC,
                        rkyv::to_bytes::<_, 256>(&events::InsertedIntoTree {
                            message_id: id,
                            index: self.insertions.len() as u32,
                        })
                        .unwrap()
                        .to_vec(),
                        false,
                    ),
                ],
            );
            self.dispatches.push((height, id));
            self.insertions.push((height, id));
            self.dispatch_tx.push(tx);
        }
        for attempt in 0..reverted {
            // A reverted dispatch emitted its events before failing. It used
            // the then-current nonce and left no contract state behind.
            let nonce = self.dispatches.len() as u32;
            let message = message::encode(3, nonce, ORIGIN, [9; 32], 43, [9; 32], &[0xee]);
            let tx = message::keccak256(
                &[b"reverted".as_slice(), &(attempt as u64).to_be_bytes()].concat(),
            );
            by_origin.insert(
                tx,
                vec![
                    (
                        MAILBOX,
                        events::Dispatch::TOPIC,
                        dispatch_event(message.clone()),
                        true,
                    ),
                    (
                        MAILBOX,
                        events::DispatchId::TOPIC,
                        message::id(&message).to_vec(),
                        true,
                    ),
                ],
            );
        }
        let rows: Vec<Value> = by_origin
            .into_iter()
            .flat_map(|(origin, events)| {
                events
                    .into_iter()
                    .map(move |(source, topic, data, reverted)| {
                        json!({
                            "block_height": height,
                            "block_hash": hex::encode(block_hash(height)),
                            "origin": hex::encode(origin),
                            "topic": topic,
                            "source": hex::encode(source),
                            "data": hex::encode(data),
                            "reverted": reverted,
                        })
                    })
            })
            .collect();
        self.finalized = self.finalized.max(height);
        if archived {
            self.archive(rows);
        } else {
            self.unarchived.insert(height, rows);
        }
    }

    /// Let the archive catch up with every finalized block.
    fn catch_up(&mut self) {
        for (_, rows) in std::mem::take(&mut self.unarchived) {
            self.archive(rows);
        }
    }

    fn archive(&mut self, rows: Vec<Value>) {
        for mut row in rows {
            row["id"] = json!(self.rows.len() as i64);
            self.rows.push(row);
        }
    }

    fn contract_rows(&self, contract: [u8; 32]) -> usize {
        let source = hex::encode(contract);
        self.rows
            .iter()
            .filter(|row| row["source"] == source)
            .count()
    }

    fn pages_for(&self, contract: [u8; 32]) -> Vec<Option<i64>> {
        self.pages
            .iter()
            .filter(|request| request.contract == contract)
            .map(|request| request.after)
            .collect()
    }

    fn page_requests(&self, contract: [u8; 32]) -> Vec<PageRequest> {
        self.pages
            .iter()
            .filter(|request| request.contract == contract)
            .cloned()
            .collect()
    }

    /// The reply body to one HTTP request.
    fn reply(&mut self, path: &str, body: &[u8]) -> Vec<u8> {
        if path != "/on/graphql/query" {
            let (contract, method) = path
                .strip_prefix("/on/contracts:")
                .and_then(|rest| rest.split_once('/'))
                .unwrap();
            let contract = hex::decode(contract).unwrap().try_into().unwrap();
            return self.contract_query(contract, method, body);
        }
        let query = std::str::from_utf8(body).unwrap();
        if !query.contains("finalizedEvents(") {
            return serde_json::to_vec(&self.graphql(query)).unwrap();
        }
        let (page, limit) = self.finalized_events(query);
        let mut reply = serde_json::to_vec(&page).unwrap();
        if let Some(len) = self
            .page_reply_len
            .map(|page_reply_len| page_reply_len(limit))
        {
            if reply.len() < len {
                reply.resize(len, b' ');
            }
        }
        if let Some(request) = self.pages.last_mut() {
            request.reply_len = reply.len();
        }
        reply
    }

    fn graphql(&mut self, query: &str) -> Value {
        if query == "query { lastBlockPair { json } }" {
            json!({"lastBlockPair":{"json":{
                "last_block":[self.finalized + 5,"tip"],
                "last_finalized_block":[self.finalized,"final"]
            }}})
        } else if query.contains("checkBlock(") {
            self.block_checks += 1;
            json!({"checkBlock": true})
        } else {
            panic!("unexpected GraphQL query: {query}")
        }
    }

    /// `finalizedEvents` as Rusk 1.7.3 answers it: rows with `id > cursor`
    /// in ID order, `limit` clamped to 1..=200, one extra row fetched to
    /// decide `hasNextPage`. Also returns the requested limit.
    fn finalized_events(&mut self, query: &str) -> (Value, usize) {
        let contract_hex = query
            .split_once("contractId: \"")
            .and_then(|(_, rest)| rest.split('"').next())
            .unwrap();
        let contract: [u8; 32] = hex::decode(contract_hex).unwrap().try_into().unwrap();
        let requested = query
            .split_once("limit: ")
            .map(|(_, rest)| {
                rest.split(|c: char| !c.is_ascii_digit())
                    .next()
                    .unwrap()
                    .parse::<usize>()
                    .unwrap()
            })
            .unwrap_or(50);
        let limit = requested.clamp(1, RUSK_MAX_PAGE);
        let after = query.split_once("cursor: \"").map(|(_, rest)| {
            let cursor = rest.split('"').next().unwrap();
            let text = String::from_utf8(STANDARD.decode(cursor).unwrap()).unwrap();
            text.strip_prefix("v1:").unwrap().parse::<i64>().unwrap()
        });
        self.pages.push(PageRequest {
            contract,
            after,
            limit: requested,
            reply_len: 0,
        });
        let mut page: Vec<Value> = self
            .rows
            .iter()
            .filter(|row| row["source"] == contract_hex)
            .filter(|row| row["id"].as_i64().unwrap() > after.unwrap_or(-1))
            .take(limit + 1)
            .cloned()
            .collect();
        let has_next = page.len() > limit;
        page.truncate(limit);
        let cursor = |row: Option<&Value>| {
            row.map(|row| STANDARD.encode(format!("v1:{}", row["id"].as_i64().unwrap())))
        };
        let page = json!({"finalizedEvents":{"json":{
            "events": page,
            "startCursor": cursor(page.first()),
            "endCursor": cursor(page.last()),
            "hasNextPage": has_next,
        }}});
        (page, requested)
    }

    fn contract_query(&self, contract: [u8; 32], method: &str, body: &[u8]) -> Vec<u8> {
        let index = || u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
        match (contract, method) {
            (MAILBOX, "nonce") => (self.dispatches.len() as u32).to_le_bytes().to_vec(),
            (MAILBOX, "dispatched_block_height") => {
                self.dispatches[index()].0.to_le_bytes().to_vec()
            }
            (MAILBOX, "dispatched_id") => self.dispatches[index()].1.to_vec(),
            (HOOK, "count") => (self.insertions.len() as u32).to_le_bytes().to_vec(),
            (HOOK, "inserted_block_height") => self.insertions[index()].0.to_le_bytes().to_vec(),
            (HOOK, "message_id_at") => self.insertions[index()].1.to_vec(),
            _ => panic!(
                "unexpected contract query {}/{method}",
                hex::encode(contract)
            ),
        }
    }
}

/// One emitted event: source contract, topic, data and reverted flag.
type Emitted = ([u8; 32], &'static str, Vec<u8>, bool);

fn dispatch_event(message: Vec<u8>) -> Vec<u8> {
    rkyv::to_bytes::<_, 256>(&events::Dispatch {
        sender: [7; 32],
        destination: 42,
        recipient: [8; 32],
        message,
    })
    .unwrap()
    .to_vec()
}

fn block_hash(height: u64) -> [u8; 32] {
    message::keccak256(&height.to_be_bytes())
}

fn h512(hash: [u8; 32]) -> H512 {
    let mut out = [0; 64];
    out[32..].copy_from_slice(&hash);
    H512::from(out)
}

struct Fixture {
    url: Url,
    store: TempDir,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Fixture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let node = Arc::new(Mutex::new(Node::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let (worker_node, worker_stop) = (node.clone(), stop.clone());
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1));
                        continue;
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                // A client that refuses a large reply may stop reading before
                // it closes the connection.
                stream
                    .set_write_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                let mut length = 0;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let reply = worker_node.lock().unwrap().reply(&path, &body);
                // The agent hangs up on a reply whose Content-Length is over
                // its limit without reading the body, so a failed write is
                // expected here.
                let _ = write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                );
                let _ = stream.write_all(&reply);
            }
        });
        Self {
            url,
            store: tempfile::tempdir().unwrap(),
            node,
            stop,
            worker: Some(worker),
        }
    }

    fn client(&self) -> Arc<RuesClient> {
        Arc::new(
            RuesClient::new_with_event_cursor_dir(
                self.url.clone(),
                self.store.path().to_path_buf(),
            )
            .unwrap(),
        )
    }

    fn node(&self) -> std::sync::MutexGuard<'_, Node> {
        self.node.lock().unwrap()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Err(panic) = self.worker.take().unwrap().join() {
            if !thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

/// Index every dispatch and tree insertion of one block of `dispatches`
/// dispatches (plus `reverted` reverted ones) and return the archive pages
/// each indexer fetched and the `checkBlock` calls made.
async fn index_one_block(dispatches: usize, reverted: usize) -> (usize, usize, usize) {
    let fixture = Fixture::start();
    fixture
        .node()
        .finalize_block(100, dispatches, reverted, true);
    let rues = fixture.client();
    let last = dispatches as u32 - 1;

    let messages = DuskMailboxIndexer::new(rues.clone(), H256::from(MAILBOX), ORIGIN)
        .fetch_logs_in_range(0..=last)
        .await
        .unwrap();
    let insertions = DuskMerkleTreeHookIndexer::new(rues, H256::from(HOOK))
        .fetch_logs_in_range(0..=last)
        .await
        .unwrap();

    let node = fixture.node();
    assert_eq!(messages.len(), dispatches);
    assert_eq!(insertions.len(), dispatches);
    for (nonce, (message, meta)) in messages.iter().enumerate() {
        assert_eq!(message.inner().nonce, nonce as u32);
        assert_eq!(message.inner().id(), H256::from(node.dispatches[nonce].1));
        assert_eq!(meta.transaction_id, h512(node.dispatch_tx[nonce]));
        assert_eq!(meta.block_number, 100);
    }
    for (index, (insertion, meta)) in insertions.iter().enumerate() {
        assert_eq!(insertion.inner().index(), index as u32);
        assert_eq!(meta.transaction_id, h512(node.dispatch_tx[index]));
    }

    let mailbox_pages = node.pages_for(MAILBOX).len();
    let hook_pages = node.pages_for(HOOK).len();
    let mailbox_rows = node.contract_rows(MAILBOX);
    let hook_rows = node.contract_rows(HOOK);
    eprintln!(
        "dispatches={dispatches} reverted={reverted}: mailbox rows={mailbox_rows} pages={mailbox_pages}; \
         hook rows={hook_rows} pages={hook_pages}; checkBlock calls={}",
        node.block_checks
    );
    // One pass over the block's rows, plus at most one page of slack.
    assert!(
        mailbox_pages <= mailbox_rows.div_ceil(RUSK_MAX_PAGE) + 1,
        "{mailbox_pages} Mailbox pages for {mailbox_rows} rows"
    );
    assert!(
        hook_pages <= hook_rows.div_ceil(RUSK_MAX_PAGE) + 1,
        "{hook_pages} hook pages for {hook_rows} rows"
    );
    (mailbox_pages, hook_pages, node.block_checks)
}

#[tokio::test]
async fn one_block_of_dispatches_is_read_in_one_pass() {
    for dispatches in [16, 64, 256] {
        index_one_block(dispatches, 0).await;
    }
}

#[tokio::test]
async fn reverted_dispatch_rows_do_not_multiply_the_pass() {
    index_one_block(256, 256).await;
}

#[tokio::test]
async fn a_block_check_covers_every_row_of_its_block() {
    let (_, _, block_checks) = index_one_block(64, 0).await;
    // One for the Mailbox's block and one for the hook's.
    assert_eq!(block_checks, 2);
}

#[tokio::test]
async fn an_archive_that_lags_finality_keeps_the_scan_position() {
    let fixture = Fixture::start();
    fixture.node().finalize_block(50, 2, 0, true);
    fixture.node().finalize_block(100, 3, 0, true);
    let rues = fixture.client();
    let indexer = DuskMailboxIndexer::new(rues, H256::from(MAILBOX), ORIGIN);
    assert_eq!(indexer.fetch_logs_in_range(0..=4).await.unwrap().len(), 5);

    // The node finalizes block 200 before its archive inserts the rows, and
    // the indexer polls in between.
    fixture.node().finalize_block(200, 3, 0, false);
    let error = indexer.fetch_logs_in_range(5..=7).await.unwrap_err();
    assert!(error.to_string().contains("not archived yet"), "{error}");
    let before_retry = fixture.node().pages_for(MAILBOX).len();

    fixture.node().catch_up();
    let logs = indexer.fetch_logs_in_range(5..=7).await.unwrap();
    assert_eq!(logs.len(), 3);
    for (offset, (message, meta)) in logs.iter().enumerate() {
        assert_eq!(message.inner().nonce, 5 + offset as u32);
        assert_eq!(meta.block_number, 200);
    }

    // The retry resumes where the miss stopped instead of rereading the
    // contract's history from its first row.
    let pages = fixture.node().pages_for(MAILBOX);
    assert!(
        pages[before_retry..].iter().all(Option::is_some),
        "a lagging archive must not restart the scan from genesis: {pages:?}"
    );
}

/// Whether the agent read a `finalizedEvents` reply, rather than refusing it
/// for being over its limit.
fn read_whole(request: &PageRequest) -> bool {
    request.reply_len <= RESPONSE_LIMIT
}

#[tokio::test]
async fn large_dispatches_across_blocks_are_read_in_pages_that_fit() {
    const BODY: usize = 48 * 1024;
    let fixture = Fixture::start();
    {
        let mut node = fixture.node();
        node.body_len = BODY;
        for height in [100, 101, 102] {
            node.finalize_block(height, 45, 0, true);
        }
    }
    let rues = fixture.client();
    let messages = DuskMailboxIndexer::new(rues.clone(), H256::from(MAILBOX), ORIGIN)
        .fetch_logs_in_range(0..=134)
        .await
        .unwrap();
    let insertions = DuskMerkleTreeHookIndexer::new(rues, H256::from(HOOK))
        .fetch_logs_in_range(0..=134)
        .await
        .unwrap();

    let node = fixture.node();
    assert_eq!(messages.len(), 135);
    assert_eq!(insertions.len(), 135);
    for (nonce, (message, meta)) in messages.iter().enumerate() {
        assert_eq!(message.inner().nonce, nonce as u32);
        assert_eq!(message.inner().body, vec![nonce as u8; BODY]);
        assert_eq!(message.inner().id(), H256::from(node.dispatches[nonce].1));
        assert_eq!(meta.transaction_id, h512(node.dispatch_tx[nonce]));
        assert_eq!(meta.block_number, 100 + nonce as u64 / 45);
    }

    // A block's 45 Dispatch rows are over the limit, so 200 and 100 rows are
    // too; 50 rows fit. A refused reply is asked for again at the same
    // cursor with half the rows.
    let requests = node.page_requests(MAILBOX);
    for (request, next) in requests.iter().zip(requests.iter().skip(1)) {
        if !read_whole(request) {
            assert_eq!(
                (next.after, next.limit),
                (request.after, request.limit / 2),
                "{requests:?}"
            );
        }
    }
    let read: Vec<_> = requests
        .iter()
        .filter(|request| read_whole(request))
        .collect();
    assert!(
        read.iter().all(|request| request.limit == 50),
        "{requests:?}"
    );
    let refused = requests.len() - read.len();
    eprintln!(
        "mailbox rows={} requests={} (refused {refused}, read {}); hook requests={:?}",
        node.contract_rows(MAILBOX),
        requests.len(),
        read.len(),
        node.page_requests(HOOK)
            .iter()
            .map(|request| request.limit)
            .collect::<Vec<_>>()
    );
    // Three scans, each refused at 200 and 100 rows before reading 50.
    assert_eq!(refused, 6, "{requests:?}");
    // The hook's small rows still come 200 at a time.
    assert!(node
        .page_requests(HOOK)
        .iter()
        .all(|request| request.limit == RUSK_MAX_PAGE && read_whole(request)));
}

#[tokio::test]
async fn a_reply_of_exactly_the_limit_is_read_and_one_byte_more_is_split() {
    let fixture = Fixture::start();
    {
        let mut node = fixture.node();
        node.finalize_block(100, 3, 0, true);
        node.page_reply_len = Some(|limit| {
            if limit == RUSK_MAX_PAGE {
                RESPONSE_LIMIT + 1
            } else {
                RESPONSE_LIMIT
            }
        });
    }
    let logs = DuskMailboxIndexer::new(fixture.client(), H256::from(MAILBOX), ORIGIN)
        .fetch_logs_in_range(0..=2)
        .await
        .unwrap();
    assert_eq!(logs.len(), 3);
    let requests: Vec<_> = fixture
        .node()
        .page_requests(MAILBOX)
        .iter()
        .map(|request| (request.after, request.limit, request.reply_len))
        .collect();
    assert_eq!(
        requests,
        vec![
            (None, RUSK_MAX_PAGE, RESPONSE_LIMIT + 1),
            (None, RUSK_MAX_PAGE / 2, RESPONSE_LIMIT),
        ]
    );
}

#[tokio::test]
async fn a_row_larger_than_the_limit_fails_with_a_short_error() {
    let fixture = Fixture::start();
    {
        let mut node = fixture.node();
        // The row's data is hex, so this body alone is over the limit. A Rusk
        // event carries at most 64 KiB of data, so only a faulty endpoint
        // serves such a row.
        node.body_len = RESPONSE_LIMIT / 2 + 1;
        node.finalize_block(100, 1, 0, true);
    }
    let indexer = DuskMailboxIndexer::new(fixture.client(), H256::from(MAILBOX), ORIGIN);
    for attempt in 1..=2 {
        let error = indexer
            .fetch_logs_in_range(0..=0)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not fit"), "{error}");
        assert!(error.contains(&RESPONSE_LIMIT.to_string()), "{error}");
        assert!(error.len() < 1024, "{} bytes: {error}", error.len());
        // Each attempt halves from 200 rows to one at the same cursor, then
        // gives up until contract sync retries.
        let requests = fixture.node().page_requests(MAILBOX);
        assert!(requests.iter().all(|request| request.after.is_none()));
        assert_eq!(
            requests
                .iter()
                .map(|request| request.limit)
                .collect::<Vec<_>>(),
            [200, 100, 50, 25, 12, 6, 3, 1].repeat(attempt)
        );
    }
}
