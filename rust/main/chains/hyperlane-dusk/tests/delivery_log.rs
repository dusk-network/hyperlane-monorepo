//! Deliveries indexed from the Mailbox's finalized `ProcessId` events,
//! through the public delivery indexer and a scripted Rusk archive.
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{Indexer, SequenceAwareIndexer, H256, H512, U256};
use hyperlane_dusk::{DuskDeliveryIndexer, RuesClient};
use hyperlane_dusk_types::events;
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;

const MAILBOX: [u8; 32] = [2; 32];
const RUSK_MAX_PAGE: usize = 200;

fn block_hash(height: u64) -> [u8; 32] {
    hyperlane_dusk_types::message::keccak256(&height.to_be_bytes())
}

fn message(n: u8) -> [u8; 32] {
    [n; 32]
}

fn process_id(message_id: [u8; 32]) -> Vec<u8> {
    rkyv::to_bytes::<_, 256>(&events::ProcessId { message_id })
        .unwrap()
        .to_vec()
}

/// One archive row of the Mailbox.
#[derive(Clone, Debug)]
struct Row {
    origin: [u8; 32],
    topic: &'static str,
    data: Vec<u8>,
    reverted: bool,
}

impl Row {
    fn delivery(origin: u8, message_id: [u8; 32]) -> Self {
        Self {
            origin: [origin; 32],
            topic: events::ProcessId::TOPIC,
            data: process_id(message_id),
            reverted: false,
        }
    }

    fn other(origin: u8, topic: &'static str) -> Self {
        Self {
            origin: [origin; 32],
            topic,
            data: vec![1, 2, 3],
            reverted: false,
        }
    }
}

/// A Rusk node: its finality, and an archive of the Mailbox's rows that
/// numbers rows in insertion order and inserts a block's rows at once.
#[derive(Default)]
struct Node {
    /// Each archived row: its ID, block and the row.
    rows: Vec<(i64, u64, Row)>,
    next_id: i64,
    /// Blocks the archive has finalized.
    archived: BTreeMap<u64, [u8; 32]>,
    /// The node's last final block.
    finalized: u64,
    /// Blocks checkBlock rejects although the archive holds them.
    rejected: Vec<u64>,
    /// The cursor of each finalizedEvents request.
    pages: Vec<Option<i64>>,
    /// Each transaction's block.
    transactions: HashMap<String, u64>,
}

impl Node {
    /// The node finalizes `height` and archives its rows.
    fn finalize(&mut self, height: u64, rows: Vec<Row>) {
        for row in rows {
            self.transactions.insert(hex::encode(row.origin), height);
            self.rows.push((self.next_id, height, row));
            self.next_id += 1;
        }
        self.archived.insert(height, block_hash(height));
        self.finalized = self.finalized.max(height);
    }

    fn reply(&mut self, path: &str, body: &[u8]) -> Vec<u8> {
        assert_eq!(path, "/on/graphql/query", "unexpected request {path}");
        let query = std::str::from_utf8(body).unwrap();
        let value = if query == "query { lastBlockPair { json } }" {
            let block = json!([self.finalized, hex::encode(block_hash(self.finalized))]);
            json!({"lastBlockPair": {"json": {"last_block": block, "last_finalized_block": block}}})
        } else if query.contains("checkBlock(") {
            let height: u64 = query
                .split_once("height: ")
                .and_then(|(_, rest)| rest.split(',').next())
                .unwrap()
                .parse()
                .unwrap();
            let hash = query
                .split_once("hash: \"")
                .and_then(|(_, rest)| rest.split('"').next())
                .unwrap();
            let finalized = !self.rejected.contains(&height)
                && self
                    .archived
                    .get(&height)
                    .is_some_and(|archived| hex::encode(archived) == hash);
            json!({ "checkBlock": finalized })
        } else if let Some(rest) = query.strip_prefix("query { block(height: ") {
            let height: u64 = rest.split(')').next().unwrap().parse().unwrap();
            json!({"block": {"header": {"hash": hex::encode(block_hash(height))}}})
        } else if let Some((_, rest)) = query.split_once("tx(hash: \"") {
            let hash = rest.split('"').next().unwrap();
            match self.transactions.get(hash) {
                Some(height) => json!({"tx": {"blockHeight": height}}),
                None => json!({ "tx": null }),
            }
        } else if query.contains("finalizedEvents(") {
            assert!(query.contains(&hex::encode(MAILBOX)));
            let limit = query
                .split_once("limit: ")
                .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
                .map_or(50, |limit| limit.parse::<usize>().unwrap())
                .clamp(1, RUSK_MAX_PAGE);
            let after = query.split_once("cursor: \"").map(|(_, rest)| {
                let cursor = rest.split('"').next().unwrap();
                let text = String::from_utf8(STANDARD.decode(cursor).unwrap()).unwrap();
                text.strip_prefix("v1:").unwrap().parse::<i64>().unwrap()
            });
            self.pages.push(after);
            let mut page: Vec<Value> = self
                .rows
                .iter()
                .filter(|(id, _, _)| *id > after.unwrap_or(-1))
                .take(limit + 1)
                .map(|(id, height, row)| {
                    json!({
                        "id": id, "block_height": height,
                        "block_hash": hex::encode(block_hash(*height)),
                        "origin": hex::encode(row.origin), "topic": row.topic,
                        "source": hex::encode(MAILBOX), "data": hex::encode(&row.data),
                        "reverted": row.reverted,
                    })
                })
                .collect();
            let has_next = page.len() > limit;
            page.truncate(limit);
            let cursor =
                |row: Option<&Value>| row.map(|row| STANDARD.encode(format!("v1:{}", row["id"])));
            json!({"finalizedEvents": {"json": {
                "events": page,
                "startCursor": cursor(page.first()),
                "endCursor": cursor(page.last()),
                "hasNextPage": has_next,
            }}})
        } else {
            panic!("unexpected GraphQL query {query}")
        };
        value.to_string().into_bytes()
    }
}

struct Fixture {
    url: Url,
    node: Arc<Mutex<Node>>,
    directory: TempDir,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Fixture {
    fn start(node: Node) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let node = Arc::new(Mutex::new(node));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let node = node.clone();
            let stop = stop.clone();
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    let mut stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(error) => panic!("fixture accept failed: {error}"),
                    };
                    stream.set_nonblocking(false).unwrap();
                    stream
                        .set_read_timeout(Some(Duration::from_secs(3)))
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
                        if let Some(value) =
                            header.to_ascii_lowercase().strip_prefix("content-length:")
                        {
                            length = value.trim().parse().unwrap();
                        }
                    }
                    let mut body = vec![0; length];
                    reader.read_exact(&mut body).unwrap();
                    let reply = node.lock().unwrap().reply(&path, &body);
                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        reply.len()
                    )
                    .unwrap();
                    stream.write_all(&reply).unwrap();
                }
            })
        };
        Self {
            url,
            node,
            directory: tempfile::tempdir().unwrap(),
            stop,
            worker: Some(worker),
        }
    }

    fn node(&self) -> std::sync::MutexGuard<'_, Node> {
        self.node.lock().unwrap()
    }

    /// A delivery indexer over a client that owns the fixture's event store,
    /// as after a restart when called again.
    fn indexer(&self) -> DuskDeliveryIndexer {
        let rues = Arc::new(
            RuesClient::new_with_event_cursor_dir(
                self.url.clone(),
                self.directory.path().to_path_buf(),
            )
            .unwrap(),
        );
        DuskDeliveryIndexer::new(rues, H256::from(MAILBOX))
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

fn transaction(origin: u8) -> H512 {
    let mut id = [0u8; 64];
    id[32..].copy_from_slice(&[origin; 32]);
    H512::from(id)
}

/// The message IDs and transaction origins of every delivery the indexer
/// reports, with their sequences.
async fn all_deliveries(indexer: &DuskDeliveryIndexer) -> Vec<(u32, [u8; 32], u8, u64)> {
    let (count, _) = indexer.latest_sequence_count_and_tip().await.unwrap();
    let count = count.unwrap();
    if count == 0 {
        return Vec::new();
    }
    indexer
        .fetch_logs_in_range(0..=count - 1)
        .await
        .unwrap()
        .into_iter()
        .map(|(indexed, meta)| {
            assert_eq!(meta.log_index, U256::from(indexed.sequence.unwrap()));
            assert_eq!(meta.address, H256::from(MAILBOX));
            assert_eq!(meta.block_hash, H256::from(block_hash(meta.block_number)));
            assert_eq!(&meta.transaction_id.as_bytes()[..32], &[0u8; 32]);
            (
                indexed.sequence.unwrap(),
                indexed.inner().0,
                meta.transaction_id.as_bytes()[32],
                meta.block_number,
            )
        })
        .collect()
}

#[tokio::test]
async fn deliveries_are_numbered_in_archive_order_from_finalized_process_id_rows() {
    let mut node = Node::default();
    node.finalize(
        10,
        vec![
            Row::other(1, events::Dispatch::TOPIC),
            Row::other(2, events::Process::TOPIC),
            Row::delivery(2, message(0xa)),
            // A delivery attempt that failed inside another transaction.
            Row {
                reverted: true,
                ..Row::delivery(3, message(0xe))
            },
        ],
    );
    node.finalize(11, vec![Row::other(4, events::Dispatch::TOPIC)]);
    // Rusk orders a block's rows by transaction hash.
    node.finalize(
        12,
        vec![
            Row::delivery(5, message(0xb)),
            Row::delivery(6, message(0xc)),
        ],
    );
    let fixture = Fixture::start(node);
    let indexer = fixture.indexer();
    assert_eq!(
        all_deliveries(&indexer).await,
        [
            (0, message(0xa), 2, 10),
            (1, message(0xb), 5, 12),
            (2, message(0xc), 6, 12)
        ]
    );
    // The archive has finalized the node's last final block, so every
    // delivery up to it is counted.
    assert_eq!(
        indexer.latest_sequence_count_and_tip().await.unwrap(),
        (Some(3), 12)
    );
    // A transaction's deliveries, from its block.
    let logs = indexer.fetch_logs_by_tx_hash(transaction(6)).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].0.inner().0, message(0xc));
    assert_eq!(logs[0].0.sequence, Some(2));
    assert!(indexer
        .fetch_logs_by_tx_hash(transaction(4))
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn the_count_reaches_the_last_final_block_only_once_the_archive_has_finalized_it() {
    let mut node = Node::default();
    node.finalize(10, vec![Row::delivery(1, message(1))]);
    // The node finalizes block 20 before its archive inserts it.
    node.finalized = 20;
    let fixture = Fixture::start(node);
    let indexer = fixture.indexer();
    assert_eq!(
        indexer.latest_sequence_count_and_tip().await.unwrap(),
        (Some(1), 10)
    );
    fixture
        .node()
        .finalize(20, vec![Row::delivery(2, message(2))]);
    assert_eq!(
        indexer.latest_sequence_count_and_tip().await.unwrap(),
        (Some(2), 20)
    );
    // Later final blocks without deliveries move the tip on.
    fixture.node().finalize(30, vec![]);
    assert_eq!(
        indexer.latest_sequence_count_and_tip().await.unwrap(),
        (Some(2), 30)
    );
}

#[tokio::test]
async fn a_long_history_is_read_a_bounded_number_of_pages_at_a_time() {
    let mut node = Node::default();
    // 40 blocks of 100 rows: 20 pages of 200 rows.
    for block in 0..40u64 {
        let rows = (0..100u8)
            .map(|row| {
                if row % 10 == 0 {
                    Row::delivery(row, message(block as u8))
                } else {
                    Row::other(row, events::Dispatch::TOPIC)
                }
            })
            .collect();
        node.finalize(100 + block, rows);
    }
    let fixture = Fixture::start(node);
    let indexer = fixture.indexer();
    let (first, tip) = indexer.latest_sequence_count_and_tip().await.unwrap();
    // 16 pages hold 32 blocks. The log takes a block once it reads a row of
    // the next, one page more, and stops at that boundary.
    assert_eq!((first, tip), (Some(320), 131));
    assert_eq!(fixture.node().pages.len(), 17);
    let (second, tip) = indexer.latest_sequence_count_and_tip().await.unwrap();
    assert_eq!((second, tip), (Some(400), 139));
    // The second call started at the last row of block 131.
    let pages = fixture.node().pages.clone();
    assert_eq!(pages[17], Some(3198));
}

#[tokio::test]
async fn the_log_keeps_its_numbering_across_restarts_and_renumbered_archives() {
    let mut node = Node::default();
    node.finalize(10, vec![Row::delivery(1, message(1))]);
    node.finalize(11, vec![Row::delivery(2, message(2))]);
    let fixture = Fixture::start(node);
    assert_eq!(all_deliveries(&fixture.indexer()).await.len(), 2);

    // The endpoint now serves another node's archive of the same blocks,
    // numbered from 100, and block 12 has finalized since.
    {
        let mut node = fixture.node();
        for (id, _, _) in &mut node.rows {
            *id += 100;
        }
        node.next_id += 100;
        node.finalize(12, vec![Row::delivery(3, message(3))]);
    }
    // A restarted indexer reads on from block 12 without numbering blocks
    // 10 and 11 again.
    assert_eq!(
        all_deliveries(&fixture.indexer()).await,
        [
            (0, message(1), 1, 10),
            (1, message(2), 2, 11),
            (2, message(3), 3, 12)
        ]
    );
}

#[tokio::test]
async fn a_block_the_endpoint_no_longer_reports_final_drops_the_log() {
    let mut node = Node::default();
    node.finalize(10, vec![Row::delivery(1, message(1))]);
    let fixture = Fixture::start(node);
    let indexer = fixture.indexer();
    assert_eq!(all_deliveries(&indexer).await.len(), 1);
    fixture.node().rejected.push(10);
    let error = indexer.fetch_logs_in_range(0..=0).await.unwrap_err();
    assert!(error.to_string().contains("no longer accepts"), "{error}");
    // The log was dropped; once the endpoint agrees again it is rebuilt.
    fixture.node().rejected.clear();
    assert_eq!(all_deliveries(&indexer).await, [(0, message(1), 1, 10)]);
}

#[tokio::test]
async fn a_malformed_delivery_row_stops_the_log_before_its_block() {
    let mut node = Node::default();
    node.finalize(10, vec![Row::delivery(1, message(1))]);
    node.finalize(
        11,
        vec![Row {
            data: vec![0xff; 5],
            ..Row::delivery(2, message(2))
        }],
    );
    let fixture = Fixture::start(node);
    let indexer = fixture.indexer();
    let error = indexer.latest_sequence_count_and_tip().await.unwrap_err();
    assert!(error.to_string().contains("malformed"), "{error}");
    // Block 10, read before it, was kept.
    let entries = indexer.fetch_logs_in_range(0..=0).await.unwrap();
    assert_eq!(entries.len(), 1);
    // Once the endpoint serves the row correctly, the log reads on.
    fixture.node().rows[1].2.data = process_id(message(2));
    assert_eq!(
        indexer.latest_sequence_count_and_tip().await.unwrap(),
        (Some(2), 11)
    );
}
