//! Match finalized archive rows to contract execution sequences independently
//! of transaction-hash ordering within a block.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::ops::RangeInclusive;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{Indexer, LogMeta, H256, H512};
use hyperlane_dusk::{
    DuskDeliveryIndexer, DuskInterchainGasPaymasterIndexer, DuskMailboxIndexer,
    DuskMerkleTreeHookIndexer, RuesClient,
};
use hyperlane_dusk_types::{events, message, GasPaymentRecord};
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;

const CONTRACT: [u8; 32] = [2; 32];
const HEIGHT: u64 = 85;

#[derive(Clone, Copy, Debug)]
enum Kind {
    Dispatch,
    Process,
    Merkle,
    Igp,
}

impl Kind {
    fn topic(self) -> &'static str {
        match self {
            Self::Dispatch => events::Dispatch::TOPIC,
            Self::Process => events::ProcessId::TOPIC,
            Self::Merkle => events::InsertedIntoTree::TOPIC,
            Self::Igp => events::GasPayment::TOPIC,
        }
    }
    fn event(self, index: usize) -> Vec<u8> {
        match self {
            Self::Dispatch => rkyv::to_bytes::<_, 256>(&events::Dispatch {
                sender: [7; 32],
                destination: 42,
                recipient: [8; 32],
                message: encoded(index),
            })
            .unwrap()
            .to_vec(),
            Self::Process => rkyv::to_bytes::<_, 256>(&events::ProcessId {
                message_id: id(index),
            })
            .unwrap()
            .to_vec(),
            Self::Merkle => rkyv::to_bytes::<_, 256>(&events::InsertedIntoTree {
                message_id: id(index),
                index: index as u32,
            })
            .unwrap()
            .to_vec(),
            Self::Igp => rkyv::to_bytes::<_, 256>(&events::GasPayment {
                message_id: id(index),
                gas_limit: 100 + index as u64,
                payment: 1000 + index as u64,
            })
            .unwrap()
            .to_vec(),
        }
    }
}

fn id(index: usize) -> [u8; 32] {
    [3 + index as u8; 32]
}
fn encoded(index: usize) -> Vec<u8> {
    message::encode(3, index as u32, 4242, [7; 32], 42, [8; 32], &[index as u8])
}
fn payment(index: usize) -> GasPaymentRecord {
    GasPaymentRecord {
        message_id: id(index),
        destination: 42,
        gas_limit: 100 + index as u64,
        payment: 1000 + index as u64,
        block_height: HEIGHT,
    }
}
fn cursor(id: i64) -> String {
    STANDARD.encode(format!("v1:{id}"))
}
fn tx_hash(index: usize, reversed: bool) -> [u8; 32] {
    if reversed {
        if index == 0 {
            [0xf0; 32]
        } else {
            [0x01; 32]
        }
    } else if index == 0 {
        [0x01; 32]
    } else {
        [0xf0; 32]
    }
}
fn h512(hash: [u8; 32]) -> H512 {
    let mut out = [0; 64];
    out[32..].copy_from_slice(&hash);
    H512::from(out)
}

struct Fixture {
    url: Url,
    store: TempDir,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    archive_requests: Arc<Mutex<Vec<i64>>>,
    archive_rows: Arc<Mutex<Vec<Value>>>,
}
impl Fixture {
    fn start(kind: Kind, reversed: bool, prefix_rows: usize) -> Self {
        Self::start_with_prefix_height(kind, reversed, prefix_rows, HEIGHT)
    }

    fn start_with_prefix_height(
        kind: Kind,
        reversed: bool,
        prefix_rows: usize,
        prefix_height: u64,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let archive_requests = Arc::new(Mutex::new(Vec::new()));
        let worker_requests = archive_requests.clone();
        // Match frozen Rusk's BTreeMap<EventIdentifier,...> grouping followed
        // by ID assignment (transformer.rs:89, sqlite.rs:925 and 976).
        let mut by_origin = std::collections::BTreeMap::new();
        for index in 0..2 {
            by_origin.insert(tx_hash(index, reversed), index);
        }
        let mut rows: Vec<Value> = (0..prefix_rows).map(|index| json!({
                "id": index as i64, "block_height": prefix_height, "block_hash": hex::encode([9;32]),
                "origin": hex::encode([0;32]), "topic": "admin", "source": hex::encode(CONTRACT),
                "data": "00", "reverted": false,
            })).collect();
        for (origin, index) in by_origin {
            rows.push(json!({
                    "id": rows.len() as i64, "block_height": HEIGHT, "block_hash": hex::encode([9;32]),
                    "origin": hex::encode(origin), "topic": kind.topic(), "source": hex::encode(CONTRACT),
                    "data": hex::encode(kind.event(index)), "reverted": false,
                }));
        }
        let archive_rows = Arc::new(Mutex::new(rows));
        let worker_rows = archive_rows.clone();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(c) => c,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(e) => panic!("fixture accept failed: {e}"),
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
                    if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let reply = if path == "/on/graphql/query" {
                    let query = std::str::from_utf8(&body).unwrap();
                    let value = if query == "query { lastBlockPair { json } }" {
                        json!({"lastBlockPair":{"json":{"last_block":[100,"tip"],"last_finalized_block":[90,"final"]}}})
                    } else if query.contains("checkBlock(") {
                        json!({"checkBlock":true})
                    } else if query.contains("tx(hash:") {
                        json!({"tx":{"blockHeight":HEIGHT}})
                    } else if query.contains("finalizedEvents(") {
                        assert!(
                            query.contains(&format!("contractId: \"{}\"", hex::encode(CONTRACT)))
                        );
                        let after = query
                            .split_once("cursor: \"")
                            .map(|(_, text)| {
                                let c = text.split('"').next().unwrap();
                                let text = String::from_utf8(STANDARD.decode(c).unwrap()).unwrap();
                                text.strip_prefix("v1:").unwrap().parse::<i64>().unwrap()
                            })
                            .unwrap_or(-1);
                        worker_requests.lock().unwrap().push(after);
                        let pending: Vec<_> = worker_rows
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|r| r["id"].as_i64().unwrap() > after)
                            .cloned()
                            .collect();
                        let has_next = pending.len() > 16;
                        let page: Vec<_> = pending.into_iter().take(16).collect();
                        let start = page.first().map(|r| cursor(r["id"].as_i64().unwrap()));
                        let end = page.last().map(|r| cursor(r["id"].as_i64().unwrap()));
                        json!({"finalizedEvents":{"json":{"events":page,"startCursor":start,"endCursor":end,"hasNextPage":has_next}}})
                    } else {
                        panic!("unexpected GraphQL request: {query}")
                    };
                    serde_json::to_vec(&value).unwrap()
                } else {
                    let prefix = format!("/on/contracts:{}/", hex::encode(CONTRACT));
                    let method = path.strip_prefix(&prefix).unwrap();
                    let index = || u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                    match method {
                        "nonce" | "processed_count" | "count" | "gas_payment_count" => {
                            2u32.to_le_bytes().to_vec()
                        }
                        "dispatched_block_height"
                        | "processed_block_height_at_index"
                        | "inserted_block_height" => HEIGHT.to_le_bytes().to_vec(),
                        "dispatched_message" => rkyv::to_bytes::<_, 256>(&encoded(index()))
                            .unwrap()
                            .to_vec(),
                        "processed_at_index" | "message_id_at" => id(index()).to_vec(),
                        "gas_payment_at" => rkyv::to_bytes::<_, 256>(&payment(index()))
                            .unwrap()
                            .to_vec(),
                        "gas_payments" => {
                            let start = index();
                            let count = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
                            rkyv::to_bytes::<_, 256>(
                                &(start..start + count).map(payment).collect::<Vec<_>>(),
                            )
                            .unwrap()
                            .to_vec()
                        }
                        _ => panic!("unexpected contract method: {method}"),
                    }
                };
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                )
                .unwrap();
                stream.write_all(&reply).unwrap();
            }
        });
        Self {
            url,
            store: tempfile::tempdir().unwrap(),
            stop,
            worker: Some(worker),
            archive_requests,
            archive_rows,
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

async fn read(
    kind: Kind,
    rues: Arc<RuesClient>,
    range: RangeInclusive<u32>,
    by_tx: Option<H512>,
) -> Result<Vec<LogMeta>, String> {
    let address = H256::from(CONTRACT);
    macro_rules! read_indexer {
        ($indexer:expr) => {{
            let indexer = $indexer;
            let result = match by_tx {
                Some(hash) => indexer.fetch_logs_by_tx_hash(hash).await,
                None => indexer.fetch_logs_in_range(range).await,
            };
            result
                .map(|logs| logs.into_iter().map(|(_, meta)| meta).collect())
                .map_err(|e| e.to_string())
        }};
    }
    match kind {
        Kind::Dispatch => read_indexer!(DuskMailboxIndexer::new(rues, address)),
        Kind::Process => read_indexer!(DuskDeliveryIndexer::new(rues, address)),
        Kind::Merkle => read_indexer!(DuskMerkleTreeHookIndexer::new(rues, address)),
        Kind::Igp => read_indexer!(DuskInterchainGasPaymasterIndexer::new(rues, address)),
    }
}
async fn must_read(
    kind: Kind,
    reversed: bool,
    prefix: usize,
    range: RangeInclusive<u32>,
    by_tx: bool,
) {
    let fixture = Fixture::start(kind, reversed, prefix);
    let rues = fixture.client();
    let hash = by_tx.then(|| h512(tx_hash(*range.start() as usize, reversed)));
    let result = read(kind, rues.clone(), range.clone(), hash).await;
    if let Err(error) = &result {
        eprintln!("{kind:?} first lookup: {error}");
        eprintln!(
            "{kind:?} retry lookup: {:?}",
            read(kind, rues, range.clone(), hash).await
        );
        // Fresh client, distinct DB, same immutable correct endpoint.
        let fresh_store = tempfile::tempdir().unwrap();
        let fresh = Arc::new(
            RuesClient::new_with_event_cursor_dir(
                fixture.url.clone(),
                fresh_store.path().to_path_buf(),
            )
            .unwrap(),
        );
        eprintln!(
            "{kind:?} restarted lookup: {:?}",
            read(kind, fresh, range.clone(), hash).await
        );
    }
    let logs = result.unwrap_or_else(|error| {
        panic!("valid finalized {kind:?} state and archive rows must index successfully: {error}")
    });
    let expected: Vec<_> = if by_tx {
        vec![*range.start()]
    } else {
        range.collect()
    };
    assert_eq!(logs.len(), expected.len());
    for (meta, sequence) in logs.iter().zip(expected) {
        assert_eq!(
            meta.transaction_id,
            h512(tx_hash(sequence as usize, reversed))
        );
        assert_eq!(meta.block_number, HEIGHT);
    }
}

#[tokio::test]
async fn dispatch_hash_order_matches_execution_control() {
    must_read(Kind::Dispatch, false, 0, 0..=1, false).await;
}
#[tokio::test]
async fn process_hash_order_matches_execution_control() {
    must_read(Kind::Process, false, 0, 0..=1, false).await;
}
#[tokio::test]
async fn merkle_hash_order_matches_execution_control() {
    must_read(Kind::Merkle, false, 0, 0..=1, false).await;
}
#[tokio::test]
async fn igp_hash_order_matches_execution_control() {
    must_read(Kind::Igp, false, 0, 0..=1, false).await;
}
#[tokio::test]
async fn dispatch_same_block_reverse_hash_order() {
    must_read(Kind::Dispatch, true, 0, 0..=1, false).await;
}
#[tokio::test]
async fn process_same_block_reverse_hash_order() {
    must_read(Kind::Process, true, 0, 0..=1, false).await;
}
#[tokio::test]
async fn merkle_same_block_reverse_hash_order() {
    must_read(Kind::Merkle, true, 0, 0..=1, false).await;
}
#[tokio::test]
async fn igp_same_block_reverse_hash_order() {
    must_read(Kind::Igp, true, 0, 0..=1, false).await;
}
#[tokio::test]
async fn dispatch_reverse_hash_order_across_archive_page_boundary() {
    must_read(Kind::Dispatch, true, 15, 0..=1, false).await;
}
#[tokio::test]
async fn dispatch_reverse_hash_order_later_sequence_first() {
    must_read(Kind::Dispatch, true, 0, 1..=1, false).await;
}
#[tokio::test]
async fn dispatch_reverse_hash_order_by_transaction_hash() {
    must_read(Kind::Dispatch, true, 0, 0..=0, true).await;
}

#[tokio::test]
async fn same_client_can_read_earlier_same_block_sequence_after_later_one() {
    let fixture = Fixture::start(Kind::Dispatch, true, 15);
    let rues = fixture.client();
    for sequence in [1, 0, 1, 0] {
        let logs = read(Kind::Dispatch, rues.clone(), sequence..=sequence, None)
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].transaction_id,
            h512(tx_hash(sequence as usize, true))
        );
        assert_eq!(logs[0].log_index, hyperlane_core::U256::from(sequence));
    }
}

#[tokio::test]
async fn concurrent_same_block_lookups_keep_each_rows_provenance() {
    let fixture = Fixture::start(Kind::Dispatch, true, 15);
    let rues = fixture.client();
    let (first, second) = tokio::join!(
        read(Kind::Dispatch, rues.clone(), 0..=0, None),
        read(Kind::Dispatch, rues, 1..=1, None),
    );
    for (sequence, result) in [first, second].into_iter().enumerate() {
        let logs = result.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].transaction_id, h512(tx_hash(sequence, true)));
        assert_eq!(logs[0].log_index, hyperlane_core::U256::from(sequence));
    }
}

#[tokio::test]
async fn same_block_replay_reuses_the_cursor_after_older_blocks() {
    let fixture = Fixture::start_with_prefix_height(Kind::Dispatch, true, 40, HEIGHT - 1);
    let rues = fixture.client();
    for sequence in [0, 1] {
        let logs = read(Kind::Dispatch, rues.clone(), sequence..=sequence, None)
            .await
            .unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(
            logs[0].transaction_id,
            h512(tx_hash(sequence as usize, true))
        );
    }
    assert_eq!(
        *fixture.archive_requests.lock().unwrap(),
        vec![-1, 15, 31, 39],
        "same-block lookup must retain earlier peers without replaying older-block history"
    );
}

#[tokio::test]
async fn a_missing_row_cannot_pin_a_cursor_past_repaired_archive_rows() {
    for kind in [Kind::Dispatch, Kind::Process, Kind::Merkle, Kind::Igp] {
        let fixture = Fixture::start(kind, true, 0);
        let correct = fixture.archive_rows.lock().unwrap().clone();
        *fixture.archive_rows.lock().unwrap() = vec![json!({
            "id": 1000, "block_height": HEIGHT - 1, "block_hash": hex::encode([9;32]),
            "origin": hex::encode([0;32]), "topic": "admin", "source": hex::encode(CONTRACT),
            "data": "00", "reverted": false,
        })];
        let rues = fixture.client();
        assert!(read(kind, rues.clone(), 0..=0, None).await.is_err());
        *fixture.archive_rows.lock().unwrap() = correct;
        let logs = read(kind, rues, 0..=0, None)
            .await
            .unwrap_or_else(|e| panic!("{kind:?} must recover after a failed archive lookup: {e}"));
        assert_eq!(logs[0].transaction_id, h512(tx_hash(0, true)));
        assert_eq!(*fixture.archive_requests.lock().unwrap(), vec![-1, -1]);
    }
}

#[tokio::test]
async fn a_successful_lookup_hint_is_discarded_when_archive_ids_are_rebuilt() {
    for kind in [Kind::Dispatch, Kind::Process, Kind::Merkle, Kind::Igp] {
        let fixture = Fixture::start_with_prefix_height(kind, true, 40, HEIGHT - 1);
        let rues = fixture.client();
        assert_eq!(
            read(kind, rues.clone(), 0..=0, None).await.unwrap().len(),
            1
        );
        {
            let mut rows = fixture.archive_rows.lock().unwrap();
            rows.retain(|row| row["topic"] == kind.topic());
            for (index, row) in rows.iter_mut().enumerate() {
                row["id"] = json!(index);
            }
        }
        // The bounded first lookup may exhaust the stale prefix, but the next
        // retry on this same client must replay the repaired endpoint.
        assert!(read(kind, rues.clone(), 1..=1, None).await.is_err());
        let logs = read(kind, rues, 1..=1, None)
            .await
            .unwrap_or_else(|e| panic!("{kind:?} stale successful hint must not pin retries: {e}"));
        assert_eq!(logs[0].transaction_id, h512(tx_hash(1, true)));
        assert_eq!(
            *fixture.archive_requests.lock().unwrap(),
            vec![-1, 15, 31, 39, -1]
        );
    }
}
