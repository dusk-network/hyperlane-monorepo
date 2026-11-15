//! What the Dusk clients cost a node, counted at a scripted one: a catch-up
//! under Rusk 1.7.3's rate limit.
//!
//! Run with `--nocapture` to see the counts.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{Indexer, H256};
use hyperlane_dusk::{
    DuskInterchainGasPaymasterIndexer, DuskMailboxIndexer, DuskMerkleTreeHookIndexer,
    QueryRateLimit, RuesClient,
};
use hyperlane_dusk_types::{events, merkle::IncrementalMerkle, message, GasPaymentRecord};
use serde_json::{json, Value};
use url::Url;

const MAILBOX: [u8; 32] = [2; 32];
const HOOK: [u8; 32] = [3; 32];
const IGP: [u8; 32] = [4; 32];
const ORIGIN: u32 = 4242;
const DESTINATION: u32 = 42;
/// The height of the first record; record `i` is in block `FIRST + i`.
const FIRST: u64 = 100;
/// Rusk 1.7.3's contract query limit: 360 in a window of 2 s.
const RUSK_WINDOW: Duration = Duration::from_secs(2);
const RUSK_WINDOW_QUERIES: u32 = 360;
const RUSK_MAX_PAGE: usize = 200;

fn hash_hex(height: u64) -> String {
    hex::encode(message::keccak256(&height.to_be_bytes()))
}

fn encoded(nonce: u32) -> Vec<u8> {
    message::encode(
        3,
        nonce,
        ORIGIN,
        [7; 32],
        DESTINATION,
        [8; 32],
        &[nonce as u8],
    )
}

fn message_id(nonce: u32) -> [u8; 32] {
    message::id(&encoded(nonce))
}

fn payment(index: u32) -> GasPaymentRecord {
    GasPaymentRecord {
        message_id: message_id(index),
        destination: DESTINATION,
        gas_limit: 100,
        payment: 1_000,
        block_height: FIRST + u64::from(index),
    }
}

fn rkyv<T: rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<256>>>(value: &T) -> Vec<u8> {
    rkyv::to_bytes::<_, 256>(value).unwrap().to_vec()
}

fn cursor(id: i64) -> String {
    STANDARD.encode(format!("v1:{id}"))
}

/// Read one HTTP request: its path and body.
fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut first = String::new();
    reader.read_line(&mut first).ok()?;
    let path = first.split_whitespace().nth(1)?.to_owned();
    let mut length = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            return None;
        }
        if header == "\r\n" {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().unwrap();
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some((path, body))
}

fn reply(stream: &mut TcpStream, status: &str, extra: &str, body: &[u8]) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
}

/// A server on its own thread that answers each request with `answer`
/// until it is dropped.
struct Server {
    url: Url,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Server {
    fn start(mut answer: impl FnMut(&mut TcpStream, String, Vec<u8>) + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let url = Url::parse(&format!("http://{address}")).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            for stream in listener.incoming() {
                if worker_stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(mut stream) = stream else {
                    continue;
                };
                if let Some((path, body)) = read_request(&mut stream) {
                    answer(&mut stream, path, body);
                }
            }
        });
        Self {
            url,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop.
        let _ = TcpStream::connect(self.url.socket_addrs(|| None).unwrap()[0]);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

/// A node holding `records` dispatches, Merkle insertions and IGP payments,
/// record `i` in block `FIRST + i`, that counts the requests it answers.
struct Chain {
    records: u32,
    finalized: u64,
    tree: Vec<u8>,
    /// Archive rows by contract, hex.
    rows: BTreeMap<String, Vec<Value>>,
    /// Refuse contract queries past 360 in a window of 2 s, as Rusk does.
    rate_limit: bool,
    window: Option<(Instant, u32)>,
    requests: BTreeMap<String, usize>,
    refusals: usize,
}

impl Chain {
    fn new(records: u32, archive: bool, rate_limit: bool) -> Self {
        let mut tree = IncrementalMerkle::new();
        for index in 0..records {
            tree.insert(message_id(index));
        }
        let mut rows = BTreeMap::new();
        if archive {
            for (contract, topic) in [
                (MAILBOX, events::Dispatch::TOPIC),
                (HOOK, events::InsertedIntoTree::TOPIC),
                (IGP, events::GasPayment::TOPIC),
            ] {
                let contract_rows = (0..records)
                    .map(|index| {
                        let data = match contract {
                            MAILBOX => rkyv(&events::Dispatch {
                                sender: [7; 32],
                                destination: DESTINATION,
                                recipient: [8; 32],
                                message: encoded(index),
                            }),
                            HOOK => rkyv(&events::InsertedIntoTree {
                                message_id: message_id(index),
                                index,
                            }),
                            _ => rkyv(&events::GasPayment {
                                message_id: message_id(index),
                                gas_limit: 100,
                                payment: 1_000,
                            }),
                        };
                        let height = FIRST + u64::from(index);
                        json!({
                            "id": i64::from(index), "block_height": height,
                            "block_hash": hash_hex(height), "origin": hex::encode([9; 32]),
                            "topic": topic, "source": hex::encode(contract),
                            "data": hex::encode(data), "reverted": false,
                        })
                    })
                    .collect();
                rows.insert(hex::encode(contract), contract_rows);
            }
        }
        Self {
            records,
            finalized: FIRST + u64::from(records) + 5,
            tree: rkyv(&tree),
            rows,
            rate_limit,
            window: None,
            requests: BTreeMap::new(),
            refusals: 0,
        }
    }

    fn count(&mut self, kind: impl Into<String>) {
        *self.requests.entry(kind.into()).or_default() += 1;
    }

    /// Rusk's fixed window: 360 contract queries from the first one, for
    /// 2 s; the rest are refused.
    fn admit_query(&mut self) -> bool {
        if !self.rate_limit {
            return true;
        }
        let now = Instant::now();
        let (start, used) = match self.window {
            Some((start, used)) if now < start + RUSK_WINDOW => (start, used),
            _ => (now, 0),
        };
        if used >= RUSK_WINDOW_QUERIES {
            self.refusals += 1;
            return false;
        }
        self.window = Some((start, used + 1));
        true
    }

    fn answer(&mut self, stream: &mut TcpStream, path: String, body: Vec<u8>) {
        if path == "/on/graphql/query" {
            let query = String::from_utf8(body).unwrap();
            let field = query
                .trim_start_matches("query {")
                .trim_start()
                .split(|c: char| !c.is_ascii_alphanumeric())
                .next()
                .unwrap()
                .to_owned();
            self.count(format!("gql:{field}"));
            let value = self.graphql(&field, &query);
            reply(stream, "200 OK", "", value.to_string().as_bytes());
            return;
        }
        let (contract, method) = path
            .strip_prefix("/on/contracts:")
            .and_then(|rest| rest.split_once('/'))
            .unwrap_or_else(|| panic!("unexpected request {path}"));
        let name = match hex::decode(contract).unwrap().as_slice() {
            c if c == MAILBOX => "mailbox",
            c if c == HOOK => "hook",
            c if c == IGP => "igp",
            _ => panic!("unexpected contract {contract}"),
        };
        if !self.admit_query() {
            reply(
                stream,
                "429 Too Many Requests",
                "Retry-After: 1\r\nContent-Type: application/json\r\n",
                br#"{"error":"too_many_requests"}"#,
            );
            return;
        }
        self.count(format!("{name}.{method}"));
        let index = || u32::from_le_bytes(body[..4].try_into().unwrap());
        let height = |index: u32| FIRST + u64::from(index);
        let value = match (name, method) {
            ("mailbox", "nonce") | ("hook", "count") | ("igp", "gas_payment_count") => {
                self.records.to_le_bytes().to_vec()
            }
            ("mailbox", "dispatched_block_height") | ("hook", "inserted_block_height") => {
                height(index()).to_le_bytes().to_vec()
            }
            ("mailbox", "dispatched_id") | ("hook", "message_id_at") => {
                message_id(index()).to_vec()
            }
            ("mailbox", "delivered") => rkyv(&false),
            ("hook", "message_ids") => {
                let limit = u32::from_le_bytes(body[4..8].try_into().unwrap());
                rkyv(
                    &(index()..index() + limit)
                        .map(message_id)
                        .collect::<Vec<_>>(),
                )
            }
            ("hook", "tree") => self.tree.clone(),
            ("igp", "gas_payment_at") => rkyv(&payment(index())),
            ("igp", "gas_payments") => {
                let limit = u32::from_le_bytes(body[4..8].try_into().unwrap());
                rkyv(&(index()..index() + limit).map(payment).collect::<Vec<_>>())
            }
            _ => panic!("unexpected contract query {name}.{method}"),
        };
        reply(stream, "200 OK", "", &value);
    }

    fn graphql(&self, field: &str, query: &str) -> Value {
        let number_after = |key: &str| -> u64 {
            query
                .split_once(key)
                .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
                .unwrap()
                .parse()
                .unwrap()
        };
        let quoted_after = |key: &str| -> String {
            query
                .split_once(key)
                .and_then(|(_, rest)| rest.split('"').next())
                .unwrap()
                .to_owned()
        };
        match field {
            "lastBlockPair" => {
                let latest = self.finalized + 1;
                json!({"lastBlockPair": {"json": {
                    "last_block": [latest, hash_hex(latest)],
                    "last_finalized_block": [self.finalized, hash_hex(self.finalized)],
                }}})
            }
            "checkBlock" => {
                let height = number_after("height: ");
                let finalized =
                    height <= self.finalized && quoted_after("hash: \"") == hash_hex(height);
                json!({ "checkBlock": finalized })
            }
            "finalizedEvents" => {
                let source = quoted_after("contractId: \"");
                let limit = (number_after("limit: ") as usize).clamp(1, RUSK_MAX_PAGE);
                let after = if query.contains("cursor: \"") {
                    let cursor = quoted_after("cursor: \"");
                    let text = String::from_utf8(STANDARD.decode(cursor).unwrap()).unwrap();
                    text.strip_prefix("v1:").unwrap().parse::<i64>().unwrap()
                } else {
                    -1
                };
                let rows = self.rows.get(&source).cloned().unwrap_or_default();
                let pending: Vec<Value> = rows
                    .into_iter()
                    .filter(|row| row["id"].as_i64().unwrap() > after)
                    .collect();
                let has_next = pending.len() > limit;
                let page: Vec<Value> = pending.into_iter().take(limit).collect();
                let start = page.first().map(|row| cursor(row["id"].as_i64().unwrap()));
                let end = page.last().map(|row| cursor(row["id"].as_i64().unwrap()));
                json!({"finalizedEvents": {"json": {
                    "events": page, "startCursor": start, "endCursor": end,
                    "hasNextPage": has_next,
                }}})
            }
            _ => panic!("unexpected GraphQL query: {query}"),
        }
    }
}

/// A scripted node serving a `Chain`.
struct Node {
    chain: Arc<Mutex<Chain>>,
    server: Server,
}

impl Node {
    fn start(chain: Chain) -> Self {
        let chain = Arc::new(Mutex::new(chain));
        let served = chain.clone();
        let server = Server::start(move |stream, path, body| {
            served.lock().unwrap().answer(stream, path, body)
        });
        Self { chain, server }
    }

    fn url(&self) -> Url {
        self.server.url.clone()
    }

    /// The requests answered since the last call, by kind.
    fn take_requests(&self) -> BTreeMap<String, usize> {
        std::mem::take(&mut self.chain.lock().unwrap().requests)
    }

    fn refusals(&self) -> usize {
        self.chain.lock().unwrap().refusals
    }
}

fn total(requests: &BTreeMap<String, usize>) -> usize {
    requests.values().sum()
}

/// The relayer's three record indexers read 120 new records at once, as
/// after a restart, from a node that refuses contract queries past Rusk
/// 1.7.3's default limit. Returns how long that took and how many queries
/// the node refused.
async fn catch_up(limit: QueryRateLimit) -> (Duration, usize, BTreeMap<String, usize>) {
    const RECORDS: u32 = 120;
    let node = Node::start(Chain::new(RECORDS, true, true));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf())
            .unwrap()
            .with_query_rate_limit(limit),
    );
    let dispatches = DuskMailboxIndexer::new(rues.clone(), H256::from(MAILBOX), ORIGIN);
    let insertions = DuskMerkleTreeHookIndexer::new(rues.clone(), H256::from(HOOK));
    let payments = DuskInterchainGasPaymasterIndexer::new(rues.clone(), H256::from(IGP));
    let started = Instant::now();
    let range = 0..=RECORDS - 1;
    let (dispatched, inserted, paid) = tokio::join!(
        dispatches.fetch_logs_in_range(range.clone()),
        insertions.fetch_logs_in_range(range.clone()),
        payments.fetch_logs_in_range(range),
    );
    let elapsed = started.elapsed();
    assert_eq!(dispatched.unwrap().len(), RECORDS as usize);
    assert_eq!(inserted.unwrap().len(), RECORDS as usize);
    assert_eq!(paid.unwrap().len(), RECORDS as usize);
    RuesClient::forget_shared_client(&node.url(), store.path());
    (elapsed, node.refusals(), node.take_requests())
}

#[tokio::test]
async fn a_catch_up_stays_within_the_nodes_rate_limit() {
    // At the default pace the node refuses nothing.
    let (elapsed, refusals, requests) = catch_up(QueryRateLimit::DEFAULT).await;
    eprintln!(
        "paced catch-up: {elapsed:?}, {refusals} refused, {} requests: {requests:?}",
        total(&requests)
    );
    assert_eq!(refusals, 0);
    // Unpaced, the same reads run into the limit, and the refused queries
    // are sent again until they pass.
    let unpaced = QueryRateLimit {
        per_second: 1_000_000,
        burst: 1_000_000,
    };
    let (elapsed, refusals, requests) = catch_up(unpaced).await;
    eprintln!(
        "unpaced catch-up: {elapsed:?}, {refusals} refused, {} requests: {requests:?}",
        total(&requests)
    );
    assert!(refusals > 0);
}
