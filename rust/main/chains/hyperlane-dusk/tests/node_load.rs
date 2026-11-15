//! What the Dusk clients cost a node, counted at a scripted one: requests
//! at idle as the history grows, a catch-up under Rusk 1.7.3's rate limit,
//! and how many blocks queued deliveries take.
//!
//! Run with `--nocapture` to see the counts.
#![cfg(unix)]

use std::collections::{BTreeMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{
    config::OpSubmissionConfig, HyperlaneDomain, HyperlaneDomainProtocol,
    HyperlaneDomainTechnicalStack, HyperlaneMessage, Indexer, Mailbox, MerkleTreeHook, Metadata,
    NativeToken, ReorgPeriod, SequenceAwareIndexer, H256, U256,
};
use hyperlane_dusk::{
    ConnectionConf, DuskDeliveryIndexer, DuskInterchainGasPaymasterIndexer, DuskMailbox,
    DuskMailboxIndexer, DuskMerkleTreeHook, DuskMerkleTreeHookIndexer, DuskProvider, DuskSigner,
    QueryRateLimit, RuesClient,
};
use hyperlane_dusk_types::{events, merkle::IncrementalMerkle, message, GasPaymentRecord};
use serde_json::{json, Value};
use tempfile::TempDir;
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

    /// The node finalizes one more block, without new records.
    fn finalize_a_block(&self) {
        self.chain.lock().unwrap().finalized += 1;
    }

    /// The requests answered since the last call, by kind.
    fn take_requests(&self) -> BTreeMap<String, usize> {
        std::mem::take(&mut self.chain.lock().unwrap().requests)
    }

    fn refusals(&self) -> usize {
        self.chain.lock().unwrap().refusals
    }
}

fn domain() -> HyperlaneDomain {
    HyperlaneDomain::from_config(
        ORIGIN,
        "dusk-load",
        HyperlaneDomainProtocol::Dusk,
        HyperlaneDomainTechnicalStack::Other,
    )
    .unwrap()
}

fn connection(url: &Url, directory: &Path, gas_limit: u64) -> ConnectionConf {
    ConnectionConf {
        url: url.clone(),
        chain_id: 0,
        event_cursor_dir: directory.to_path_buf(),
        gas_limit,
        gas_price: 1,
        native_token: NativeToken::default(),
        op_submission_config: OpSubmissionConfig::default(),
        query_rate_limit: QueryRateLimit::default(),
    }
}

fn mailbox(rues: Arc<RuesClient>, url: &Url, directory: &Path, signer: bool) -> DuskMailbox {
    let provider = Arc::new(DuskProvider::new(domain(), rues.clone()));
    DuskMailbox::new(
        provider,
        rues,
        H256::from(MAILBOX),
        H256::from(HOOK),
        domain(),
        // A synthetic fixture key.
        signer.then(|| DuskSigner::new(H256::repeat_byte(1)).unwrap()),
        connection(url, directory, 5_000_000),
    )
}

fn total(requests: &BTreeMap<String, usize>) -> usize {
    requests.values().sum()
}

/// The relayer's Dusk clients at idle for a minute, one tick every 5 s and
/// a block finalized every 10 s with no new record: requests by kind.
async fn relayer_idle_minute(records: u32) -> BTreeMap<String, usize> {
    let node = Node::start(Chain::new(records, false, false));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf()).unwrap(),
    );
    let dispatches = DuskMailboxIndexer::new(rues.clone(), H256::from(MAILBOX), ORIGIN);
    let insertions = DuskMerkleTreeHookIndexer::new(rues.clone(), H256::from(HOOK));
    let payments = DuskInterchainGasPaymasterIndexer::new(rues.clone(), H256::from(IGP));
    let deliveries = DuskDeliveryIndexer::new(rues.clone(), H256::from(MAILBOX));
    let destination = mailbox(rues.clone(), &node.url(), store.path(), false);
    let waiting = H256::repeat_byte(0x55);
    let tick = |first: bool| {
        let (dispatches, insertions, payments, deliveries, destination) = (
            &dispatches,
            &insertions,
            &payments,
            &deliveries,
            &destination,
        );
        async move {
            let (count, _) = dispatches.latest_sequence_count_and_tip().await.unwrap();
            assert_eq!(count, Some(records));
            let (count, _) = insertions.latest_sequence_count_and_tip().await.unwrap();
            assert_eq!(count, Some(records));
            let (count, _) = payments.latest_sequence_count_and_tip().await.unwrap();
            assert_eq!(count, Some(records));
            deliveries.latest_sequence_count_and_tip().await.unwrap();
            // A message waiting for its signatures is checked every 0.5 s.
            for _ in 0..if first { 1 } else { 10 } {
                assert!(!destination.delivered(waiting).await.unwrap());
            }
        }
    };
    // Startup.
    tick(true).await;
    node.take_requests();
    for second in (0..60).step_by(5) {
        if second % 10 == 0 {
            node.finalize_a_block();
        }
        rues.expire_block_pair().await;
        tick(false).await;
    }
    RuesClient::forget_shared_client(&node.url(), store.path());
    node.take_requests()
}

/// A validator's Dusk clients at idle for a minute: its Merkle hook indexer
/// and its checkpoint reader, which has its own client.
async fn validator_idle_minute(records: u32) -> BTreeMap<String, usize> {
    let node = Node::start(Chain::new(records, false, false));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf()).unwrap(),
    );
    let insertions = DuskMerkleTreeHookIndexer::new(rues.clone(), H256::from(HOOK));
    let reader = Arc::new(RuesClient::new(node.url()).unwrap());
    let hook = DuskMerkleTreeHook::new(mailbox(reader, &node.url(), store.path(), false));
    let tick = || async {
        let (count, _) = insertions.latest_sequence_count_and_tip().await.unwrap();
        assert_eq!(count, Some(records));
        let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await;
        assert_eq!(checkpoint.is_ok(), records > 0);
    };
    tick().await;
    node.take_requests();
    for second in (0..60).step_by(5) {
        if second % 10 == 0 {
            node.finalize_a_block();
        }
        rues.expire_block_pair().await;
        tick().await;
    }
    RuesClient::forget_shared_client(&node.url(), store.path());
    node.take_requests()
}

#[tokio::test]
async fn idle_requests_do_not_grow_with_history() {
    let mut relayer = Vec::new();
    let mut validator = Vec::new();
    for records in [0, 100, 10_000] {
        let requests = relayer_idle_minute(records).await;
        eprintln!(
            "relayer, {records} records: {} requests a minute: {requests:?}",
            total(&requests)
        );
        relayer.push(requests);
        let requests = validator_idle_minute(records).await;
        eprintln!(
            "validator, {records} records: {} requests a minute: {requests:?}",
            total(&requests)
        );
        validator.push(requests);
    }
    // The same requests whatever the history.
    assert_eq!(relayer[1], relayer[2]);
    assert_eq!(validator[1], validator[2]);
    // One lastBlockPair a tick; per newly finalized block, the relayer reads
    // each contract's count, checks the block and reads the Mailbox's new
    // rows once, and checks a waiting message once.
    for requests in &relayer {
        assert_eq!(requests.get("gql:lastBlockPair"), Some(&12));
        assert!(total(requests) <= 12 + 6 * 6, "{requests:?}");
    }
    // Per newly finalized block a validator reads the hook's count once,
    // for its indexer and its checkpoints together.
    for requests in &validator {
        assert_eq!(requests.get("gql:lastBlockPair"), Some(&12));
        assert!(total(requests) <= 12 + 6, "{requests:?}");
    }
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

/// A chain whose blocks come every `BLOCK_TIME`. Each block holds the
/// transactions the mempool held when the block before it came, as Rusk
/// builds a candidate block when its round starts: a transaction sent after
/// that waits for the next block. Transactions are named by their nonce.
struct TxChain {
    directory: TempDir,
    server: Server,
    state: Arc<Mutex<TxState>>,
    stop: Arc<AtomicBool>,
    producer: Option<JoinHandle<()>>,
}

#[derive(Default)]
struct TxState {
    height: u64,
    /// Transactions by nonce, and the block each is in.
    included: BTreeMap<u64, u64>,
    /// The mempool when the last block came.
    candidate: Vec<u64>,
}

const BLOCK_TIME: Duration = Duration::from_millis(200);

impl TxChain {
    /// `pipelines`: the helper takes `--nonce` and `--no-wait`. `refuse`:
    /// the node refuses the transaction of this nonce before propagation.
    fn start(pipelines: bool, refuse: Option<u64>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let dir = directory.path().to_path_buf();
        std::fs::write(dir.join("mempool"), "").unwrap();
        std::fs::write(dir.join("included"), "").unwrap();
        if pipelines {
            std::fs::write(dir.join("pipelines"), "").unwrap();
        }
        if let Some(nonce) = refuse {
            std::fs::write(dir.join("refuse"), nonce.to_string()).unwrap();
        }
        let helper = dir.join("dusk-tx");
        std::fs::write(&helper, HELPER).unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();

        let state = Arc::new(Mutex::new(TxState::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let producer = {
            let (state, stop, dir) = (state.clone(), stop.clone(), dir.clone());
            thread::spawn(move || {
                while !stop.load(Ordering::SeqCst) {
                    thread::sleep(BLOCK_TIME);
                    let mempool = mempool(&dir);
                    let mut state = state.lock().unwrap();
                    state.height += 1;
                    let height = state.height;
                    let candidate = std::mem::take(&mut state.candidate);
                    let mut lines = String::new();
                    for nonce in candidate {
                        state.included.insert(nonce, height);
                        lines.push_str(&format!("{}\n", tx_id(nonce)));
                    }
                    let mut file = std::fs::OpenOptions::new()
                        .append(true)
                        .open(dir.join("included"))
                        .unwrap();
                    file.write_all(lines.as_bytes()).unwrap();
                    state.candidate = mempool
                        .into_iter()
                        .filter(|nonce| !state.included.contains_key(nonce))
                        .collect();
                }
            })
        };
        let server = {
            let (state, dir) = (state.clone(), dir.clone());
            Server::start(move |stream, path, body| {
                let state = state.lock().unwrap();
                if path.ends_with("/process") {
                    // The simulation passes.
                    reply(stream, "200 OK", "", &rkyv(&()));
                } else if path.starts_with("/on/account:") {
                    let committed = state.included.len() as u64;
                    let pending: HashSet<u64> = mempool(&dir).into_iter().collect();
                    let mut next = committed + 1;
                    while pending.contains(&next) && !state.included.contains_key(&next) {
                        next += 1;
                    }
                    let value =
                        json!({"balance": 1u64 << 40, "nonce": committed, "next_nonce": next});
                    reply(stream, "200 OK", "", value.to_string().as_bytes());
                } else {
                    assert_eq!(path, "/on/graphql/query");
                    let query = String::from_utf8(body).unwrap();
                    let tx = query
                        .split_once("hash: \"")
                        .and_then(|(_, rest)| rest.split('"').next())
                        .unwrap();
                    let nonce = u64::from_str_radix(tx, 16).unwrap();
                    let included = state.included.get(&nonce).copied();
                    let held = mempool(&dir).contains(&nonce);
                    let value = if query.contains("mempoolTx") {
                        json!({
                            "mempoolTx": (held && included.is_none()).then(|| json!({"id": tx})),
                            "tx": included.map(|height| json!({"blockHeight": height})),
                        })
                    } else {
                        json!({"tx": included.map(|_| json!({"gasSpent": 2_000_000, "err": null}))})
                    };
                    reply(stream, "200 OK", "", value.to_string().as_bytes());
                }
            })
        };
        Self {
            directory,
            server,
            state,
            stop,
            producer: Some(producer),
        }
    }

    fn helper(&self) -> PathBuf {
        self.directory.path().join("dusk-tx")
    }

    fn height(&self) -> u64 {
        self.state.lock().unwrap().height
    }

    fn blocks_of(&self, nonces: impl Iterator<Item = u64>) -> HashSet<u64> {
        let state = self.state.lock().unwrap();
        nonces
            .filter_map(|nonce| state.included.get(&nonce).copied())
            .collect()
    }

    /// The block of the last transaction.
    fn last_block(&self) -> u64 {
        let state = self.state.lock().unwrap();
        state
            .included
            .values()
            .copied()
            .max()
            .unwrap_or(state.height)
    }

    fn mailbox(&self) -> DuskMailbox {
        let rues = Arc::new(RuesClient::new(self.server.url.clone()).unwrap());
        mailbox(rues, &self.server.url, self.directory.path(), true)
    }
}

impl Drop for TxChain {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(producer) = self.producer.take() {
            let _ = producer.join();
        }
    }
}

fn tx_id(nonce: u64) -> String {
    format!("{nonce:064x}")
}

fn mempool(dir: &Path) -> Vec<u64> {
    std::fs::read_to_string(dir.join("mempool"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| u64::from_str_radix(line.trim(), 16).ok())
        .collect()
}

/// A `dusk-tx` stand-in. Without `--nonce` it takes the account's next
/// nonce and waits until its transaction is in a block, as the helper does;
/// with `--nonce N --no-wait` (when the `pipelines` file exists) it signs
/// with N and returns once the node holds the transaction.
const HELPER: &str = r#"#!/bin/sh
dir="$(dirname "$0")"
if [ "$2" = "--help" ]; then
  if [ -f "$dir/pipelines" ]; then echo "--nonce <NONCE>  --no-wait"; fi
  exit 0
fi
IFS= read -r url
IFS= read -r key
nonce=""
wait=1
while [ $# -gt 0 ]; do
  case "$1" in
    --nonce) nonce="$2"; shift ;;
    --no-wait) wait="" ;;
  esac
  shift
done
if [ -z "$nonce" ]; then
  nonce=$(( $(wc -l < "$dir/included") + 1 ))
fi
tx=$(printf '%064x' "$nonce")
printf '  Prepared TX %s; reconcile this exact hash before retrying if submission is interrupted\n' "$tx" >&2
if [ -f "$dir/refuse" ] && [ "$(cat "$dir/refuse")" = "$nonce" ]; then
  printf '{"success":false,"error":"Transaction %s submission failed: Preverify rejected before propagation (400 Bad Request): refused; retain tx_id=%s"}\n' "$tx" "$tx"
  exit 1
fi
echo "$tx" >> "$dir/mempool"
if [ -n "$wait" ]; then
  while ! grep -q "^$tx" "$dir/included"; do sleep 0.02; done
fi
printf '{"success":true,"tx_id":"%s"}\n' "$tx"
"#;

/// The process environment names the helper, so one test at a time uses it.
static HELPER_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn delivery(nonce: u32) -> (HyperlaneMessage, Metadata, Option<U256>) {
    let message = HyperlaneMessage {
        version: 3,
        nonce,
        origin: 7,
        sender: H256::repeat_byte(4),
        destination: ORIGIN,
        recipient: H256::repeat_byte(5),
        body: vec![nonce as u8],
    };
    (message, Metadata::new(vec![]), Some(U256::from(5_000_000)))
}

#[tokio::test]
async fn queued_deliveries_share_blocks_when_the_helper_takes_a_nonce() {
    const QUEUED: u32 = 12;
    let _env = HELPER_ENV.lock().await;

    // One at a time: each transaction is sent once the one before is in a
    // block, after the next block's round started, so it waits a block.
    let chain = TxChain::start(false, None);
    std::env::set_var("DUSK_TX_BIN", chain.helper());
    let mailbox = chain.mailbox();
    let start = chain.height();
    for nonce in 0..QUEUED {
        let (message, metadata, gas_limit) = delivery(nonce);
        let outcome = mailbox
            .process(&message, &metadata, gas_limit)
            .await
            .unwrap();
        assert!(outcome.executed);
    }
    let serial_blocks = chain.last_block() - start;
    // A batch from this helper is left to be sent one at a time.
    let batch = mailbox
        .process_deliveries(&(0..3).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    assert!(batch.outcome.is_none());
    assert_eq!(batch.failed_indexes, vec![0, 1, 2]);
    drop(chain);

    // Pipelined: consecutive nonces, each sent once the node holds the one
    // before, all in the next blocks.
    let chain = TxChain::start(true, None);
    std::env::set_var("DUSK_TX_BIN", chain.helper());
    let mailbox = chain.mailbox();
    let start = chain.height();
    let batch = mailbox
        .process_deliveries(&(0..QUEUED).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    let pipelined_blocks = chain.last_block() - start;
    let outcome = batch.outcome.unwrap();
    assert!(batch.failed_indexes.is_empty());
    assert!(outcome.executed);
    assert_eq!(
        outcome.gas_used,
        U256::from(2_000_000u64 * u64::from(QUEUED))
    );
    let blocks = chain.blocks_of(1..=u64::from(QUEUED));
    eprintln!(
        "{QUEUED} deliveries: one at a time {serial_blocks} blocks, pipelined {pipelined_blocks} blocks, in {} distinct blocks",
        blocks.len()
    );
    assert!(
        serial_blocks >= u64::from(QUEUED) * 2 - 1,
        "{serial_blocks}"
    );
    assert!(pipelined_blocks * 4 <= serial_blocks, "{pipelined_blocks}");
    assert!(blocks.len() <= 3, "{blocks:?}");
    std::env::remove_var("DUSK_TX_BIN");
}

#[tokio::test]
async fn a_refused_delivery_ends_the_batch_and_the_rest_go_one_at_a_time() {
    let _env = HELPER_ENV.lock().await;
    // The node refuses the third transaction, nonce 3.
    let chain = TxChain::start(true, Some(3));
    std::env::set_var("DUSK_TX_BIN", chain.helper());
    let batch = chain
        .mailbox()
        .process_deliveries(&(0..5).map(delivery).collect::<Vec<_>>())
        .await
        .unwrap();
    // The first two are sent; the refused one and every later one are left
    // out, so no later nonce waits behind the one the node refused.
    assert_eq!(batch.failed_indexes, vec![2, 3, 4]);
    let outcome = batch.outcome.unwrap();
    assert_eq!(outcome.gas_used, U256::from(4_000_000u64));
    assert_eq!(
        &outcome.transaction_id.as_bytes()[32..],
        &hex::decode(tx_id(1)).unwrap()[..]
    );
    assert_eq!(mempool(chain.directory.path()), vec![1, 2]);
    std::env::remove_var("DUSK_TX_BIN");
}
