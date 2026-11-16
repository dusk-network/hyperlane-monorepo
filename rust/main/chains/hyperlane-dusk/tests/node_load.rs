//! What the Dusk clients cost a node, counted at a scripted one: requests
//! at idle as the history grows, a catch-up under Rusk 1.7.3's rate limit,
//! and a restart's catch-up with and without a multicall. How many blocks
//! queued deliveries take is in `delivery_submission.rs`.
//!
//! Run with `--nocapture` to see the counts, and
//! `restart_catch_up_requests_for_ten_thousand_records` with `--ignored`
//! for the long measurement.
#![cfg(unix)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::{engine::general_purpose::STANDARD, Engine};
use hyperlane_core::{
    config::OpSubmissionConfig, HyperlaneDomain, HyperlaneDomainProtocol,
    HyperlaneDomainTechnicalStack, Indexer, Mailbox, MerkleTreeHook, NativeToken, ReorgPeriod,
    SequenceAwareIndexer, H256,
};
use hyperlane_dusk::multicall::{Aggregate, Call};
use hyperlane_dusk::{
    ConnectionConf, DeploymentIdentity, DuskDeliveryIndexer, DuskInterchainGasPaymasterIndexer,
    DuskMailbox, DuskMailboxIndexer, DuskMerkleTreeHook, DuskMerkleTreeHookIndexer, DuskProvider,
    DuskSigner, QueryRateLimit, RuesClient,
};
use hyperlane_dusk_types::{events, merkle::IncrementalMerkle, message, GasPaymentRecord};
use serde_json::{json, Value};
use url::Url;

const MAILBOX: [u8; 32] = [2; 32];
const HOOK: [u8; 32] = [3; 32];
const IGP: [u8; 32] = [4; 32];
const VALIDATOR_ANNOUNCE: [u8; 32] = [5; 32];
/// The genesis transfer contract, which reports the chain ID.
const TRANSFER: [u8; 32] = {
    let mut id = [0; 32];
    id[0] = 1;
    id
};
/// Where mainnet and testnet run Multicall for Dusk.
const MULTICALL: [u8; 32] = [
    0x93, 0xde, 0x28, 0x7b, 0x7d, 0x34, 0x9d, 0x67, 0x8a, 0x00, 0x38, 0xfe, 0x77, 0x72, 0xe8, 0x96,
    0x0e, 0x1a, 0x84, 0xa1, 0xb5, 0xda, 0x2d, 0x0b, 0x1f, 0x62, 0xf5, 0x32, 0x61, 0x6c, 0xe1, 0xd4,
];
const IDENTITY: DeploymentIdentity = DeploymentIdentity {
    chain_id: 0,
    domain: ORIGIN,
    mailbox: MAILBOX,
    validator_announce: VALIDATOR_ANNOUNCE,
    merkle_tree_hook: HOOK,
};
/// A pace no catch-up reaches, for counting requests rather than time.
const UNPACED: QueryRateLimit = QueryRateLimit {
    per_second: 1_000_000,
    burst: 1_000_000,
};
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

fn payment(index: u32, per_block: u32) -> GasPaymentRecord {
    GasPaymentRecord {
        message_id: message_id(index),
        destination: DESTINATION,
        gas_limit: 100,
        payment: 1_000,
        block_height: FIRST + u64::from(index / per_block),
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
/// record `i` in block `FIRST + i / per_block`, that counts the requests it
/// answers. Its multicall, when it has one, answers an aggregate as one
/// contract query.
struct Chain {
    records: u32,
    per_block: u32,
    finalized: u64,
    tree: Vec<u8>,
    root: [u8; 32],
    /// Archive rows by contract, hex.
    rows: BTreeMap<String, Vec<Value>>,
    /// Refuse contract queries past 360 in a window of 2 s, as Rusk does.
    rate_limit: bool,
    window: Option<(Instant, u32)>,
    multicall: bool,
    requests: BTreeMap<String, usize>,
    /// The calls aggregates made, by kind.
    calls: BTreeMap<String, usize>,
    refusals: usize,
}

impl Chain {
    fn new(records: u32, archive: bool, rate_limit: bool) -> Self {
        Self::with_blocks(records, 1, archive, rate_limit)
    }

    /// Records `per_block` to a block.
    fn with_blocks(records: u32, per_block: u32, archive: bool, rate_limit: bool) -> Self {
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
                        let height = FIRST + u64::from(index / per_block);
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
            per_block,
            finalized: FIRST + u64::from(records / per_block) + 5,
            root: tree.root(),
            tree: rkyv(&tree),
            rows,
            rate_limit,
            window: None,
            multicall: false,
            requests: BTreeMap::new(),
            calls: BTreeMap::new(),
            refusals: 0,
        }
    }

    /// This chain, with Multicall for Dusk deployed.
    fn with_multicall(mut self, multicall: bool) -> Self {
        self.multicall = multicall;
        self
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
        let contract: [u8; 32] = hex::decode(contract).unwrap().try_into().unwrap();
        if !self.admit_query() {
            reply(
                stream,
                "429 Too Many Requests",
                "Retry-After: 1\r\nContent-Type: application/json\r\n",
                br#"{"error":"too_many_requests"}"#,
            );
            return;
        }
        if contract == MULTICALL {
            self.count(format!("multicall.{method}"));
            match self.multicall(method, &body) {
                Some(value) => reply(stream, "200 OK", "", &value),
                // Rusk 1.7.3 answers every failed query alike.
                None => reply(
                    stream,
                    "500 Internal Server Error",
                    "",
                    br#"{"error":"Internal server error"}"#,
                ),
            }
            return;
        }
        self.count(format!("{}.{method}", name(&contract)));
        let value = self.query(&contract, method, &body);
        reply(stream, "200 OK", "", &value);
    }

    /// The multicall's reply, or `None` where the query fails.
    fn multicall(&mut self, method: &str, body: &[u8]) -> Option<Vec<u8>> {
        if !self.multicall {
            return None;
        }
        if method == "version" {
            return Some(rkyv(&1u32));
        }
        assert_eq!(method, "aggregate");
        let calls: Vec<Call> = hyperlane_dusk::rkyv_deserialize(body).unwrap();
        if calls.len() > 256 {
            return None;
        }
        let results = calls
            .iter()
            .map(|call| {
                *self
                    .calls
                    .entry(format!("{}.{}", name(&call.contract), call.fn_name))
                    .or_default() += 1;
                Ok(self.query(&call.contract, &call.fn_name, &call.fn_args))
            })
            .collect();
        let encoded = rkyv(&Aggregate {
            height: self.finalized + 1,
            results,
        });
        // The contract's own limit: its reply must fit 64 KiB.
        (encoded.len() <= 64 * 1024).then_some(encoded)
    }

    /// A view's reply.
    fn query(&self, contract: &[u8; 32], method: &str, body: &[u8]) -> Vec<u8> {
        let index = || u32::from_le_bytes(body[..4].try_into().unwrap());
        let per_block = self.per_block;
        let height = |index: u32| FIRST + u64::from(index / per_block);
        match (name(contract), method) {
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
            ("hook", "root") => self.root.to_vec(),
            ("igp", "gas_payment_at") => rkyv(&payment(index(), per_block)),
            ("igp", "gas_payments") => {
                let limit = u32::from_le_bytes(body[4..8].try_into().unwrap());
                rkyv(
                    &(index()..index() + limit)
                        .map(|index| payment(index, per_block))
                        .collect::<Vec<_>>(),
                )
            }
            // The deployment check.
            ("transfer", "chain_id") => vec![0],
            ("mailbox" | "announce", "local_domain") => ORIGIN.to_le_bytes().to_vec(),
            ("mailbox", "state_version") => 5u32.to_le_bytes().to_vec(),
            ("announce" | "hook", "state_version") => 2u32.to_le_bytes().to_vec(),
            ("announce" | "hook", "mailbox") => MAILBOX.to_vec(),
            ("mailbox", "required_hook") => HOOK.to_vec(),
            ("hook", "hook_type") => vec![3],
            (name, method) => panic!("unexpected contract query {name}.{method}"),
        }
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

/// The name a request count gives a contract.
fn name(contract: &[u8; 32]) -> &'static str {
    match *contract {
        MAILBOX => "mailbox",
        HOOK => "hook",
        IGP => "igp",
        VALIDATOR_ANNOUNCE => "announce",
        TRANSFER => "transfer",
        _ => panic!("unexpected contract {}", hex::encode(contract)),
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

    /// The calls aggregates made since the last call, by kind.
    fn take_calls(&self) -> BTreeMap<String, usize> {
        std::mem::take(&mut self.chain.lock().unwrap().calls)
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
        pipeline_deliveries: false,
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

/// The multicall clients batch through, if `multicall`.
fn multicall_of(multicall: bool) -> Option<[u8; 32]> {
    multicall.then_some(MULTICALL)
}

/// The relayer's Dusk clients at idle for a minute, one tick every 5 s and
/// a block finalized every 10 s with no new record: requests by kind.
async fn relayer_idle_minute(records: u32, multicall: bool) -> BTreeMap<String, usize> {
    let node = Node::start(Chain::new(records, false, false).with_multicall(multicall));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf())
            .unwrap()
            .with_multicall(multicall_of(multicall)),
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
async fn validator_idle_minute(records: u32, multicall: bool) -> BTreeMap<String, usize> {
    let node = Node::start(Chain::new(records, false, false).with_multicall(multicall));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf())
            .unwrap()
            .with_multicall(multicall_of(multicall)),
    );
    let insertions = DuskMerkleTreeHookIndexer::new(rues.clone(), H256::from(HOOK));
    let reader = Arc::new(
        RuesClient::new(node.url())
            .unwrap()
            .with_multicall(multicall_of(multicall)),
    );
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
        let requests = relayer_idle_minute(records, false).await;
        eprintln!(
            "relayer, {records} records: {} requests a minute: {requests:?}",
            total(&requests)
        );
        relayer.push(requests);
        let requests = validator_idle_minute(records, false).await;
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

#[tokio::test]
async fn idle_requests_with_a_multicall() {
    for records in [0, 100, 10_000] {
        let relayer = relayer_idle_minute(records, true).await;
        eprintln!(
            "relayer with a multicall, {records} records: {} requests a minute: {relayer:?}",
            total(&relayer)
        );
        // Per newly finalized block, one aggregate carries the three counts
        // and the waiting message's delivery check, where they were four
        // queries.
        assert_eq!(relayer.get("multicall.aggregate"), Some(&6), "{relayer:?}");
        assert_eq!(total(&relayer), 12 + 6 * 3, "{relayer:?}");
        let validator = validator_idle_minute(records, true).await;
        eprintln!(
            "validator with a multicall, {records} records: {} requests a minute: {validator:?}",
            total(&validator)
        );
        // A validator reads one count per final block: nothing to batch.
        assert_eq!(total(&validator), 12 + 6, "{validator:?}");
    }
}

/// Every record a sequence indexer has, in the ranges of 2,000 that contract
/// sync's forward cursor reads (`index.chunk` 1999), after reading its count.
async fn index_all<T, I>(indexer: &I, records: u32) -> usize
where
    T: Send + Sync + 'static,
    I: hyperlane_core::SequenceAwareIndexer<T>,
{
    let (count, _) = indexer.latest_sequence_count_and_tip().await.unwrap();
    assert_eq!(count, Some(records));
    let mut logs = 0;
    let mut next = 0;
    while next < records {
        let end = next.saturating_add(1_999).min(records - 1);
        logs += indexer.fetch_logs_in_range(next..=end).await.unwrap().len();
        next = end + 1;
    }
    logs
}

/// A relayer started after `records` dispatches, Merkle insertions and IGP
/// payments, `per_block` to a block: its deployment check, then each record
/// indexer's count and records, together. Requests by kind, and the calls
/// aggregates made.
async fn restart_catch_up(
    records: u32,
    per_block: u32,
    multicall: bool,
) -> (BTreeMap<String, usize>, BTreeMap<String, usize>) {
    let node =
        Node::start(Chain::with_blocks(records, per_block, true, false).with_multicall(multicall));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new_with_event_cursor_dir(node.url(), store.path().to_path_buf())
            .unwrap()
            .with_query_rate_limit(UNPACED)
            .with_multicall(multicall_of(multicall))
            .with_identity(IDENTITY),
    );
    let dispatches = DuskMailboxIndexer::new(rues.clone(), H256::from(MAILBOX), ORIGIN);
    let insertions = DuskMerkleTreeHookIndexer::new(rues.clone(), H256::from(HOOK));
    let payments = DuskInterchainGasPaymasterIndexer::new(rues.clone(), H256::from(IGP));
    let (dispatched, inserted, paid) = tokio::join!(
        index_all(&dispatches, records),
        index_all(&insertions, records),
        index_all(&payments, records),
    );
    assert_eq!(
        (dispatched, inserted, paid),
        (records as usize, records as usize, records as usize)
    );
    RuesClient::forget_shared_client(&node.url(), store.path());
    let (requests, calls) = (node.take_requests(), node.take_calls());
    // The deployment check's ten views: ten queries, or the multicall's
    // probe and one aggregate.
    let views = [
        "transfer.chain_id",
        "mailbox.local_domain",
        "announce.local_domain",
        "mailbox.state_version",
        "announce.state_version",
        "announce.mailbox",
        "mailbox.required_hook",
        "hook.hook_type",
        "hook.state_version",
        "hook.mailbox",
    ];
    for view in views {
        let (sent, batched) = if multicall {
            (None, Some(&1))
        } else {
            (Some(&1), None)
        };
        assert_eq!(
            (requests.get(view), calls.get(view)),
            (sent, batched),
            "{view}"
        );
    }
    assert_eq!(
        requests.get("multicall.version"),
        multicall.then_some(&1),
        "{requests:?}"
    );
    (requests, calls)
}

/// Contract queries among `requests`: one per aggregate.
fn contract_queries(requests: &BTreeMap<String, usize>) -> usize {
    requests
        .iter()
        .filter(|(kind, _)| !kind.starts_with("gql:"))
        .map(|(_, count)| count)
        .sum()
}

async fn report_catch_up(records: u32, per_block: u32) -> [usize; 2] {
    let mut queries = [0; 2];
    for (multicall, queries) in [false, true].into_iter().zip(&mut queries) {
        let started = Instant::now();
        let (requests, calls) = restart_catch_up(records, per_block, multicall).await;
        *queries = contract_queries(&requests);
        eprintln!(
            "restart catch-up of {records} records, {per_block} a block, {}: {} requests, {} of them contract queries, in {:?}: {requests:?}; calls in aggregates: {calls:?}",
            if multicall { "with a multicall" } else { "one query a read" },
            total(&requests),
            queries,
            started.elapsed()
        );
    }
    queries
}

#[tokio::test]
async fn restart_catch_up_requests() {
    // The deployment check, three counts, three searches over the records
    // and the records themselves: one query each, or a few aggregates.
    let [single, batched] = report_catch_up(100, 1).await;
    assert!(single >= 330, "{single}");
    assert!(batched <= 16, "{batched}");
    let [single, batched] = report_catch_up(1_000, 10).await;
    assert!(single >= 3_000, "{single}");
    assert!(batched <= 40, "{batched}");
}

#[tokio::test]
#[ignore = "a long measurement: about 36,000 requests to a scripted node"]
async fn restart_catch_up_requests_for_ten_thousand_records() {
    let [single, batched] = report_catch_up(10_000, 10).await;
    assert!(single >= 30_000, "{single}");
    assert!(batched <= 200, "{batched}");
}

/// The relayer checks whether `messages` messages it prepares together are
/// delivered, at two final blocks in turn, as its prepare stage does for a
/// batch of operations: requests by kind at each block.
async fn delivery_checks(messages: u8, multicall: bool) -> Vec<BTreeMap<String, usize>> {
    let node = Node::start(Chain::new(0, false, false).with_multicall(multicall));
    let store = tempfile::tempdir().unwrap();
    let rues = Arc::new(
        RuesClient::new(node.url())
            .unwrap()
            .with_multicall(multicall_of(multicall)),
    );
    let destination = mailbox(rues.clone(), &node.url(), store.path(), false);
    // Startup: the probe, and a check the relayer made before.
    assert!(!destination.delivered(H256::zero()).await.unwrap());
    node.take_requests();
    let mut blocks = Vec::new();
    for _ in 0..2 {
        node.finalize_a_block();
        rues.expire_block_pair().await;
        let mut checks = tokio::task::JoinSet::new();
        for message in 1..=messages {
            let destination = destination.clone();
            checks.spawn(async move { destination.delivered(H256::repeat_byte(message)).await });
        }
        while let Some(check) = checks.join_next().await {
            assert!(!check.unwrap().unwrap());
        }
        blocks.push(node.take_requests());
    }
    blocks
}

#[tokio::test]
async fn delivery_checks_prepared_together_share_aggregates() {
    let single = delivery_checks(16, false).await;
    for block in &single {
        assert_eq!(block.get("mailbox.delivered"), Some(&16), "{block:?}");
    }
    let batched = delivery_checks(16, true).await;
    eprintln!(
        "16 delivery checks at two final blocks: one query a read {single:?}, with a multicall {batched:?}"
    );
    // At the first block, the checks go together as they are asked, in a
    // few aggregates; at the next, all of them in one, with the check made
    // before them.
    let first = batched[0].get("multicall.aggregate").copied().unwrap_or(0);
    assert!((1..=4).contains(&first), "{batched:?}");
    assert_eq!(
        batched[1].get("multicall.aggregate"),
        Some(&1),
        "{batched:?}"
    );
    for block in &batched {
        assert!(!block.contains_key("mailbox.delivered"), "{block:?}");
        assert_eq!(block.get("gql:lastBlockPair"), Some(&1), "{block:?}");
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
