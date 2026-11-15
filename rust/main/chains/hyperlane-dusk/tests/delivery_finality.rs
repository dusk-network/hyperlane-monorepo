//! Deliveries and validator announcements count only once their block is
//! final, through the public adapters and a scripted Rusk node whose chain a
//! test extends, finalizes and rolls back.
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine;
use hyperlane_core::{
    config::OpSubmissionConfig, HyperlaneDomain, HyperlaneDomainProtocol,
    HyperlaneDomainTechnicalStack, Mailbox, NativeToken, ValidatorAnnounce, H256,
};
use hyperlane_dusk::{
    ConnectionConf, DuskMailbox, DuskProvider, DuskValidatorAnnounce, RuesClient,
};
use hyperlane_dusk_types::{events, EthAddress};
use url::Url;

const MAILBOX: [u8; 32] = [0x02; 32];
const VALIDATOR_ANNOUNCE: [u8; 32] = [0x05; 32];
const STAKE: [u8; 32] = {
    let mut id = [0u8; 32];
    id[0] = 2;
    id
};
const MESSAGE: [u8; 32] = [0x11; 32];
const VALIDATOR: [u8; 20] = [0x22; 20];
const LOCATION: &str = "s3://fixture/checkpoints";

#[derive(Clone, Debug)]
struct Event {
    source: [u8; 32],
    topic: &'static str,
    data: Vec<u8>,
    reverted: bool,
}

impl Event {
    fn processed(message_id: [u8; 32]) -> Self {
        Self {
            source: MAILBOX,
            topic: events::ProcessId::TOPIC,
            data: rkyv::to_bytes::<_, 256>(&events::ProcessId { message_id })
                .unwrap()
                .to_vec(),
            reverted: false,
        }
    }

    fn announced(validator: [u8; 20], storage_location: &str) -> Self {
        Self {
            source: VALIDATOR_ANNOUNCE,
            topic: events::ValidatorAnnouncement::TOPIC,
            data: rkyv::to_bytes::<_, 256>(&events::ValidatorAnnouncement {
                validator: EthAddress(validator),
                storage_location: storage_location.to_owned(),
            })
            .unwrap()
            .to_vec(),
            reverted: false,
        }
    }
}

#[derive(Clone, Debug)]
struct Block {
    height: u64,
    hash: [u8; 32],
    prev: [u8; 32],
    events: Vec<Event>,
    archived: bool,
}

/// Changes a node once the Mailbox's `delivered` query was answered a
/// number of times.
type Hook = (usize, Box<dyn FnOnce(&mut Node) + Send>);

/// A Rusk node: its ledger chain and finality, its archive, and the
/// contract state its queries read.
struct Node {
    chain: Vec<Block>,
    finalized: u64,
    /// Newest blocks that `lastBlockPair` does not report yet.
    unreported: u64,
    /// Message IDs the Mailbox reports delivered.
    delivered: HashSet<[u8; 32]>,
    /// Locations ValidatorAnnounce reports for `VALIDATOR`.
    locations: Vec<String>,
    /// GraphQL queries the node answered.
    queries: Vec<String>,
    delivered_queries: usize,
    after_delivered_queries: Option<Hook>,
    /// The same, after ValidatorAnnounce's location list was read a number
    /// of times.
    location_queries: usize,
    after_location_queries: Option<Hook>,
    /// The archive refuses every `finalizedEvents` request.
    finalized_events_fail: bool,
    forks: u8,
}

impl Node {
    /// Blocks 0 to `height`, all final.
    fn new(height: u64) -> Self {
        let mut node = Self {
            chain: Vec::new(),
            finalized: 0,
            unreported: 0,
            delivered: HashSet::new(),
            locations: Vec::new(),
            queries: Vec::new(),
            delivered_queries: 0,
            after_delivered_queries: None,
            location_queries: 0,
            after_location_queries: None,
            finalized_events_fail: false,
            forks: 0,
        };
        for _ in 0..=height {
            node.push(vec![]);
        }
        node.finalized = height;
        node
    }

    fn tip(&self) -> u64 {
        self.chain.len() as u64 - 1
    }

    /// Append a block with `events` and the stake reward every block emits.
    fn push(&mut self, mut events: Vec<Event>) -> u64 {
        let height = self.chain.len() as u64;
        let prev = self.chain.last().map_or([0; 32], |block| block.hash);
        let mut hash = [0xaa; 32];
        hash[..8].copy_from_slice(&height.to_be_bytes());
        hash[8] = self.forks;
        events.push(Event {
            source: STAKE,
            topic: "reward",
            data: vec![1],
            reverted: false,
        });
        self.chain.push(Block {
            height,
            hash,
            prev,
            events,
            archived: true,
        });
        height
    }

    /// Drop every block above `height`; later blocks fork from it.
    fn roll_back_to(&mut self, height: u64) {
        assert!(
            height >= self.finalized,
            "final blocks are never rolled back"
        );
        self.chain.truncate(height as usize + 1);
        self.forks += 1;
    }

    fn block(&self, height: u64) -> &Block {
        &self.chain[height as usize]
    }

    fn reply(&mut self, path: &str, body: &[u8]) -> Vec<u8> {
        if path == "/on/graphql/query" {
            let query = String::from_utf8(body.to_vec()).unwrap();
            self.queries.push(query.clone());
            return self.graphql(&query).to_string().into_bytes();
        }
        let mailbox = format!("/on/contracts:{}/", hex::encode(MAILBOX));
        let announce = format!("/on/contracts:{}/", hex::encode(VALIDATOR_ANNOUNCE));
        if let Some(method) = path.strip_prefix(&mailbox) {
            assert_eq!(method, "delivered");
            let id: [u8; 32] = body.try_into().unwrap();
            let reply = rkyv::to_bytes::<_, 256>(&self.delivered.contains(&id))
                .unwrap()
                .to_vec();
            self.delivered_queries += 1;
            if self
                .after_delivered_queries
                .as_ref()
                .is_some_and(|(queries, _)| *queries == self.delivered_queries)
            {
                let (_, hook) = self.after_delivered_queries.take().unwrap();
                hook(self);
            }
            return reply;
        }
        if let Some(method) = path.strip_prefix(&announce) {
            assert_eq!(method, "get_announced_storage_location_hashes");
            assert_eq!(body, &VALIDATOR[..]);
            let hashes: Vec<[u8; 32]> = self
                .locations
                .iter()
                .map(|location| hyperlane_dusk_types::message::keccak256(location.as_bytes()))
                .collect();
            self.location_queries += 1;
            if self
                .after_location_queries
                .as_ref()
                .is_some_and(|(queries, _)| *queries == self.location_queries)
            {
                let (_, hook) = self.after_location_queries.take().unwrap();
                hook(self);
            }
            return rkyv::to_bytes::<_, 256>(&hashes).unwrap().to_vec();
        }
        panic!("unexpected request {path}");
    }

    /// Every event of the final blocks the archive has stored, numbered
    /// in chain order as the archive numbers its rows.
    fn finalized_rows(&self) -> Vec<(i64, &Block, &Event)> {
        self.chain
            .iter()
            .filter(|block| block.height <= self.finalized && block.archived)
            .flat_map(|block| block.events.iter().map(move |event| (block, event)))
            .enumerate()
            .map(|(id, (block, event))| (id as i64, block, event))
            .collect()
    }

    fn graphql(&self, query: &str) -> serde_json::Value {
        if query.contains("finalizedEvents(") && self.finalized_events_fail {
            return serde_json::json!({"errors": [{"message": "archive unavailable"}]});
        }
        if query.contains("finalizedEvents(") {
            let source = query
                .split_once("contractId: \"")
                .and_then(|(_, rest)| rest.split('"').next())
                .unwrap();
            let limit: usize = query
                .split_once("limit: ")
                .and_then(|(_, rest)| rest.split(|c: char| !c.is_ascii_digit()).next())
                .unwrap()
                .parse()
                .unwrap();
            let after = query.split_once("cursor: \"").map(|(_, rest)| {
                let cursor = rest.split('"').next().unwrap();
                let text = String::from_utf8(
                    base64::engine::general_purpose::STANDARD
                        .decode(cursor)
                        .unwrap(),
                )
                .unwrap();
                text.strip_prefix("v1:").unwrap().parse::<i64>().unwrap()
            });
            let mut page: Vec<_> = self
                .finalized_rows()
                .into_iter()
                .filter(|(id, _, event)| {
                    hex::encode(event.source) == source && *id > after.unwrap_or(-1)
                })
                .take(limit + 1)
                .map(|(id, block, event)| {
                    serde_json::json!({
                        "id": id, "block_height": block.height,
                        "block_hash": hex::encode(block.hash), "origin": "33".repeat(32),
                        "topic": event.topic, "source": hex::encode(event.source),
                        "data": hex::encode(&event.data), "reverted": event.reverted,
                    })
                })
                .collect();
            let has_next = page.len() > limit;
            page.truncate(limit);
            let cursor = |row: Option<&serde_json::Value>| {
                row.map(|row| {
                    base64::engine::general_purpose::STANDARD.encode(format!("v1:{}", row["id"]))
                })
            };
            return serde_json::json!({"finalizedEvents": {"json": {
                "events": page,
                "startCursor": cursor(page.first()),
                "endCursor": cursor(page.last()),
                "hasNextPage": has_next,
            }}});
        }
        if query.contains("checkBlock(") {
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
            let finalized = height <= self.finalized
                && self
                    .chain
                    .get(height as usize)
                    .is_some_and(|block| block.archived && hex::encode(block.hash) == hash);
            return serde_json::json!({ "checkBlock": finalized });
        }
        if let Some(rest) = query.strip_prefix("query { block(height: ") {
            let height: usize = rest.split(')').next().unwrap().parse().unwrap();
            return serde_json::json!({"block": self.chain.get(height).map(|block| {
                serde_json::json!({"header": {"hash": hex::encode(block.hash)}})
            })});
        }
        if query == "query { lastBlockPair { json } }" {
            let tip = self.block(self.tip() - self.unreported);
            let finalized = self.block(self.finalized);
            return serde_json::json!({"lastBlockPair": {"json": {
                "last_block": [tip.height, hex::encode(tip.hash)],
                "last_finalized_block": [finalized.height, hex::encode(finalized.hash)],
            }}});
        }
        if let Some(range) = query.strip_prefix("query { blocks(range: [") {
            let (range, rest) = range.split_once("])").unwrap();
            assert_eq!(rest, " { header { height hash prevBlockHash } } }");
            let (first, last) = range.split_once(", ").unwrap();
            let (first, last): (u64, u64) = (first.parse().unwrap(), last.parse().unwrap());
            let blocks: Vec<_> = (first..=last.min(self.tip()))
                .map(|height| {
                    let block = self.block(height);
                    serde_json::json!({"header": {
                        "height": block.height,
                        "hash": hex::encode(block.hash),
                        "prevBlockHash": hex::encode(block.prev),
                    }})
                })
                .collect();
            return serde_json::json!({ "blocks": blocks });
        }
        if let Some(hash) = query.strip_prefix("query { contractEvents(hash: \"") {
            let (hash, rest) = hash.split_once('"').unwrap();
            assert_eq!(rest, ") { json } }");
            // The archive drops a rolled-back block's events.
            let rows: Vec<_> = self
                .chain
                .iter()
                .filter(|block| hex::encode(block.hash) == hash && block.archived)
                .flat_map(|block| block.events.iter())
                .map(|event| {
                    serde_json::json!({
                        "origin": "33".repeat(32),
                        "topic": event.topic,
                        "source": hex::encode(event.source),
                        "data": hex::encode(&event.data),
                        "reverted": event.reverted,
                    })
                })
                .collect();
            return serde_json::json!({"contractEvents": {"json": rows}});
        }
        panic!("unexpected GraphQL query {query}");
    }
}

struct Fixture {
    url: Url,
    node: Arc<Mutex<Node>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
    _directory: tempfile::TempDir,
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
            stop,
            worker: Some(worker),
            _directory: tempfile::tempdir().unwrap(),
        }
    }

    fn node(&self) -> std::sync::MutexGuard<'_, Node> {
        self.node.lock().unwrap()
    }

    fn parts(
        &self,
    ) -> (
        HyperlaneDomain,
        Arc<RuesClient>,
        Arc<DuskProvider>,
        ConnectionConf,
    ) {
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-finality",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let rues = Arc::new(RuesClient::new(self.url.clone()).unwrap());
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        let conf = ConnectionConf {
            url: self.url.clone(),
            chain_id: 0,
            event_cursor_dir: self._directory.path().to_path_buf(),
            gas_limit: 5_000_000,
            gas_price: 2_000,
            native_token: NativeToken::default(),
            op_submission_config: OpSubmissionConfig::default(),
        };
        (domain, rues, provider, conf)
    }

    /// A Mailbox adapter whose client keeps a delivery log in the
    /// fixture's event store.
    fn mailbox_with_log(&self) -> DuskMailbox {
        let (domain, _, _, conf) = self.parts();
        let rues = Arc::new(
            RuesClient::new_with_event_cursor_dir(
                self.url.clone(),
                self._directory.path().join("events"),
            )
            .unwrap(),
        );
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        DuskMailbox::new(
            provider,
            rues,
            H256::from(MAILBOX),
            H256::repeat_byte(3),
            domain,
            None,
            conf,
        )
    }

    fn mailbox(&self) -> DuskMailbox {
        let (domain, rues, provider, conf) = self.parts();
        DuskMailbox::new(
            provider,
            rues,
            H256::from(MAILBOX),
            H256::repeat_byte(3),
            domain,
            None,
            conf,
        )
    }

    /// An announcement reader whose client keeps the announcement log in
    /// the fixture's event store, as relayers read locations.
    fn announcements_with_log(&self) -> DuskValidatorAnnounce {
        let (domain, _, _, conf) = self.parts();
        let rues = Arc::new(
            RuesClient::new_with_event_cursor_dir(
                self.url.clone(),
                self._directory.path().join("events"),
            )
            .unwrap(),
        );
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        DuskValidatorAnnounce::new(
            provider,
            rues,
            H256::from(VALIDATOR_ANNOUNCE),
            domain,
            None,
            conf,
        )
    }

    fn announcements(&self) -> DuskValidatorAnnounce {
        let (domain, rues, provider, conf) = self.parts();
        DuskValidatorAnnounce::new(
            provider,
            rues,
            H256::from(VALIDATOR_ANNOUNCE),
            domain,
            None,
            conf,
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

fn message_id() -> H256 {
    H256::from(MESSAGE)
}

fn validator() -> H256 {
    let mut validator = [0u8; 32];
    validator[12..].copy_from_slice(&VALIDATOR);
    H256::from(validator)
}

async fn delivered(fixture: &Fixture) -> Result<bool, String> {
    fixture
        .mailbox()
        .delivered(message_id())
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn a_delivery_counts_only_once_its_block_is_final() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    node.push(vec![]);
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);

    // The tip holds the delivery, but its block can still be rolled back.
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(
        error.contains(&format!("delivered in Dusk block {block}")) && error.contains("not final"),
        "{error}"
    );

    // Once that block is final, the delivery counts.
    fixture.node().finalized = block;
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
    // Only through the delivery log: a client that keeps none never counts
    // it, final or not.
    let error = delivered(&fixture).await.unwrap_err();
    assert!(
        error.contains("cannot show it final yet") && error.contains("no eventCursorDir"),
        "{error}"
    );
}

#[tokio::test]
async fn a_rolled_back_delivery_is_not_delivered_and_counts_again_once_final() {
    let mut node = Node::new(100);
    node.push(vec![Event::processed(MESSAGE)]);
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    assert!(delivered_with_log(&fixture).await.is_err());

    // A rollback drops the delivery: the relayer must relay the message again.
    {
        let mut node = fixture.node();
        node.roll_back_to(100);
        node.push(vec![]);
        node.delivered.clear();
    }
    assert_eq!(delivered_with_log(&fixture).await, Ok(false));

    // A new delivery waits for its own block.
    let block = {
        let mut node = fixture.node();
        node.delivered.insert(MESSAGE);
        let block = node.push(vec![Event::processed(MESSAGE)]);
        node.push(vec![]);
        block
    };
    assert!(delivered_with_log(&fixture).await.is_err());
    fixture.node().finalized = block + 1;
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
}

#[tokio::test]
async fn a_rollback_while_a_delivery_is_checked_is_noticed() {
    // The state shows the delivery when the check reads it, and its block is
    // rolled back before the blocks above the last final one are read. Read
    // after the rollback alone, those blocks hold no event for it.
    for replaced in [false, true] {
        let mut node = Node::new(100);
        node.push(vec![Event::processed(MESSAGE)]);
        node.delivered.insert(MESSAGE);
        node.after_delivered_queries = Some((
            2,
            Box::new(move |node: &mut Node| {
                node.roll_back_to(100);
                if replaced {
                    node.push(vec![]);
                }
                node.delivered.clear();
            }),
        ));
        let fixture = Fixture::start(node);
        let error = delivered_with_log(&fixture).await.unwrap_err();
        assert!(
            error.contains("chain changed since its tip was block 101"),
            "{error}"
        );
        assert_eq!(delivered_with_log(&fixture).await, Ok(false));
    }
}

#[tokio::test]
async fn an_undelivered_message_reads_no_blocks() {
    let mut node = Node::new(100);
    node.push(vec![Event::processed([0x12; 32])]);
    let fixture = Fixture::start(node);
    assert_eq!(delivered(&fixture).await, Ok(false));
    assert!(fixture.node().queries.is_empty());
}

#[tokio::test]
async fn events_that_are_not_the_delivery_never_make_it_count() {
    let decoys = || {
        vec![
            // Another message's delivery.
            Event::processed([0x12; 32]),
            // A failed attempt to deliver this message.
            Event {
                reverted: true,
                ..Event::processed(MESSAGE)
            },
            // The same bytes from another contract.
            Event {
                source: VALIDATOR_ANNOUNCE,
                ..Event::processed(MESSAGE)
            },
        ]
    };
    let mut node = Node::new(100);
    // Decoys in a final block, and in a block that is not final yet.
    node.push(decoys());
    node.finalized = 101;
    node.push(decoys());
    node.push(vec![]);
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    let mailbox = fixture.mailbox_with_log();
    let error = mailbox
        .delivered(message_id())
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("no finalized block up to block 101") && error.contains("no block above"),
        "{error}"
    );

    // A client reads each block's events once.
    assert!(mailbox.delivered(message_id()).await.is_err());
    let events_read = fixture
        .node()
        .queries
        .iter()
        .filter(|query| query.contains("contractEvents"))
        .count();
    assert_eq!(events_read, 2);

    // The delivery itself counts once its block is final.
    let block = fixture.node().push(vec![Event::processed(MESSAGE)]);
    fixture.node().finalized = block;
    assert!(mailbox.delivered(message_id()).await.unwrap());
}

#[tokio::test]
async fn a_block_the_archive_has_not_stored_keeps_the_delivery_unsettled() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    node.chain[block as usize].archived = false;
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(error.contains("has not stored the events"), "{error}");

    {
        let mut node = fixture.node();
        node.chain[block as usize].archived = true;
        node.finalized = block;
    }
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
}

#[tokio::test]
async fn a_block_stored_after_the_reported_tip_is_read_too() {
    // The state a query reads can hold a block that the ledger stores only
    // after `lastBlockPair` has answered.
    let mut node = Node::new(100);
    node.push(vec![]);
    node.push(vec![Event::processed(MESSAGE)]);
    node.unreported = 1;
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(error.contains("delivered in Dusk block 102"), "{error}");
}

#[tokio::test]
async fn a_finality_stall_waits_instead_of_reading_every_block() {
    let mut node = Node::new(100);
    node.push(vec![]);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    for _ in 0..63 {
        node.push(vec![]);
    }
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(error.contains("65 blocks behind the tip"), "{error}");
    let queries = fixture.node().queries.clone();
    assert!(
        queries
            .iter()
            .all(|query| !query.contains("blocks(range") && !query.contains("contractEvents")),
        "{queries:?}"
    );

    // At 64 blocks behind, the check reads them and names the delivery's.
    fixture.node().finalized = 101;
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(
        error.contains(&format!("delivered in Dusk block {block}")),
        "{error}"
    );
    fixture.node().finalized = block;
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
}

#[tokio::test]
async fn validator_startup_waits_until_its_announcement_is_final() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::announced(VALIDATOR, LOCATION)]);
    node.push(vec![]);
    node.locations = vec![LOCATION.to_owned()];
    let fixture = Fixture::start(node);
    let strict = fixture.announcements_with_log().with_own_location(LOCATION);

    let error = strict
        .get_announced_storage_locations(&[validator()])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains(LOCATION) && error.contains("no finalized block up to block 100"),
        "{error}"
    );

    // Relayers read locations from finalized announcements only.
    let relayer = fixture.announcements_with_log();
    assert_eq!(
        relayer
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![Vec::<String>::new()]
    );

    fixture.node().finalized = block;
    assert_eq!(
        strict
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![vec![LOCATION.to_owned()]]
    );
    assert_eq!(
        relayer
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![vec![LOCATION.to_owned()]]
    );
    // A reader without an announcement log never reports it, final or not.
    let error = fixture
        .announcements()
        .with_own_location(LOCATION)
        .get_announced_storage_locations(&[validator()])
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no eventCursorDir"), "{error}");
}

#[tokio::test]
async fn with_an_announcement_log_startup_counts_only_a_finalized_announcement() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::announced(VALIDATOR, LOCATION)]);
    node.push(vec![]);
    node.locations = vec![LOCATION.to_owned()];
    let fixture = Fixture::start(node);
    let strict = fixture.announcements_with_log().with_own_location(LOCATION);
    let error = strict
        .get_announced_storage_locations(&[validator()])
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("no finalized block up to block 100"),
        "{error}"
    );
    fixture.node().finalized = block;
    assert_eq!(
        strict
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![vec![LOCATION.to_owned()]]
    );
    // The finalized announcement settled it: no block above the last final
    // one was read.
    assert!(fixture
        .node()
        .queries
        .iter()
        .all(|query| !query.contains("blocks(range")));
}

#[tokio::test]
async fn a_validator_that_has_not_announced_reads_no_blocks() {
    let mut node = Node::new(100);
    node.push(vec![Event::announced([0x44; 20], LOCATION)]);
    let fixture = Fixture::start(node);
    assert_eq!(
        fixture
            .announcements()
            .with_own_location(LOCATION)
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![Vec::<String>::new()]
    );
    assert!(fixture.node().queries.is_empty());
}

#[tokio::test]
async fn a_rolled_back_announcement_is_not_announced() {
    let mut node = Node::new(100);
    node.push(vec![Event::announced(VALIDATOR, LOCATION)]);
    node.locations = vec![LOCATION.to_owned()];
    let fixture = Fixture::start(node);
    let strict = fixture.announcements().with_own_location(LOCATION);
    assert!(strict
        .get_announced_storage_locations(&[validator()])
        .await
        .is_err());

    // The validator announces again once the location is gone from state.
    {
        let mut node = fixture.node();
        node.roll_back_to(100);
        node.push(vec![]);
        node.locations.clear();
    }
    assert_eq!(
        strict
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![Vec::<String>::new()]
    );
}

#[tokio::test]
async fn another_validators_pending_announcement_does_not_hold_startup() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::announced(VALIDATOR, LOCATION)]);
    node.push(vec![Event::announced([0x44; 20], LOCATION)]);
    node.locations = vec![LOCATION.to_owned()];
    node.finalized = block;
    let fixture = Fixture::start(node);
    assert_eq!(
        fixture
            .announcements_with_log()
            .with_own_location(LOCATION)
            .get_announced_storage_locations(&[validator()])
            .await
            .unwrap(),
        vec![vec![LOCATION.to_owned()]]
    );
}

async fn delivered_with_log(fixture: &Fixture) -> Result<bool, String> {
    fixture
        .mailbox_with_log()
        .delivered(message_id())
        .await
        .map_err(|error| error.to_string())
}

#[tokio::test]
async fn a_delivery_the_delivery_log_finds_final_reads_no_non_final_block() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    node.push(vec![]);
    node.delivered.insert(MESSAGE);
    node.finalized = block;
    let fixture = Fixture::start(node);
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
    let queries = fixture.node().queries.clone();
    assert!(
        queries
            .iter()
            .all(|query| !query.contains("blocks(range") && !query.contains("contractEvents")),
        "{queries:?}"
    );
}

#[tokio::test]
async fn a_delivery_rolled_back_within_the_check_never_counts_with_a_delivery_log() {
    // The state holds a delivery in block 101, which the ledger stores only
    // after `lastBlockPair` has answered. Between the check's state reads
    // and its read of the blocks that are not final, block 101 is rolled
    // back and replaced by a block without the delivery: those blocks then
    // hold no event of it, and the chain still extends the tip the check
    // read. Only the finalized deliveries can show that it is not final.
    let mut node = Node::new(100);
    node.push(vec![Event::processed(MESSAGE)]);
    node.unreported = 1;
    node.delivered.insert(MESSAGE);
    node.after_delivered_queries = Some((
        2,
        Box::new(|node: &mut Node| {
            node.roll_back_to(100);
            node.push(vec![]);
            node.unreported = 0;
            node.delivered.clear();
        }),
    ));
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(
        error.contains("no finalized block up to block 100"),
        "{error}"
    );
    // The relayer checks again later and relays the message again.
    assert_eq!(delivered_with_log(&fixture).await, Ok(false));
}

#[tokio::test]
async fn a_delivery_log_short_of_the_last_final_block_waits_for_it() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    node.push(vec![]);
    node.delivered.insert(MESSAGE);
    node.finalized = block;
    // The archive has not stored the last final block yet, so the log
    // cannot hold every final delivery. The blocks above the last final one
    // hold no event of it, which does not make it final.
    node.chain[block as usize].archived = false;
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(
        error.contains("cannot show it final yet")
            && error.contains("short of the last final block"),
        "{error}"
    );
    assert!(fixture
        .node()
        .queries
        .iter()
        .all(|query| !query.contains("blocks(range")));

    // Once the archive stores it, the log finds the delivery there.
    fixture.node().chain[block as usize].archived = true;
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
}

#[tokio::test]
async fn with_a_delivery_log_a_delivery_in_a_block_that_is_not_final_waits_for_it() {
    let mut node = Node::new(100);
    let block = node.push(vec![Event::processed(MESSAGE)]);
    node.push(vec![]);
    node.delivered.insert(MESSAGE);
    let fixture = Fixture::start(node);
    let error = delivered_with_log(&fixture).await.unwrap_err();
    assert!(
        error.contains(&format!("delivered in Dusk block {block}")) && error.contains("not final"),
        "{error}"
    );
    fixture.node().finalized = block;
    assert_eq!(delivered_with_log(&fixture).await, Ok(true));
}

/// What a check can learn from its finalized event log while a rollback
/// happens under it.
#[derive(Clone, Copy, Debug)]
enum LogCondition {
    /// The client keeps no event log: it has no `eventCursorDir`.
    Missing,
    /// The log cannot reach the last final block, which the archive has not
    /// stored yet.
    Behind,
    /// Reading the archive for the log fails.
    Failing,
}

impl LogCondition {
    const ALL: [Self; 3] = [Self::Missing, Self::Behind, Self::Failing];

    fn apply(self, node: &mut Node) {
        match self {
            Self::Missing => {}
            Self::Behind => {
                let last_final = node.finalized as usize;
                node.chain[last_final].archived = false;
            }
            Self::Failing => node.finalized_events_fail = true,
        }
    }
}

#[tokio::test]
async fn a_delivery_rolled_back_within_the_check_never_counts_without_a_complete_delivery_log() {
    // As in `a_delivery_rolled_back_within_the_check_never_counts_with_a_delivery_log`:
    // the state holds a delivery in block 101, which `lastBlockPair` does
    // not report yet, and block 101 is replaced by a block without it
    // between the check's state reads and its read of the blocks that are
    // not final. Here the delivery log cannot show what the final blocks
    // hold. That the blocks above the last final one hold no event of the
    // delivery then shows nothing: the delivery must not count.
    for condition in LogCondition::ALL {
        let mut node = Node::new(100);
        node.push(vec![Event::processed(MESSAGE)]);
        node.unreported = 1;
        node.delivered.insert(MESSAGE);
        node.after_delivered_queries = Some((
            2,
            Box::new(|node: &mut Node| {
                node.roll_back_to(100);
                node.push(vec![]);
                node.unreported = 0;
                node.delivered.clear();
            }),
        ));
        condition.apply(&mut node);
        let fixture = Fixture::start(node);
        let mailbox = match condition {
            LogCondition::Missing => fixture.mailbox(),
            LogCondition::Behind | LogCondition::Failing => fixture.mailbox_with_log(),
        };
        let first = mailbox
            .delivered(message_id())
            .await
            .map_err(|error| error.to_string());
        let error = first.expect_err(&format!("{condition:?}: a rolled-back delivery counted"));
        assert!(
            error.contains("delivered at the Dusk tip") && error.contains("final"),
            "{condition:?}: {error}"
        );
        // The relayer checks again later and relays the message again.
        assert_eq!(
            mailbox
                .delivered(message_id())
                .await
                .map_err(|error| error.to_string()),
            Ok(false),
            "{condition:?}"
        );
    }
}

#[tokio::test]
async fn an_announcement_rolled_back_within_the_check_never_counts_without_a_complete_log() {
    // The same race at validator startup: the location list holds the
    // announcement of block 101, which is replaced by a block without it
    // after the check read the list twice.
    for condition in LogCondition::ALL {
        let mut node = Node::new(100);
        node.push(vec![Event::announced(VALIDATOR, LOCATION)]);
        node.unreported = 1;
        node.locations = vec![LOCATION.to_owned()];
        node.after_location_queries = Some((
            2,
            Box::new(|node: &mut Node| {
                node.roll_back_to(100);
                node.push(vec![]);
                node.unreported = 0;
                node.locations.clear();
            }),
        ));
        condition.apply(&mut node);
        let fixture = Fixture::start(node);
        let strict = match condition {
            LogCondition::Missing => fixture.announcements(),
            LogCondition::Behind | LogCondition::Failing => fixture.announcements_with_log(),
        }
        .with_own_location(LOCATION);
        let first = strict
            .get_announced_storage_locations(&[validator()])
            .await
            .map_err(|error| error.to_string());
        let error = first.expect_err(&format!(
            "{condition:?}: a rolled-back announcement counted"
        ));
        assert!(
            error.contains(LOCATION) && error.contains("final"),
            "{condition:?}: {error}"
        );
        // The validator checks again and, the location gone, announces.
        assert_eq!(
            strict
                .get_announced_storage_locations(&[validator()])
                .await
                .map_err(|error| error.to_string()),
            Ok(vec![Vec::<String>::new()]),
            "{condition:?}"
        );
    }
}
