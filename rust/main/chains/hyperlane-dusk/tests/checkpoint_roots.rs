//! Checkpoint roots computed from the MerkleTreeHook's stored message IDs,
//! through the public adapter and a scripted node, and checked against the
//! hook's own tree.
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use hyperlane_core::{
    accumulator::incremental::IncrementalMerkle, config::OpSubmissionConfig, HyperlaneDomain,
    HyperlaneDomainProtocol, HyperlaneDomainTechnicalStack, MerkleTreeHook, NativeToken,
    ReorgPeriod, H256,
};
use hyperlane_dusk::multicall::{Aggregate, Call};
use hyperlane_dusk::{ConnectionConf, DuskMailbox, DuskMerkleTreeHook, DuskProvider, RuesClient};
use url::Url;

const HOOK: [u8; 32] = [2; 32];
const MULTICALL: [u8; 32] = [0x93; 32];

fn id(leaf: usize) -> [u8; 32] {
    hyperlane_dusk_types::message::keccak256(&(leaf as u64).to_be_bytes())
}

/// The root hyperlane-core computes for these leaves.
fn root_of(ids: &[[u8; 32]]) -> H256 {
    let mut tree = IncrementalMerkle::default();
    for id in ids {
        tree.ingest(H256::from(*id));
    }
    tree.root()
}

/// The hook's state, and how the node serves it.
#[derive(Default)]
struct Hook {
    /// Stored message IDs and their insertion heights.
    ids: Vec<[u8; 32]>,
    heights: Vec<u64>,
    finalized: u64,
    /// IDs `message_ids` serves instead of the stored ones, by leaf index.
    served: HashMap<usize, [u8; 32]>,
    /// Serve every `message_ids` page from this leaf on one ID short.
    short_from: Option<usize>,
    /// The root `root` serves instead of the tree's.
    served_root: Option<[u8; 32]>,
    /// The height aggregates report instead of the tip's.
    state_height: Option<u64>,
    /// Each contract query, and each call of an aggregate: its method and
    /// its arguments.
    queries: Vec<(String, Vec<u8>)>,
    /// The methods each aggregate called.
    aggregates: Vec<Vec<String>>,
}

impl Hook {
    fn push(&mut self, count: usize, height: u64) {
        for _ in 0..count {
            self.ids.push(id(self.ids.len()));
            self.heights.push(height);
        }
    }

    fn tree(&self) -> hyperlane_dusk_types::merkle::IncrementalMerkle {
        let mut tree = hyperlane_dusk_types::merkle::IncrementalMerkle::new();
        for id in &self.ids {
            tree.insert(*id);
        }
        tree
    }

    /// The `(start, limit)` of each `message_ids` query.
    fn pages(&self) -> Vec<(u32, u32)> {
        self.queries
            .iter()
            .filter(|(method, _)| method == "message_ids")
            .map(|(_, body)| {
                (
                    u32::from_le_bytes(body[..4].try_into().unwrap()),
                    u32::from_le_bytes(body[4..8].try_into().unwrap()),
                )
            })
            .collect()
    }

    fn reply(&mut self, path: &str, body: &[u8]) -> Vec<u8> {
        if path == "/on/graphql/query" {
            assert_eq!(body, b"query { lastBlockPair { json } }");
            return serde_json::json!({"lastBlockPair": {"json": {
                "last_block": [self.finalized + 5, "7777777777777777777777777777777777777777777777777777777777777777"],
                "last_finalized_block": [self.finalized, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"],
            }}})
            .to_string()
            .into_bytes();
        }
        if let Some(method) =
            path.strip_prefix(&format!("/on/contracts:{}/", hex::encode(MULTICALL)))
        {
            if method == "version" {
                return rkyv::to_bytes::<_, 256>(&1u32).unwrap().to_vec();
            }
            assert_eq!(method, "aggregate");
            let calls: Vec<Call> = hyperlane_dusk::rkyv_deserialize(body).unwrap();
            self.aggregates
                .push(calls.iter().map(|call| call.fn_name.clone()).collect());
            let results = calls
                .iter()
                .map(|call| {
                    assert_eq!(call.contract, HOOK);
                    Ok(self.view(&call.fn_name, &call.fn_args))
                })
                .collect();
            let aggregate = Aggregate {
                height: self.state_height.unwrap_or(self.finalized + 5),
                results,
            };
            return rkyv::to_bytes::<_, 256>(&aggregate).unwrap().to_vec();
        }
        let method = path
            .strip_prefix(&format!("/on/contracts:{}/", hex::encode(HOOK)))
            .unwrap()
            .to_owned();
        self.view(&method, body)
    }

    fn view(&mut self, method: &str, body: &[u8]) -> Vec<u8> {
        self.queries.push((method.to_owned(), body.to_vec()));
        let index = || u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
        match method {
            "count" => (self.ids.len() as u32).to_le_bytes().to_vec(),
            "inserted_block_height" => self.heights[index()].to_le_bytes().to_vec(),
            "message_id_at" => self.ids[index()].to_vec(),
            "message_ids" => {
                let start = index();
                let limit = u32::from_le_bytes(body[4..8].try_into().unwrap()) as usize;
                assert!(limit <= 256, "MerkleTreeHook: page too large");
                let served = if self.short_from.is_some_and(|from| start >= from) {
                    limit - 1
                } else {
                    limit
                };
                let page: Vec<[u8; 32]> = (start..start + served)
                    .map(|leaf| *self.served.get(&leaf).unwrap_or(&self.ids[leaf]))
                    .collect();
                rkyv::to_bytes::<_, 256>(&page).unwrap().to_vec()
            }
            "tree" => rkyv::to_bytes::<_, 256>(&self.tree()).unwrap().to_vec(),
            "root" => self.served_root.unwrap_or(self.tree().root()).to_vec(),
            method => panic!("unexpected contract query: {method}"),
        }
    }
}

struct Fixture {
    url: Url,
    hook: Arc<Mutex<Hook>>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Fixture {
    fn start(hook: Hook) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let hook = Arc::new(Mutex::new(hook));
        let stop = Arc::new(AtomicBool::new(false));
        let worker = {
            let hook = hook.clone();
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
                    let reply = hook.lock().unwrap().reply(&path, &body);
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
            hook,
            stop,
            worker: Some(worker),
        }
    }

    fn hook_state(&self) -> std::sync::MutexGuard<'_, Hook> {
        self.hook.lock().unwrap()
    }

    /// Let a tick pass: every client of the node reads its last final block
    /// again.
    async fn next_tick(&self) {
        RuesClient::new(self.url.clone())
            .unwrap()
            .expire_block_pair()
            .await;
    }

    fn adapter(&self) -> DuskMerkleTreeHook {
        self.adapter_with(None)
    }

    /// The adapter of a chain that names `multicall`.
    fn adapter_with(&self, multicall: Option<[u8; 32]>) -> DuskMerkleTreeHook {
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-roots",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let rues = Arc::new(
            RuesClient::new(self.url.clone())
                .unwrap()
                .with_multicall(multicall),
        );
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        DuskMerkleTreeHook::new(DuskMailbox::new(
            provider,
            rues,
            H256::from([1; 32]),
            H256::from(HOOK),
            domain,
            None,
            ConnectionConf {
                url: self.url.clone(),
                chain_id: 0,
                event_cursor_dir: std::env::temp_dir(),
                gas_limit: 1,
                gas_price: 1,
                native_token: NativeToken::default(),
                op_submission_config: OpSubmissionConfig::default(),
                query_rate_limit: hyperlane_dusk::QueryRateLimit::default(),
            },
        ))
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

fn hook_with(count: usize, height: u64, finalized: u64) -> Hook {
    let mut hook = Hook {
        finalized,
        ..Hook::default()
    };
    hook.push(count, height);
    hook
}

#[tokio::test]
async fn a_checkpoint_root_is_computed_from_the_stored_ids_and_checked_against_the_hook() {
    let fixture = Fixture::start(hook_with(600, 10, 100));
    let hook = fixture.adapter();
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    let ids = fixture.hook_state().ids.clone();
    assert_eq!(checkpoint.checkpoint.index, 599);
    assert_eq!(checkpoint.checkpoint.root, root_of(&ids));
    assert_eq!(
        checkpoint.checkpoint.merkle_tree_hook_address,
        H256::from(HOOK)
    );
    // Pages of at most 256, then the last ID and the hook's own tree, since
    // the hook holds exactly these leaves.
    {
        let state = fixture.hook_state();
        assert_eq!(state.pages(), [(0, 256), (256, 256), (512, 88)]);
        let checks: Vec<_> = state
            .queries
            .iter()
            .map(|(method, _)| method.as_str())
            .filter(|method| ["message_id_at", "tree"].contains(method))
            .collect();
        assert_eq!(checks, ["message_id_at", "tree"]);
    }

    // The start tree a validator reads is the same tree.
    let tree = hook.tree(&ReorgPeriod::None).await.unwrap();
    assert_eq!(tree.tree.count(), 600);
    assert_eq!(tree.tree.root(), root_of(&ids));
    assert_eq!(tree.block_height, Some(100));
}

#[tokio::test]
async fn a_later_checkpoint_reads_only_the_new_ids() {
    let fixture = Fixture::start(hook_with(300, 10, 100));
    let hook = fixture.adapter();
    hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    // 100 more insertions in block 101, which the node then finalizes.
    {
        let mut state = fixture.hook_state();
        state.push(100, 101);
        state.finalized = 101;
    }
    fixture.next_tick().await;
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(checkpoint.checkpoint.index, 399);
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    assert_eq!(
        fixture.hook_state().pages(),
        [(0, 256), (256, 44), (300, 100)]
    );
    // Clones of the adapter share the tree, and until the node finalizes
    // another block, its checkpoint: no contract query at all.
    fixture.hook_state().queries.clear();
    hook.clone()
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap();
    assert!(fixture.hook_state().queries.is_empty());
    // The next final block holds no insertion: one count, and no check of
    // a tree already checked.
    fixture.hook_state().finalized = 102;
    fixture.next_tick().await;
    hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    let methods: Vec<String> = fixture
        .hook_state()
        .queries
        .iter()
        .map(|(method, _)| method.clone())
        .collect();
    assert_eq!(methods, ["count"]);
}

#[tokio::test]
async fn an_earlier_checkpoint_is_built_on_its_own() {
    let mut state = hook_with(200, 10, 30);
    state.push(100, 20);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter();
    hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    let ids = fixture.hook_state().ids.clone();

    let earlier = hook.latest_checkpoint_at_block(15).await.unwrap();
    assert_eq!(earlier.checkpoint.index, 199);
    assert_eq!(earlier.checkpoint.root, root_of(&ids[..200]));
    assert_eq!(earlier.block_height, Some(15));
    // Its tree has fewer leaves than the hook: only its last ID can be
    // checked, and the shared tree of 300 leaves is kept.
    fixture.hook_state().queries.clear();
    let latest = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(latest.checkpoint.root, root_of(&ids));
    assert!(fixture.hook_state().pages().is_empty());
}

#[tokio::test]
async fn ids_that_disagree_with_the_hook_are_never_signed_and_are_discarded() {
    let mut state = hook_with(5, 10, 100);
    // The endpoint serves another ID at leaf 2 than the hook's tree holds.
    state.served.insert(2, [0xee; 32]);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter();
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("disagrees with the hook") && error.contains("the hook's own tree"),
        "{error}"
    );
    assert!(hook.tree(&ReorgPeriod::None).await.is_err());

    // Once it serves the hook's IDs, the tree is rebuilt from the first leaf.
    fixture.hook_state().served.clear();
    fixture.hook_state().queries.clear();
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    assert_eq!(fixture.hook_state().pages(), [(0, 5)]);
}

#[tokio::test]
async fn a_last_id_that_disagrees_with_the_hook_fails_before_later_leaves_are_final() {
    // Five final insertions and three in blocks that are not final yet, so
    // the hook's own tree holds more leaves than the checkpoint.
    let mut state = hook_with(5, 10, 100);
    state.push(3, 200);
    state.served.insert(4, [0xee; 32]);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter();
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("message_id_at(4)"), "{error}");
}

#[tokio::test]
async fn a_short_page_fails_and_keeps_the_ids_read_before_it() {
    let fixture = Fixture::start(hook_with(600, 10, 100));
    let hook = fixture.adapter();
    fixture.hook_state().short_from = Some(256);
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("returned 255 message IDs for page start=256 limit=256"),
        "{error}"
    );
    // The first page was ingested and nothing of the short one: the retry
    // reads on from the second page.
    fixture.hook_state().short_from = None;
    fixture.hook_state().queries.clear();
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    assert_eq!(fixture.hook_state().pages(), [(256, 256), (512, 88)]);
}

/// The methods of the queries sent on their own, not in an aggregate.
fn single_queries(hook: &Hook) -> Vec<String> {
    let in_aggregates: usize = hook.aggregates.iter().map(Vec::len).sum();
    let mut queries: Vec<String> = hook
        .queries
        .iter()
        .map(|(method, _)| method.clone())
        .collect();
    // Calls of aggregates are recorded as they are made, after the
    // queries sent before them; count them out by kind.
    let mut calls: HashMap<String, usize> = HashMap::new();
    for method in hook.aggregates.iter().flatten() {
        *calls.entry(method.clone()).or_default() += 1;
    }
    queries.retain(|method| match calls.get_mut(method) {
        Some(left) if *left > 0 => {
            *left -= 1;
            false
        }
        _ => true,
    });
    assert_eq!(
        queries.len() + in_aggregates,
        hook.queries.len(),
        "every call is a query"
    );
    queries
}

const SNAPSHOT_CHECKS: [&str; 4] = ["count", "root", "tree", "message_id_at"];

#[tokio::test]
async fn with_a_multicall_the_checks_and_last_pages_are_one_aggregate_at_one_state() {
    let fixture = Fixture::start(hook_with(600, 10, 100));
    let hook = fixture.adapter_with(Some(MULTICALL));
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    let ids = fixture.hook_state().ids.clone();
    assert_eq!(checkpoint.checkpoint.index, 599);
    assert_eq!(checkpoint.checkpoint.root, root_of(&ids));
    {
        let state = fixture.hook_state();
        // The probe, the search's two reads of 256 and 2 insertion
        // heights, then the three pages with the hook's checks, at one
        // state, instead of the pages, `message_id_at` and `tree` apart.
        let sizes: Vec<usize> = state.aggregates.iter().map(Vec::len).collect();
        assert_eq!(sizes, [0, 256, 2, 7]);
        let mut snapshot = vec!["message_ids"; 3];
        snapshot.extend(SNAPSHOT_CHECKS);
        assert_eq!(state.aggregates[3], snapshot);
        assert_eq!(state.pages(), [(0, 256), (256, 256), (512, 88)]);
        // Only the count, read once per final block, went on its own.
        assert_eq!(single_queries(&state), ["count"]);
    }
    // The start tree a validator reads is the same tree, and a checked
    // tree is not checked again.
    fixture.hook_state().aggregates.clear();
    let tree = hook.tree(&ReorgPeriod::None).await.unwrap();
    assert_eq!(tree.tree.root(), root_of(&ids));
    assert!(fixture.hook_state().aggregates.is_empty());
}

#[tokio::test]
async fn with_a_multicall_a_later_checkpoint_reads_its_new_ids_with_the_checks() {
    let fixture = Fixture::start(hook_with(300, 10, 100));
    let hook = fixture.adapter_with(Some(MULTICALL));
    hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    {
        let mut state = fixture.hook_state();
        state.push(100, 101);
        state.finalized = 101;
        state.aggregates.clear();
        state.queries.clear();
    }
    fixture.next_tick().await;
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(checkpoint.checkpoint.index, 399);
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    let state = fixture.hook_state();
    // The heights of the 100 new insertions in one read, then their IDs
    // and the checks in another: two aggregates and the count.
    let mut snapshot = vec!["message_ids"];
    snapshot.extend(SNAPSHOT_CHECKS);
    assert_eq!(state.aggregates.len(), 2);
    assert_eq!(state.aggregates[0].len(), 100);
    assert_eq!(state.aggregates[1], snapshot);
    assert_eq!(state.pages(), [(300, 100)]);
    assert_eq!(single_queries(&state), ["count"]);
}

#[tokio::test]
async fn with_a_multicall_a_long_history_reads_its_first_pages_before_the_snapshot() {
    let fixture = Fixture::start(hook_with(3_000, 10, 100));
    let hook = fixture.adapter_with(Some(MULTICALL));
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    let state = fixture.hook_state();
    // Twelve pages: four ahead in one aggregate, then the seven full pages
    // and the short last one that fit beside the checks.
    let count = state.aggregates.len();
    assert_eq!(state.aggregates[count - 2], ["message_ids"; 4]);
    let snapshot = &state.aggregates[count - 1];
    assert_eq!(snapshot.len(), 8 + 4);
    assert_eq!(snapshot[8..], SNAPSHOT_CHECKS);
    assert_eq!(state.pages().len(), 12);
}

#[tokio::test]
async fn with_a_multicall_a_root_that_disagrees_is_never_signed_and_is_discarded() {
    let mut state = hook_with(5, 10, 100);
    state.served_root = Some([0xee; 32]);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter_with(Some(MULTICALL));
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("disagrees with the hook") && error.contains("the hook's root"),
        "{error}"
    );
    // The tree was dropped: once the hook answers its own root, the tree
    // is rebuilt from the first leaf.
    fixture.hook_state().served_root = None;
    fixture.hook_state().queries.clear();
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    assert_eq!(fixture.hook_state().pages(), [(0, 5)]);
}

#[tokio::test]
async fn with_a_multicall_ids_that_disagree_with_the_hook_are_never_signed() {
    let mut state = hook_with(5, 10, 100);
    state.served.insert(2, [0xee; 32]);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter_with(Some(MULTICALL));
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("disagrees with the hook") && error.contains("the hook's own tree"),
        "{error}"
    );
}

#[tokio::test]
async fn with_a_multicall_a_state_below_the_checkpoints_block_is_read_again_later() {
    let mut state = hook_with(5, 10, 100);
    // A node behind the URL whose state lags the final block the count was
    // found at.
    state.state_height = Some(99);
    let fixture = Fixture::start(state);
    let hook = fixture.adapter_with(Some(MULTICALL));
    let error = hook
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("state at height 99 is below block 100") && !error.contains("disagrees"),
        "{error}"
    );
    // Nothing of that state was taken: the next read, once the state is not
    // behind, reads the IDs with the checks.
    fixture.hook_state().state_height = None;
    fixture.hook_state().queries.clear();
    let checkpoint = hook.latest_checkpoint(&ReorgPeriod::None).await.unwrap();
    assert_eq!(
        checkpoint.checkpoint.root,
        root_of(&fixture.hook_state().ids)
    );
    assert_eq!(fixture.hook_state().pages(), [(0, 5)]);
}
