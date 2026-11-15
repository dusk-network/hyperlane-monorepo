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
use hyperlane_dusk::{ConnectionConf, DuskMailbox, DuskMerkleTreeHook, DuskProvider, RuesClient};
use url::Url;

const HOOK: [u8; 32] = [2; 32];

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
    /// Each contract query: its method and its arguments.
    queries: Vec<(String, Vec<u8>)>,
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
                "last_block": [self.finalized + 5, "tip"],
                "last_finalized_block": [self.finalized, "final"],
            }}})
            .to_string()
            .into_bytes();
        }
        let method = path
            .strip_prefix(&format!("/on/contracts:{}/", hex::encode(HOOK)))
            .unwrap()
            .to_owned();
        self.queries.push((method.clone(), body.to_vec()));
        let index = || u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
        match method.as_str() {
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

    fn adapter(&self) -> DuskMerkleTreeHook {
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-roots",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let rues = Arc::new(RuesClient::new(self.url.clone()).unwrap());
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
    fixture.hook_state().push(100, 20);
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
    // Clones of the adapter share the tree.
    fixture.hook_state().queries.clear();
    hook.clone()
        .latest_checkpoint(&ReorgPeriod::None)
        .await
        .unwrap();
    assert!(fixture.hook_state().pages().is_empty());
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
