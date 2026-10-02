//! Dusk checkpoint endpoints retain their voting slots while offline and must
//! authenticate chain and contract identities before producing state reads.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use hyperlane_base::{
    settings::{parser::RawAgentConf, ChainConf, Settings},
    CoreMetrics,
};
use hyperlane_core::{
    config::{ConfigPath, FromRawConf},
    CheckpointAtBlock, MerkleTreeHook, ReorgPeriod, H256,
};
use hyperlane_metric::prometheus_metric::RpcRole;
use prometheus::Registry;
use serde_json::Value;
use tempfile::TempDir;
use url::Url;

use crate::checkpoint_consensus::{CheckpointConsensus, CheckpointReader};
use crate::reorg_reporter::{LatestCheckpointReorgReporter, ReorgReporter};
use crate::rpc::build_validator_per_url_hooks;
use crate::settings::{RawValidatorSettings, ValidatorSettings};

const DOMAIN: u32 = 1337;
const CHAIN_ID: u8 = 7;
const READ_DEADLINE: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
#[repr(u8)]
enum Fault {
    None,
    Unavailable,
    WrongChain,
    WrongMailboxDomain,
    WrongAnnounceDomain,
}

struct Node {
    url: Url,
    fault: Arc<AtomicU8>,
    identity_reads: Arc<AtomicUsize>,
    state_reads: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl Node {
    fn start(initial: Fault) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fixture listener");
        listener
            .set_nonblocking(true)
            .expect("make fixture listener nonblocking");
        let url = Url::parse(&format!(
            "http://{}",
            listener.local_addr().expect("get fixture address")
        ))
        .expect("parse fixture URL");
        let fault = Arc::new(AtomicU8::new(initial as u8));
        let identity_reads = Arc::new(AtomicUsize::new(0));
        let state_reads = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let observed_fault = fault.clone();
        let observed_identity = identity_reads.clone();
        let observed_state = state_reads.clone();
        let stopped = stop.clone();
        let worker = thread::spawn(move || {
            while !stopped.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                };
                stream
                    .set_nonblocking(false)
                    .expect("make request socket blocking");
                stream
                    .set_read_timeout(Some(READ_DEADLINE))
                    .expect("bound fixture request reads");
                let mut reader = BufReader::new(&mut stream);
                let mut request = String::new();
                if reader.read_line(&mut request).expect("read HTTP request") == 0 {
                    continue;
                }
                let path = request
                    .split_whitespace()
                    .nth(1)
                    .expect("HTTP request has a target")
                    .to_owned();
                let mut length = 0;
                loop {
                    let mut header = String::new();
                    assert_ne!(
                        reader.read_line(&mut header).expect("read HTTP header"),
                        0,
                        "request ended before its headers"
                    );
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().expect("parse Content-Length");
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).expect("read HTTP body");
                drop(reader);

                let chain_query = path.ends_with("/chain_id");
                let mailbox_query = path.ends_with(&format!("{:064x}/local_domain", 1));
                let announce_query = path.ends_with(&format!("{:064x}/local_domain", 3));
                if chain_query || mailbox_query || announce_query {
                    observed_identity.fetch_add(1, Ordering::SeqCst);
                } else {
                    observed_state.fetch_add(1, Ordering::SeqCst);
                }
                let current = observed_fault.load(Ordering::SeqCst);
                let (status, reply) = if current == Fault::Unavailable as u8 {
                    (503, b"temporarily unavailable".to_vec())
                } else if chain_query {
                    (
                        200,
                        vec![if current == Fault::WrongChain as u8 {
                            CHAIN_ID + 1
                        } else {
                            CHAIN_ID
                        }],
                    )
                } else if mailbox_query || announce_query {
                    let mismatched = (mailbox_query && current == Fault::WrongMailboxDomain as u8)
                        || (announce_query && current == Fault::WrongAnnounceDomain as u8);
                    (
                        200,
                        (if mismatched { 1338u32 } else { DOMAIN })
                            .to_le_bytes()
                            .to_vec(),
                    )
                } else if path == "/on/graphql/query" {
                    assert_eq!(body, b"query { lastBlockPair { json } }");
                    (200, br#"{"lastBlockPair":{"json":{"last_block":[100,"tip"],"last_finalized_block":[90,"final"]}}}"#.to_vec())
                } else if path.ends_with("/count") {
                    (200, 1u32.to_le_bytes().to_vec())
                } else if path.ends_with("/inserted_block_height") {
                    (200, 85u64.to_le_bytes().to_vec())
                } else if path.ends_with("/root_at") {
                    (200, vec![9; 32])
                } else {
                    panic!("unexpected fixture path {path}");
                };
                write!(
                    stream,
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                )
                .expect("write HTTP response headers");
                stream.write_all(&reply).expect("write HTTP response body");
            }
        });
        Self {
            url,
            fault,
            identity_reads,
            state_reads,
            stop,
            worker: Some(worker),
        }
    }

    fn set_fault(&self, fault: Fault) {
        self.fault.store(fault as u8, Ordering::SeqCst);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Err(panic) = self.worker.take().expect("fixture owns its worker").join() {
            if !thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}

struct Configuration {
    chain: ChainConf,
    validator: ValidatorSettings,
    _directory: TempDir,
}

impl Configuration {
    fn new(urls: &[Url]) -> Self {
        let directory = tempfile::tempdir().expect("create fixture directory");
        let raw: Value = serde_json::json!({
            "originchainname": "test",
            "validator": {"type": "hexKey", "key": format!("0x{}", "11".repeat(32))},
            "checkpointsyncer": {"type": "localStorage", "path": directory.path().join("checkpoints")},
            "db": directory.path().join("validator-db"),
            "chains": {"test": {
                "name": "test", "domainid": DOMAIN, "chainid": CHAIN_ID, "protocol": "dusk",
                "rpcconsensustype": "majority",
                "rpcurls": urls.iter().map(|url| serde_json::json!({"http": url.as_str()})).collect::<Vec<_>>(),
                "eventcursordir": directory.path().join("events"),
                "nativetoken": {"decimals": 9, "symbol": "DUSK", "denom": "LUX"},
                "mailbox": "0x0000000000000000000000000000000000000001",
                "interchaingaspaymaster": "0x0000000000000000000000000000000000000002",
                "validatorannounce": "0x0000000000000000000000000000000000000003",
                "merkletreehook": "0x0000000000000000000000000000000000000004"
            }}
        });
        let base = Settings::from_config(
            serde_json::from_value::<RawAgentConf>(raw.clone()).expect("deserialize base settings"),
            &ConfigPath::default(),
            "validator",
        )
        .expect("parse base settings");
        let chain = base.chains[&base
            .lookup_domain("test")
            .expect("configured domain exists")]
            .clone();
        let validator = ValidatorSettings::from_config_filtered(
            RawValidatorSettings(raw),
            &ConfigPath::default(),
            (),
            "validator",
        )
        .expect("parse validator settings");
        Self {
            chain,
            validator,
            _directory: directory,
        }
    }
}

fn metrics() -> Arc<CoreMetrics> {
    Arc::new(
        CoreMetrics::new("dusk-rpc-fixture", 0, Registry::new()).expect("create fixture metrics"),
    )
}

fn assert_checkpoint(checkpoint: &CheckpointAtBlock) {
    assert_eq!(checkpoint.block_height, Some(90));
    assert_eq!(checkpoint.checkpoint.mailbox_domain, DOMAIN);
    assert_eq!(
        checkpoint.checkpoint.merkle_tree_hook_address,
        H256::from_low_u64_be(4)
    );
    assert_eq!(checkpoint.checkpoint.index, 0);
    assert_eq!(checkpoint.checkpoint.root, H256::from([9; 32]));
}

async fn build_hooks(configuration: &Configuration, urls: &[Url]) -> Vec<Arc<dyn MerkleTreeHook>> {
    tokio::time::timeout(
        READ_DEADLINE,
        build_validator_per_url_hooks(
            &configuration.chain,
            "rpcUrls",
            RpcRole::Primary,
            urls,
            &metrics(),
        ),
    )
    .await
    .expect("endpoint pool construction finishes")
    .expect("offline secondary does not veto endpoint pool")
    .into_iter()
    .map(|(_, hook)| hook)
    .collect()
}

#[tokio::test]
async fn dusk_unavailable_secondary_recovers_without_changing_quorum_denominator() {
    let nodes = [
        Node::start(Fault::None),
        Node::start(Fault::Unavailable),
        Node::start(Fault::None),
    ];
    let urls: Vec<_> = nodes.iter().map(|node| node.url.clone()).collect();
    let configuration = Configuration::new(&urls);
    let hooks = build_hooks(&configuration, &urls).await;
    assert_eq!(hooks.len(), 3);
    for node in &nodes {
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
        assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
    }
    for consensus in [CheckpointConsensus::Majority, CheckpointConsensus::Quorum] {
        let reader =
            CheckpointReader::new(consensus, hooks.clone()).expect("configured pool is nonempty");
        assert_eq!(reader.endpoint_count(), 3);
        assert_eq!(reader.consensus.required(reader.endpoint_count()), 2);
        let votes = tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
            .await
            .expect("checkpoint reads finish")
            .expect("two endpoints form a quorum");
        assert!(votes[1].is_none());
        assert_eq!(votes.iter().flatten().count(), 2);
        for checkpoint in votes.iter().flatten() {
            assert_checkpoint(checkpoint);
        }
        nodes[2].set_fault(Fault::Unavailable);
        assert!(
            tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
                .await
                .expect("failed reads finish")
                .is_err()
        );
        assert_eq!(reader.endpoint_count(), 3);
        nodes[2].set_fault(Fault::None);
    }
    let reader = CheckpointReader::new(CheckpointConsensus::Majority, hooks)
        .expect("configured pool is nonempty");
    nodes[1].set_fault(Fault::None);
    let votes = tokio::time::timeout(READ_DEADLINE, reader.checkpoints(&ReorgPeriod::None))
        .await
        .expect("recovered reads finish")
        .expect("recovered endpoint rejoins quorum");
    assert_eq!(votes.iter().flatten().count(), 3);
    for checkpoint in votes.iter().flatten() {
        assert_checkpoint(checkpoint);
    }
    assert_eq!(nodes[0].identity_reads.load(Ordering::SeqCst), 3);
    assert_eq!(nodes[2].identity_reads.load(Ordering::SeqCst), 3);
}

async fn identity_failure_blocks_every_read_and_can_recover(fault: Fault) {
    let node = Node::start(fault);
    let configuration = Configuration::new(std::slice::from_ref(&node.url));
    let hook = configuration
        .chain
        .build_merkle_tree_hook(&metrics())
        .await
        .expect("state hook construction is lazy");
    assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
    assert!(hook.tree(&ReorgPeriod::None).await.is_err());
    assert!(hook.count(&ReorgPeriod::None).await.is_err());
    assert!(hook.latest_checkpoint(&ReorgPeriod::None).await.is_err());
    assert!(hook.latest_checkpoint_at_block(90).await.is_err());
    assert_eq!(node.state_reads.load(Ordering::SeqCst), 0);
    node.set_fault(Fault::None);
    let recovered = tokio::time::timeout(READ_DEADLINE, hook.latest_checkpoint(&ReorgPeriod::None))
        .await
        .expect("identity recovery read finishes")
        .expect("corrected endpoint returns a checkpoint");
    assert_checkpoint(&recovered);
    let successful_validation_count = node.identity_reads.load(Ordering::SeqCst);
    assert_checkpoint(
        &hook
            .latest_checkpoint_at_block(90)
            .await
            .expect("validated hook returns a checkpoint at height"),
    );
    assert_eq!(
        node.identity_reads.load(Ordering::SeqCst),
        successful_validation_count
    );
}

#[tokio::test]
async fn dusk_wrong_chain_cannot_return_any_state_read() {
    identity_failure_blocks_every_read_and_can_recover(Fault::WrongChain).await;
}

#[tokio::test]
async fn dusk_wrong_mailbox_domain_cannot_return_any_state_read() {
    identity_failure_blocks_every_read_and_can_recover(Fault::WrongMailboxDomain).await;
}

#[tokio::test]
async fn dusk_wrong_announce_domain_cannot_return_any_state_read() {
    identity_failure_blocks_every_read_and_can_recover(Fault::WrongAnnounceDomain).await;
}

#[tokio::test]
async fn dusk_concurrent_first_reads_share_identity_validation() {
    let node = Node::start(Fault::None);
    let configuration = Configuration::new(std::slice::from_ref(&node.url));
    let hook = configuration
        .chain
        .build_merkle_tree_hook(&metrics())
        .await
        .expect("state hook construction is lazy");
    let (count, checkpoint) = tokio::join!(
        hook.count(&ReorgPeriod::None),
        hook.latest_checkpoint(&ReorgPeriod::None)
    );
    assert_eq!(count.expect("healthy hook returns count"), 1);
    assert_checkpoint(&checkpoint.expect("healthy hook returns checkpoint"));
    assert_eq!(node.identity_reads.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn dusk_reorg_reporter_construction_tolerates_unavailable_secondary() {
    let nodes = [
        Node::start(Fault::None),
        Node::start(Fault::Unavailable),
        Node::start(Fault::None),
    ];
    let urls: Vec<_> = nodes.iter().map(|node| node.url.clone()).collect();
    let configuration = Configuration::new(&urls);
    let reporter = tokio::time::timeout(
        READ_DEADLINE,
        LatestCheckpointReorgReporter::from_settings(&configuration.validator, &metrics()),
    )
    .await
    .expect("diagnostic construction finishes")
    .expect("offline secondary does not veto diagnostics");
    for node in &nodes {
        assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
    }
    tokio::time::timeout(
        READ_DEADLINE,
        ReorgReporter::report_with_reorg_period(&reporter, &ReorgPeriod::None),
    )
    .await
    .expect("diagnostic reads finish");
    assert!(nodes[0].state_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(nodes[1].state_reads.load(Ordering::SeqCst), 0);
    assert!(nodes[2].state_reads.load(Ordering::SeqCst) > 0);
}

#[tokio::test]
async fn dusk_invalid_url_is_still_rejected_during_pool_construction() {
    let node = Node::start(Fault::None);
    let configuration = Configuration::new(std::slice::from_ref(&node.url));
    let urls = [
        node.url.clone(),
        Url::parse("file:///tmp/dusk-rpc-invalid").expect("parse unsupported URL fixture"),
    ];
    assert!(build_validator_per_url_hooks(
        &configuration.chain,
        "rpcUrls",
        RpcRole::Primary,
        &urls,
        &metrics()
    )
    .await
    .is_err());
    assert_eq!(node.identity_reads.load(Ordering::SeqCst), 0);
}
