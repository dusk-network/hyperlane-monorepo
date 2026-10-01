//! Exercise checkpoint reads through the public adapter and binary RUES boundary.
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
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

struct ArchiveFixture {
    url: Url,
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

impl ArchiveFixture {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let worker = thread::spawn(move || {
            let ids = [[3u8; 32], [4u8; 32]];
            let mut tree = IncrementalMerkle::default();
            let roots: Vec<_> = ids
                .iter()
                .map(|id| {
                    tree.ingest(H256::from(*id));
                    tree.root().0
                })
                .collect();
            while !worker_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(connection) => connection,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(error) => panic!("fixture accept failed: {error}"),
                };
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
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let reply = if path == "/on/graphql/query" {
                    assert_eq!(body, b"query { lastBlockPair { json } }");
                    br#"{"lastBlockPair":{"json":{"last_block":[100,"tip"],"last_finalized_block":[90,"final"]}}}"#.to_vec()
                } else {
                    let prefix = format!("/on/contracts:{}/", hex::encode([2u8; 32]));
                    match path
                        .strip_prefix(&prefix)
                        .expect("must query the configured hook")
                    {
                        "count" => 2u32.to_le_bytes().to_vec(),
                        "inserted_block_height" => {
                            let index = u32::from_le_bytes(body.try_into().unwrap()) as usize;
                            [60u64, 85][index].to_le_bytes().to_vec()
                        }
                        "root_at" => {
                            let index = u32::from_le_bytes(body.try_into().unwrap()) as usize;
                            roots[index].to_vec()
                        }
                        "message_ids" => {
                            assert_eq!(body.len(), 8);
                            let start = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                            let count = u32::from_le_bytes(body[4..].try_into().unwrap()) as usize;
                            rkyv::to_bytes::<_, 256>(&ids[start..start + count].to_vec())
                                .unwrap()
                                .to_vec()
                        }
                        method => panic!("unexpected contract query: {method}"),
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
            stop,
            worker: Some(worker),
        }
    }

    fn hook(&self) -> DuskMerkleTreeHook {
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-test",
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
            H256::from([2; 32]),
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

impl Drop for ArchiveFixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.worker.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn checkpoint_reads_respect_delay_without_exceeding_finality() {
    let fixture = ArchiveFixture::start();
    let hook = fixture.hook();
    for (period, height, count) in [
        (ReorgPeriod::from_blocks(30), 70, 1),
        (ReorgPeriod::None, 90, 2),
        (ReorgPeriod::from_blocks(1), 90, 2),
        (ReorgPeriod::Tag("finalized".to_owned()), 90, 2),
        (ReorgPeriod::from_blocks(101), 0, 0),
    ] {
        assert_eq!(
            hook.count(&period).await.unwrap(),
            count,
            "period {period:?}"
        );
        let tree = hook.tree(&period).await.unwrap();
        assert_eq!(tree.block_height, Some(height));
        assert_eq!(tree.tree.count(), count as usize);
        let checkpoint = hook.latest_checkpoint(&period).await;
        if count == 0 {
            assert!(
                checkpoint.is_err(),
                "no checkpoint exists before the first insertion"
            );
        } else {
            let checkpoint = checkpoint.unwrap();
            assert_eq!(checkpoint.block_height, Some(height));
            assert_eq!(checkpoint.checkpoint.index, count - 1);
            assert_eq!(checkpoint.checkpoint.root, tree.tree.root());
            assert_eq!(
                checkpoint.checkpoint.merkle_tree_hook_address,
                H256::from([2; 32])
            );
        }
    }
}

#[tokio::test]
async fn unsupported_checkpoint_tag_is_rejected() {
    let fixture = ArchiveFixture::start();
    let hook = fixture.hook();
    let period = ReorgPeriod::Tag("safe".to_owned());
    assert!(hook.count(&period).await.is_err());
    assert!(hook.tree(&period).await.is_err());
    assert!(hook.latest_checkpoint(&period).await.is_err());
}
