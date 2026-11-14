use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ethers::utils::keccak256;
use futures_util::future::join_all;
use serde::Serialize;
use tracing::{info, warn};
use url::Url;

use hyperlane_base::CheckpointSyncer;
use hyperlane_core::{
    ChainCommunicationError, CheckpointAtBlock, MerkleTreeHook, ReorgPeriod, H256,
};
use redact::Secret;

const REORG_DIAGNOSTIC_RPC_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
pub trait ReorgReporter: Send + Sync + Debug {
    async fn report_at_block(&self, height: u64);
    async fn report_with_reorg_period(&self, reorg_period: &ReorgPeriod);
}

#[derive(Debug)]
pub struct LatestCheckpointReorgReporter {
    merkle_tree_hooks: HashMap<Secret<Url>, Arc<dyn MerkleTreeHook>>,
    storage_writer: Arc<dyn CheckpointSyncer>,
}

#[derive(Serialize)]
struct ReorgReportRpcResponse {
    rpc_url_hash: H256,
    rpc_host_hash: H256,
    requested_height: Option<u64>,
    observed_height: Option<u64>,
    endpoint_lag: bool,
    reorg_period: Option<ReorgPeriod>,
    merkle_root_index: Option<u32>,
    merkle_root_hash: Option<H256>,
    error: Option<String>,
    timestamp: String,
}

impl ReorgReportRpcResponse {
    fn new(
        url: Url,
        latest_checkpoint: CheckpointAtBlock,
        requested_height: Option<u64>,
        reorg_period: Option<ReorgPeriod>,
    ) -> Self {
        let observed_height = latest_checkpoint.block_height;
        let (rpc_url_hash, rpc_host_hash) = rpc_hashes(&url);
        ReorgReportRpcResponse {
            rpc_host_hash,
            rpc_url_hash,
            requested_height,
            observed_height,
            endpoint_lag: requested_height
                .zip(observed_height)
                .is_some_and(|(requested, observed)| observed < requested),
            reorg_period,
            merkle_root_hash: Some(latest_checkpoint.checkpoint.root),
            merkle_root_index: Some(latest_checkpoint.checkpoint.index),
            error: None,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    fn failure(
        url: Url,
        requested_height: Option<u64>,
        reorg_period: Option<ReorgPeriod>,
        error: String,
    ) -> Self {
        let (rpc_url_hash, rpc_host_hash) = rpc_hashes(&url);
        Self {
            rpc_host_hash,
            rpc_url_hash,
            requested_height,
            observed_height: None,
            endpoint_lag: false,
            reorg_period,
            merkle_root_index: None,
            merkle_root_hash: None,
            error: Some(error),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}

fn public_diagnostic_error(error: &ChainCommunicationError) -> String {
    match error {
        ChainCommunicationError::TransactionTimeout => {
            "diagnostic RPC transaction timed out".to_owned()
        }
        ChainCommunicationError::SignerUnavailable => {
            "diagnostic RPC signer unavailable".to_owned()
        }
        _ => "diagnostic RPC request failed".to_owned(),
    }
}

fn rpc_hashes(url: &Url) -> (H256, H256) {
    // Commit only to scheme/host/port. Hashing the full URL would still let a
    // public diagnostic act as an offline oracle for low-entropy basic-auth,
    // path, or query credentials.
    let endpoint_identity = url.origin().ascii_serialization();
    (
        H256::from_slice(&keccak256(endpoint_identity.as_bytes())),
        H256::from_slice(&keccak256(url.host_str().unwrap_or("").as_bytes())),
    )
}

#[derive(Clone)]
enum ReportTarget {
    Block(u64),
    Finality(ReorgPeriod),
}

#[async_trait]
impl ReorgReporter for LatestCheckpointReorgReporter {
    async fn report_at_block(&self, height: u64) {
        self.report(ReportTarget::Block(height)).await;
    }

    async fn report_with_reorg_period(&self, period: &ReorgPeriod) {
        self.report(ReportTarget::Finality(period.clone())).await;
    }
}

impl LatestCheckpointReorgReporter {
    pub(crate) fn new(
        hooks: impl IntoIterator<Item = (Url, Arc<dyn MerkleTreeHook>)>,
        storage_writer: Arc<dyn CheckpointSyncer>,
    ) -> Self {
        Self {
            merkle_tree_hooks: hooks
                .into_iter()
                .map(|(url, hook)| (Secret::new(url), hook))
                .collect(),
            storage_writer,
        }
    }

    async fn report(&self, target: ReportTarget) {
        let (height, period) = match &target {
            ReportTarget::Block(height) => (Some(*height), None),
            ReportTarget::Finality(period) => (None, Some(period.clone())),
        };
        info!(?height, ?period, "Reporting latest checkpoint on reorg");
        // One bounded attempt per endpoint: an unresponsive RPC must not stall
        // the fail-stop path. Failures are recorded without endpoint or error
        // details.
        let reports = join_all(self.merkle_tree_hooks.iter().map(|(url, hook)| {
            let url = url.expose_secret().clone();
            let hook = hook.clone();
            let target = target.clone();
            let period = period.clone();
            async move {
                let observation = match target {
                    ReportTarget::Block(height) => {
                        tokio::time::timeout(
                            REORG_DIAGNOSTIC_RPC_TIMEOUT,
                            hook.latest_checkpoint_at_block(height),
                        )
                        .await
                    }
                    ReportTarget::Finality(period) => {
                        tokio::time::timeout(
                            REORG_DIAGNOSTIC_RPC_TIMEOUT,
                            hook.latest_checkpoint(&period),
                        )
                        .await
                    }
                };
                match observation {
                    Ok(Ok(checkpoint)) => {
                        let (rpc_url_hash, _) = rpc_hashes(&url);
                        info!(
                            ?rpc_url_hash,
                            ?height,
                            ?period,
                            ?checkpoint,
                            "Report latest checkpoint on reorg"
                        );
                        ReorgReportRpcResponse::new(url, checkpoint, height, period)
                    }
                    Ok(Err(error)) => ReorgReportRpcResponse::failure(
                        url,
                        height,
                        period,
                        public_diagnostic_error(&error),
                    ),
                    Err(_) => ReorgReportRpcResponse::failure(
                        url,
                        height,
                        period,
                        format!(
                            "diagnostic RPC timed out after {}s",
                            REORG_DIAGNOSTIC_RPC_TIMEOUT.as_secs()
                        ),
                    ),
                }
            }
        }))
        .await;
        self.submit_to_storage_writer(&reports).await;
    }

    async fn submit_to_storage_writer(&self, storage_logs_entries: &[ReorgReportRpcResponse]) {
        let json_string = serde_json::to_string_pretty(storage_logs_entries).unwrap_or_else(|e| {
            warn!("Error serializing json: {}", e);
            String::from("{\"error\": \"Error formatting the string\"}")
        });
        self.storage_writer
            .write_reorg_rpc_responses(json_string)
            .await
            .unwrap_or_else(|e| {
                warn!("Error writing checkpoint syncer to reorg log: {}", e);
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyperlane_core::Checkpoint;

    #[test]
    fn reorg_report_keeps_requested_and_observed_heights_distinct() {
        let response = ReorgReportRpcResponse::new(
            Url::parse("https://rpc.example").unwrap(),
            CheckpointAtBlock {
                checkpoint: Checkpoint {
                    merkle_tree_hook_address: H256::zero(),
                    mailbox_domain: 1000,
                    root: H256::from_low_u64_be(7),
                    index: 8,
                },
                block_height: Some(39),
            },
            Some(42),
            None,
        );

        assert_eq!(response.requested_height, Some(42));
        assert_eq!(response.observed_height, Some(39));
        assert!(response.endpoint_lag);
    }

    #[test]
    fn public_reorg_report_never_serializes_rpc_url_or_error_details() {
        let url = Url::parse(
            "https://basic-user:basic-password@rpc.example/private-path?token=query-sentinel",
        )
        .unwrap();
        let error = ChainCommunicationError::CustomError(
            "private-path query-sentinel basic-user basic-password".into(),
        );
        let response = ReorgReportRpcResponse::failure(
            url.clone(),
            Some(42),
            None,
            public_diagnostic_error(&error),
        );
        let serialized = serde_json::to_string(&response).unwrap();
        for secret in [
            "private-path",
            "query-sentinel",
            "basic-user",
            "basic-password",
        ] {
            assert!(!serialized.contains(secret));
        }
        assert_eq!(
            rpc_hashes(&url),
            rpc_hashes(&Url::parse("https://rpc.example/").unwrap()),
            "userinfo, path, query, and fragment must not influence public hashes"
        );
        assert_ne!(
            rpc_hashes(&url).0,
            rpc_hashes(&Url::parse("https://rpc.example:8443/").unwrap()).0,
            "the public endpoint identity should retain an explicit port"
        );
    }

    use crate::test_utils::mock_merkle_tree_hook::MockMerkleTreeHook;

    #[test]
    fn reporter_debug_omits_rpc_credentials() {
        let url =
            Url::parse("https://user:private-password@localhost/private-path?key=private-query")
                .unwrap();
        let mut hook = MockMerkleTreeHook::new();
        hook.expect_fmt()
            .returning(|formatter| formatter.write_str("MockMerkleTreeHook"));
        let hook: Arc<dyn MerkleTreeHook> = Arc::new(hook);
        let storage = hyperlane_base::tests::mock_checkpoint_syncer::MockCheckpointSyncer::new();
        let reporter = LatestCheckpointReorgReporter::new([(url, hook)], Arc::new(storage));
        let output = format!("{reporter:?}");
        assert!(!output.contains("private-"));
        assert!(output.contains("merkle_tree_hooks"));
        assert!(output.contains("MockMerkleTreeHook"));
        assert!(output.contains("[REDACTED"));
    }
    #[tokio::test]
    async fn shared_hook_reports_are_persisted_without_endpoint_credentials() {
        use hyperlane_base::LocalStorage;
        use hyperlane_core::Checkpoint;
        use mockall::predicate::eq;

        for at_block in [true, false] {
            let directory = tempfile::tempdir().expect("report directory");
            let storage =
                LocalStorage::new(directory.path().to_owned(), None).expect("report storage");
            let checkpoint = CheckpointAtBlock {
                checkpoint: Checkpoint {
                    mailbox_domain: 1,
                    merkle_tree_hook_address: H256::from_low_u64_be(2),
                    root: H256::repeat_byte(3),
                    index: 7,
                },
                block_height: Some(12),
            };
            let mut hook = MockMerkleTreeHook::new();
            if at_block {
                hook.expect_latest_checkpoint_at_block()
                    .with(eq(12))
                    .once()
                    .return_once(move |_| Ok(checkpoint));
            } else {
                hook.expect_latest_checkpoint()
                    .with(eq(ReorgPeriod::None))
                    .once()
                    .return_once(move |_| Ok(checkpoint));
            }
            let hook: Arc<dyn MerkleTreeHook> = Arc::new(hook);
            let url = Url::parse(
                "https://user:private-password@localhost/private-path?key=private-query",
            )
            .expect("endpoint");
            let reporter =
                LatestCheckpointReorgReporter::new([(url, hook.clone())], Arc::new(storage));
            assert!(Arc::ptr_eq(
                reporter
                    .merkle_tree_hooks
                    .values()
                    .next()
                    .expect("shared hook"),
                &hook
            ));
            if at_block {
                ReorgReporter::report_at_block(&reporter, 12).await;
            } else {
                ReorgReporter::report_with_reorg_period(&reporter, &ReorgPeriod::None).await;
            }
            let json = tokio::fs::read_to_string(directory.path().join("reorg_rpc_responses.json"))
                .await
                .expect("persisted report");
            assert!(!json.contains("private-"));
            let rows: serde_json::Value = serde_json::from_str(&json).expect("report JSON");
            assert_eq!(rows[0]["merkle_root_index"], 7);
            assert_eq!(
                rows[0]["requested_height"],
                if at_block {
                    serde_json::json!(12)
                } else {
                    serde_json::Value::Null
                }
            );
        }
    }

    #[tokio::test]
    async fn failed_endpoint_is_reported_without_details() {
        use hyperlane_base::LocalStorage;

        let directory = tempfile::tempdir().expect("report directory");
        let storage = LocalStorage::new(directory.path().to_owned(), None).expect("report storage");
        let mut hook = MockMerkleTreeHook::new();
        hook.expect_latest_checkpoint_at_block()
            .once()
            .return_once(|_| {
                Err(ChainCommunicationError::CustomError(
                    "private-detail".into(),
                ))
            });
        let hook: Arc<dyn MerkleTreeHook> = Arc::new(hook);
        let url =
            Url::parse("https://user:private-password@localhost/private-path").expect("endpoint");
        let reporter = LatestCheckpointReorgReporter::new([(url, hook)], Arc::new(storage));
        ReorgReporter::report_at_block(&reporter, 12).await;
        let json = tokio::fs::read_to_string(directory.path().join("reorg_rpc_responses.json"))
            .await
            .expect("persisted report");
        assert!(!json.contains("private-"));
        let rows: serde_json::Value = serde_json::from_str(&json).expect("report JSON");
        assert_eq!(rows[0]["requested_height"], 12);
        assert_eq!(rows[0]["merkle_root_index"], serde_json::Value::Null);
        assert_eq!(rows[0]["error"], "diagnostic RPC request failed");
    }
}
