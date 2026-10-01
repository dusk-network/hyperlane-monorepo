use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use ethers::utils::keccak256;
use futures_util::future::join_all;
use serde::Serialize;
use tracing::{info, warn};
use url::Url;

use crate::rpc::{chain_conf_for_read_url, dedupe_rpc_urls, state_read_urls};
use hyperlane_base::{CheckpointSyncer, CoreMetrics};
use hyperlane_core::{
    ChainCommunicationError, CheckpointAtBlock, MerkleTreeHook, ReorgPeriod, H256,
};
use hyperlane_metric::prometheus_metric::RpcRole;

use crate::settings::ValidatorSettings;

const REORG_DIAGNOSTIC_RPC_TIMEOUT: Duration = Duration::from_secs(10);

#[async_trait]
pub trait ReorgReporter: Send + Sync + Debug {
    async fn report_at_block(&self, height: u64);
    async fn report_with_reorg_period(&self, reorg_period: &ReorgPeriod);
}

pub struct LatestCheckpointReorgReporter {
    merkle_tree_hooks: HashMap<Url, Arc<dyn MerkleTreeHook>>,
}

impl fmt::Debug for LatestCheckpointReorgReporter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LatestCheckpointReorgReporter")
            .field("endpoint_count", &self.merkle_tree_hooks.len())
            .finish()
    }
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

#[async_trait]
impl ReorgReporter for LatestCheckpointReorgReporter {
    async fn report_at_block(&self, height: u64) {
        self.report_at_block(height).await;
    }

    async fn report_with_reorg_period(&self, reorg_period: &ReorgPeriod) {
        self.report_with_reorg_period(reorg_period).await;
    }
}

impl LatestCheckpointReorgReporter {
    async fn report_at_block(&self, height: u64) -> Vec<ReorgReportRpcResponse> {
        info!(?height, "Reporting latest checkpoint on reorg");
        let mut futures = vec![];
        for (url, merkle_tree_hook) in &self.merkle_tree_hooks {
            let url = url.clone();
            let merkle_tree_hook = merkle_tree_hook.clone();
            let future = async move {
                match tokio::time::timeout(
                    REORG_DIAGNOSTIC_RPC_TIMEOUT,
                    merkle_tree_hook.latest_checkpoint_at_block(height),
                )
                .await
                {
                    Ok(Ok(latest_checkpoint)) => {
                        let (rpc_url_hash, _) = rpc_hashes(&url);
                        info!(
                            ?rpc_url_hash,
                            ?height,
                            ?latest_checkpoint,
                            "Report latest checkpoint on reorg"
                        );
                        ReorgReportRpcResponse::new(url, latest_checkpoint, Some(height), None)
                    }
                    Ok(Err(error)) => ReorgReportRpcResponse::failure(
                        url,
                        Some(height),
                        None,
                        public_diagnostic_error(&error),
                    ),
                    Err(_) => ReorgReportRpcResponse::failure(
                        url,
                        Some(height),
                        None,
                        format!(
                            "diagnostic RPC timed out after {}s",
                            REORG_DIAGNOSTIC_RPC_TIMEOUT.as_secs()
                        ),
                    ),
                }
            };

            futures.push(future);
        }

        join_all(futures).await
    }

    async fn report_with_reorg_period(
        &self,
        reorg_period: &ReorgPeriod,
    ) -> Vec<ReorgReportRpcResponse> {
        info!(?reorg_period, "Reporting latest checkpoint on reorg");
        let mut futures = vec![];
        for (url, merkle_tree_hook) in &self.merkle_tree_hooks {
            let url = url.clone();
            let merkle_tree_hook = merkle_tree_hook.clone();
            let reorg_period = reorg_period.clone();
            let future = async move {
                match tokio::time::timeout(
                    REORG_DIAGNOSTIC_RPC_TIMEOUT,
                    merkle_tree_hook.latest_checkpoint(&reorg_period),
                )
                .await
                {
                    Ok(Ok(latest_checkpoint)) => {
                        let (rpc_url_hash, _) = rpc_hashes(&url);
                        info!(
                            ?rpc_url_hash,
                            ?reorg_period,
                            ?latest_checkpoint,
                            "Report latest checkpoint on reorg"
                        );
                        ReorgReportRpcResponse::new(
                            url,
                            latest_checkpoint,
                            None,
                            Some(reorg_period),
                        )
                    }
                    Ok(Err(error)) => ReorgReportRpcResponse::failure(
                        url,
                        None,
                        Some(reorg_period),
                        public_diagnostic_error(&error),
                    ),
                    Err(_) => ReorgReportRpcResponse::failure(
                        url,
                        None,
                        Some(reorg_period),
                        format!(
                            "diagnostic RPC timed out after {}s",
                            REORG_DIAGNOSTIC_RPC_TIMEOUT.as_secs()
                        ),
                    ),
                }
            };

            futures.push(future);
        }

        join_all(futures).await
    }
}

impl LatestCheckpointReorgReporter {
    pub(crate) async fn from_settings(
        settings: &ValidatorSettings,
        metrics: &CoreMetrics,
    ) -> eyre::Result<Self> {
        let chain = settings.chain_setup(&settings.origin_chain)?;
        let rpc_urls = settings
            .rpcs
            .iter()
            .enumerate()
            .map(|(i, rpc)| {
                Url::parse(&rpc.url).map_err(|_| eyre::eyre!("Invalid rpcUrls[{i}] URL"))
            })
            .collect::<eyre::Result<Vec<_>>>()?;
        let (source, urls) = state_read_urls(chain, rpc_urls);
        let mut merkle_tree_hooks = HashMap::new();
        for url in dedupe_rpc_urls(urls, source) {
            let hook = chain_conf_for_read_url(chain, url.clone(), RpcRole::Primary)
                .build_merkle_tree_hook(metrics)
                .await?;
            merkle_tree_hooks.insert(url, Arc::from(hook));
        }
        Ok(Self { merkle_tree_hooks })
    }
}

#[derive(Debug)]
pub struct LatestCheckpointReorgReporterWithStorageWriter {
    /// `LatestCheckpointReorgReporterWithStorageWriter` is an extension to
    /// `LatestCheckpointReorgReporter`
    latest_checkpoint_reorg_reporter: LatestCheckpointReorgReporter,

    /// Currently, the storage abstraction is tied to the checkpoint syncer, which is why
    /// it is used here.
    storage_writer: Arc<dyn CheckpointSyncer>,
}

#[async_trait]
impl ReorgReporter for LatestCheckpointReorgReporterWithStorageWriter {
    async fn report_at_block(&self, height: u64) {
        let logs = self
            .latest_checkpoint_reorg_reporter
            .report_at_block(height)
            .await;
        self.submit_to_storage_writer(&logs).await;
    }

    async fn report_with_reorg_period(&self, reorg_period: &ReorgPeriod) {
        let logs = self
            .latest_checkpoint_reorg_reporter
            .report_with_reorg_period(reorg_period)
            .await;
        self.submit_to_storage_writer(&logs).await;
    }
}

impl LatestCheckpointReorgReporterWithStorageWriter {
    pub(crate) fn new(
        latest_checkpoint_reorg_reporter: LatestCheckpointReorgReporter,
        storage_writer: Arc<dyn CheckpointSyncer>,
    ) -> Self {
        Self {
            latest_checkpoint_reorg_reporter,
            storage_writer,
        }
    }

    async fn submit_to_storage_writer(&self, storage_logs_entries: &Vec<ReorgReportRpcResponse>) {
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
    fn reporter_debug_exposes_only_the_endpoint_count() {
        let reporter = LatestCheckpointReorgReporter {
            merkle_tree_hooks: HashMap::new(),
        };

        assert_eq!(
            format!("{reporter:?}"),
            "LatestCheckpointReorgReporter { endpoint_count: 0 }"
        );
    }

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
}
