use std::{fmt, path::PathBuf};

use hyperlane_core::config::OpSubmissionConfig;
use hyperlane_core::NativeToken;
use url::Url;

use crate::QueryRateLimit;

/// Configuration for connecting to a Dusk node via RUES.
#[derive(Clone)]
pub struct ConnectionConf {
    /// RUES HTTP endpoint URL (e.g. `https://nodes.dusk.network`).
    pub url: Url,
    /// Dusk chain ID (used in Moonlight transactions).
    pub chain_id: u8,
    /// Agent-owned directory for independently authenticated finalized-event
    /// rows and the archive scan positions that find them. A scan position is
    /// checked against the endpoint's archive before it is reused. Different
    /// agent processes must use different directories.
    pub event_cursor_dir: PathBuf,
    /// Default gas limit for transactions.
    pub gas_limit: u64,
    /// Default gas price in LUX.
    pub gas_price: u64,
    /// Native token configuration.
    pub native_token: NativeToken,
    /// Operation submission configuration.
    pub op_submission_config: OpSubmissionConfig,
    /// How many contract queries the agent sends the endpoint
    /// (`contractQueriesPerSecond`, `contractQueryBurst`).
    pub query_rate_limit: QueryRateLimit,
    /// Whether the relayer may pipeline deliveries (`pipelineDeliveries`,
    /// off by default). Pipelining is experimental: it also needs
    /// `maxBatchSize` above 1 (`pipelines_deliveries`).
    pub pipeline_deliveries: bool,
}

impl ConnectionConf {
    /// The Multicall for Dusk contract the chain batches its contract reads
    /// through: its `batchContractAddress`, the chain setting that names
    /// Multicall3 on EVM chains. Unset, every read is one query.
    pub fn multicall(&self) -> Option<[u8; 32]> {
        self.op_submission_config
            .batch_contract_address
            .map(|address| address.into())
    }

    /// Whether the relayer pipelines deliveries: sends a batch of them as
    /// consecutive transactions of its account, each signed with a nonce
    /// the agent chose. Only with both `pipelineDeliveries` and a
    /// `maxBatchSize` above 1; otherwise every delivery goes one at a time,
    /// signed with the nonce `dusk-tx` reads from the node, and waits for
    /// its block. A configuration that only sets `maxBatchSize`, as the
    /// deploy scripts do, delivers one at a time.
    pub fn pipelines_deliveries(&self) -> bool {
        self.pipeline_deliveries && self.op_submission_config.max_batch_size > 1
    }
}

impl fmt::Debug for ConnectionConf {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConnectionConf")
            .field("url", &"<redacted>")
            .field("chain_id", &self.chain_id)
            .field("event_cursor_dir", &self.event_cursor_dir)
            .field("gas_limit", &self.gas_limit)
            .field("gas_price", &self.gas_price)
            .field("native_token", &self.native_token)
            .field("op_submission_config", &self.op_submission_config)
            .field("query_rate_limit", &self.query_rate_limit)
            .field("pipeline_deliveries", &self.pipeline_deliveries)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_debug_never_contains_url_credentials_or_private_components() {
        let connection = ConnectionConf {
            url: Url::parse(
                "https://debug-user:debug-password@rpc.example/private-path?token=query-sentinel",
            )
            .unwrap(),
            chain_id: 1,
            event_cursor_dir: PathBuf::from("event-cursors"),
            gas_limit: 30_000_000,
            gas_price: 2_000,
            native_token: NativeToken::default(),
            op_submission_config: OpSubmissionConfig::default(),
            query_rate_limit: QueryRateLimit::default(),
            pipeline_deliveries: false,
        };
        let rendered = format!("{connection:?}");

        assert!(rendered.contains("<redacted>"));
        for secret in [
            "debug-user",
            "debug-password",
            "private-path",
            "query-sentinel",
        ] {
            assert!(!rendered.contains(secret));
        }
    }

    #[test]
    fn deliveries_are_pipelined_only_when_opted_in_with_a_batch_above_one() {
        let connection = |pipeline_deliveries: bool, max_batch_size: u32| ConnectionConf {
            url: Url::parse("http://127.0.0.1:1").unwrap(),
            chain_id: 1,
            event_cursor_dir: PathBuf::from("event-cursors"),
            gas_limit: 5_000_000,
            gas_price: 1,
            native_token: NativeToken::default(),
            op_submission_config: OpSubmissionConfig {
                max_batch_size,
                ..OpSubmissionConfig::default()
            },
            query_rate_limit: QueryRateLimit::default(),
            pipeline_deliveries,
        };
        // What the deploy scripts write: a batch size, no opt-in.
        assert!(!connection(false, 16).pipelines_deliveries());
        assert!(!connection(false, 1).pipelines_deliveries());
        // Opted in, but batches of one.
        assert!(!connection(true, 1).pipelines_deliveries());
        assert!(!connection(true, 0).pipelines_deliveries());
        assert!(connection(true, 2).pipelines_deliveries());
        assert!(connection(true, 16).pipelines_deliveries());
    }
}
