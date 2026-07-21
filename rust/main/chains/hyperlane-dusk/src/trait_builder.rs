use std::path::PathBuf;

use hyperlane_core::config::OpSubmissionConfig;
use hyperlane_core::NativeToken;
use url::Url;

/// Configuration for connecting to a Dusk node via RUES.
#[derive(Debug, Clone)]
pub struct ConnectionConf {
    /// RUES HTTP endpoint URL (e.g. `https://nodes.dusk.network`).
    pub url: Url,
    /// Dusk chain ID (used in Moonlight transactions).
    pub chain_id: u8,
    /// Agent-owned directory for independently authenticated finalized-event
    /// rows. Endpoint-owned scan cursors remain process-local. Different agent
    /// processes must use different directories.
    pub event_cursor_dir: PathBuf,
    /// Default gas limit for transactions.
    pub gas_limit: u64,
    /// Default gas price in LUX.
    pub gas_price: u64,
    /// Native token configuration.
    pub native_token: NativeToken,
    /// Operation submission configuration.
    pub op_submission_config: OpSubmissionConfig,
}
