use hyperlane_core::{ChainCommunicationError, HyperlaneCustomErrorWrapper, H512};

/// Errors specific to the Dusk chain integration.
#[derive(Debug, thiserror::Error)]
pub enum HyperlaneDuskError {
    /// HTTP error communicating with the RUES endpoint.
    #[error("RUES HTTP request failed ({kind}, status={status:?})")]
    RuesHttp {
        /// Stable failure class without request URL or response details.
        kind: &'static str,
        /// HTTP status when the failure reached a response.
        status: Option<u16>,
    },
    /// Non-success response from the RUES endpoint.
    #[error("RUES error response ({status}): {body}")]
    RuesResponse {
        /// HTTP status code.
        status: u16,
        /// Response body.
        body: String,
    },
    /// rkyv deserialization error.
    #[error("rkyv deserialization error: {0}")]
    RkyvDeserialize(String),
    /// Transaction not found.
    #[error("Transaction not found: {0:?}")]
    TransactionNotFound(H512),
    /// Block not found.
    #[error("Block not found at height: {0}")]
    BlockNotFound(u64),
    /// Signer is not configured.
    #[error("Signer unavailable")]
    SignerUnavailable,
    /// The helper constructed a transaction but could not determine whether
    /// propagation reached the node. Callers must reconcile this exact hash.
    #[error("Dusk transaction {tx_id} propagation outcome is unknown: {detail}")]
    SubmissionOutcomeUnknown {
        /// Canonical 32-byte Dusk transaction hash (lowercase hex).
        tx_id: String,
        /// Original helper diagnostic.
        detail: String,
    },
    /// The configured BLS secret key is invalid.
    #[error("Invalid BLS secret key: {0}")]
    InvalidBlsSecretKey(String),
    /// Generic error.
    #[error("{0}")]
    Other(String),
}

impl From<reqwest::Error> for HyperlaneDuskError {
    fn from(error: reqwest::Error) -> Self {
        let error = error.without_url();
        let status = error.status().map(|status| status.as_u16());
        let kind = if error.is_timeout() {
            "timeout"
        } else if error.is_connect() {
            "connect"
        } else if error.is_request() {
            "request"
        } else if error.is_body() {
            "body"
        } else if error.is_decode() {
            "decode"
        } else if error.is_status() {
            "status"
        } else {
            "other"
        };
        Self::RuesHttp { kind, status }
    }
}

impl From<HyperlaneDuskError> for ChainCommunicationError {
    fn from(value: HyperlaneDuskError) -> Self {
        match value {
            HyperlaneDuskError::SignerUnavailable => ChainCommunicationError::SignerUnavailable,
            other => {
                ChainCommunicationError::Other(HyperlaneCustomErrorWrapper::new(Box::new(other)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signer_unavailable_keeps_its_non_retryable_error_domain() {
        assert!(matches!(
            ChainCommunicationError::from(HyperlaneDuskError::SignerUnavailable),
            ChainCommunicationError::SignerUnavailable
        ));
        assert!(matches!(
            ChainCommunicationError::from(HyperlaneDuskError::Other("rpc".into())),
            ChainCommunicationError::Other(_)
        ));
    }

    #[tokio::test]
    async fn rues_http_errors_never_retain_url_credentials_or_paths() {
        let sentinel = "private-path-sentinel";
        let error = reqwest::Client::new()
            .get(format!(
                "http://user:password@127.0.0.1:1/{sentinel}?token=query-sentinel"
            ))
            .send()
            .await
            .unwrap_err();
        let public = HyperlaneDuskError::from(error).to_string();
        for secret in [sentinel, "query-sentinel", "user", "password"] {
            assert!(!public.contains(secret));
        }
    }
}
