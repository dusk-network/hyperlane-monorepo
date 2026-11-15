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
    /// A response body over the agent's transport limit, refused from its
    /// declared length or once reading it passed the limit.
    #[error("{context} response exceeds {limit} bytes")]
    ResponseTooLarge {
        /// What the request was for.
        context: &'static str,
        /// The limit, in bytes.
        limit: usize,
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
    /// The helper observed an included execution failure. Reconcile its exact
    /// receipt so callers can account for gas despite unsuccessful execution.
    #[error("Dusk transaction {tx_id} execution failed: {detail}")]
    TransactionExecutionFailed {
        /// Canonical 32-byte Dusk transaction hash (lowercase hex).
        tx_id: String,
        /// Original helper diagnostic.
        detail: String,
    },
    /// The configured BLS secret key is invalid.
    #[error("Invalid BLS secret key: {0}")]
    InvalidBlsSecretKey(String),
    /// An archived event is demonstrably inconsistent with contract state or
    /// finalized block provenance. Transport failures must not use this variant.
    #[error("{0}")]
    FinalizedEventMismatch(String),
    /// The endpoint has not been shown to serve the configured deployment
    /// yet: its chain ID, contracts and their bindings. No request was sent.
    #[error("Dusk endpoint identity is not verified yet: {0}")]
    IdentityUnverified(String),
    /// A record is in the node's current state, but the agent cannot show
    /// yet that the block holding it is final. The caller checks again later.
    #[error("{0}")]
    NotFinal(String),
    /// Finalized contract state proves rows exist in a finalized block that
    /// the endpoint's archive has passed without them. Retrying the same
    /// archive does not help; its node has to repair it.
    #[error("{0}")]
    ArchiveMissingRows(String),
    /// An event log found that the endpoint's finalized history changed
    /// under sequences consumers have read. It stops, rather than number
    /// them differently, until the operator resets the agent's database and
    /// its event store.
    #[error("{0}")]
    EventLogHalted(String),
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
