use hyperlane_core::{ChainCommunicationError, HyperlaneCustomErrorWrapper, H512};

/// Errors specific to the Dusk chain integration.
#[derive(Debug, thiserror::Error)]
pub enum HyperlaneDuskError {
    /// HTTP error communicating with the RUES endpoint.
    #[error("RUES HTTP error: {0}")]
    RuesHttp(#[from] reqwest::Error),
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
}
