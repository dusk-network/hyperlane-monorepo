use async_trait::async_trait;
use derive_new::new;

use hyperlane_core::HyperlaneMessage;
use hyperlane_operation_verifier::{
    ApplicationOperationVerifier, ApplicationOperationVerifierReport,
};

/// Application context verifier for Dusk (no-op).
#[derive(new)]
pub struct DuskApplicationOperationVerifier {}

#[async_trait]
impl ApplicationOperationVerifier for DuskApplicationOperationVerifier {
    async fn verify(
        &self,
        _app_context: &Option<String>,
        _message: &HyperlaneMessage,
    ) -> Option<ApplicationOperationVerifierReport> {
        None
    }
}
