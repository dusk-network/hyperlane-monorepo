use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use num_traits::FromPrimitive;

use hyperlane_core::{
    ChainResult, HyperlaneChain, HyperlaneContract, HyperlaneDomain, HyperlaneMessage,
    HyperlaneProvider, InterchainSecurityModule, Metadata, ModuleType, H256, U256,
};

use crate::{DuskProvider, RuesClient};

/// Dusk ISM implementation.
#[derive(Debug, Clone)]
pub struct DuskIsm {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    ism_id: [u8; 32],
    domain: HyperlaneDomain,
}

impl DuskIsm {
    /// Create a new DuskIsm.
    pub fn new(
        provider: Arc<DuskProvider>,
        rues: Arc<RuesClient>,
        ism_id: H256,
        domain: HyperlaneDomain,
    ) -> Self {
        Self {
            provider,
            rues,
            ism_id: ism_id.into(),
            domain,
        }
    }
}

impl HyperlaneChain for DuskIsm {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new((*self.provider).clone())
    }
}

impl HyperlaneContract for DuskIsm {
    fn address(&self) -> H256 {
        H256::from_slice(&self.ism_id)
    }
}

#[async_trait]
impl InterchainSecurityModule for DuskIsm {
    async fn module_type(&self) -> ChainResult<ModuleType> {
        let module_type_u8: u8 = self
            .rues
            .contract_query(&self.ism_id, "module_type", &())
            .await?;
        Ok(ModuleType::from_u8(module_type_u8).unwrap_or_default())
    }

    async fn dry_run_verify(
        &self,
        message: &HyperlaneMessage,
        metadata: &Metadata,
    ) -> ChainResult<Option<U256>> {
        let encoded = hyperlane_dusk_types::message::encode(
            message.version,
            message.nonce,
            message.origin,
            message.sender.into(),
            message.destination,
            message.recipient.into(),
            &message.body,
        );
        let metadata_bytes = metadata.to_owned();

        let result: Result<bool, _> = self
            .rues
            .contract_query(&self.ism_id, "verify", &(metadata_bytes, encoded))
            .await;

        match result {
            Ok(true) => Ok(Some(U256::zero())),
            Ok(false) | Err(_) => Ok(None),
        }
    }
}
