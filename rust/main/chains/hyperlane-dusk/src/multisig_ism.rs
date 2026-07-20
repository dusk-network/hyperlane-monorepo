use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;

use hyperlane_core::{
    ChainResult, HyperlaneChain, HyperlaneContract, HyperlaneDomain, HyperlaneMessage,
    HyperlaneProvider, MultisigIsm, H256,
};

use hyperlane_dusk_types::EthAddress;

use crate::{DuskProvider, RuesClient};

/// Dusk MultisigISM implementation.
#[derive(Debug, Clone)]
pub struct DuskMultisigIsm {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    ism_id: [u8; 32],
    domain: HyperlaneDomain,
}

impl DuskMultisigIsm {
    /// Create a new DuskMultisigIsm.
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

impl HyperlaneChain for DuskMultisigIsm {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new((*self.provider).clone())
    }
}

impl HyperlaneContract for DuskMultisigIsm {
    fn address(&self) -> H256 {
        H256::from_slice(&self.ism_id)
    }
}

#[async_trait]
impl MultisigIsm for DuskMultisigIsm {
    async fn validators_and_threshold(
        &self,
        _message: &HyperlaneMessage,
    ) -> ChainResult<(Vec<H256>, u8)> {
        // Read the pair in one contract query so an owner update cannot be
        // observed half-applied across independent RUES requests.
        let (validators, threshold): (Vec<EthAddress>, u8) = self
            .rues
            .contract_query(&self.ism_id, "validators_and_threshold", &())
            .await?;

        // Convert 20-byte Ethereum addresses to H256 (left-pad with 12 zero bytes).
        let validators_h256: Vec<H256> = validators
            .iter()
            .map(|addr| {
                let mut h256 = [0u8; 32];
                h256[12..32].copy_from_slice(&addr.0);
                H256::from_slice(&h256)
            })
            .collect();

        Ok((validators_h256, threshold))
    }
}
