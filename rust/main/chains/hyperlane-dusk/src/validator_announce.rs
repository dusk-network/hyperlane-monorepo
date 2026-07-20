use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use hyperlane_core::{
    Announcement, ChainResult, FixedPointNumber, HyperlaneChain, HyperlaneContract,
    HyperlaneDomain, HyperlaneProvider, SignedType, TxOutcome, ValidatorAnnounce, H256, U256,
};

use hyperlane_dusk_types::EthAddress;

use crate::{ConnectionConf, DuskProvider, DuskSigner, HyperlaneDuskError, RuesClient};

const TX_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Dusk ValidatorAnnounce implementation.
#[derive(Debug, Clone)]
pub struct DuskValidatorAnnounce {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    va_id: [u8; 32],
    domain: HyperlaneDomain,
    signer: Option<DuskSigner>,
    conn: ConnectionConf,
}

impl DuskValidatorAnnounce {
    /// Create a new DuskValidatorAnnounce.
    pub fn new(
        provider: Arc<DuskProvider>,
        rues: Arc<RuesClient>,
        va_id: H256,
        domain: HyperlaneDomain,
        signer: Option<DuskSigner>,
        conn: ConnectionConf,
    ) -> Self {
        Self {
            provider,
            rues,
            va_id: va_id.into(),
            domain,
            signer,
            conn,
        }
    }
}

impl HyperlaneChain for DuskValidatorAnnounce {
    fn domain(&self) -> &HyperlaneDomain {
        &self.domain
    }

    fn provider(&self) -> Box<dyn HyperlaneProvider> {
        Box::new((*self.provider).clone())
    }
}

impl HyperlaneContract for DuskValidatorAnnounce {
    fn address(&self) -> H256 {
        H256::from_slice(&self.va_id)
    }
}

#[async_trait]
impl ValidatorAnnounce for DuskValidatorAnnounce {
    async fn get_announced_storage_locations(
        &self,
        validators: &[H256],
    ) -> ChainResult<Vec<Vec<String>>> {
        // Convert H256 validator addresses to 20-byte EthAddress (last 20 bytes).
        let eth_addrs: Vec<EthAddress> = validators
            .iter()
            .map(|h| {
                let mut addr = [0u8; 20];
                addr.copy_from_slice(&h.as_bytes()[12..]);
                EthAddress(addr)
            })
            .collect();

        let locations: Vec<Vec<String>> = self
            .rues
            .contract_query(
                &self.va_id,
                "get_announced_storage_locations",
                &(eth_addrs,),
            )
            .await?;

        Ok(locations)
    }

    async fn announce(&self, announcement: SignedType<Announcement>) -> ChainResult<TxOutcome> {
        let signer = self
            .signer
            .as_ref()
            .ok_or(HyperlaneDuskError::SignerUnavailable)?;

        info!(
            validator = ?announcement.value.validator,
            location = %announcement.value.storage_location,
            "Announcing validator storage location on Dusk via dusk-tx"
        );

        // Extract the 20-byte validator Ethereum address from the H256.
        let mut validator_eth_addr = [0u8; 20];
        validator_eth_addr.copy_from_slice(&announcement.value.validator.as_bytes()[12..]);

        // Extract the 65-byte ECDSA signature.
        let signature: [u8; 65] = announcement.signature.into();

        let args = crate::tx_sender::announce_args(
            validator_eth_addr,
            &announcement.value.storage_location,
            &signature,
        )?;

        let res = crate::tx_sender::dusk_tx_call(
            &self.conn,
            signer,
            &self.va_id,
            "announce",
            &args,
            None,
        )
        .await?;

        let tx_id = res.get("tx_id").and_then(|v| v.as_str()).ok_or_else(|| {
            HyperlaneDuskError::Other(format!("dusk-tx response is missing string tx_id: {res}"))
        })?;
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(tx_id)?;
        let confirmed = self
            .rues
            .wait_for_tx(tx_id, TX_CONFIRMATION_TIMEOUT)
            .await?;
        let executed = confirmed.error.is_none();
        if let Some(error) = &confirmed.error {
            warn!(tx_id, %error, "Dusk validator announcement execution failed");
        }

        Ok(TxOutcome {
            transaction_id,
            executed,
            gas_used: U256::from(confirmed.gas_spent),
            gas_price: FixedPointNumber::from(self.conn.gas_price),
        })
    }

    async fn announce_tokens_needed(
        &self,
        _announcement: SignedType<Announcement>,
        _chain_signer: H256,
    ) -> Option<U256> {
        // No deposit required for announcements on Dusk.
        Some(U256::zero())
    }
}
