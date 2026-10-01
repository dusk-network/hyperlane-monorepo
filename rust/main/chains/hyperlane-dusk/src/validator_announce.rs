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
const MAX_ANNOUNCED_LOCATION_BYTES: usize = 1024;
const MAX_LOCATIONS_PER_VALIDATOR: usize = 16;

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
        // Query one validator at a time. A legacy/poisoned record or one
        // unavailable response must not prevent the relayer from using enough
        // healthy validators to satisfy the ISM threshold.
        let mut all_locations = Vec::with_capacity(validators.len());
        for validator in validators {
            let mut address = [0u8; 20];
            address.copy_from_slice(&validator.as_bytes()[12..]);
            let result: Result<Vec<String>, _> = self
                .rues
                .contract_query(
                    &self.va_id,
                    "get_announced_storage_locations_for_validator",
                    &EthAddress(address),
                )
                .await;
            match result {
                Ok(locations)
                    if locations.len() <= MAX_LOCATIONS_PER_VALIDATOR
                        && locations.iter().all(|location| {
                            !location.is_empty() && location.len() <= MAX_ANNOUNCED_LOCATION_BYTES
                        }) =>
                {
                    all_locations.push(locations)
                }
                Ok(locations) => {
                    warn!(
                        validator = ?validator,
                        locations = locations.len(),
                        "Ignoring invalid Dusk validator location history"
                    );
                    all_locations.push(Vec::new());
                }
                Err(error) => {
                    warn!(
                        validator = ?validator,
                        %error,
                        "Ignoring unavailable Dusk validator location history"
                    );
                    all_locations.push(Vec::new());
                }
            }
        }
        Ok(all_locations)
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

        // `Announcement::validator` is already an H160. Slicing it as though it
        // were an H256 leaves only eight bytes and panics on every normal
        // self-announcement.
        let validator_eth_addr = *announcement.value.validator.as_fixed_bytes();

        // Extract the 65-byte ECDSA signature.
        let signature: [u8; 65] = announcement.signature.into();

        let args = crate::tx_sender::announce_args(
            validator_eth_addr,
            &announcement.value.storage_location,
            &signature,
        )?;

        let call_result = crate::tx_sender::dusk_tx_call(
            &self.conn,
            signer,
            &self.va_id,
            "announce",
            &args,
            None,
        )
        .await;

        let tx_id = match call_result {
            Ok(response) => response
                .get("tx_id")
                .and_then(|value| value.as_str())
                .ok_or_else(|| {
                    HyperlaneDuskError::Other(format!(
                        "dusk-tx response is missing string tx_id: {response}"
                    ))
                })?
                .to_owned(),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, detail }) => {
                warn!(%tx_id, %detail, "Reconciling outcome-unknown Dusk announcement by exact hash");
                tx_id
            }
            Err(error) => return Err(error.into()),
        };
        let transaction_id = crate::tx_sender::dusk_tx_id_to_h512(&tx_id)?;
        let confirmed = self
            .rues
            .wait_for_tx(&tx_id, TX_CONFIRMATION_TIMEOUT)
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
