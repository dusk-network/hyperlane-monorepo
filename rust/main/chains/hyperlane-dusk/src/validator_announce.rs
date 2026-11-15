use std::fmt::Debug;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tracing::{info, warn};

use hyperlane_core::{
    Announcement, ChainResult, FixedPointNumber, HyperlaneChain, HyperlaneContract,
    HyperlaneDomain, HyperlaneProvider, SignedType, TxOutcome, ValidatorAnnounce, H256, U256,
};

use hyperlane_dusk_types::{events, EthAddress};

use crate::rues::{rkyv_deserialize, BlockPair};
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
    require_all_reads: bool,
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
            require_all_reads: false,
        }
    }

    /// Require observed, final announcement state before making a submission
    /// decision. Relayer aggregation retains the default per-validator
    /// partial results, which may include announcements that are not final.
    pub fn with_strict_reads(mut self) -> Self {
        self.require_all_reads = true;
        self
    }

    /// Fail while any of `locations`, read from the node's tip after `pair`,
    /// was announced in a block that is not final yet. A validator's startup
    /// waits for its announcement until then: Rusk can still roll the block
    /// back, and the validator would sign checkpoints that relayers cannot
    /// find.
    async fn ensure_final_locations(
        &self,
        pair: &BlockPair,
        validators: &[H256],
        locations: &[Vec<String>],
    ) -> ChainResult<()> {
        if locations.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let not_final = |detail: String| {
            HyperlaneDuskError::NotFinal(format!(
                "Dusk validator announcements are at the tip, but {detail}"
            ))
        };
        let pending = self
            .rues
            .non_final_events(pair, &self.va_id, events::ValidatorAnnouncement::TOPIC)
            .await
            .map_err(|error| not_final(format!("their finality cannot be checked yet: {error}")))?;
        for event in pending {
            let announcement = rkyv_deserialize::<events::ValidatorAnnouncement>(&event.data)
                .map_err(|error| {
                    not_final(format!(
                        "non-final block {} holds an undecodable announcement: {error}",
                        event.block_height
                    ))
                })?;
            let announced = validators
                .iter()
                .zip(locations)
                .any(|(validator, locations)| {
                    validator.as_bytes()[12..] == announcement.validator.0
                        && locations.contains(&announcement.storage_location)
                });
            if announced {
                return Err(not_final(format!(
                    "validator 0x{} announced {} in block {} ({:?}), which is not final yet",
                    hex::encode(announcement.validator.0),
                    crate::rues::bounded_text(&announcement.storage_location),
                    event.block_height,
                    event.block_hash
                ))
                .into());
            }
        }
        Ok(())
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

impl DuskValidatorAnnounce {
    /// Each validator's announced storage locations at the node's tip.
    async fn tip_storage_locations(&self, validators: &[H256]) -> ChainResult<Vec<Vec<String>>> {
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
                    if self.require_all_reads {
                        return Err(HyperlaneDuskError::Other(
                            "Invalid Dusk validator location history".into(),
                        )
                        .into());
                    }
                    warn!(
                        validator = ?validator,
                        locations = locations.len(),
                        "Ignoring invalid Dusk validator location history"
                    );
                    all_locations.push(Vec::new());
                }
                Err(error) => {
                    if self.require_all_reads {
                        return Err(error.into());
                    }
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
}

#[async_trait]
impl ValidatorAnnounce for DuskValidatorAnnounce {
    async fn get_announced_storage_locations(
        &self,
        validators: &[H256],
    ) -> ChainResult<Vec<Vec<String>>> {
        let locations = self.tip_storage_locations(validators).await?;
        if !self.require_all_reads || locations.iter().all(Vec::is_empty) {
            return Ok(locations);
        }
        // A strict read reports locations only once they are final. Read the
        // chain before the locations it checks, so that no rollback in
        // between goes unnoticed.
        let pair = self.rues.block_pair().await?;
        let locations = self.tip_storage_locations(validators).await?;
        self.ensure_final_locations(&pair, validators, &locations)
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
                        "dusk-tx response is missing string tx_id: {}",
                        crate::rues::bounded_json(&response)
                    ))
                })?
                .to_owned(),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, detail })
            | Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id, detail }) => {
                warn!(%tx_id, %detail, "Reconciling Dusk announcement receipt by exact hash");
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
            let error = crate::rues::bounded_text(error);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rues::rkyv_serialize;
    use hyperlane_core::{
        config::OpSubmissionConfig, HyperlaneDomainProtocol, HyperlaneDomainTechnicalStack,
        NativeToken,
    };
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Instant;

    fn reader(
        responses: Vec<(u16, Vec<u8>)>,
    ) -> (
        DuskValidatorAnnounce,
        thread::JoinHandle<()>,
        tempfile::TempDir,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let worker = thread::spawn(move || {
            for (status, body) in responses {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut stream = loop {
                    match listener.accept() {
                        Ok((stream, _)) => break stream,
                        Err(error) => {
                            assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
                            assert!(
                                Instant::now() < deadline,
                                "announcement fixture request timed out"
                            );
                            thread::sleep(Duration::from_millis(2));
                        }
                    }
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut request = BufReader::new(&mut stream);
                let mut first = String::new();
                request.read_line(&mut first).unwrap();
                assert!(
                    first.contains("/get_announced_storage_locations_for_validator ")
                        || first.contains("/on/graphql/query ")
                );
                let mut length = 0;
                loop {
                    let mut header = String::new();
                    assert_ne!(request.read_line(&mut header).unwrap(), 0);
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:")
                    {
                        length = value.trim().parse::<usize>().unwrap();
                    }
                }
                let mut input = vec![0; length];
                request.read_exact(&mut input).unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status} Fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .unwrap();
                stream.write_all(&body).unwrap();
            }
        });
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-fixture",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let rues = Arc::new(RuesClient::new(url.clone()).unwrap());
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        let directory = tempfile::tempdir().unwrap();
        let reader = DuskValidatorAnnounce::new(
            provider,
            rues,
            H256::repeat_byte(2),
            domain,
            None,
            ConnectionConf {
                url,
                chain_id: 7,
                event_cursor_dir: directory.path().to_path_buf(),
                gas_limit: 1_000_000,
                gas_price: 1,
                native_token: NativeToken::default(),
                op_submission_config: OpSubmissionConfig::default(),
            },
        );
        (reader, worker, directory)
    }

    #[tokio::test]
    async fn strict_announcement_observation_rejects_errors_and_recovers() {
        let existing = vec!["file:///fixture/checkpoints".to_owned()];
        let invalid_histories = [
            vec![String::new()],
            vec!["x".repeat(1025)],
            vec!["file:///fixture".to_owned(); 17],
        ];
        let mut failures = vec![
            (503, b"temporarily unavailable".to_vec()),
            (200, vec![1, 2, 3]),
        ];
        failures.extend(
            invalid_histories
                .into_iter()
                .map(|locations| (200, rkyv_serialize(&locations).unwrap())),
        );
        // A location counts once its block is final; here every block is.
        let hash = "ab".repeat(32);
        let final_tip = format!(
            r#"{{"lastBlockPair":{{"json":{{"last_block":[9,"{hash}"],"last_finalized_block":[9,"{hash}"]}}}}}}"#
        )
        .into_bytes();
        for failure in failures {
            let (client, worker, _directory) = reader(vec![
                failure,
                (200, rkyv_serialize(&existing).unwrap()),
                (200, final_tip.clone()),
                (200, rkyv_serialize(&existing).unwrap()),
                (200, br#"{"blocks":[]}"#.to_vec()),
            ]);
            let client = client.with_strict_reads();
            assert!(client
                .get_announced_storage_locations(&[H256::repeat_byte(1)])
                .await
                .is_err());
            assert_eq!(
                client
                    .get_announced_storage_locations(&[H256::repeat_byte(1)])
                    .await
                    .unwrap(),
                vec![existing.clone()]
            );
            worker.join().unwrap();
        }
    }

    #[tokio::test]
    async fn partial_announcement_observation_retains_healthy_validator_positions() {
        let first = vec!["file:///fixture/first".to_owned()];
        let last = vec!["file:///fixture/last".to_owned()];
        let (client, worker, _directory) = reader(vec![
            (200, rkyv_serialize(&first).unwrap()),
            (503, vec![]),
            (200, rkyv_serialize(&last).unwrap()),
        ]);
        assert_eq!(
            client
                .get_announced_storage_locations(&[
                    H256::repeat_byte(1),
                    H256::repeat_byte(2),
                    H256::repeat_byte(3)
                ])
                .await
                .unwrap(),
            vec![first, vec![], last]
        );
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn strict_announcement_observation_accepts_confirmed_absence() {
        let (client, worker, _directory) =
            reader(vec![(200, rkyv_serialize(&Vec::<String>::new()).unwrap())]);
        assert_eq!(
            client
                .with_strict_reads()
                .get_announced_storage_locations(&[H256::repeat_byte(1)])
                .await
                .unwrap(),
            vec![Vec::<String>::new()]
        );
        worker.join().unwrap();
    }
}
