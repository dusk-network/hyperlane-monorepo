use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, info, warn};

use hyperlane_core::{
    Announcement, ChainResult, FixedPointNumber, HyperlaneChain, HyperlaneContract,
    HyperlaneDomain, HyperlaneProvider, SignedType, TxOutcome, ValidatorAnnounce, H256, U256,
};

use hyperlane_dusk_types::message::keccak256;
use hyperlane_dusk_types::{events, EthAddress};

use crate::rues::{rkyv_deserialize, rkyv_serialize, EventLog, LogLookup};
use crate::{ConnectionConf, DuskProvider, DuskSigner, HyperlaneDuskError, RuesClient};

const TX_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_ANNOUNCED_LOCATION_BYTES: usize = 1024;
const MAX_LOCATIONS_PER_VALIDATOR: usize = 16;
/// Archive pages a read of announced locations reads to bring the
/// announcement log up to date, on to the end of the block being read.
const ANNOUNCEMENT_LOG_PAGES: usize = 16;
/// Validators whose last read a ValidatorAnnounce keeps.
const MAX_ANNOUNCEMENT_READS: usize = 1024;

/// A validator's own startup check: its location, or why it is not final.
type OwnCheck = Result<Vec<String>, String>;

/// The last read of each validator's announced locations, and the last final
/// block it was made at. Only final announcements are reported, so a read
/// holds until the node finalizes another block.
#[derive(Debug, Default)]
struct AnnouncementReads {
    /// The locations a relayer reads, by validator.
    locations: HashMap<[u8; 20], (u64, Vec<String>)>,
    /// A validator's own startup check, by validator: its location, or why
    /// it is not final yet. A final location holds for good.
    own: HashMap<[u8; 20], (u64, OwnCheck)>,
}

impl AnnouncementReads {
    fn bound(&mut self) {
        if self.locations.len() > MAX_ANNOUNCEMENT_READS {
            self.locations.clear();
        }
        if self.own.len() > MAX_ANNOUNCEMENT_READS {
            self.own.clear();
        }
    }
}

/// The finalized event log of a ValidatorAnnounce's announcements, keyed by
/// validator and location hash.
fn announcement_log(va_id: [u8; 32]) -> EventLog {
    EventLog {
        contract_id: va_id,
        topic: events::ValidatorAnnouncement::TOPIC,
        key_of: announcement_key,
        limits: crate::rues::BLOCK_LIMITS,
    }
}

/// The key of a validator's announced location: its address and the
/// keccak256 of the location, as the contract lists it.
fn location_key(validator: &[u8; 20], location_hash: &[u8; 32]) -> Vec<u8> {
    let mut key = validator.to_vec();
    key.extend_from_slice(location_hash);
    key
}

/// The key an announcement event names, if its data is exactly the encoding
/// of an announcement the contract accepts: a location of 1 to 1024 bytes.
fn announcement_key(data: &[u8]) -> Option<Vec<u8>> {
    let announcement = decode_announcement(data)?;
    Some(location_key(
        &announcement.validator.0,
        &keccak256(announcement.storage_location.as_bytes()),
    ))
}

fn decode_announcement(data: &[u8]) -> Option<events::ValidatorAnnouncement> {
    let announcement: events::ValidatorAnnouncement = rkyv_deserialize(data).ok()?;
    let location = &announcement.storage_location;
    (!location.is_empty()
        && location.len() <= MAX_ANNOUNCED_LOCATION_BYTES
        && rkyv_serialize(&announcement).ok()? == data)
        .then_some(announcement)
}

fn eth_address(validator: &H256) -> [u8; 20] {
    let mut address = [0u8; 20];
    address.copy_from_slice(&validator.as_bytes()[12..]);
    address
}

/// Dusk ValidatorAnnounce implementation.
///
/// The contract keeps the keccak256 of each location a validator announced,
/// oldest first, and the locations are in its `ValidatorAnnouncement`
/// events. A location is the validator's when its hash is in the list.
#[derive(Debug, Clone)]
pub struct DuskValidatorAnnounce {
    provider: Arc<DuskProvider>,
    rues: Arc<RuesClient>,
    va_id: [u8; 32],
    domain: HyperlaneDomain,
    signer: Option<DuskSigner>,
    conn: ConnectionConf,
    /// The location a validator's own startup check looks for.
    own_location: Option<String>,
    /// The last reads of announced locations, shared by every clone.
    reads: Arc<StdMutex<AnnouncementReads>>,
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
            own_location: None,
            reads: Arc::default(),
        }
    }

    fn reads(&self) -> std::sync::MutexGuard<'_, AnnouncementReads> {
        self.reads.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The reader of a validator's startup check, which decides whether to
    /// announce `location`. A validator has announced it iff its hash is in
    /// the validator's list, and the check reports it only once its
    /// announcement is final, failing until then: Rusk can still roll the
    /// block back, and the validator would sign checkpoints that relayers
    /// cannot find. Finality comes from the client's announcement log alone,
    /// so a client without an `eventCursorDir` never reports the location.
    /// Reads fail instead of reporting partial results.
    pub fn with_own_location(mut self, location: impl Into<String>) -> Self {
        self.own_location = Some(location.into());
        self
    }

    /// The hashes of the locations `validator` announced at the node's tip,
    /// oldest first.
    async fn location_hashes(&self, validator: [u8; 20]) -> ChainResult<Vec<[u8; 32]>> {
        let hashes: Vec<[u8; 32]> = self
            .rues
            .contract_query(
                &self.va_id,
                "get_announced_storage_location_hashes",
                &EthAddress(validator),
            )
            .await?;
        if hashes.len() > MAX_LOCATIONS_PER_VALIDATOR {
            return Err(HyperlaneDuskError::Other(format!(
                "Dusk ValidatorAnnounce lists {} locations for validator 0x{}, more than its {MAX_LOCATIONS_PER_VALIDATOR}",
                hashes.len(),
                hex::encode(validator)
            ))
            .into());
        }
        Ok(hashes)
    }

    /// The locations of `hashes` that `validator` announced in finalized
    /// events the log has read, in the list's order. A hash with no event
    /// read yet is left out.
    fn announced_locations(
        &self,
        log: &EventLog,
        validator: [u8; 20],
        hashes: &[[u8; 32]],
    ) -> ChainResult<Vec<String>> {
        let mut locations = Vec::with_capacity(hashes.len());
        for hash in hashes {
            let found = self
                .rues
                .event_log_find(log, &location_key(&validator, hash))?
                .into_iter()
                .find_map(|entry| decode_announcement(&entry.data))
                .filter(|announcement| {
                    announcement.validator.0 == validator
                        && keccak256(announcement.storage_location.as_bytes()) == *hash
                });
            match found {
                Some(announcement) => locations.push(announcement.storage_location),
                None => debug!(
                    validator = %hex::encode(validator),
                    location_hash = %hex::encode(hash),
                    "No finalized Dusk announcement event read yet for a listed location"
                ),
            }
        }
        Ok(locations)
    }

    /// Whether `validator` has announced `location` in a final block: `[]`
    /// when its hash is not in the list, `[location]` once the announcement
    /// log holds the announcement in a block `checkBlock(onlyFinalized)`
    /// accepts, and an error until then: while the announcement is in no
    /// final block, and while the log does not reach the last final block
    /// (read before the list), cannot be read, or the client keeps none.
    /// That no block above the last final one holds the announcement never
    /// shows that it is final: a block accepted and rolled back within the
    /// check's own requests leaves no trace there. A check holds until the
    /// node finalizes another block, and a final announcement for good.
    async fn own_announcement(
        &self,
        log: &EventLog,
        validator: [u8; 20],
        location: &str,
    ) -> ChainResult<Vec<String>> {
        // Read the chain before the list the check holds, so that no
        // rollback in between goes unnoticed.
        let pair = self.rues.block_pair().await?;
        let finalized = pair.finalized();
        let known = self.reads().own.get(&validator).and_then(|(at, check)| {
            (*at == finalized || check.as_ref().is_ok_and(|found| !found.is_empty()))
                .then(|| check.clone())
        });
        let check = match known {
            Some(check) => check,
            None => {
                let check = self
                    .check_own_announcement(log, validator, location, &pair)
                    .await?;
                let mut reads = self.reads();
                reads.own.insert(validator, (finalized, check.clone()));
                reads.bound();
                check
            }
        };
        check.map_err(|reason| {
            HyperlaneDuskError::NotFinal(format!(
                "Dusk validator 0x{} announced {} at the tip, but {reason}",
                hex::encode(validator),
                crate::rues::bounded_text(location)
            ))
            .into()
        })
    }

    /// One check of `own_announcement`, whose list is read after `pair`.
    /// Fails only when the list cannot be read.
    async fn check_own_announcement(
        &self,
        log: &EventLog,
        validator: [u8; 20],
        location: &str,
        pair: &crate::rues::BlockPair,
    ) -> ChainResult<OwnCheck> {
        let location_hash = keccak256(location.as_bytes());
        // An unannounced location reads no blocks.
        if !self
            .location_hashes(validator)
            .await?
            .contains(&location_hash)
        {
            return Ok(Ok(Vec::new()));
        }
        let key = location_key(&validator, &location_hash);
        Ok(Err(
            match self
                .rues
                .finalized_event_log_lookup(log, &key, pair, ANNOUNCEMENT_LOG_PAGES)
                .await
            {
                Ok(LogLookup::Found(_)) => return Ok(Ok(vec![location.to_owned()])),
                Ok(LogLookup::Absent { final_block }) => format!(
                    "no finalized block up to block {final_block}, the last final one, holds the announcement"
                ),
                Ok(LogLookup::Unknown(reason)) => {
                    debug!(%reason, "Dusk announcement log cannot show the announcement final yet");
                    format!("the announcement log cannot show it final yet: {reason}")
                }
                Err(error) => {
                    let error = crate::rues::bounded_error(&error);
                    debug!(%error, "Dusk announcement log cannot be read; the announcement does not count yet");
                    format!(
                        "the announcement log that would show it final cannot be read: {error}"
                    )
                }
            },
        ))
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
    /// Each validator's announced locations, oldest first: those whose hash
    /// the contract lists for it, from the finalized announcement events the
    /// agent has read. A validator whose list cannot be read gets none, so
    /// enough healthy validators can still meet an ISM threshold. A reader
    /// made `with_own_location` instead reports, for each validator, whether
    /// it has announced that location in a final block.
    async fn get_announced_storage_locations(
        &self,
        validators: &[H256],
    ) -> ChainResult<Vec<Vec<String>>> {
        let log = announcement_log(self.va_id);
        if let Some(location) = &self.own_location {
            let mut all_locations = Vec::with_capacity(validators.len());
            for validator in validators {
                all_locations.push(
                    self.own_announcement(&log, eth_address(validator), location)
                        .await?,
                );
            }
            return Ok(all_locations);
        }
        // Only final announcements are reported, so reads hold until the
        // node finalizes another block.
        let finalized = match self.rues.block_pair().await {
            Ok(pair) => {
                if let Err(error) = self
                    .rues
                    .advance_event_log(&log, ANNOUNCEMENT_LOG_PAGES, Some(&pair))
                    .await
                {
                    warn!(%error, "Could not read new Dusk validator announcements; using those read before");
                }
                Some(pair.finalized())
            }
            Err(error) => {
                warn!(%error, "Could not read the Dusk node's last final block; using the announcements read before");
                None
            }
        };
        let mut all_locations = Vec::with_capacity(validators.len());
        for validator in validators {
            let address = eth_address(validator);
            let known = finalized.and_then(|finalized| {
                self.reads()
                    .locations
                    .get(&address)
                    .filter(|(at, _)| *at == finalized)
                    .map(|(_, locations)| locations.clone())
            });
            if let Some(locations) = known {
                all_locations.push(locations);
                continue;
            }
            let locations = match self.location_hashes(address).await {
                Ok(hashes) => self.announced_locations(&log, address, &hashes),
                Err(error) => Err(error),
            };
            match locations {
                Ok(locations) => {
                    if let Some(finalized) = finalized {
                        let mut reads = self.reads();
                        reads
                            .locations
                            .insert(address, (finalized, locations.clone()));
                        reads.bound();
                    }
                    all_locations.push(locations)
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
        // The helper submits to the endpoint directly: check first that it
        // serves the configured deployment.
        self.rues.ensure_identity().await?;

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
    use hyperlane_core::{
        config::OpSubmissionConfig, HyperlaneDomainProtocol, HyperlaneDomainTechnicalStack,
        NativeToken,
    };
    use std::io::{BufRead, BufReader, Read, Write};
    use std::net::TcpListener;
    use std::thread;
    use std::time::Instant;

    const LOCATION: &str = "file:///fixture/checkpoints";

    /// A reader of a node that answers `responses` in order, with an event
    /// store when `with_store` is set.
    fn reader(
        responses: Vec<(u16, Vec<u8>)>,
        with_store: bool,
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
                    first.contains("/get_announced_storage_location_hashes ")
                        || first.contains("/on/graphql/query "),
                    "{first}"
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
        let directory = tempfile::tempdir().unwrap();
        let rues = Arc::new(if with_store {
            RuesClient::new_with_event_cursor_dir(url.clone(), directory.path().join("events"))
                .unwrap()
        } else {
            RuesClient::new(url.clone()).unwrap()
        });
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
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
                query_rate_limit: crate::QueryRateLimit::default(),
            },
        );
        (reader, worker, directory)
    }

    fn hashes(locations: &[&str]) -> Vec<u8> {
        let hashes: Vec<[u8; 32]> = locations
            .iter()
            .map(|location| keccak256(location.as_bytes()))
            .collect();
        rkyv_serialize(&hashes).unwrap()
    }

    fn validator(byte: u8) -> H256 {
        let mut validator = [0u8; 32];
        validator[12..].copy_from_slice(&[byte; 20]);
        H256::from(validator)
    }

    fn announcement_row(id: i64, validator: u8, location: &str) -> serde_json::Value {
        let data = rkyv_serialize(&events::ValidatorAnnouncement {
            validator: EthAddress([validator; 20]),
            storage_location: location.to_owned(),
        })
        .unwrap();
        serde_json::json!({
            "id": id, "block_height": 9, "block_hash": "ab".repeat(32),
            "origin": hex::encode([validator; 32]), "topic": events::ValidatorAnnouncement::TOPIC,
            "source": hex::encode([2u8; 32]), "data": hex::encode(data), "reverted": false,
        })
    }

    #[test]
    fn announcement_keys_need_the_exact_encoding_of_a_valid_announcement() {
        let encode = |location: &str| {
            rkyv_serialize(&events::ValidatorAnnouncement {
                validator: EthAddress([1; 20]),
                storage_location: location.to_owned(),
            })
            .unwrap()
        };
        assert_eq!(
            announcement_key(&encode(LOCATION)),
            Some(location_key(&[1; 20], &keccak256(LOCATION.as_bytes())))
        );
        assert_eq!(announcement_key(&encode("")), None);
        assert_eq!(announcement_key(&encode(&"x".repeat(1025))), None);
        let mut padded = vec![0u8; 8];
        padded.extend_from_slice(&encode(LOCATION));
        assert_eq!(announcement_key(&padded), None);
    }

    #[tokio::test]
    async fn locations_are_resolved_from_finalized_events_by_their_listed_hashes() {
        let page = serde_json::json!({"finalizedEvents": {"json": {
            "events": [
                announcement_row(0, 1, "s3://first/old"),
                announcement_row(1, 1, "s3://first/new"),
                announcement_row(2, 3, "s3://third"),
            ],
            "startCursor": "djE6MA==", "endCursor": "djE6Mg==", "hasNextPage": false,
        }}});
        let final_tip = format!(
            r#"{{"lastBlockPair":{{"json":{{"last_block":[9,"{hash}"],"last_finalized_block":[9,"{hash}"]}}}}}}"#,
            hash = "ab".repeat(32)
        );
        let (client, worker, _directory) = reader(
            vec![
                (200, final_tip.into_bytes()),
                // The archive has finalized the node's last final block.
                (200, br#"{"checkBlock":true}"#.to_vec()),
                (200, page.to_string().into_bytes()),
                (200, br#"{"checkBlock":true}"#.to_vec()),
                // Listed in order of announcement; one listed hash has no
                // event read yet and is left out.
                (
                    200,
                    hashes(&["s3://first/old", "s3://first/new", "s3://first/pending"]),
                ),
                (503, b"temporarily unavailable".to_vec()),
                (200, hashes(&["s3://third"])),
            ],
            true,
        );
        assert_eq!(
            client
                .get_announced_storage_locations(&[validator(1), validator(2), validator(3)])
                .await
                .unwrap(),
            vec![
                vec!["s3://first/old".to_owned(), "s3://first/new".to_owned()],
                vec![],
                vec!["s3://third".to_owned()],
            ]
        );
        worker.join().unwrap();
    }

    #[tokio::test]
    async fn a_validators_own_check_rejects_errors_and_recovers() {
        // Every block is final, and block 9 holds the announcement.
        let hash = "ab".repeat(32);
        let final_tip = format!(
            r#"{{"lastBlockPair":{{"json":{{"last_block":[9,"{hash}"],"last_finalized_block":[9,"{hash}"]}}}}}}"#
        )
        .into_bytes();
        let finalized = br#"{"checkBlock":true}"#.to_vec();
        let page = serde_json::json!({"finalizedEvents": {"json": {
            "events": [announcement_row(0, 1, LOCATION)],
            "startCursor": "djE6MA==", "endCursor": "djE6MA==", "hasNextPage": false,
        }}})
        .to_string()
        .into_bytes();
        let too_many: Vec<String> = (0..17).map(|n| format!("s3://{n}")).collect();
        let too_many: Vec<&str> = too_many.iter().map(String::as_str).collect();
        for failure in [
            (503, b"temporarily unavailable".to_vec()),
            (200, vec![1, 2, 3]),
            (200, hashes(&too_many)),
        ] {
            let (client, worker, _directory) = reader(
                vec![
                    // The chain, read once for both checks, then the list.
                    (200, final_tip.clone()),
                    failure,
                    (200, hashes(&[LOCATION])),
                    // The announcement log: the archive has finalized the
                    // last final block, holds the announcement there, and
                    // checkBlock accepts its block when it is taken and
                    // when it is found.
                    (200, finalized.clone()),
                    (200, page.clone()),
                    (200, finalized.clone()),
                    (200, finalized.clone()),
                ],
                true,
            );
            let client = client.with_own_location(LOCATION);
            assert!(client
                .get_announced_storage_locations(&[validator(1)])
                .await
                .is_err());
            assert_eq!(
                client
                    .get_announced_storage_locations(&[validator(1)])
                    .await
                    .unwrap(),
                vec![vec![LOCATION.to_owned()]]
            );
            worker.join().unwrap();
            // A final announcement holds for good: no request.
            assert_eq!(
                client
                    .get_announced_storage_locations(&[validator(1)])
                    .await
                    .unwrap(),
                vec![vec![LOCATION.to_owned()]]
            );
        }
    }

    #[tokio::test]
    async fn a_validators_own_check_reads_its_location_and_no_other() {
        // Another location's hash is listed, not this one: not announced.
        let final_tip = format!(
            r#"{{"lastBlockPair":{{"json":{{"last_block":[9,"{hash}"],"last_finalized_block":[9,"{hash}"]}}}}}}"#,
            hash = "ab".repeat(32)
        );
        let (client, worker, _directory) = reader(
            vec![
                (200, final_tip.into_bytes()),
                (200, hashes(&["s3://another"])),
            ],
            false,
        );
        assert_eq!(
            client
                .with_own_location(LOCATION)
                .get_announced_storage_locations(&[validator(1)])
                .await
                .unwrap(),
            vec![Vec::<String>::new()]
        );
        worker.join().unwrap();
    }
}
