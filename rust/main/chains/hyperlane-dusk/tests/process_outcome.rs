#![cfg(unix)]

// Public process regression for receipt accounting. The helper is local, consumes a
// synthetic fixture signer from stdin without printing it, and never submits.
use hyperlane_core::{
    config::OpSubmissionConfig, ChainCommunicationError, FixedPointNumber, HyperlaneDomain,
    HyperlaneDomainProtocol, HyperlaneDomainTechnicalStack, HyperlaneMessage, Mailbox, Metadata,
    NativeToken, TxCostEstimate, TxOutcome, H256, U256,
};
use hyperlane_dusk::{ConnectionConf, DuskMailbox, DuskProvider, DuskSigner, RuesClient};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tempfile::TempDir;
use url::Url;

static HELPER_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const FULL_GAS: u64 = 500_000_000;
const TX: [u8; 32] = [0x11; 32];
const MAILBOX: [u8; 32] = [0x02; 32];

/// How the endpoint answers the unsigned `process` query.
#[derive(Clone, Copy, Debug)]
enum Simulation {
    /// Rusk's answer when `process` returns: the encoding of `()`, which is
    /// empty.
    Passes,
    /// Rusk's answer to a contract panic in a query.
    Panics,
    /// A proxy in front of the node answers with its own page and status 200.
    ProxyPage,
    /// A success reply that is not the encoding of `()`.
    UnexpectedValue,
    /// An error reply with a body far larger than any diagnostic needs.
    HugeError,
}

/// Size of the `HugeError` body.
const HUGE_ERROR_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug)]
enum Helper {
    Success,
    OutcomeUnknown,
    IncludedFailure,
    RejectedBeforeSubmission,
    AbruptExit,
    Interrupted,
    MalformedOutput,
    MissingTransactionId,
    UnrecognizedError,
    PreverifyRejected,
    PreverifyUnavailable,
    SuccessDifferentIdentity,
    ErrorDifferentIdentity,
    OpaqueDiagnosticIdentity,
}
struct Fixture {
    url: Url,
    directory: TempDir,
    stop: Arc<AtomicBool>,
    queries: Arc<AtomicUsize>,
    simulations: Arc<AtomicUsize>,
    worker: Option<JoinHandle<()>>,
}
impl Fixture {
    fn start(helper: Helper) -> Self {
        Self::start_with_simulation(helper, Simulation::Passes)
    }
    fn start_with_simulation(helper: Helper, simulation: Simulation) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let tx = hex::encode(TX);
        let (value, code) = match helper {
            Helper::Success => (serde_json::json!({"success":true,"tx_id":tx}), 0),
            Helper::SuccessDifferentIdentity => (
                serde_json::json!({"success":true,"tx_id":"22".repeat(32)}),
                0,
            ),
            Helper::ErrorDifferentIdentity => (
                serde_json::json!({"success":false,"error":format!(
                    "Transaction {} failed: unrelated observation", "22".repeat(32))}),
                1,
            ),
            Helper::OpaqueDiagnosticIdentity => (
                serde_json::json!({"success":false,"error":format!(
                    "Transaction {tx} submission failed: Propagation outcome unknown: diagnostic tx_id={}; retain tx_id={tx}", "22".repeat(32))}),
                1,
            ),

            Helper::OutcomeUnknown => (
                serde_json::json!({"success":false,"error":format!(
                "Transaction {tx} confirmation outcome unknown: temporary query error; retain tx_id={tx} and reconcile this exact hash before retrying")}),
                1,
            ),
            Helper::IncludedFailure => (
                serde_json::json!({"success":false,"error":format!(
                "Transaction {tx} failed: recipient rejected")}),
                1,
            ),
            Helper::RejectedBeforeSubmission => (
                serde_json::json!({"success":false,"error":"Insufficient balance before transaction preparation"}),
                1,
            ),
            Helper::AbruptExit | Helper::Interrupted => (serde_json::Value::Null, 1),
            Helper::MalformedOutput => (serde_json::Value::Null, 0),
            Helper::MissingTransactionId => (serde_json::json!({"success":true}), 0),
            Helper::UnrecognizedError => (
                serde_json::json!({"success":false,"error":"Observation interrupted"}),
                1,
            ),
            Helper::PreverifyRejected => (
                serde_json::json!({"success":false,"error":format!(
                    "Transaction {tx} submission failed: Preverify rejected before propagation (400 Bad Request): invalid transaction; retain tx_id={tx}")}),
                1,
            ),
            Helper::PreverifyUnavailable => (
                serde_json::json!({"success":false,"error":format!(
                    "Transaction {tx} submission failed: Preverify failed before propagation: connection closed; retain tx_id={tx}")}),
                1,
            ),
        };
        let helper_path = directory.path().join("fixture-helper");
        let output = match helper {
            Helper::AbruptExit | Helper::Interrupted => String::new(),
            Helper::MalformedOutput => "{".to_owned(),
            _ => serde_json::to_string(&value).unwrap(),
        };
        let termination = if matches!(helper, Helper::Interrupted) {
            "kill -TERM \"$$\"".to_owned()
        } else {
            format!("exit {code}")
        };
        let prepared = if matches!(helper, Helper::RejectedBeforeSubmission) {
            String::new()
        } else {
            format!("printf '%s\\n' '  Prepared TX {tx}; reconcile this exact hash before retrying if submission is interrupted' >&2\n")
        };
        std::fs::write(&helper_path,format!(
            "#!/bin/sh\n: > \"$0.invoked\"\nIFS= read -r fixture_url\nIFS= read -r fixture_input\n[ -n \"$fixture_url\" ] && [ -n \"$fixture_input\" ] || exit 89\nfor fixture_arg do\n  [ \"$fixture_arg\" != \"$fixture_url\" ] && [ \"$fixture_arg\" != \"$fixture_input\" ] || exit 90\ndone\n{prepared}printf '%s\\n' '{output}'\n{termination}\n")).unwrap();
        std::fs::set_permissions(&helper_path, std::fs::Permissions::from_mode(0o700)).unwrap();
        // This is only the test process environment; live agents are separate.
        std::env::set_var("DUSK_TX_BIN", &helper_path);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = stop.clone();
        let queries = Arc::new(AtomicUsize::new(0));
        let worker_queries = queries.clone();
        let simulations = Arc::new(AtomicUsize::new(0));
        let worker_simulations = simulations.clone();
        let simulation_path = format!("/on/contracts:{}/process", hex::encode(MAILBOX));
        let simulation_args = rkyv::to_bytes::<_, 256>(&(Vec::<u8>::new(), encoded_message()))
            .unwrap()
            .to_vec();
        let worker = thread::spawn(move || {
            while !worker_stop.load(Ordering::SeqCst) {
                let (mut stream, _) = match listener.accept() {
                    Ok(c) => c,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                        continue;
                    }
                    Err(e) => panic!("fixture accept: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = BufReader::new(&mut stream);
                let mut request = String::new();
                reader.read_line(&mut request).unwrap();
                let path = request.split_whitespace().nth(1).unwrap().to_owned();
                let mut length = 0usize;
                loop {
                    let mut header = String::new();
                    reader.read_line(&mut header).unwrap();
                    if header == "\r\n" {
                        break;
                    }
                    if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = v.trim().parse().unwrap();
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).unwrap();
                let (status, reply) = if path == simulation_path {
                    assert_eq!(body, simulation_args, "unexpected process query argument");
                    worker_simulations.fetch_add(1, Ordering::SeqCst);
                    match simulation {
                        Simulation::Passes => ("200 OK", Vec::new()),
                        Simulation::Panics => (
                            "500 Internal Server Error",
                            br#"{"error":"Internal server error"}"#.to_vec(),
                        ),
                        Simulation::ProxyPage => (
                            "200 OK",
                            b"<html><body>502 Bad Gateway</body></html>".to_vec(),
                        ),
                        Simulation::UnexpectedValue => ("200 OK", vec![0]),
                        Simulation::HugeError => {
                            ("500 Internal Server Error", vec![b'x'; HUGE_ERROR_BYTES])
                        }
                    }
                } else {
                    assert_eq!(path, "/on/graphql/query");
                    let query = String::from_utf8(body).unwrap();
                    assert_eq!(
                        query,
                        format!("query {{ tx(hash: \"{tx}\") {{ gasSpent err }} }}")
                    );
                    worker_queries.fetch_add(1, Ordering::SeqCst);
                    let reply = serde_json::to_vec(&serde_json::json!({"tx":{
                        "gasSpent":FULL_GAS,"err":"recipient rejected"}}))
                    .unwrap();
                    ("200 OK", reply)
                };
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    reply.len()
                )
                .unwrap();
                stream.write_all(&reply).unwrap();
            }
        });
        Self {
            url,
            directory,
            stop,
            queries,
            simulations,
            worker: Some(worker),
        }
    }
    fn simulations(&self) -> usize {
        self.simulations.load(Ordering::SeqCst)
    }
    fn helper_invoked(&self) -> bool {
        self.directory
            .path()
            .join("fixture-helper.invoked")
            .exists()
    }
    fn mailbox(&self) -> DuskMailbox {
        let domain = HyperlaneDomain::from_config(
            4242,
            "dusk-fixture",
            HyperlaneDomainProtocol::Dusk,
            HyperlaneDomainTechnicalStack::Other,
        )
        .unwrap();
        let rues = Arc::new(RuesClient::new(self.url.clone()).unwrap());
        let provider = Arc::new(DuskProvider::new(domain.clone(), rues.clone()));
        let signer = DuskSigner::new(H256::repeat_byte(1)).unwrap(); // synthetic fixture only
        DuskMailbox::new(
            provider,
            rues,
            H256::from(MAILBOX),
            H256::repeat_byte(3),
            domain,
            Some(signer),
            ConnectionConf {
                url: self.url.clone(),
                chain_id: 0,
                event_cursor_dir: self.directory.path().to_path_buf(),
                gas_limit: FULL_GAS,
                gas_price: 2,
                native_token: NativeToken::default(),
                op_submission_config: OpSubmissionConfig::default(),
            },
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        std::env::remove_var("DUSK_TX_BIN");
        if let Err(panic) = self.worker.take().unwrap().join() {
            if !thread::panicking() {
                std::panic::resume_unwind(panic);
            }
        }
    }
}
fn message() -> HyperlaneMessage {
    HyperlaneMessage {
        version: 3,
        nonce: 0,
        origin: 7,
        sender: H256::repeat_byte(4),
        destination: 4242,
        recipient: H256::repeat_byte(5),
        body: vec![0],
    }
}
/// `message()` in the Dusk wire format, built independently of the adapter.
fn encoded_message() -> Vec<u8> {
    hyperlane_dusk_types::message::encode(3, 0, 7, [4; 32], 4242, [5; 32], &[0])
}
async fn process(fixture: &Fixture) -> Result<TxOutcome, String> {
    fixture
        .mailbox()
        .process(
            &message(),
            &Metadata::new(vec![]),
            Some(U256::from(FULL_GAS)),
        )
        .await
        .map_err(|e| e.to_string())
}
fn assert_failed_outcome(outcome: TxOutcome) {
    assert!(!outcome.executed);
    assert_eq!(outcome.gas_used, U256::from(FULL_GAS));
    assert_eq!(&outcome.transaction_id.as_bytes()[32..], &TX);
}
#[tokio::test]
async fn successful_helper_control_reconciles_a_failed_ledger_receipt() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::Success);
    assert_failed_outcome(process(&fixture).await.unwrap());
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn outcome_unknown_control_reconciles_a_failed_ledger_receipt() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::OutcomeUnknown);
    assert_failed_outcome(process(&fixture).await.unwrap());
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn pre_submission_rejection_control_does_not_charge_a_receipt() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::RejectedBeforeSubmission);
    assert!(process(&fixture)
        .await
        .unwrap_err()
        .contains("Insufficient balance"));
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn included_failure_must_preserve_outcome_for_gas_accounting() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::IncludedFailure);
    let outcome = process(&fixture).await;
    eprintln!(
        "included failure result: {outcome:?}; receipt queries: {}",
        fixture.queries.load(Ordering::SeqCst)
    );
    assert_failed_outcome(outcome.expect(
        "an included failure must return its TxOutcome so PendingMessage records gas expenditure",
    ));
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 1);
}

async fn assert_interrupted_helper_reconciles(helper: Helper) {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(helper);
    assert_failed_outcome(process(&fixture).await.unwrap_or_else(|error| {
        panic!("{helper:?} must reconcile the prepared transaction: {error}")
    }));
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn abrupt_helper_exit_reconciles_prepared_transaction() {
    assert_interrupted_helper_reconciles(Helper::AbruptExit).await;
}

#[tokio::test]
async fn signaled_helper_reconciles_prepared_transaction() {
    assert_interrupted_helper_reconciles(Helper::Interrupted).await;
}

#[tokio::test]
async fn malformed_helper_output_reconciles_prepared_transaction() {
    assert_interrupted_helper_reconciles(Helper::MalformedOutput).await;
}

#[tokio::test]
async fn missing_output_transaction_id_reconciles_prepared_transaction() {
    assert_interrupted_helper_reconciles(Helper::MissingTransactionId).await;
}

#[tokio::test]
async fn unrecognized_helper_error_reconciles_prepared_transaction() {
    assert_interrupted_helper_reconciles(Helper::UnrecognizedError).await;
}

#[tokio::test]
async fn complete_preverify_failures_do_not_query_or_charge_a_receipt() {
    let _env_guard = HELPER_ENV.lock().await;
    for helper in [Helper::PreverifyRejected, Helper::PreverifyUnavailable] {
        let fixture = Fixture::start(helper);
        assert!(process(&fixture)
            .await
            .unwrap_err()
            .contains("before propagation"));
        assert_eq!(fixture.queries.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn helper_results_cannot_replace_the_prepared_transaction_identity() {
    for helper in [
        Helper::SuccessDifferentIdentity,
        Helper::ErrorDifferentIdentity,
        Helper::OpaqueDiagnosticIdentity,
    ] {
        assert_interrupted_helper_reconciles(helper).await;
    }
}

#[tokio::test]
async fn passing_simulation_proceeds_to_submission() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::Success);
    assert_failed_outcome(process(&fixture).await.unwrap());
    assert_eq!(fixture.simulations(), 1);
    assert!(fixture.helper_invoked());
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_simulation_never_invokes_the_helper() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start_with_simulation(Helper::Success, Simulation::Panics);
    let error = fixture
        .mailbox()
        .process(
            &message(),
            &Metadata::new(vec![]),
            Some(U256::from(FULL_GAS)),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, ChainCommunicationError::SimulationFailed(_)),
        "{error}"
    );
    assert_eq!(fixture.simulations(), 1);
    assert!(!fixture.helper_invoked());
    assert_eq!(fixture.queries.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn failed_simulation_fails_the_estimate() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start_with_simulation(Helper::Success, Simulation::Panics);
    let error = fixture
        .mailbox()
        .process_estimate_costs(&message(), &Metadata::new(vec![]))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ChainCommunicationError::SimulationFailed(_)),
        "{error}"
    );
    assert!(
        error.to_string().contains("Internal server error"),
        "{error}"
    );
    assert_eq!(fixture.simulations(), 1);
    assert!(!fixture.helper_invoked());
}

#[tokio::test]
async fn passing_simulation_estimates_the_configured_ceiling() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::Success);
    let estimate = fixture
        .mailbox()
        .process_estimate_costs(&message(), &Metadata::new(vec![]))
        .await
        .unwrap();
    assert_eq!(
        estimate,
        TxCostEstimate {
            gas_limit: U256::from(FULL_GAS),
            gas_price: FixedPointNumber::from(2),
            l2_gas_limit: None,
        }
    );
    assert_eq!(fixture.simulations(), 1);
    assert!(!fixture.helper_invoked());
}

#[tokio::test]
async fn a_success_reply_other_than_unit_fails_the_simulation() {
    let _env_guard = HELPER_ENV.lock().await;
    for simulation in [Simulation::ProxyPage, Simulation::UnexpectedValue] {
        let fixture = Fixture::start_with_simulation(Helper::Success, simulation);
        let estimate = fixture
            .mailbox()
            .process_estimate_costs(&message(), &Metadata::new(vec![]))
            .await
            .unwrap_err();
        let process = fixture
            .mailbox()
            .process(
                &message(),
                &Metadata::new(vec![]),
                Some(U256::from(FULL_GAS)),
            )
            .await
            .unwrap_err();
        for error in [estimate, process] {
            assert!(
                matches!(error, ChainCommunicationError::SimulationFailed(_)),
                "{simulation:?}: {error}"
            );
            assert!(
                error.to_string().contains("instead of the encoding of ()"),
                "{simulation:?}: {error}"
            );
        }
        assert_eq!(fixture.simulations(), 2);
        assert!(!fixture.helper_invoked(), "{simulation:?}");
        assert_eq!(fixture.queries.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn a_huge_simulation_error_body_is_cut_before_it_reaches_the_error() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start_with_simulation(Helper::Success, Simulation::HugeError);
    let error = fixture
        .mailbox()
        .process_estimate_costs(&message(), &Metadata::new(vec![]))
        .await
        .unwrap_err();
    assert!(
        matches!(error, ChainCommunicationError::SimulationFailed(_)),
        "{error}"
    );
    let error = error.to_string();
    assert!(error.len() < 2048, "{} bytes", error.len());
    assert!(
        error.contains(&format!("[truncated, {HUGE_ERROR_BYTES} bytes]")),
        "{error}"
    );
    assert!(!fixture.helper_invoked());
}

#[tokio::test]
async fn oversized_process_payload_fails_before_simulation() {
    let _env_guard = HELPER_ENV.lock().await;
    let fixture = Fixture::start(Helper::Success);
    let error = fixture
        .mailbox()
        .process_estimate_costs(&message(), &Metadata::new(vec![0; 60 * 1024]))
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("argument limit of the dusk-tx helper"),
        "{error}"
    );
    assert_eq!(fixture.simulations(), 0);
}
