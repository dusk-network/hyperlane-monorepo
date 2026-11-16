//! Dusk transaction sender — invokes the `dusk-tx` CLI binary to construct
//! and submit Moonlight transactions.
//!
//! The monorepo uses stable Rust, but Moonlight TX construction requires
//! `dusk-core` which needs nightly. The `dusk-tx` binary (built in the
//! `dusk/` workspace with nightly) handles all TX construction and signing.
//! This module invokes it as a subprocess.

use std::process::Stdio;
use std::sync::{Arc, Mutex as StdMutex, PoisonError};
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use hyperlane_core::H512;
use url::Url;

use crate::rues::{bounded_error, bounded_text, rkyv_serialize};
use crate::{ConnectionConf, DuskSigner, HyperlaneDuskError};

const DUSK_TX_HELPER_TIMEOUT: Duration = Duration::from_secs(120);
const DUSK_TX_DRAIN_TIMEOUT: Duration = Duration::from_millis(500);
const MAX_RUES_URL_STDIN_BYTES: usize = 8 * 1024;
const MAX_DUSK_TX_STREAM_BYTES: usize = 1024 * 1024;
/// The most bytes of serialized call arguments the `dusk-tx` helper accepts
/// (`MAX_CALL_ARGS_BYTES` in the companion repository's `dusk-tx`).
const MAX_DUSK_TX_ARGS_BYTES: usize = 60 * 1024;

/// Send a contract call via the `dusk-tx` CLI binary, which signs it with
/// the account's next nonce and returns once the transaction is in a block.
///
/// Returns the parsed JSON output on success.
pub async fn dusk_tx_call(
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
    gas_limit: Option<u64>,
) -> Result<Value, HyperlaneDuskError> {
    run_dusk_tx(
        conn,
        signer,
        contract_id,
        fn_name,
        args_bytes,
        gas_limit,
        &[],
    )
    .await
}

/// A transaction the helper signed, which nothing has sent yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SignedTransaction {
    /// Its hash, lowercase hex.
    pub tx_id: String,
    /// Its bytes, as the helper would propagate them.
    pub bytes: Vec<u8>,
}

/// The transfer contract's `chain_id` query, as the helper sends it.
const CHAIN_ID_ROUTE: &str =
    "/on/contracts:0100000000000000000000000000000000000000000000000000000000000000/chain_id";
/// How long one request to the capture endpoint may take.
const CAPTURE_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Most bytes of a request's head the capture endpoint reads.
const MAX_CAPTURE_HEAD_BYTES: usize = 16 * 1024;
/// Most bytes of a request's body the capture endpoint reads: a Moonlight
/// transaction is at most 64 KiB of arguments and its envelope.
const MAX_CAPTURE_BODY_BYTES: usize = 256 * 1024;

/// What the helper sent the capture endpoint.
#[derive(Debug, Default)]
struct Captured {
    preverified: Option<Vec<u8>>,
    propagated: Option<Vec<u8>>,
}

/// Have the helper sign a contract call with Moonlight nonce `nonce`,
/// without sending it. The helper is given the URL of an endpoint of this
/// process on the loopback interface, under a path it cannot guess: it
/// answers the helper's chain ID query with the configured chain ID (the
/// agent checks the node's own before it sends anything) and keeps the
/// transaction the helper then preverifies and propagates there. The
/// caller keeps the transaction's hash before it sends it
/// (`RuesClient::submit_transaction`), so no transaction of a pipelined
/// delivery reaches the node unnamed, whenever the agent stops. Needs a
/// `dusk-tx` with `--nonce` and `--no-wait` (`helper_pipelines`). Ends by
/// `deadline`, killing the helper if it must; nothing reaches the node
/// whatever happens.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dusk_tx_sign(
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
    gas_limit: u64,
    nonce: u64,
    deadline: tokio::time::Instant,
) -> Result<SignedTransaction, HyperlaneDuskError> {
    let unsent = |detail: String| {
        HyperlaneDuskError::Other(format!(
            "dusk-tx did not sign {fn_name} with nonce {nonce}, and nothing was sent: {detail}"
        ))
    };
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .map_err(|error| unsent(format!("no loopback port for the helper: {error}")))?;
    let port = listener
        .local_addr()
        .map_err(|error| unsent(format!("no loopback port for the helper: {error}")))?
        .port();
    let prefix = format!("/{}", capture_token());
    let url = Url::parse(&format!("http://127.0.0.1:{port}{prefix}"))
        .map_err(|error| unsent(format!("invalid capture URL: {error}")))?;
    let captured = Arc::new(StdMutex::new(Captured::default()));
    // Stops serving when this returns or is dropped.
    let server = AbortOnDrop(tokio::spawn(serve_capture(
        listener,
        prefix,
        conn.chain_id,
        captured.clone(),
    )));
    let timeout = deadline
        .saturating_duration_since(tokio::time::Instant::now())
        .min(DUSK_TX_HELPER_TIMEOUT);
    let extra = [
        "--nonce".to_owned(),
        nonce.to_string(),
        "--no-wait".to_owned(),
    ];
    let result = run_dusk_tx_at(
        &url,
        conn,
        signer,
        contract_id,
        fn_name,
        args_bytes,
        Some(gas_limit),
        &extra,
        timeout,
    )
    .await;
    drop(server);
    let json = result.map_err(|error| unsent(bounded_error(&error)))?;
    let tx_id = json
        .get("tx_id")
        .and_then(Value::as_str)
        .filter(|tx_id| tx_id.len() == 64 && tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| unsent("the helper named no transaction".into()))?
        .to_ascii_lowercase();
    let captured = std::mem::take(&mut *captured.lock().unwrap_or_else(PoisonError::into_inner));
    let bytes = captured
        .propagated
        .ok_or_else(|| unsent("the helper propagated no transaction".into()))?;
    if captured
        .preverified
        .as_ref()
        .is_some_and(|preverified| *preverified != bytes)
    {
        return Err(unsent(
            "the helper preverified one transaction and propagated another".into(),
        ));
    }
    Ok(SignedTransaction { tx_id, bytes })
}

/// A task aborted when this is dropped.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A path component no other local process can guess: 128 bits from the
/// process's random hasher keys and the clock.
fn capture_token() -> String {
    use std::hash::{BuildHasher, Hasher};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let state = std::collections::hash_map::RandomState::new();
    let mut first = state.build_hasher();
    first.write_u128(nanos);
    let first = first.finish();
    let mut second = std::collections::hash_map::RandomState::new().build_hasher();
    second.write_u64(first);
    second.write_u128(nanos);
    format!("{first:016x}{:016x}", second.finish())
}

/// Answer the helper's requests at `listener`, one at a time, until
/// aborted.
async fn serve_capture(
    listener: tokio::net::TcpListener,
    prefix: String,
    chain_id: u8,
    captured: Arc<StdMutex<Captured>>,
) {
    while let Ok((mut stream, _)) = listener.accept().await {
        let _ = tokio::time::timeout(
            CAPTURE_REQUEST_TIMEOUT,
            answer_capture(&mut stream, &prefix, chain_id, &captured),
        )
        .await;
    }
}

/// Answer one request at the capture endpoint: the chain ID query, and the
/// transaction the helper preverifies and propagates, which is kept and
/// sent nowhere.
async fn answer_capture(
    stream: &mut tokio::net::TcpStream,
    prefix: &str,
    chain_id: u8,
    captured: &StdMutex<Captured>,
) -> std::io::Result<()> {
    let Some((path, body)) = read_http_request(stream).await? else {
        return respond(stream, "400 Bad Request", b"").await;
    };
    let route = path.strip_prefix(prefix).unwrap_or_default();
    let captured = || captured.lock().unwrap_or_else(PoisonError::into_inner);
    match route {
        CHAIN_ID_ROUTE => respond(stream, "200 OK", &[chain_id]).await,
        "/on/transactions/preverify" => {
            captured().preverified = Some(body);
            respond(stream, "200 OK", b"").await
        }
        "/on/transactions/propagate" => {
            captured().propagated = Some(body);
            respond(stream, "200 OK", b"").await
        }
        _ => respond(stream, "404 Not Found", b"").await,
    }
}

/// Read one HTTP/1.1 request with a `Content-Length` body: its path and
/// body. `None` for a request this endpoint does not take.
async fn read_http_request(
    stream: &mut tokio::net::TcpStream,
) -> std::io::Result<Option<(String, Vec<u8>)>> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(end) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break end;
        }
        if buffer.len() > MAX_CAPTURE_HEAD_BYTES {
            return Ok(None);
        }
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(None);
        }
        buffer.extend_from_slice(&chunk[..read]);
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request = lines.next().unwrap_or_default().split(' ');
    let (Some("POST"), Some(path)) = (request.next(), request.next()) else {
        return Ok(None);
    };
    let mut length: Option<usize> = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => match value.trim().parse::<usize>() {
                Ok(parsed) => length = Some(parsed),
                Err(_) => return Ok(None),
            },
            "transfer-encoding" => return Ok(None),
            _ => {}
        }
    }
    // Without a length or a transfer encoding, a request has no body, as
    // reqwest sends an empty one.
    let length = length.unwrap_or(0);
    if length > MAX_CAPTURE_BODY_BYTES {
        return Ok(None);
    }
    let mut body = buffer.split_off(head_end + 4);
    while body.len() < length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Ok(None);
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(length);
    Ok(Some((path.to_owned(), body)))
}

async fn respond(
    stream: &mut tokio::net::TcpStream,
    status: &str,
    body: &[u8],
) -> std::io::Result<()> {
    let head = format!(
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body).await?;
    stream.shutdown().await
}

/// How long the `call --help` probe of a helper may take.
const HELPER_PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Whether the configured `dusk-tx` can sign with a given nonce and return
/// once the node has accepted the transaction (`call --nonce --no-wait`),
/// which pipelined deliveries need. Asked once per helper path, from its
/// `call --help`, which reads no key and sends nothing; a helper that fails
/// the probe is taken not to.
pub(crate) async fn helper_pipelines() -> bool {
    static PROBES: std::sync::OnceLock<
        tokio::sync::Mutex<std::collections::HashMap<String, bool>>,
    > = std::sync::OnceLock::new();
    let bin = std::env::var("DUSK_TX_BIN").unwrap_or_else(|_| "dusk-tx".into());
    let mut probes = PROBES
        .get_or_init(|| tokio::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .await;
    if let Some(known) = probes.get(&bin) {
        return *known;
    }
    let mut command = Command::new(&bin);
    command
        .kill_on_drop(true)
        .arg("call")
        .arg("--help")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    let help = match tokio::time::timeout(HELPER_PROBE_TIMEOUT, command.output()).await {
        Ok(Ok(output)) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).into_owned()
        }
        _ => String::new(),
    };
    let pipelines = help.contains("--nonce") && help.contains("--no-wait");
    if !pipelines {
        info!(
            bin = %bin,
            "The dusk-tx helper cannot sign with a given nonce (call --nonce --no-wait); Dusk deliveries are submitted one at a time"
        );
    }
    probes.insert(bin, pipelines);
    pipelines
}

async fn run_dusk_tx(
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
    gas_limit: Option<u64>,
    extra_args: &[String],
) -> Result<Value, HyperlaneDuskError> {
    run_dusk_tx_at(
        &conn.url,
        conn,
        signer,
        contract_id,
        fn_name,
        args_bytes,
        gas_limit,
        extra_args,
        DUSK_TX_HELPER_TIMEOUT,
    )
    .await
}

/// Run the helper's `call` against the RUES endpoint at `url`, for at most
/// `timeout`.
#[allow(clippy::too_many_arguments)]
async fn run_dusk_tx_at(
    url: &Url,
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
    gas_limit: Option<u64>,
    extra_args: &[String],
    timeout: Duration,
) -> Result<Value, HyperlaneDuskError> {
    if args_bytes.len() > MAX_DUSK_TX_ARGS_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "dusk-tx arguments for {fn_name} exceed the {MAX_DUSK_TX_ARGS_BYTES}-byte helper transport limit"
        )));
    }
    if url.as_str().len() + 1 > MAX_RUES_URL_STDIN_BYTES {
        return Err(HyperlaneDuskError::Other(
            "Dusk RUES URL exceeds the helper's 8192-byte stdin line limit".into(),
        ));
    }
    let bin = std::env::var("DUSK_TX_BIN").unwrap_or_else(|_| "dusk-tx".into());
    let contract_hex = hex::encode(contract_id);
    let args_hex = hex::encode(args_bytes);
    let secret_key_hex = hex::encode(signer.key().as_bytes());

    debug!(
        bin = %bin,
        contract = %contract_hex,
        fn_name = %fn_name,
        args_len = args_bytes.len(),
        "Invoking dusk-tx call"
    );

    let mut command = Command::new(&bin);
    command.kill_on_drop(true);
    command.arg("call");
    let mut child = command
        .arg("--rues-url-stdin")
        .arg("--expected-chain-id")
        .arg(conn.chain_id.to_string())
        .arg("--secret-key-stdin")
        .arg("--contract")
        .arg(&contract_hex)
        .arg("--fn-name")
        .arg(fn_name)
        .arg("--args")
        .arg(&args_hex)
        .arg("--gas-limit")
        .arg(gas_limit.unwrap_or(conn.gas_limit).to_string())
        .arg("--gas-price")
        .arg(conn.gas_price.to_string())
        .args(extra_args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            HyperlaneDuskError::Other(format!(
                "Failed to invoke dusk-tx binary at '{bin}': {e}. \
                 Set DUSK_TX_BIN environment variable to the path of the dusk-tx binary."
            ))
        })?;

    // Keep both the authenticated URL and signing key out of process arguments.
    // The helper consumes the URL line first, then the key after chain validation.
    if let Some(mut stdin) = child.stdin.take() {
        let input = format!("{}\n{secret_key_hex}\n", url.as_str());
        tokio::time::timeout(timeout, stdin.write_all(input.as_bytes()))
            .await
            .map_err(|_| {
                HyperlaneDuskError::Other("dusk-tx stdin write exceeded its deadline".into())
            })?
            .map_err(|error| {
                HyperlaneDuskError::Other(format!("Failed to write dusk-tx private stdin: {error}"))
            })?;
    }

    let (output, prepared_tx_id) = wait_for_child_output(child, fn_name, timeout).await?;
    let json = helper_result(fn_name, &output, prepared_tx_id.as_deref())?;

    let tx_id = json
        .get("tx_id")
        .and_then(Value::as_str)
        .unwrap_or_default();
    info!(
        fn_name = %fn_name,
        contract = %contract_hex,
        tx_id = %tx_id,
        "dusk-tx call succeeded"
    );

    Ok(json)
}

/// The result of a finished helper call: its JSON output, or the error its
/// exit status and output describe. Stdout and stderr can each be up to
/// `MAX_DUSK_TX_STREAM_BYTES`, so only a bounded part of either goes into an
/// error or a log line. The helper's error text is classified whole.
fn helper_result(
    fn_name: &str,
    output: &std::process::Output,
    prepared_tx_id: Option<&str>,
) -> Result<Value, HyperlaneDuskError> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stderr.is_empty() {
        debug!(stderr = %bounded_text(&stderr), "dusk-tx stderr");
    }

    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        warn!(
            code = code,
            stderr = %bounded_text(&stderr),
            stdout = %bounded_text(&stdout),
            "dusk-tx call failed"
        );
        // Try to parse error from JSON output
        if let Ok(json) = serde_json::from_str::<Value>(&stdout) {
            if let Some(err) = json.get("error").and_then(|e| e.as_str()) {
                // Diagnostic bodies are opaque. Only the helper's anchored
                // transaction prefix can report an identity, and it cannot
                // replace the locally prepared hash captured from stderr.
                if let Some(prepared) = prepared_tx_id {
                    if transaction_error_parts(err)
                        .is_some_and(|(reported, _)| !prepared.eq_ignore_ascii_case(reported))
                    {
                        return Err(helper_failure(
                            Some(prepared),
                            "dusk-tx error transaction ID disagrees with the prepared transaction"
                                .into(),
                        ));
                    }
                }
                let detail = bounded_text(err);
                if let Some(tx_id) = included_failure_tx_id(err) {
                    return Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id, detail });
                }
                if let Some(tx_id) = outcome_unknown_tx_id(err) {
                    return Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, detail });
                }
                let detail = format!("dusk-tx call {fn_name} failed: {detail}");
                // A complete preverify diagnostic proves propagation never ran.
                if before_propagation_error(err) {
                    return Err(HyperlaneDuskError::Other(detail));
                }
                return Err(helper_failure(prepared_tx_id, detail));
            }
        }
        return Err(helper_failure(
            prepared_tx_id,
            format!(
                "dusk-tx call {fn_name} failed (exit code {code}): {}",
                bounded_text(&stderr)
            ),
        ));
    }

    let json: Value = serde_json::from_str(&stdout).map_err(|e| {
        helper_failure(
            prepared_tx_id,
            format!(
                "Failed to parse dusk-tx output as JSON: {}. Output: {}",
                bounded_error(e),
                bounded_text(&stdout)
            ),
        )
    })?;
    let returned_id = json
        .get("tx_id")
        .and_then(Value::as_str)
        .and_then(|tx_id| dusk_tx_id_to_h512(tx_id).ok());
    if json.get("success").and_then(Value::as_bool) != Some(true)
        || returned_id.is_none()
        || prepared_tx_id.is_some_and(|prepared| dusk_tx_id_to_h512(prepared).ok() != returned_id)
    {
        return Err(helper_failure(
            prepared_tx_id,
            "dusk-tx output is missing a successful result with the prepared transaction ID".into(),
        ));
    }
    Ok(json)
}

async fn wait_for_child_output(
    mut child: tokio::process::Child,
    fn_name: &str,
    timeout: Duration,
) -> Result<(std::process::Output, Option<String>), HyperlaneDuskError> {
    let stdout = child.stdout.take().ok_or_else(|| {
        HyperlaneDuskError::Other("dusk-tx stdout pipe was not configured".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        HyperlaneDuskError::Other("dusk-tx stderr pipe was not configured".into())
    })?;
    let (prepared_tx_sender, mut prepared_tx_receiver) = oneshot::channel();
    let mut stdout_task = tokio::spawn(read_bounded_stream(stdout, "stdout", None));
    let mut stderr_task = tokio::spawn(read_bounded_stream(
        stderr,
        "stderr",
        Some(prepared_tx_sender),
    ));

    let status = tokio::time::timeout(timeout, child.wait())
        .await
        .map_err(|_| {
            format!(
                "dusk-tx call {fn_name} exceeded the {}s helper deadline",
                timeout.as_secs()
            )
        })
        .and_then(|result| {
            result.map_err(|error| {
                format!("Failed to wait on dusk-tx child process for {fn_name}: {error}")
            })
        });
    let status = match status {
        Ok(status) => status,
        Err(detail) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            drain_timed_out_readers(&mut stdout_task, &mut stderr_task).await;
            return Err(helper_failure(
                prepared_tx_receiver.try_recv().ok().as_deref(),
                detail,
            ));
        }
    };

    // Observe both readers before classifying a failed read or parsing output.
    // A normal or signaled exit can interrupt the helper after propagation just
    // as a timeout can; the prepared hash remains authoritative in every case.
    let (stdout, stderr) = tokio::join!(stdout_task, stderr_task);
    let prepared_tx_id = prepared_tx_receiver.try_recv().ok();
    let stdout = stdout
        .map_err(|error| {
            helper_failure(
                prepared_tx_id.as_deref(),
                format!("Failed to join dusk-tx stdout reader: {error}"),
            )
        })?
        .map_err(|error| helper_failure(prepared_tx_id.as_deref(), error.to_string()))?;
    let stderr = stderr
        .map_err(|error| {
            helper_failure(
                prepared_tx_id.as_deref(),
                format!("Failed to join dusk-tx stderr reader: {error}"),
            )
        })?
        .map_err(|error| helper_failure(prepared_tx_id.as_deref(), error.to_string()))?;
    Ok((
        std::process::Output {
            status,
            stdout,
            stderr,
        },
        prepared_tx_id,
    ))
}

fn helper_failure(prepared_tx_id: Option<&str>, detail: String) -> HyperlaneDuskError {
    match prepared_tx_id {
        Some(tx_id) => HyperlaneDuskError::SubmissionOutcomeUnknown {
            tx_id: tx_id.to_owned(),
            detail,
        },
        None => HyperlaneDuskError::Other(detail),
    }
}

fn before_propagation_error(error: &str) -> bool {
    let Some((tx_id, detail)) = error
        .strip_prefix("Transaction ")
        .and_then(|error| error.split_once(" submission failed: "))
    else {
        return false;
    };
    tx_id.len() == 64
        && tx_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        && (detail.starts_with("Preverify failed before propagation: ")
            || detail.starts_with("Preverify rejected before propagation ("))
}

type PipeReader = tokio::task::JoinHandle<Result<Vec<u8>, HyperlaneDuskError>>;

async fn drain_timed_out_readers(stdout: &mut PipeReader, stderr: &mut PipeReader) {
    // Killing the child closes its pipes, but bytes already emitted can still be
    // buffered. Give the readers time to observe the prepared hash before aborting.
    // Descendants retaining a pipe cannot extend this deadline indefinitely.
    if tokio::time::timeout(DUSK_TX_DRAIN_TIMEOUT, async {
        let _ = (&mut *stderr).await;
        let _ = (&mut *stdout).await;
    })
    .await
    .is_err()
    {
        stdout.abort();
        stderr.abort();
    }
}

async fn read_bounded_stream(
    reader: impl AsyncRead + Unpin,
    stream_name: &'static str,
    mut prepared_tx_sender: Option<oneshot::Sender<String>>,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let mut reader = reader.take((MAX_DUSK_TX_STREAM_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await.map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to read dusk-tx {stream_name}: {error}"))
        })?;
        if count == 0 {
            return Ok(bytes);
        }
        bytes.extend_from_slice(&chunk[..count]);
        if prepared_tx_sender.is_some() {
            if let Some(tx_id) = prepared_tx_id(&bytes) {
                if let Some(sender) = prepared_tx_sender.take() {
                    let _ = sender.send(tx_id);
                }
            }
        }
        if bytes.len() > MAX_DUSK_TX_STREAM_BYTES {
            return Err(HyperlaneDuskError::Other(format!(
                "dusk-tx {stream_name} exceeds {MAX_DUSK_TX_STREAM_BYTES} bytes"
            )));
        }
    }
}

fn prepared_tx_id(stderr: &[u8]) -> Option<String> {
    String::from_utf8_lossy(stderr).lines().find_map(|line| {
        let (tx_id, _) = line
            .trim_start()
            .strip_prefix("Prepared TX ")?
            .split_once(';')?;
        (tx_id.len() == 64 && tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| tx_id.to_ascii_lowercase())
    })
}

/// Convert a 32-byte Dusk transaction ID (hex) into a `H512` by left-padding with zeros.
pub fn dusk_tx_id_to_h512(tx_id_hex: &str) -> Result<H512, HyperlaneDuskError> {
    let tx_id_hex = tx_id_hex
        .strip_prefix("0x")
        .or_else(|| tx_id_hex.strip_prefix("0X"))
        .unwrap_or(tx_id_hex);
    let bytes = hex::decode(tx_id_hex).map_err(|e| {
        HyperlaneDuskError::Other(format!(
            "Invalid dusk tx_id hex '{}': {}",
            bounded_text(tx_id_hex),
            bounded_error(e)
        ))
    })?;
    if bytes.len() != 32 {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk tx_id must be 32 bytes, got {}",
            bytes.len()
        )));
    }
    let mut h512 = [0u8; 64];
    h512[32..64].copy_from_slice(&bytes);
    Ok(H512::from_slice(&h512))
}

/// Recover a Dusk transaction ID from its left-padded common representation.
pub fn h512_to_dusk_tx_id(transaction_id: &H512) -> Result<String, HyperlaneDuskError> {
    let bytes = transaction_id.as_bytes();
    if bytes[..32] != [0u8; 32] {
        return Err(HyperlaneDuskError::Other(
            "Dusk H512 transaction ID has nonzero padding".into(),
        ));
    }
    Ok(hex::encode(&bytes[32..]))
}

/// Build rkyv-serialized args for mailbox.process(metadata, encoded_message).
///
/// The `dusk-tx` helper limits the serialized arguments, not the payload, so
/// this refuses exactly what the helper would refuse. Serialized arguments
/// are never shorter than the bytes they carry, so a payload over the limit
/// is refused before it is copied.
pub fn process_args(
    metadata: &[u8],
    encoded_message: &[u8],
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let payload_len = metadata
        .len()
        .checked_add(encoded_message.len())
        .ok_or_else(|| HyperlaneDuskError::Other("Dusk process payload length overflow".into()))?;
    if payload_len > MAX_DUSK_TX_ARGS_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk process metadata and message ({payload_len} bytes) exceed the {MAX_DUSK_TX_ARGS_BYTES}-byte argument limit of the dusk-tx helper"
        )));
    }
    let args = rkyv_serialize(&(metadata.to_vec(), encoded_message.to_vec()))?;
    if args.len() > MAX_DUSK_TX_ARGS_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "Serialized Dusk process arguments ({} bytes) exceed the {MAX_DUSK_TX_ARGS_BYTES}-byte argument limit of the dusk-tx helper",
            args.len()
        )));
    }
    Ok(args)
}

// Only the helper-owned leading fields carry transaction identity. RPC response
// text is opaque and must not supply a replacement through a quoted tx_id field.
fn transaction_error_parts(error: &str) -> Option<(&str, &str)> {
    let (tx_id, detail) = error.strip_prefix("Transaction ")?.split_once(' ')?;
    (tx_id.len() == 64 && tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .then_some((tx_id, detail))
}

fn included_failure_tx_id(error: &str) -> Option<String> {
    let (tx_id, detail) = transaction_error_parts(error)?;
    detail
        .starts_with("failed: ")
        .then(|| tx_id.to_ascii_lowercase())
}

fn outcome_unknown_tx_id(error: &str) -> Option<String> {
    let (tx_id, detail) = transaction_error_parts(error)?;
    (detail.starts_with("submission failed: Propagation outcome unknown")
        || detail.starts_with("confirmation outcome unknown"))
    .then(|| tx_id.to_ascii_lowercase())
}

/// Build rkyv-serialized args for validator_announce.announce(validator, location, signature).
pub fn announce_args(
    validator_eth_addr: [u8; 20],
    storage_location: &str,
    signature: &[u8],
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let eth_addr = hyperlane_dusk_types::EthAddress(validator_eth_addr);
    rkyv_serialize(&(eth_addr, String::from(storage_location), signature.to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_payload_is_bounded_before_helper_transport() {
        let oversized = vec![0u8; MAX_DUSK_TX_ARGS_BYTES + 1];
        let error = process_args(&oversized, &[]).unwrap_err();
        assert!(error.to_string().contains("dusk-tx helper"), "{error}");
    }

    /// The helper refuses serialized arguments over `MAX_DUSK_TX_ARGS_BYTES`
    /// and nothing else, so the adapter accepts every message whose
    /// serialized arguments fit and refuses every one that does not.
    #[test]
    fn process_arguments_are_limited_exactly_as_the_helper_limits_them() {
        let metadata = vec![7u8; 1_000];
        let serialized_len = |message_len: usize| {
            rkyv_serialize(&(metadata.clone(), vec![0u8; message_len]))
                .unwrap()
                .len()
        };
        // The longest message whose serialized arguments fit the helper.
        let fitting = (0..MAX_DUSK_TX_ARGS_BYTES)
            .rev()
            .find(|len| serialized_len(*len) <= MAX_DUSK_TX_ARGS_BYTES)
            .unwrap();
        // Well past the old 60,416-byte payload reserve.
        assert!(metadata.len() + fitting > MAX_DUSK_TX_ARGS_BYTES - 1024);
        let args = process_args(&metadata, &vec![0u8; fitting]).unwrap();
        assert_eq!(args.len(), serialized_len(fitting));
        assert!(args.len() <= MAX_DUSK_TX_ARGS_BYTES);
        let error = process_args(&metadata, &vec![0u8; fitting + 1]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("Serialized Dusk process arguments"),
            "{error}"
        );
    }

    #[test]
    fn only_outcome_unknown_errors_yield_a_reconciliation_hash() {
        let tx_id = "ab".repeat(32);
        let unknown = format!(
            "Transaction {tx_id} submission failed: Propagation outcome unknown: closed; retain tx_id={tx_id} and reconcile this exact hash"
        );
        assert_eq!(outcome_unknown_tx_id(&unknown), Some(tx_id.clone()));

        let confirmation_unknown = format!(
            "Transaction {tx_id} confirmation outcome unknown: timed out; retain tx_id={tx_id}"
        );
        assert_eq!(
            outcome_unknown_tx_id(&confirmation_unknown),
            Some(tx_id.clone())
        );

        let rejected = format!(
            "Transaction {tx_id} submission failed: Propagation rejected; retain tx_id={tx_id}"
        );
        assert_eq!(outcome_unknown_tx_id(&rejected), None);
    }

    #[test]
    fn opaque_diagnostics_do_not_change_error_identity_or_classification() {
        let tx_id = "ab".repeat(32);
        let other = "cd".repeat(32);
        let error = format!("Transaction {tx_id} confirmation outcome unknown: diagnostic tx_id={other}; retain tx_id={tx_id}");
        assert_eq!(outcome_unknown_tx_id(&error), Some(tx_id.clone()));
        let preverify = format!("Transaction {tx_id} submission failed: Preverify rejected before propagation (400): diagnostic confirmation outcome unknown tx_id={other}");
        assert_eq!(outcome_unknown_tx_id(&preverify), None);
        assert!(before_propagation_error(&preverify));
        assert_eq!(outcome_unknown_tx_id(&format!("Diagnostic: {error}")), None);
    }

    #[test]
    fn only_included_failures_yield_an_execution_failure_hash() {
        let tx_id = "AB".repeat(32);
        assert_eq!(
            included_failure_tx_id(&format!("Transaction {tx_id} failed: recipient rejected")),
            Some(tx_id.to_ascii_lowercase())
        );
        for error in [
            format!("Transaction {tx_id} submission failed: preverify rejected"),
            format!("Preverify rejected: Transaction {tx_id} failed: quoted node error"),
            "Transaction short failed: recipient rejected".into(),
            format!("Transaction {} failed: recipient rejected", "zz".repeat(32)),
        ] {
            assert_eq!(included_failure_tx_id(&error), None);
        }
    }

    #[test]
    fn transaction_ids_are_left_padded_without_changing_dusk_bytes() {
        let dusk_id = [0xabu8; 32];
        let transaction_id = dusk_tx_id_to_h512(&hex::encode(dusk_id)).unwrap();
        assert_eq!(&transaction_id.as_bytes()[..32], &[0u8; 32]);
        assert_eq!(&transaction_id.as_bytes()[32..], &dusk_id);
        assert!(dusk_tx_id_to_h512("abcd").is_err());
        assert_eq!(
            h512_to_dusk_tx_id(&transaction_id).unwrap(),
            hex::encode(dusk_id)
        );
        assert!(h512_to_dusk_tx_id(&H512::from([1u8; 64])).is_err());
    }

    /// The endpoint the helper signs against answers what `dusk-tx` asks
    /// with reqwest, keeps the transaction it preverifies and propagates,
    /// and serves nothing outside its secret path.
    #[tokio::test]
    async fn the_capture_endpoint_keeps_the_helpers_transaction_and_sends_it_nowhere() {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let captured = Arc::new(StdMutex::new(Captured::default()));
        let server = AbortOnDrop(tokio::spawn(serve_capture(
            listener,
            "/secret".into(),
            7,
            captured.clone(),
        )));
        let client = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{port}/secret");
        let chain_id = client
            .post(format!("{base}{CHAIN_ID_ROUTE}"))
            .header("Content-Type", "application/octet-stream")
            .body(Vec::new())
            .send()
            .await
            .unwrap();
        assert_eq!(chain_id.status(), 200);
        assert_eq!(chain_id.bytes().await.unwrap().as_ref(), [7]);
        // A transaction longer than one read.
        let transaction: Vec<u8> = (0..70_000).map(|byte| byte as u8).collect();
        for route in ["/on/transactions/preverify", "/on/transactions/propagate"] {
            let reply = client
                .post(format!("{base}{route}"))
                .header("Content-Type", "application/octet-stream")
                .body(transaction.clone())
                .send()
                .await
                .unwrap();
            assert_eq!(reply.status(), 200);
        }
        for url in [
            format!("http://127.0.0.1:{port}/on/transactions/propagate"),
            format!("{base}/on/transactions/other"),
        ] {
            let reply = client.post(url).body(vec![9]).send().await.unwrap();
            assert_eq!(reply.status(), 404);
        }
        let captured = std::mem::take(&mut *captured.lock().unwrap());
        assert_eq!(captured.preverified.as_deref(), Some(&transaction[..]));
        assert_eq!(captured.propagated.as_deref(), Some(&transaction[..]));
        // Dropped, it stops serving.
        drop(server);
        tokio::task::yield_now().await;
        assert!(client
            .post(format!("{base}{CHAIN_ID_ROUTE}"))
            .timeout(Duration::from_secs(2))
            .send()
            .await
            .is_err());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn stalled_helper_is_bounded_by_deadline() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("sleep 10")
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command.spawn().unwrap();
        let started = tokio::time::Instant::now();
        let error = wait_for_child_output(child, "test", Duration::from_millis(20))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("helper deadline"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_helper_preserves_prepared_transaction_for_reconciliation() {
        let tx_id = "ab".repeat(32);
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(format!(
                "printf '  Prepared TX {tx_id}; reconcile this exact hash before retrying if submission is interrupted\\n' >&2; exec sleep 10"
            ))
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command.spawn().unwrap();
        let error = wait_for_child_output(child, "process", Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id: observed, .. }
                if observed == tx_id
        ));
    }

    #[tokio::test]
    async fn prepared_transaction_is_retained_before_stderr_closes() {
        let tx_id = "AB".repeat(32);
        let (mut writer, reader) = tokio::io::duplex(256);
        let (sender, mut receiver) = oneshot::channel();
        let reader_task = tokio::spawn(read_bounded_stream(reader, "stderr", Some(sender)));
        writer.write_all(b"  Prepared TX ").await.unwrap();
        writer.write_all(&tx_id.as_bytes()[..32]).await.unwrap();
        tokio::task::yield_now().await;
        assert!(matches!(
            receiver.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        writer.write_all(&tx_id.as_bytes()[32..]).await.unwrap();
        writer
            .write_all(b"; reconcile this exact hash\n")
            .await
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed, tx_id.to_ascii_lowercase());
        drop(writer);
        reader_task.await.unwrap().unwrap();
        assert!(prepared_tx_id(b"Prepared TX abc; reconcile").is_none());
        assert!(
            prepared_tx_id(format!("Prepared TX {}; reconcile", "z".repeat(64)).as_bytes())
                .is_none()
        );
    }

    #[tokio::test]
    async fn timeout_drain_keeps_a_prepared_hash_already_buffered_in_the_pipe() {
        let tx_id = "ab".repeat(32);
        let (mut writer, reader) = tokio::io::duplex(256);
        writer
            .write_all(format!("Prepared TX {tx_id}; reconcile\n").as_bytes())
            .await
            .unwrap();
        drop(writer);
        let (sender, mut receiver) = oneshot::channel();
        // Neither reader has run when drain starts on this single-thread runtime.
        let mut stdout = tokio::spawn(read_bounded_stream(tokio::io::empty(), "stdout", None));
        let mut stderr = tokio::spawn(read_bounded_stream(reader, "stderr", Some(sender)));
        drain_timed_out_readers(&mut stdout, &mut stderr).await;
        assert_eq!(receiver.try_recv().unwrap(), tx_id);
    }

    #[cfg(unix)]
    #[test]
    fn large_helper_output_is_cut_before_it_reaches_errors() {
        use std::os::unix::process::ExitStatusExt;

        let huge = "x".repeat(MAX_DUSK_TX_STREAM_BYTES);
        let tx_id = "ab".repeat(32);
        let output = |code: i32, stdout: String, stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(code << 8),
            stdout: stdout.into_bytes(),
            stderr: stderr.as_bytes().to_vec(),
        };
        let error_json = |error: String| serde_json::json!({ "error": error }).to_string();
        for (output, prepared) in [
            // A failure without a JSON error reports stderr.
            (output(1, String::new(), &huge), None),
            // The helper's own error text: unclassified, an included
            // failure, and an unknown propagation outcome.
            (output(1, error_json(huge.clone()), ""), None),
            (
                output(
                    1,
                    error_json(format!("Transaction {tx_id} failed: {huge}")),
                    "",
                ),
                None,
            ),
            (
                output(
                    1,
                    error_json(format!(
                        "Transaction {tx_id} submission failed: Propagation outcome unknown: {huge}"
                    )),
                    "",
                ),
                Some(tx_id.as_str()),
            ),
            // A successful exit whose output is not JSON.
            (output(0, huge.clone(), ""), Some(tx_id.as_str())),
        ] {
            let error = helper_result("process", &output, prepared)
                .unwrap_err()
                .to_string();
            assert!(error.len() < 3 * 1024, "{} bytes", error.len());
            assert!(error.contains("[truncated, "), "{error}");
        }
        // Classification still reads the whole error text.
        assert!(matches!(
            helper_result(
                "process",
                &output(1, error_json(format!("Transaction {tx_id} failed: {huge}")), ""),
                None
            ),
            Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id: failed, .. }) if failed == tx_id
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_noisy_failing_helper_yields_a_short_error() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg("head -c 900000 /dev/zero | tr '\\0' x >&2; exit 3")
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command.spawn().unwrap();
        let (output, prepared) = wait_for_child_output(child, "process", Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(output.stderr.len(), 900_000);
        let error = helper_result("process", &output, prepared.as_deref())
            .unwrap_err()
            .to_string();
        assert!(error.len() < 2 * 1024, "{} bytes", error.len());
        assert!(error.contains("exit code 3"), "{error}");
        assert!(
            error.ends_with("[truncated, 900000 bytes]"),
            "{}",
            &error[error.len().saturating_sub(64)..]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn noisy_helper_output_is_bounded() {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(format!(
                "head -c {} /dev/zero",
                MAX_DUSK_TX_STREAM_BYTES + 1
            ))
            .kill_on_drop(true)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let child = command.spawn().unwrap();
        let error = wait_for_child_output(child, "test", Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stdout exceeds"));
    }
}
