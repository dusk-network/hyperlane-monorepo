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

/// The flag of `dusk-tx call` that makes it wait for the agent's word
/// before it sends the transaction it prepared.
const ACK_BEFORE_SEND: &str = "--ack-before-send";
/// The word, a line on the helper's stdin, that lets it send.
const SEND_WORD: &[u8] = b"send\n";
/// What a `dusk-tx` that takes `--ack-before-send` is, for the error that
/// refuses deliveries without one.
const ACK_HELPER: &str =
    "a dusk-tx built from dusk-network/hyperlane-dusk at de7e2a6 or later, whose `call --help` lists `--ack-before-send`";
/// How the helper starts the stderr line that says it sent nothing.
const NOT_SUBMITTED: &str = "Not submitted:";

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
    run_dusk_tx_at(
        &conn.url,
        conn,
        signer,
        contract_id,
        fn_name,
        args_bytes,
        gas_limit,
        &[],
        DUSK_TX_HELPER_TIMEOUT,
        None,
    )
    .await
}

/// `dusk_tx_call`, whose transaction the helper sends only once `keep` has
/// kept its hash (`--ack-before-send`). The helper prints
/// `Prepared TX <hash>;` once it has signed the transaction and waits for
/// a word on its stdin: `keep` is called with that hash, and only once it
/// succeeds does the agent write `send`, which lets the helper preverify
/// and propagate the transaction. When `keep` fails, the agent closes the
/// helper's stdin instead, so the helper sends nothing and the call fails
/// `NotSubmitted`; so does a call whose agent stops or dies before it gave
/// the word, since its end of stdin closes with it. `keep` is called at
/// most once.
///
/// Needs a helper that lists `--ack-before-send` in its `call --help`:
/// with another, nothing is signed, and the call fails.
pub(crate) async fn dusk_tx_call_kept(
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
    gas_limit: Option<u64>,
    keep: &Keep<'_>,
) -> Result<Value, HyperlaneDuskError> {
    match helper_flags().await {
        (_, Some(flags)) if flags.acks => {}
        (bin, Some(_)) => {
            return Err(HyperlaneDuskError::Other(format!(
                "The dusk-tx helper at '{bin}' does not take `call {ACK_BEFORE_SEND}`, so it would send a {fn_name} transaction before the agent kept its hash; nothing was signed. Dusk deliveries need {ACK_HELPER}."
            )))
        }
        (bin, None) => {
            return Err(HyperlaneDuskError::Other(format!(
                "Could not read `call --help` of the dusk-tx helper at '{bin}', so nothing was signed: Dusk deliveries need {ACK_HELPER}. Set DUSK_TX_BIN to its path."
            )))
        }
    }
    run_dusk_tx_at(
        &conn.url,
        conn,
        signer,
        contract_id,
        fn_name,
        args_bytes,
        gas_limit,
        &[],
        DUSK_TX_HELPER_TIMEOUT,
        Some(keep),
    )
    .await
}

/// What keeps the hash of the transaction the helper prepared, with the
/// nonce it is signed with when the helper names it, before the helper may
/// send it.
pub(crate) type Keep<'a> = dyn Fn(&str, Option<u64>) -> Result<(), HyperlaneDuskError> + Sync + 'a;

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
        None,
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

/// What the configured `dusk-tx` takes, as its `call --help` lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HelperFlags {
    /// It can sign with a given nonce and return once the node has
    /// accepted the transaction (`--nonce`, `--no-wait`), which pipelined
    /// deliveries need.
    pub pipelines: bool,
    /// It sends the transaction it prepared only once the agent says so
    /// (`--ack-before-send`), which deliveries one at a time need.
    pub acks: bool,
}

/// The configured `dusk-tx`'s path and what it takes, from its
/// `call --help`, which reads no key and sends nothing: `None` when the
/// help cannot be read. Asked once per helper path; a probe that cannot
/// read the help is not kept, so the next call asks again.
pub(crate) async fn helper_flags() -> (String, Option<HelperFlags>) {
    static PROBES: std::sync::OnceLock<
        tokio::sync::Mutex<std::collections::HashMap<String, HelperFlags>>,
    > = std::sync::OnceLock::new();
    let bin = std::env::var("DUSK_TX_BIN").unwrap_or_else(|_| "dusk-tx".into());
    let mut probes = PROBES
        .get_or_init(|| tokio::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .await;
    if let Some(known) = probes.get(&bin) {
        return (bin, Some(*known));
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
        _ => {
            warn!(
                bin = %bin,
                "Could not read the dusk-tx helper's `call --help`; it is asked again at the next submission"
            );
            return (bin, None);
        }
    };
    let flags = HelperFlags {
        pipelines: help.contains("--nonce") && help.contains("--no-wait"),
        acks: help.contains(ACK_BEFORE_SEND),
    };
    if !flags.acks {
        warn!(
            bin = %bin,
            "The dusk-tx helper does not take `call {ACK_BEFORE_SEND}`: Dusk deliveries are refused until it is replaced with {ACK_HELPER}"
        );
    }
    if !flags.pipelines {
        info!(
            bin = %bin,
            "The dusk-tx helper cannot sign with a given nonce (call --nonce --no-wait); Dusk deliveries are submitted one at a time"
        );
    }
    probes.insert(bin.clone(), flags);
    (bin, Some(flags))
}

/// Whether the configured `dusk-tx` can sign with a given nonce and return
/// once the node has accepted the transaction (`call --nonce --no-wait`),
/// which pipelined deliveries need (`helper_flags`).
pub(crate) async fn helper_pipelines() -> bool {
    helper_flags().await.1.is_some_and(|flags| flags.pipelines)
}

/// Run the helper's `call` against the RUES endpoint at `url`, for at most
/// `timeout`. With `keep`, the helper sends the transaction it prepared
/// only once `keep` has kept its hash (`dusk_tx_call_kept`).
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
    keep: Option<&Keep<'_>>,
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
        .args(keep.map(|_| ACK_BEFORE_SEND))
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
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| HyperlaneDuskError::Other("dusk-tx stdin pipe was not configured".into()))?;
    let input = format!("{}\n{secret_key_hex}\n", url.as_str());
    tokio::time::timeout(timeout, stdin.write_all(input.as_bytes()))
        .await
        .map_err(|_| HyperlaneDuskError::Other("dusk-tx stdin write exceeded its deadline".into()))?
        .map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to write dusk-tx private stdin: {error}"))
        })?;
    // Without `--ack-before-send` the helper reads nothing more. With it,
    // stdin stays open for the word that lets it send: dropped, by this
    // call's end or its cancellation, it closes, and the helper sends
    // nothing it was not let send.
    let ack = match keep {
        Some(keep) => Some(Ack { stdin, keep }),
        None => {
            drop(stdin);
            None
        }
    };

    let run = wait_for_child_output(child, fn_name, timeout, ack).await?;
    if let Word::Withheld(why) = &run.word {
        return Err(withheld(fn_name, &run, why));
    }
    let json = helper_result(fn_name, &run.output, run.prepared.as_deref())?;

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
                // A complete preverify diagnostic proves propagation never
                // ran, and so does the helper's word that it sent nothing.
                if before_propagation_error(err) || said_not_submitted(&stderr) {
                    return Err(HyperlaneDuskError::NotSubmitted(detail));
                }
                return Err(helper_failure(prepared_tx_id, detail));
            }
        }
        let detail = format!(
            "dusk-tx call {fn_name} failed (exit code {code}): {}",
            bounded_text(&stderr)
        );
        if said_not_submitted(&stderr) {
            return Err(HyperlaneDuskError::NotSubmitted(detail));
        }
        return Err(helper_failure(prepared_tx_id, detail));
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

/// The agent's side of `--ack-before-send`: the helper's stdin, still open
/// after the key line, and what keeps the hash of the transaction it
/// prepared before the word that lets it send goes there.
struct Ack<'a> {
    stdin: tokio::process::ChildStdin,
    keep: &'a Keep<'a>,
}

/// Whether the helper was let send the transaction it prepared.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Word {
    /// It sends on its own: it was run without `--ack-before-send`.
    NotAsked,
    /// The agent kept the transaction's hash and wrote the word: the helper
    /// may have sent it.
    Given,
    /// The agent did not write the word, why. A helper that takes
    /// `--ack-before-send` then sends nothing.
    Withheld(String),
}

/// A helper call that ended.
#[derive(Debug)]
struct HelperRun {
    output: std::process::Output,
    /// The transaction the helper named as prepared, if it did.
    prepared: Option<String>,
    word: Word,
}

/// Keep `tx_id`, signed with `nonce` when the helper named it, with
/// `ack`'s `keep`, and only once that succeeds write the word that lets the
/// helper send it. What the helper was told.
async fn answer(ack: Ack<'_>, tx_id: &str, nonce: Option<u64>) -> Word {
    let Ack { mut stdin, keep } = ack;
    if let Err(error) = keep(tx_id, nonce) {
        // Dropping stdin closes it: the helper sends nothing.
        return Word::Withheld(format!(
            "its hash could not be kept: {}",
            bounded_error(&error)
        ));
    }
    match stdin.write_all(SEND_WORD).await {
        Ok(()) => Word::Given,
        // The helper closed its stdin, so it stopped before it read the
        // word: it sent nothing.
        Err(error) => Word::Withheld(format!(
            "the helper stopped before it read the word to send it: {error}"
        )),
    }
}

/// The helper's output and the transaction it named, once it exits, or
/// `timeout`'s error. The helper names the transaction it prepared before
/// it sends it; with `ack`, that hash is kept as soon as it is named, and
/// only then is the helper let send it (`answer`). Cancelled, this drops
/// the helper's stdin and kills the helper, so a helper not yet let send
/// sends nothing.
async fn wait_for_child_output(
    mut child: tokio::process::Child,
    fn_name: &str,
    timeout: Duration,
    mut ack: Option<Ack<'_>>,
) -> Result<HelperRun, HyperlaneDuskError> {
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

    let mut prepared_tx_id: Option<String> = None;
    let mut word = if ack.is_some() {
        Word::Withheld("the helper named no transaction".into())
    } else {
        Word::NotAsked
    };
    let waited = tokio::time::timeout(timeout, async {
        let mut naming = true;
        loop {
            tokio::select! {
                status = child.wait() => return status,
                prepared = &mut prepared_tx_receiver, if naming => {
                    naming = false;
                    if let Ok((tx_id, nonce)) = prepared {
                        if let Some(ack) = ack.take() {
                            word = answer(ack, &tx_id, nonce).await;
                        }
                        prepared_tx_id = Some(tx_id);
                    }
                }
            }
        }
    })
    .await;
    // Not let send by now, the helper never will be.
    drop(ack);
    let status = waited
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
            let prepared_tx_id = prepared_tx_id
                .or_else(|| prepared_tx_receiver.try_recv().ok().map(|(tx_id, _)| tx_id));
            if let Word::Withheld(why) = word {
                return Err(HyperlaneDuskError::NotSubmitted(format!(
                    "{detail}; it was not let send a transaction ({why}), so it sent none"
                )));
            }
            return Err(helper_failure(prepared_tx_id.as_deref(), detail));
        }
    };

    // Observe both readers before classifying a failed read or parsing output.
    // A normal or signaled exit can interrupt the helper after propagation just
    // as a timeout can; the prepared hash remains authoritative in every case.
    let (stdout, stderr) = tokio::join!(stdout_task, stderr_task);
    let prepared_tx_id =
        prepared_tx_id.or_else(|| prepared_tx_receiver.try_recv().ok().map(|(tx_id, _)| tx_id));
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
    Ok(HelperRun {
        output: std::process::Output {
            status,
            stdout,
            stderr,
        },
        prepared: prepared_tx_id,
        word,
    })
}

/// The error of a helper call whose transaction the agent did not let it
/// send, why: nothing was sent. A helper that named no transaction failed
/// before it signed one, and says why itself.
fn withheld(fn_name: &str, run: &HelperRun, why: &str) -> HyperlaneDuskError {
    let Some(tx_id) = &run.prepared else {
        return match helper_result(fn_name, &run.output, None) {
            Err(error) => error,
            Ok(_) => HyperlaneDuskError::NotSubmitted(format!(
                "dusk-tx call {fn_name} reported success without naming the transaction it prepared, which it was never let send"
            )),
        };
    };
    let stderr = String::from_utf8_lossy(&run.output.stderr);
    let said = stderr
        .lines()
        .map(str::trim_start)
        .find(|line| line.starts_with(NOT_SUBMITTED))
        .map_or_else(
            || format!("exit code {}", run.output.status.code().unwrap_or(-1)),
            bounded_text,
        );
    warn!(fn_name, %tx_id, %why, "dusk-tx was not let send the transaction it prepared");
    HyperlaneDuskError::NotSubmitted(format!(
        "dusk-tx prepared {fn_name} transaction {tx_id} but was not let send it, since {why}; it sent nothing ({said})"
    ))
}

/// Whether the helper said it sent nothing: a line of its stderr starts
/// with `Not submitted:`, which a helper that takes `--ack-before-send`
/// prints when it stops without the word to send.
fn said_not_submitted(stderr: &str) -> bool {
    stderr
        .lines()
        .any(|line| line.trim_start().starts_with(NOT_SUBMITTED))
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
            || detail.starts_with("Preverify rejected before propagation (")
            // `--ack-before-send` without the word to send.
            || detail.starts_with(NOT_SUBMITTED))
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
    mut prepared_tx_sender: Option<oneshot::Sender<(String, Option<u64>)>>,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let mut reader = reader.take((MAX_DUSK_TX_STREAM_BYTES + 1) as u64);
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let count = reader.read(&mut chunk).await.map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to read dusk-tx {stream_name}: {error}"))
        })?;
        if count == 0 {
            // Stderr closed: what came of the prepared line is all there is.
            if let (Some(sender), Some(prepared)) =
                (prepared_tx_sender.take(), prepared_tx(&bytes, true))
            {
                let _ = sender.send(prepared);
            }
            return Ok(bytes);
        }
        bytes.extend_from_slice(&chunk[..count]);
        if prepared_tx_sender.is_some() {
            if let Some(prepared) = prepared_tx(&bytes, false) {
                if let Some(sender) = prepared_tx_sender.take() {
                    let _ = sender.send(prepared);
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

/// The transaction the helper prepared, from its stderr so far: the hash
/// on its `Prepared TX <hash>; nonce <N>; ...` line, and the nonce, which a
/// helper from before it named the nonce leaves out (`Prepared TX <hash>;
/// ...`). The hash is everything before the line's first `;`, and is read
/// once that `;` has come; the nonce once its own `;` has, since the helper
/// may write the line in pieces. With `ended`, stderr is closed, and what
/// came of the line is all there is. A nonce that is not a number, or that
/// lacks its `;` and so may be cut short, is left out.
fn prepared_tx(stderr: &[u8], ended: bool) -> Option<(String, Option<u64>)> {
    String::from_utf8_lossy(stderr).lines().find_map(|line| {
        let mut fields = line.trim_start().strip_prefix("Prepared TX ")?.split(';');
        let tx_id = fields.next()?;
        let after = fields.next()?;
        if tx_id.len() != 64 || !tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return None;
        }
        let tx_id = tx_id.to_ascii_lowercase();
        match after.strip_prefix(" nonce ") {
            Some(nonce) => {
                let whole = fields.next().is_some();
                if !whole && !ended {
                    return None;
                }
                let digits = !nonce.is_empty() && nonce.bytes().all(|byte| byte.is_ascii_digit());
                Some((
                    tx_id,
                    (whole && digits).then(|| nonce.parse().ok()).flatten(),
                ))
            }
            // The nonce may still come.
            None if " nonce ".starts_with(after) && !ended => None,
            None => Some((tx_id, None)),
        }
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
        let error = wait_for_child_output(child, "test", Duration::from_millis(20), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("helper deadline"));
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    /// The prepared line, as `dusk-tx` prints it for a transaction signed
    /// with nonce 7.
    fn prepared_line(tx_id: &str) -> String {
        format!("printf '  Prepared TX {tx_id}; nonce 7; reconcile this exact hash before retrying if submission is interrupted\\n' >&2")
    }

    /// A shell helper running `script`, with a stdin the caller writes.
    fn shell_helper(script: &str) -> tokio::process::Child {
        let mut command = Command::new("sh");
        command
            .arg("-c")
            .arg(script)
            .kill_on_drop(true)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.spawn().unwrap()
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timed_out_helper_preserves_prepared_transaction_for_reconciliation() {
        let tx_id = "ab".repeat(32);
        // Sent on its own, or let send: either may have reached the node.
        for acks in [false, true] {
            let mut child = shell_helper(&format!(
                "{}; read -r word; exec sleep 10",
                prepared_line(&tx_id)
            ));
            let kept = StdMutex::new(Vec::new());
            let keep = |tx_id: &str, _: Option<u64>| {
                kept.lock().unwrap().push(tx_id.to_owned());
                Ok(())
            };
            let stdin = child.stdin.take().unwrap();
            let ack = acks.then_some(Ack { stdin, keep: &keep });
            let error = wait_for_child_output(child, "process", Duration::from_millis(200), ack)
                .await
                .unwrap_err();
            assert!(
                matches!(
                    &error,
                    HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id: observed, .. }
                        if *observed == tx_id
                ),
                "{error}"
            );
            assert_eq!(kept.into_inner().unwrap().len(), usize::from(acks));
        }
    }

    /// The helper names its transaction before it propagates it, then waits
    /// for its block: the caller keeps the hash at once, not when the helper
    /// is done, and only then lets the helper send it.
    #[cfg(unix)]
    #[tokio::test]
    async fn the_prepared_transaction_is_kept_while_the_helper_runs_and_only_then_sent() {
        let tx_id = "cd".repeat(32);
        let mut child = shell_helper(&format!(
            "{}; IFS= read -r word; sleep 1; printf '{{\"word\":\"%s\"}}' \"$word\"",
            prepared_line(&tx_id)
        ));
        let started = std::time::Instant::now();
        let kept = StdMutex::new(Vec::new());
        let keep = |tx_id: &str, nonce: Option<u64>| {
            kept.lock()
                .unwrap()
                .push((tx_id.to_owned(), nonce, started.elapsed()));
            Ok(())
        };
        let ack = Ack {
            stdin: child.stdin.take().unwrap(),
            keep: &keep,
        };
        let run = wait_for_child_output(child, "process", Duration::from_secs(5), Some(ack))
            .await
            .unwrap();
        let finished = started.elapsed();
        assert_eq!(run.prepared.as_deref(), Some(tx_id.as_str()));
        assert_eq!(run.word, Word::Given);
        assert_eq!(run.output.stdout, br#"{"word":"send"}"#);
        let kept = kept.into_inner().unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!((kept[0].0.as_str(), kept[0].1), (tx_id.as_str(), Some(7)));
        // Kept about a second before the helper was done.
        assert!(
            kept[0].2 + Duration::from_millis(500) < finished,
            "{:?} {finished:?}",
            kept[0].2
        );
    }

    /// A transaction whose hash could not be kept is never let go: the
    /// helper's stdin closes without the word, and it sends nothing.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_helper_whose_transaction_was_not_kept_sends_nothing() {
        let tx_id = "ef".repeat(32);
        let directory = tempfile::tempdir().unwrap();
        let sent = directory.path().join("sent");
        let mut child = shell_helper(&format!(
            "{}; if IFS= read -r word && [ \"$word\" = send ]; then : > '{}'; printf '{{}}'; else echo 'Not submitted: the agent did not say send' >&2; exit 1; fi",
            prepared_line(&tx_id),
            sent.display()
        ));
        let keep = |_: &str, _: Option<u64>| {
            Err(HyperlaneDuskError::Other("the event store is gone".into()))
        };
        let ack = Ack {
            stdin: child.stdin.take().unwrap(),
            keep: &keep,
        };
        let run = wait_for_child_output(child, "process", Duration::from_secs(5), Some(ack))
            .await
            .unwrap();
        assert!(!sent.exists());
        let Word::Withheld(why) = &run.word else {
            panic!("{:?}", run.word);
        };
        assert!(why.contains("the event store is gone"), "{why}");
        let error = withheld("process", &run, why);
        let HyperlaneDuskError::NotSubmitted(detail) = &error else {
            panic!("{error:?}");
        };
        assert!(detail.contains(&tx_id), "{detail}");
        assert!(
            detail.contains("Not submitted: the agent did not say send"),
            "{detail}"
        );
    }

    /// The helper's word that it sent nothing makes a call `NotSubmitted`,
    /// but never outweighs an outcome it reports for the transaction.
    #[cfg(unix)]
    #[test]
    fn a_helper_that_says_it_sent_nothing_did_not_submit() {
        use std::os::unix::process::ExitStatusExt;

        let tx_id = "ab".repeat(32);
        let output = |stdout: String, stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(1 << 8),
            stdout: stdout.into_bytes(),
            stderr: stderr.as_bytes().to_vec(),
        };
        let prepared = format!("  Prepared TX {tx_id}; reconcile\n");
        let reason = "stdin closed before the line \"send\"";
        let said = format!("{prepared}Not submitted: {reason}\n");
        // As dusk-tx says it: on stderr, and in its error, after the
        // transaction's `submission failed: `. Either is enough.
        let refused = serde_json::json!({ "success": false, "error": format!(
            "Transaction {tx_id} submission failed: Not submitted: {reason}"
        ) })
        .to_string();
        for (stdout, stderr) in [
            (refused.clone(), said.as_str()),
            (refused, prepared.as_str()),
            (String::new(), said.as_str()),
            (
                serde_json::json!({ "success": false, "error": "no word" }).to_string(),
                said.as_str(),
            ),
        ] {
            assert!(matches!(
                helper_result("process", &output(stdout, stderr), Some(&tx_id)),
                Err(HyperlaneDuskError::NotSubmitted(_))
            ));
        }
        // Without that word, a helper that named its transaction may have
        // sent it.
        assert!(matches!(
            helper_result("process", &output(String::new(), &prepared), Some(&tx_id)),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { .. })
        ));
        let unknown = serde_json::json!({ "success": false, "error": format!(
            "Transaction {tx_id} submission failed: Propagation outcome unknown: closed; retain tx_id={tx_id}"
        ) })
        .to_string();
        assert!(matches!(
            helper_result("process", &output(unknown, &said), Some(&tx_id)),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { .. })
        ));
    }

    /// The helper may write its prepared line in pieces: the hash is read
    /// once its `;` has come, and the nonce once its own `;` has, so a
    /// nonce cut short is never read.
    #[tokio::test]
    async fn prepared_transaction_is_retained_before_stderr_closes() {
        let tx_id = "AB".repeat(32);
        let (mut writer, reader) = tokio::io::duplex(256);
        let (sender, mut receiver) = oneshot::channel();
        let reader_task = tokio::spawn(read_bounded_stream(reader, "stderr", Some(sender)));
        for piece in [
            &b"  Prepared TX "[..],
            &tx_id.as_bytes()[..32],
            &tx_id.as_bytes()[32..],
            b";",
            b" nonce 4",
        ] {
            writer.write_all(piece).await.unwrap();
            tokio::task::yield_now().await;
            assert!(matches!(
                receiver.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ));
        }
        writer
            .write_all(b"2; reconcile this exact hash\n")
            .await
            .unwrap();
        let observed = tokio::time::timeout(Duration::from_secs(1), receiver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(observed, (tx_id.to_ascii_lowercase(), Some(42)));
        drop(writer);
        reader_task.await.unwrap().unwrap();
    }

    /// The prepared line names the hash, and the nonce after it when the
    /// helper names one: a helper from before it did is still read.
    #[test]
    fn the_prepared_line_names_the_hash_and_maybe_the_nonce() {
        let tx_id = "ab".repeat(32);
        let read = |line: String, ended| prepared_tx(line.as_bytes(), ended);
        assert_eq!(
            read(
                format!("  Prepared TX {tx_id}; nonce 42; reconcile\n"),
                false
            ),
            Some((tx_id.clone(), Some(42)))
        );
        // A helper from before it named the nonce.
        assert_eq!(
            read(
                format!("  Prepared TX {tx_id}; reconcile this exact hash\n"),
                false
            ),
            Some((tx_id.clone(), None))
        );
        // A nonce that is not a number is left out.
        assert_eq!(
            read(
                format!("  Prepared TX {tx_id}; nonce x7; reconcile\n"),
                false
            ),
            Some((tx_id.clone(), None))
        );
        // Stderr closed after a nonce without its `;`: it may be cut short.
        assert_eq!(
            read(format!("  Prepared TX {tx_id}; nonce 4"), true),
            Some((tx_id.clone(), None))
        );
        assert_eq!(read(format!("  Prepared TX {tx_id}; nonce 4"), false), None);
        assert_eq!(read(format!("  Prepared TX {tx_id};"), false), None);
        assert_eq!(
            read(format!("  Prepared TX {tx_id};"), true),
            Some((tx_id.clone(), None))
        );
        assert_eq!(read("Prepared TX abc; reconcile".into(), true), None);
        assert_eq!(
            read(format!("Prepared TX {}; reconcile", "z".repeat(64)), true),
            None
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
        assert_eq!(receiver.try_recv().unwrap(), (tx_id, None));
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
        let run = wait_for_child_output(child, "process", Duration::from_secs(5), None)
            .await
            .unwrap();
        assert_eq!(run.output.stderr.len(), 900_000);
        let error = helper_result("process", &run.output, run.prepared.as_deref())
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
        let error = wait_for_child_output(child, "test", Duration::from_secs(2), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("stdout exceeds"));
    }
}
