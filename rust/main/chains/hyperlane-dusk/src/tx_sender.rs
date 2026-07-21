//! Dusk transaction sender — invokes the `dusk-tx` CLI binary to construct
//! and submit Moonlight transactions.
//!
//! The monorepo uses stable Rust, but Moonlight TX construction requires
//! `dusk-core` which needs nightly. The `dusk-tx` binary (built in the
//! `dusk/` workspace with nightly) handles all TX construction and signing.
//! This module invokes it as a subprocess.

use std::process::Stdio;
use std::time::Duration;

use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use tracing::{debug, info, warn};

use hyperlane_core::H512;

use crate::rues::rkyv_serialize;
use crate::{ConnectionConf, DuskSigner, HyperlaneDuskError};

const DUSK_TX_HELPER_TIMEOUT: Duration = Duration::from_secs(120);
const MAX_DUSK_TX_STREAM_BYTES: usize = 1024 * 1024;
const MAX_DUSK_TX_ARGS_BYTES: usize = 60 * 1024;
const MAX_PROCESS_PAYLOAD_BYTES: usize = MAX_DUSK_TX_ARGS_BYTES - 1024;

/// Send a contract call via the `dusk-tx` CLI binary.
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
    if args_bytes.len() > MAX_DUSK_TX_ARGS_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "dusk-tx arguments for {fn_name} exceed the {MAX_DUSK_TX_ARGS_BYTES}-byte helper transport limit"
        )));
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
        .arg("--rues-url")
        .arg(conn.url.as_str())
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

    // Provide the secret key via stdin so it is not exposed in the process list.
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(secret_key_hex.as_bytes())
            .await
            .map_err(|e| {
                HyperlaneDuskError::Other(format!(
                    "Failed to write dusk secret key to dusk-tx stdin: {e}"
                ))
            })?;
        stdin.write_all(b"\n").await.map_err(|e| {
            HyperlaneDuskError::Other(format!("Failed to finalize dusk-tx stdin: {e}"))
        })?;
    }

    let output = wait_for_child_output(child, fn_name, DUSK_TX_HELPER_TIMEOUT).await?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    if !stderr.is_empty() {
        debug!(stderr = %stderr, "dusk-tx stderr");
    }

    if !output.status.success() {
        let code = output.status.code().unwrap_or(-1);
        warn!(
            code = code,
            stderr = %stderr,
            stdout = %stdout,
            "dusk-tx call failed"
        );
        // Try to parse error from JSON output
        if let Ok(json) = serde_json::from_str::<Value>(&stdout) {
            if let Some(err) = json.get("error").and_then(|e| e.as_str()) {
                if let Some(tx_id) = outcome_unknown_tx_id(err) {
                    return Err(HyperlaneDuskError::SubmissionOutcomeUnknown {
                        tx_id,
                        detail: err.to_owned(),
                    });
                }
                return Err(HyperlaneDuskError::Other(format!(
                    "dusk-tx call {fn_name} failed: {err}"
                )));
            }
        }
        return Err(HyperlaneDuskError::Other(format!(
            "dusk-tx call {fn_name} failed (exit code {code}): {stderr}"
        )));
    }

    let json: Value = serde_json::from_str(&stdout).map_err(|e| {
        HyperlaneDuskError::Other(format!(
            "Failed to parse dusk-tx output as JSON: {e}. Output: {stdout}"
        ))
    })?;

    info!(
        fn_name = %fn_name,
        contract = %contract_hex,
        "dusk-tx call succeeded"
    );

    Ok(json)
}

async fn wait_for_child_output(
    mut child: tokio::process::Child,
    fn_name: &str,
    timeout: Duration,
) -> Result<std::process::Output, HyperlaneDuskError> {
    let stdout = child.stdout.take().ok_or_else(|| {
        HyperlaneDuskError::Other("dusk-tx stdout pipe was not configured".into())
    })?;
    let stderr = child.stderr.take().ok_or_else(|| {
        HyperlaneDuskError::Other("dusk-tx stderr pipe was not configured".into())
    })?;
    let stdout_task = tokio::spawn(read_bounded_stream(stdout, "stdout"));
    let stderr_task = tokio::spawn(read_bounded_stream(stderr, "stderr"));

    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result.map_err(|error| {
            HyperlaneDuskError::Other(format!(
                "Failed to wait on dusk-tx child process for {fn_name}: {error}"
            ))
        })?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            stdout_task.abort();
            stderr_task.abort();
            return Err(HyperlaneDuskError::Other(format!(
                "dusk-tx call {fn_name} exceeded the {}s helper deadline",
                timeout.as_secs()
            )));
        }
    };

    let stdout = stdout_task.await.map_err(|error| {
        HyperlaneDuskError::Other(format!("Failed to join dusk-tx stdout reader: {error}"))
    })??;
    let stderr = stderr_task.await.map_err(|error| {
        HyperlaneDuskError::Other(format!("Failed to join dusk-tx stderr reader: {error}"))
    })??;
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

async fn read_bounded_stream(
    reader: impl AsyncRead + Unpin,
    stream_name: &'static str,
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_DUSK_TX_STREAM_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| {
            HyperlaneDuskError::Other(format!("Failed to read dusk-tx {stream_name}: {error}"))
        })?;
    if bytes.len() > MAX_DUSK_TX_STREAM_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "dusk-tx {stream_name} exceeds {MAX_DUSK_TX_STREAM_BYTES} bytes"
        )));
    }
    Ok(bytes)
}

/// Convert a 32-byte Dusk transaction ID (hex) into a `H512` by left-padding with zeros.
pub fn dusk_tx_id_to_h512(tx_id_hex: &str) -> Result<H512, HyperlaneDuskError> {
    let tx_id_hex = tx_id_hex
        .strip_prefix("0x")
        .or_else(|| tx_id_hex.strip_prefix("0X"))
        .unwrap_or(tx_id_hex);
    let bytes = hex::decode(tx_id_hex).map_err(|e| {
        HyperlaneDuskError::Other(format!("Invalid dusk tx_id hex '{tx_id_hex}': {e}"))
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
pub fn process_args(
    metadata: &[u8],
    encoded_message: &[u8],
) -> Result<Vec<u8>, HyperlaneDuskError> {
    let payload_len = metadata
        .len()
        .checked_add(encoded_message.len())
        .ok_or_else(|| HyperlaneDuskError::Other("Dusk process payload length overflow".into()))?;
    if payload_len > MAX_PROCESS_PAYLOAD_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "Dusk process metadata and message exceed the {MAX_PROCESS_PAYLOAD_BYTES}-byte payload limit"
        )));
    }
    let args = rkyv_serialize(&(metadata.to_vec(), encoded_message.to_vec()))?;
    if args.len() > MAX_DUSK_TX_ARGS_BYTES {
        return Err(HyperlaneDuskError::Other(format!(
            "Serialized Dusk process arguments exceed the {MAX_DUSK_TX_ARGS_BYTES}-byte helper transport limit"
        )));
    }
    Ok(args)
}

fn outcome_unknown_tx_id(error: &str) -> Option<String> {
    if !error.contains("submission failed: Propagation outcome unknown")
        && !error.contains("confirmation outcome unknown")
    {
        return None;
    }
    let tx_id = error.split("tx_id=").nth(1)?.split_whitespace().next()?;
    let tx_id = tx_id.trim_end_matches(|character: char| !character.is_ascii_hexdigit());
    (tx_id.len() == 64 && tx_id.bytes().all(|byte| byte.is_ascii_hexdigit()))
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
        let oversized = vec![0u8; MAX_PROCESS_PAYLOAD_BYTES + 1];
        let error = process_args(&oversized, &[]).unwrap_err();
        assert!(error.to_string().contains("payload limit"));
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
