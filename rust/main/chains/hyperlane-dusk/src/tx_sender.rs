//! Dusk transaction sender — invokes the `dusk-tx` CLI binary to construct
//! and submit Moonlight transactions.
//!
//! The monorepo uses stable Rust, but Moonlight TX construction requires
//! `dusk-core` which needs nightly. The `dusk-tx` binary (built in the
//! `dusk/` workspace with nightly) handles all TX construction and signing.
//! This module invokes it as a subprocess.

use std::process::Stdio;

use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::{debug, info, warn};

use hyperlane_core::H512;

use crate::rues::rkyv_serialize;
use crate::{ConnectionConf, DuskSigner, HyperlaneDuskError};

/// Send a contract call via the `dusk-tx` CLI binary.
///
/// Returns the parsed JSON output on success.
pub async fn dusk_tx_call(
    conn: &ConnectionConf,
    signer: &DuskSigner,
    contract_id: &[u8; 32],
    fn_name: &str,
    args_bytes: &[u8],
) -> Result<Value, HyperlaneDuskError> {
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

    let mut child = Command::new(&bin)
        .arg("call")
        .arg("--rues-url")
        .arg(conn.url.as_str())
        .arg("--secret-key-stdin")
        .arg("--contract")
        .arg(&contract_hex)
        .arg("--fn-name")
        .arg(fn_name)
        .arg("--args")
        .arg(&args_hex)
        .arg("--gas-limit")
        .arg(conn.gas_limit.to_string())
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
        stdin.write_all(secret_key_hex.as_bytes()).await.map_err(|e| {
            HyperlaneDuskError::Other(format!("Failed to write dusk secret key to dusk-tx stdin: {e}"))
        })?;
        stdin.write_all(b"\n").await.map_err(|e| {
            HyperlaneDuskError::Other(format!("Failed to finalize dusk-tx stdin: {e}"))
        })?;
    }

    let output = child.wait_with_output().await.map_err(|e| {
        HyperlaneDuskError::Other(format!("Failed to wait on dusk-tx child process: {e}"))
    })?;

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

/// Convert a 32-byte Dusk transaction ID (hex) into a `H512` by left-padding with zeros.
pub fn dusk_tx_id_to_h512(tx_id_hex: &str) -> Result<H512, HyperlaneDuskError> {
    let bytes = hex::decode(tx_id_hex).map_err(|e| {
        HyperlaneDuskError::Other(format!(
            "Invalid dusk tx_id hex '{tx_id_hex}': {e}"
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

/// Build rkyv-serialized args for mailbox.process(metadata, encoded_message).
pub fn process_args(metadata: &[u8], encoded_message: &[u8]) -> Vec<u8> {
    rkyv_serialize(&(metadata.to_vec(), encoded_message.to_vec()))
}

/// Build rkyv-serialized args for validator_announce.announce(validator, location, signature).
pub fn announce_args(
    validator_eth_addr: [u8; 20],
    storage_location: &str,
    signature: &[u8],
) -> Vec<u8> {
    let eth_addr = hyperlane_dusk_types::EthAddress(validator_eth_addr);
    rkyv_serialize(&(
        eth_addr,
        String::from(storage_location),
        signature.to_vec(),
    ))
}
