//! Properties of how the agent reads the `dusk-tx` helper's output.
//!
//! The helper is a local binary, but the text of the errors it reports
//! partly comes from the node, and the agent decides from that output
//! whether a transaction may have been sent. These hold for any input.

use std::os::unix::process::ExitStatusExt;
use std::process::{ExitStatus, Output};

use serde_json::json;

use super::{dusk_tx_id_to_h512, helper_result, prepared_tx, NOT_SUBMITTED};
use crate::checks::Input;
use crate::HyperlaneDuskError;

/// What a stretch of text from the node might hold: the phrases the
/// classifiers look for, so node text that imitates the helper is tried.
const NODE_PHRASES: &[&str] = &[
    "Transaction ",
    " submission failed: ",
    "Preverify failed before propagation: ",
    "Preverify rejected before propagation (",
    "Propagation outcome unknown",
    "confirmation outcome unknown",
    " failed: ",
    "Not submitted: ",
    "Prepared TX ",
    "; nonce ",
    ";",
    "\n",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
];

/// The helper's `Prepared TX` line (`dusk-tx/src/main.rs`, `prepared_line`).
fn helper_prepared_line(tx_id: &str, nonce: Option<u64>) -> String {
    let nonce = nonce
        .map(|nonce| format!(" nonce {nonce};"))
        .unwrap_or_default();
    format!(
        "  Prepared TX {tx_id};{nonce} reconcile this exact hash before retrying if submission is interrupted"
    )
}

/// The nonce `text` spells after `Prepared TX <hash>; nonce `, up to `;`.
fn spelled_nonce(text: &str, hash: &str) -> Vec<u64> {
    let lower = text.to_ascii_lowercase();
    let anchor = format!("prepared tx {hash}; nonce ");
    lower
        .match_indices(&anchor)
        .filter_map(|(at, _)| {
            let rest = &lower[at + anchor.len()..];
            let digits = rest.split(';').next()?;
            (rest.len() > digits.len()).then(|| digits.parse().ok())?
        })
        .collect()
}

/// `prepared_tx` on any bytes, whole and cut anywhere, ended or not:
/// - it never panics;
/// - what it reads is a lowercase 64-hex hash that the bytes name on a
///   `Prepared TX` line, followed by `;`, and a nonce they spell out whole;
/// - once a prefix that has not ended names a transaction, every longer one
///   names the same, so the agent keeps what it read first;
/// - the helper's own line, among other output and read in any pieces,
///   is read as exactly its hash and nonce, from the first prefix that
///   names anything.
pub fn prepared_line(data: &[u8]) {
    let mut input = Input::new(data);
    let cut = input.below(data.len().saturating_add(1)).min(data.len());
    let later = cut
        + input
            .below(data.len().saturating_add(1) - cut)
            .min(data.len() - cut);
    for (bytes, ended) in [
        (&data[..cut], false),
        (&data[..later], false),
        (data, false),
        (data, true),
    ] {
        if let Some((hash, nonce)) = prepared_tx(bytes, ended) {
            assert_eq!(hash.len(), 64);
            assert!(hash
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f')));
            let text = String::from_utf8_lossy(bytes).to_ascii_lowercase();
            assert!(text.contains(&format!("prepared tx {hash};")), "{text:?}");
            if let Some(nonce) = nonce {
                assert!(spelled_nonce(&text, &hash).contains(&nonce), "{text:?}");
            }
        }
    }
    if let Some(first) = prepared_tx(&data[..cut], false) {
        assert_eq!(prepared_tx(&data[..later], false), Some(first.clone()));
        assert_eq!(prepared_tx(data, false), Some(first));
    }

    // The helper's line among other output, written in pieces.
    let mut input = Input::new(data);
    let hash = input.hash_hex();
    let nonce = input.bool().then(|| input.edgy_u64());
    let other_output = |input: &mut Input| -> String {
        let text = input.text(12, &["Deploying", "  [waiting]", "Prepared", "TX", ";"]);
        // The helper prints one `Prepared TX` line: no other line of its
        // output starts like one.
        text.lines()
            .map(|line| {
                if line.trim_start().starts_with("Prepared TX ") {
                    format!("# {line}")
                } else {
                    line.to_owned()
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let mut stream = other_output(&mut input);
    if !stream.is_empty() {
        stream.push('\n');
    }
    stream.push_str(&helper_prepared_line(&hash, nonce));
    stream.push('\n');
    stream.push_str(&other_output(&mut input));
    let stream = stream.into_bytes();
    let expected = Some((hash.to_ascii_lowercase(), nonce));
    let mut read = None;
    let mut end = 0;
    while end < stream.len() && read.is_none() {
        end = (end + 1 + input.below(64)).min(stream.len());
        read = prepared_tx(&stream[..end], false);
    }
    let read = read.or_else(|| prepared_tx(&stream, true));
    assert_eq!(read, expected, "{:?}", String::from_utf8_lossy(&stream));
}

/// How the agent must classify one failed helper call.
#[derive(Debug)]
enum Expected {
    /// The helper sent nothing.
    NotSubmitted,
    /// It may have been sent: reconcile this hash.
    OutcomeUnknown(String),
    /// It was included and failed: account for this hash's gas.
    ExecutionFailed(String),
    /// Not about a transaction, and none was prepared.
    Other,
}

/// `helper_result` on the errors the helper reports, around any text the
/// node chooses, with the prepared hash the agent read or none:
/// - only the helper's own refusals before propagation read as "not
///   submitted"; nothing the node writes turns another error into one;
/// - an error naming another transaction than the prepared one is an
///   unknown outcome of the prepared one;
/// - an included failure and an unknown outcome name the error's
///   transaction;
/// - a success is taken only with the prepared transaction's ID.
pub fn helper_output(data: &[u8]) {
    let mut input = Input::new(data);
    let tx_id = input.hash_hex();
    let other = input.hash_hex();
    let prepared = match input.u8() % 3 {
        0 => None,
        1 => Some(tx_id.clone()),
        _ => Some(other.clone()),
    };
    let node = input.text(24, NODE_PHRASES);
    let status = [
        "400 Bad Request",
        "500 Internal Server Error",
        "429 Too Many Requests",
    ][input.below(3)];
    let lower = tx_id.to_ascii_lowercase();
    let template = input.u8() % 11;
    let (error, expected) = match template {
        0 => (
            format!("Transaction {tx_id} submission failed: Preverify failed before propagation: {node}"),
            Expected::NotSubmitted,
        ),
        1 => (
            format!("Transaction {tx_id} submission failed: Preverify rejected before propagation ({status}): {node}"),
            Expected::NotSubmitted,
        ),
        2 => (
            format!("Transaction {tx_id} submission failed: Propagation outcome unknown ({status}): {node}; retain tx_id={tx_id} and reconcile this exact hash before retrying if the propagation outcome is unknown"),
            Expected::OutcomeUnknown(lower.clone()),
        ),
        3 => (
            format!("Transaction {tx_id} submission failed: Propagation outcome unknown: {node}; retain tx_id={tx_id} and reconcile this exact hash before retrying if the propagation outcome is unknown"),
            Expected::OutcomeUnknown(lower.clone()),
        ),
        4 => (
            format!("Transaction {tx_id} failed: {node}"),
            Expected::ExecutionFailed(lower.clone()),
        ),
        5 => (
            format!("Transaction {tx_id} submission failed: Not submitted: {node}"),
            Expected::NotSubmitted,
        ),
        6 => (
            format!("Transaction {tx_id} confirmation outcome unknown: {node}; retain tx_id={tx_id}"),
            Expected::OutcomeUnknown(lower.clone()),
        ),
        7 => {
            // An error that is not about a transaction, such as a chain ID
            // mismatch before anything was signed.
            let text = if node.starts_with("Transaction ") {
                format!("Error: {node}")
            } else {
                node.clone()
            };
            (text, Expected::Other)
        }
        8 => {
            // A submission error of a shape the helper does not write today:
            // not a refusal before propagation, so never "not submitted".
            let known = ["Preverify", "Not submitted", "Propagation outcome unknown"];
            let detail = if known.iter().any(|prefix| node.starts_with(prefix)) {
                format!("x{node}")
            } else {
                node.clone()
            };
            (
                format!("Transaction {tx_id} submission failed: {detail}"),
                Expected::Other,
            )
        }
        9 => {
            // A refusal naming something that is not a transaction ID.
            let mut bad = tx_id.clone().into_bytes();
            let at = input.below(bad.len());
            bad[at] = b'g';
            let bad = String::from_utf8(bad).unwrap_or_default();
            (
                format!("Transaction {bad} submission failed: Preverify failed before propagation: {node}"),
                Expected::Other,
            )
        }
        _ => {
            success(&mut input, prepared.as_deref(), &tx_id, &other);
            return;
        }
    };
    let mut stderr = String::new();
    if let Some(prepared) = &prepared {
        stderr.push_str(&helper_prepared_line(prepared, None));
        stderr.push('\n');
    }
    if template == 5 {
        // The helper's own refusal (`dusk-tx` main.rs, `propagate_with`).
        stderr.push_str(&format!("{NOT_SUBMITTED} {node}\n"));
    }
    let stdout = serde_json::to_string_pretty(&json!({ "success": false, "error": error }))
        .unwrap_or_default();
    let output = Output {
        status: ExitStatus::from_raw(1 << 8),
        stdout: stdout.into_bytes(),
        stderr: stderr.into_bytes(),
    };
    let result = helper_result("process", &output, prepared.as_deref());
    // An error about another transaction than the one prepared.
    let disagrees = prepared
        .as_deref()
        .is_some_and(|prepared| !prepared.eq_ignore_ascii_case(&tx_id))
        && !matches!(expected, Expected::Other);
    let expected = match (expected, &prepared) {
        _ if disagrees => Expected::OutcomeUnknown(prepared.clone().unwrap_or_default()),
        (Expected::Other, Some(prepared)) => Expected::OutcomeUnknown(prepared.clone()),
        (expected, _) => expected,
    };
    match (&expected, &result) {
        (Expected::NotSubmitted, Err(HyperlaneDuskError::NotSubmitted(_))) => {}
        (
            Expected::OutcomeUnknown(want),
            Err(HyperlaneDuskError::SubmissionOutcomeUnknown { tx_id, .. }),
        )
        | (
            Expected::ExecutionFailed(want),
            Err(HyperlaneDuskError::TransactionExecutionFailed { tx_id, .. }),
        ) => assert_eq!(tx_id, want),
        (Expected::Other, Err(HyperlaneDuskError::Other(_))) => {}
        _ => unreachable!("expected {expected:?}, got {result:?} for {error:?}"),
    }
}

/// A successful helper run reporting `tx_id`: taken only when it is a
/// transaction ID and the prepared one, if the agent read one.
fn success(input: &mut Input, prepared: Option<&str>, tx_id: &str, other: &str) {
    let reported = match input.u8() % 4 {
        0 => tx_id.to_owned(),
        1 => format!("0x{tx_id}"),
        2 => other.to_owned(),
        _ => input.text(4, &["0x", "ab", "zz"]),
    };
    let stdout =
        serde_json::to_string(&json!({ "success": input.u8() % 8 != 0, "tx_id": reported }))
            .unwrap_or_default();
    let output = Output {
        status: ExitStatus::from_raw(0),
        stdout: stdout.into_bytes(),
        stderr: Vec::new(),
    };
    let result = helper_result("process", &output, prepared);
    if let Ok(json) = result {
        assert_eq!(json["success"].as_bool(), Some(true));
        let id = dusk_tx_id_to_h512(json["tx_id"].as_str().unwrap_or_default());
        assert!(id.is_ok());
        if let Some(prepared) = prepared {
            assert_eq!(id.ok(), dusk_tx_id_to_h512(prepared).ok());
        }
    }
}

/// `dusk_tx_id_to_h512` takes exactly 64 hex digits after an optional
/// `0x`, and puts them in the low half of the hash.
pub fn tx_id(data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let digits = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(&text);
    let valid = digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit());
    match dusk_tx_id_to_h512(&text) {
        Ok(hash) => {
            assert!(valid, "{text:?}");
            assert_eq!(&hash.as_bytes()[..32], &[0u8; 32]);
            assert_eq!(
                hex::encode(&hash.as_bytes()[32..]),
                digits.to_ascii_lowercase()
            );
        }
        Err(_) => assert!(!valid, "{text:?}"),
    }
}

#[cfg(test)]
mod tests {
    use crate::checks::run_cases;

    #[test]
    fn prepared_lines_are_read_as_the_helper_wrote_them() {
        run_cases(11, 20_000, 300, super::prepared_line);
    }

    #[test]
    fn node_text_never_makes_a_sent_transaction_read_as_unsent() {
        run_cases(12, 20_000, 400, super::helper_output);
    }

    #[test]
    fn transaction_ids_are_exactly_64_hex_digits() {
        run_cases(13, 5_000, 70, super::tx_id);
        let hash = "aB".repeat(32);
        super::tx_id(hash.as_bytes());
        super::tx_id(format!("0x{hash}").as_bytes());
        super::tx_id(format!("0X{hash}").as_bytes());
        super::tx_id(format!("0x0x{hash}").as_bytes());
    }
}
