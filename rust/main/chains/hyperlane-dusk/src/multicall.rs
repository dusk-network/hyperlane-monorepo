//! The ABI of Multicall for Dusk (`HDauven/dusk-multicall`): one contract
//! query that makes many contract reads, all at one state.
//!
//! `aggregate` takes a `Vec<Call>` and returns an [`Aggregate`]: the height
//! of the block whose state the query ran on and, in call order, each call's
//! reply bytes or why it failed. A failing call never stops the others.
//! `version` returns [`VERSION`]. Both run only as direct queries, and the
//! contract holds no state. Arguments and replies are rkyv 0.7 archives
//! (`size_32`), as every Dusk contract's are.
//!
//! These types replicate the contract's own, so that the agent does not
//! depend on its crate. `tests/fixtures/multicall_vectors.json`, generated
//! from that crate, holds the bytes they must write and read, and
//! `tests/fixtures/multicall_testnet.json` requests and replies of the
//! deployed contract on testnet.
//!
//! Mainnet and testnet run it at
//! `93de287b7d349d678a0038fe7772e8960e1a84a1b5da2d0b1f62f532616ce1d4`.

use std::mem::{align_of, size_of};
use std::ops::Range;

use rkyv::string::repr::INLINE_CAPACITY;
use rkyv::{Archive, Archived, Deserialize, Serialize};

/// The ABI version `version` returns.
pub const VERSION: u32 = 1;

/// The most calls one `aggregate` takes.
pub const MAX_CALLS: usize = 256;

/// The VM's argument buffer, which holds an `aggregate`'s argument and its
/// reply. A reply that would not fit fails the whole query.
pub const ARGBUF_LEN: usize = 64 * 1024;

/// The most bytes of a callee's panic message a result keeps.
pub const MAX_PANIC_BYTES: usize = 200;

/// One read of an `aggregate`.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Call {
    /// The contract to call (a `ContractId`, archived as its 32 bytes).
    pub contract: [u8; 32],
    /// The function to call.
    pub fn_name: String,
    /// The argument, in the callee's own encoding.
    pub fn_args: Vec<u8>,
    /// The most gas the call may spend, or `0` for a fair share of what the
    /// query has left.
    pub gas_limit: u64,
}

/// What `aggregate` returns.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub struct Aggregate {
    /// The height of the block whose state the query ran on. A height alone
    /// does not name a block: a block that is rolled back and the one that
    /// replaces it share theirs.
    pub height: u64,
    /// One result per call, in call order: exactly the bytes the callee
    /// returned, or why the call failed.
    pub results: Vec<Result<Vec<u8>, CallError>>,
}

/// Why a call of an `aggregate` failed.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[archive(check_bytes)]
pub enum CallError {
    /// The callee panicked with this message, cut to [`MAX_PANIC_BYTES`].
    Panic(String),
    /// The callee ran out of the gas the call had.
    OutOfGas,
    /// No contract has this ID.
    DoesNotExist,
    /// Any other failure, such as a missing function or a trap.
    Unknown,
}

/// The bytes a result adds to an `aggregate` reply beside its fixed slot:
/// its reply, or a panic message too long to sit inside the slot.
pub fn out_of_line(result: &Result<Vec<u8>, CallError>) -> usize {
    match result {
        Ok(bytes) => bytes.len(),
        Err(CallError::Panic(message)) if message.len() > INLINE_CAPACITY => message.len(),
        Err(_) => 0,
    }
}

/// The length of an `aggregate` reply to `calls` calls whose results add
/// `out_of_line` bytes beside their slots, as the contract computes it.
pub fn reply_len(calls: usize, out_of_line: usize) -> usize {
    type Slot = Archived<Result<Vec<u8>, CallError>>;
    let slots = out_of_line.next_multiple_of(align_of::<Slot>()) + calls * size_of::<Slot>();
    slots.next_multiple_of(align_of::<ArchivedAggregate>()) + size_of::<ArchivedAggregate>()
}

/// The most bytes a call's result can add to a reply when the call's own
/// reply is at most `max_reply` bytes: that reply, or a panic message.
pub fn worst_out_of_line(max_reply: usize) -> usize {
    max_reply.max(MAX_PANIC_BYTES)
}

/// More than the bytes `call` adds to an `aggregate` argument: its slot,
/// its name and its argument, each padded.
pub fn argument_bytes(fn_name: &str, fn_args: &[u8]) -> usize {
    size_of::<ArchivedCall>() + fn_name.len() + fn_args.len() + 2 * align_of::<ArchivedCall>()
}

/// Splits calls, given by their name and argument lengths and the longest
/// reply each can give, into runs that one `aggregate` takes and answers
/// within its limits whatever each call does: at most [`MAX_CALLS`] calls,
/// and an argument and a reply within [`ARGBUF_LEN`] even if every call
/// fails with the longest panic message. A call that cannot fit with any
/// other gets a run of its own.
pub fn plan(calls: impl IntoIterator<Item = (usize, usize)>) -> Vec<Range<usize>> {
    // The argument's root and the reply's root.
    const ROOT: usize = 16;
    let mut runs = Vec::new();
    let mut start = 0;
    let (mut count, mut argument, mut out_of_line) = (0usize, ROOT, 0usize);
    for (index, (argument_len, max_reply)) in calls.into_iter().enumerate() {
        let fits = count < MAX_CALLS
            && argument + argument_len <= ARGBUF_LEN
            && reply_len(count + 1, out_of_line + worst_out_of_line(max_reply)) <= ARGBUF_LEN;
        if !fits && count > 0 {
            runs.push(start..index);
            start = index;
            (count, argument, out_of_line) = (0, ROOT, 0);
        }
        count += 1;
        argument += argument_len;
        out_of_line += worst_out_of_line(max_reply);
    }
    if count > 0 {
        runs.push(start..start + count);
    }
    runs
}

#[cfg(test)]
mod tests {
    //! The encodings in `tests/fixtures/multicall_vectors.json` come from
    //! the contract's own crate (`multicall/` of `HDauven/dusk-multicall`
    //! at 0009171, rkyv 0.7.46 `size_32`, Rust 1.95.0): a helper that
    //! depends on it by path encodes sample call lists with
    //! `rkyv::to_bytes`, and replies the way the contract's `reply` export
    //! does, into a 64 KiB buffer with 1 KiB of scratch space.

    use serde_json::Value;

    use super::*;
    use crate::{rkyv_deserialize, rkyv_serialize};

    fn vectors() -> Value {
        serde_json::from_str(include_str!("../tests/fixtures/multicall_vectors.json")).unwrap()
    }

    fn bytes(value: &Value) -> Vec<u8> {
        hex::decode(value.as_str().unwrap()).unwrap()
    }

    fn number(value: &Value) -> u64 {
        value
            .as_u64()
            .or_else(|| value.as_str().and_then(|text| text.parse().ok()))
            .unwrap()
    }

    fn call_error(value: &Value) -> CallError {
        match value {
            Value::String(kind) => match kind.as_str() {
                "OutOfGas" => CallError::OutOfGas,
                "DoesNotExist" => CallError::DoesNotExist,
                "Unknown" => CallError::Unknown,
                other => unreachable!("unknown call error {other}"),
            },
            Value::Object(fields) => CallError::Panic(fields["Panic"].as_str().unwrap().to_owned()),
            other => unreachable!("unknown call error {other}"),
        }
    }

    fn call_result(value: &Value) -> Result<Vec<u8>, CallError> {
        match value.get("Ok") {
            Some(reply) => Ok(bytes(reply)),
            None => Err(call_error(&value["Err"])),
        }
    }

    #[test]
    fn archived_layouts_are_the_contracts() {
        let vectors = vectors();
        let layout = &vectors["layout"];
        type Slot = Archived<Result<Vec<u8>, CallError>>;
        assert_eq!(
            size_of::<ArchivedCall>() as u64,
            number(&layout["call_size"])
        );
        assert_eq!(
            align_of::<ArchivedCall>() as u64,
            number(&layout["call_align"])
        );
        assert_eq!(size_of::<Slot>() as u64, number(&layout["slot_size"]));
        assert_eq!(align_of::<Slot>() as u64, number(&layout["slot_align"]));
        assert_eq!(
            size_of::<ArchivedAggregate>() as u64,
            number(&layout["aggregate_size"])
        );
        assert_eq!(
            align_of::<ArchivedAggregate>() as u64,
            number(&layout["aggregate_align"])
        );
        let limits = &vectors["limits"];
        assert_eq!(MAX_CALLS as u64, number(&limits["max_calls"]));
        assert_eq!(ARGBUF_LEN as u64, number(&limits["argbuf_len"]));
        assert_eq!(MAX_PANIC_BYTES as u64, number(&limits["max_panic_bytes"]));
        assert_eq!(INLINE_CAPACITY as u64, number(&limits["inline_capacity"]));
        assert_eq!(u64::from(VERSION), number(&vectors["version"]["value"]));
        let version = bytes(&vectors["version"]["encoded"]);
        assert_eq!(rkyv_serialize(&VERSION).unwrap(), version);
        assert_eq!(rkyv_deserialize::<u32>(&version).unwrap(), VERSION);
    }

    #[test]
    fn call_lists_encode_as_the_contract_reads_them() {
        let vectors = vectors();
        let fixtures = vectors["calls"].as_array().unwrap();
        assert!(fixtures.len() >= 5);
        for fixture in fixtures {
            let calls: Vec<Call> = fixture["calls"]
                .as_array()
                .unwrap()
                .iter()
                .map(|call| Call {
                    contract: bytes(&call["contract"]).try_into().unwrap(),
                    fn_name: call["fn_name"].as_str().unwrap().to_owned(),
                    fn_args: bytes(&call["fn_args"]),
                    gas_limit: number(&call["gas_limit"]),
                })
                .collect();
            let encoded = bytes(&fixture["encoded"]);
            assert_eq!(
                rkyv_serialize(&calls).unwrap(),
                encoded,
                "{}",
                fixture["name"]
            );
            assert_eq!(
                rkyv_deserialize::<Vec<Call>>(&encoded).unwrap(),
                calls,
                "{}",
                fixture["name"]
            );
            let bound: usize = 16
                + calls
                    .iter()
                    .map(|call| argument_bytes(&call.fn_name, &call.fn_args))
                    .sum::<usize>();
            assert!(encoded.len() <= bound, "{}", fixture["name"]);
        }
    }

    #[test]
    fn replies_decode_as_the_contract_writes_them() {
        let vectors = vectors();
        let fixtures = vectors["aggregates"].as_array().unwrap();
        assert!(fixtures.len() >= 4);
        for fixture in fixtures {
            let expected = Aggregate {
                height: number(&fixture["height"]),
                results: fixture["results"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(call_result)
                    .collect(),
            };
            let encoded = bytes(&fixture["encoded"]);
            assert_eq!(
                rkyv_deserialize::<Aggregate>(&encoded).unwrap(),
                expected,
                "{}",
                fixture["name"]
            );
            // The agent's own encoding of a reply, which the tests' nodes
            // send, is the contract's.
            assert_eq!(
                rkyv_serialize(&expected).unwrap(),
                encoded,
                "{}",
                fixture["name"]
            );
            let out_of_line: usize = expected.results.iter().map(out_of_line).sum();
            assert_eq!(
                reply_len(expected.results.len(), out_of_line),
                encoded.len(),
                "{}",
                fixture["name"]
            );
        }
        for fixture in vectors["call_errors"].as_array().unwrap() {
            let error = call_error(&fixture["error"]);
            let encoded = bytes(&fixture["encoded"]);
            assert_eq!(rkyv_serialize(&error).unwrap(), encoded);
            assert_eq!(rkyv_deserialize::<CallError>(&encoded).unwrap(), error);
        }
    }

    #[test]
    fn the_deployed_multicall_takes_and_gives_these_bytes() {
        let testnet: Value =
            serde_json::from_str(include_str!("../tests/fixtures/multicall_testnet.json")).unwrap();
        let version = bytes(&testnet["version"]["reply"]);
        assert_eq!(rkyv_deserialize::<u32>(&version).unwrap(), VERSION);
        // The requests the agent sends: the probe's empty aggregate, and the
        // transfer contract's `chain_id`, which the deployment check reads.
        assert_eq!(
            rkyv_serialize(&Vec::<Call>::new()).unwrap(),
            bytes(&testnet["empty"]["request"])
        );
        let mut transfer = [0u8; 32];
        transfer[0] = 1;
        let chain_id = vec![Call {
            contract: transfer,
            fn_name: "chain_id".into(),
            fn_args: Vec::new(),
            gas_limit: 0,
        }];
        assert_eq!(
            rkyv_serialize(&chain_id).unwrap(),
            bytes(&testnet["chain_id"]["request"])
        );
        // The replies it reads: the call's reply is the single query's.
        assert_eq!(
            rkyv_deserialize::<Aggregate>(&bytes(&testnet["empty"]["reply"])).unwrap(),
            Aggregate {
                height: number(&testnet["empty"]["height"]),
                results: Vec::new(),
            }
        );
        assert_eq!(
            rkyv_deserialize::<Aggregate>(&bytes(&testnet["chain_id"]["reply"])).unwrap(),
            Aggregate {
                height: number(&testnet["chain_id"]["height"]),
                results: vec![Ok(bytes(&testnet["chain_id"]["single_query_reply"]))],
            }
        );
    }

    #[test]
    fn reply_lengths_are_the_contracts() {
        let vectors = vectors();
        for fixture in vectors["reply_len"].as_array().unwrap() {
            let calls = number(&fixture["calls"]) as usize;
            let out_of_line = number(&fixture["out_of_line"]) as usize;
            assert_eq!(
                reply_len(calls, out_of_line) as u64,
                number(&fixture["len"]),
                "{fixture}"
            );
        }
    }

    #[test]
    fn a_reply_that_is_not_an_aggregate_is_refused() {
        let vectors = vectors();
        let encoded = bytes(&vectors["aggregates"][2]["encoded"]);
        // Cut, or with a result's tag out of range.
        assert!(rkyv_deserialize::<Aggregate>(&encoded[..encoded.len() - 1]).is_err());
        let mut tampered = encoded.clone();
        let slots = tampered.len() - size_of::<ArchivedAggregate>() - 12 * 16;
        tampered[slots] = 7;
        assert!(rkyv_deserialize::<Aggregate>(&tampered).is_err());
        assert!(rkyv_deserialize::<Aggregate>(&[]).is_err());
    }

    #[test]
    fn plans_keep_every_aggregate_within_its_limits() {
        // Small reads: a full batch fits even if every call panics.
        let runs = plan(std::iter::repeat_n((80, 8), 600));
        assert_eq!(runs, vec![0..256, 256..512, 512..600]);
        assert!(reply_len(256, 256 * MAX_PANIC_BYTES) <= ARGBUF_LEN);
        // Pages of 256 message IDs: seven fit.
        let page = 8 + 256 * 32;
        let runs = plan(std::iter::repeat_n((80, page), 10));
        assert_eq!(runs, vec![0..7, 7..10]);
        assert!(reply_len(7, 7 * page) <= ARGBUF_LEN);
        assert!(reply_len(8, 8 * page) > ARGBUF_LEN);
        // A read that cannot fit with others gets a run of its own.
        let runs = plan([(80, 8), (80, ARGBUF_LEN), (80, 8), (80, 8)]);
        assert_eq!(runs, vec![0..1, 1..2, 2..4]);
        // Large arguments end a run before the buffer does.
        let runs = plan(std::iter::repeat_n((20_000, 8), 7));
        assert_eq!(runs, vec![0..3, 3..6, 6..7]);
        assert!(plan(std::iter::empty()).is_empty());
    }
}
