//! Properties of what the relayer reads back about its own transactions:
//! where the node says a transaction is, and the records it keeps of the
//! transactions it prepared.

use serde_json::{json, Value as JsonValue};

use super::{found, PreparedDelivery, SentDelivery, TransactionStatus};
use crate::checks::Input;

/// A reply field as a node, a proxy or a cache in front of it might write
/// it: absent, null, a transaction, or something malformed.
fn field(input: &mut Input) -> Option<JsonValue> {
    let entry = match input.u8() % 9 {
        0 => return None,
        1 | 2 => return Some(JsonValue::Null),
        3 => json!({}),
        4 => json!({
            "blockHeight": input.edgy_u64() % 1_000,
            "gasSpent": input.edgy_u64() % 1_000_000,
            "err": JsonValue::Null,
        }),
        5 => json!({
            "blockHeight": input.edgy_u64() % 1_000,
            "gasSpent": 1,
            "err": "Panic",
        }),
        6 => json!({ "gasSpent": 1, "err": JsonValue::Null }),
        7 => return Some(json!(false)),
        _ => return Some(json!([])),
    };
    Some(named(input, entry))
}

/// `entry` naming the transaction looked up (`ab`), in either case,
/// another one, something that is not an ID, or nothing.
fn named(input: &mut Input, mut entry: JsonValue) -> JsonValue {
    let id = match input.u8() % 6 {
        0 => return entry,
        1 => json!(7),
        2 => json!("cd"),
        3 => json!("AB"),
        _ => json!("ab"),
    };
    entry["id"] = id;
    entry
}

/// `found` on any reply to a lookup that asked for `asked`:
/// - "in neither place", which lets the relayer forget a transaction it
///   kept and sign its message again, only when the reply holds every
///   field it asked for, each null: a reply that lacks one, or is not an
///   object at all, is an error, never "neither";
/// - "included" only with a numeric `blockHeight` and a receipt, and in
///   the mempool too exactly when the reply lists it there;
/// - "pending" only with a non-null `mempoolTx` and no ledger entry;
/// - any entry that does not name the transaction looked up is an error.
pub fn lookup_reply(data: &[u8]) {
    let mut input = Input::new(data);
    let asked: &[&str] = match input.u8() % 3 {
        0 => &["mempoolTx", "tx"],
        1 => &["tx"],
        _ => &["mempoolTx"],
    };
    let mut reply = serde_json::Map::new();
    for name in ["mempoolTx", "tx"] {
        if let Some(value) = field(&mut input) {
            reply.insert(name.into(), value);
        }
    }
    let reply = match input.u8() % 10 {
        0 => JsonValue::Null,
        1 => json!("ok"),
        2 => json!([JsonValue::Object(reply)]),
        _ => JsonValue::Object(reply),
    };
    let result = found("ab", &reply, asked);
    let present = |name: &str| reply.get(name).is_some_and(|value| !value.is_null());
    let names_it = |name: &str| {
        reply
            .get(name)
            .and_then(|entry| entry.get("id"))
            .and_then(JsonValue::as_str)
            .is_some_and(|id| id.eq_ignore_ascii_case("ab"))
    };
    match &result {
        Ok(None) => {
            assert!(
                asked
                    .iter()
                    .all(|name| reply.get(name).is_some_and(JsonValue::is_null)),
                "{reply} read as in neither place for {asked:?}"
            );
            assert!(!present("tx") && !present("mempoolTx"), "{reply}");
        }
        Ok(Some(TransactionStatus::Included {
            block_height,
            in_mempool,
            ..
        })) => {
            assert_eq!(reply["tx"]["blockHeight"].as_u64(), Some(*block_height));
            assert!(names_it("tx"), "{reply}");
            assert_eq!(*in_mempool, present("mempoolTx"), "{reply}");
        }
        Ok(Some(TransactionStatus::Pending)) => {
            assert!(!present("tx"));
            assert!(present("mempoolTx") && names_it("mempoolTx"), "{reply}");
        }
        Ok(Some(TransactionStatus::Unknown)) | Err(_) => {}
    }
    if ["mempoolTx", "tx"]
        .iter()
        .any(|name| present(name) && !names_it(name))
    {
        assert!(result.is_err(), "{reply} read for another transaction");
    }
}

/// Kept records: every record reads back as itself; only the three lengths
/// a record has ever had read at all, and a record from before the failure
/// flag reads as reported, so its gas is never charged twice.
pub fn kept_records(data: &[u8]) {
    if let Some(record) = PreparedDelivery::decode(data) {
        assert!(matches!(data.len(), 40 | 41 | 49), "{} bytes", data.len());
        if data.len() == 40 {
            assert!(record.failure_reported && record.nonce.is_none());
        } else {
            assert_eq!(record.encode(), data);
        }
    }
    if let Some(sent) = SentDelivery::decode(data) {
        // A record without a transaction ignores the bytes of its slot.
        let mut canonical = data.to_vec();
        if data[16] == 0 {
            canonical[17..].fill(0);
        }
        assert_eq!(sent.encode(), canonical);
    }
    let mut input = Input::new(data);
    let record = PreparedDelivery {
        tx_id: std::array::from_fn(|_| input.u8()),
        prepared_at: input.edgy_u64(),
        failure_reported: input.bool(),
        nonce: input.bool().then(|| input.edgy_u64()),
    };
    assert_eq!(PreparedDelivery::decode(&record.encode()), Some(record));
    let sent = SentDelivery {
        nonce: input.edgy_u64(),
        tx_id: input.bool().then(|| std::array::from_fn(|_| input.u8())),
        sent_at: input.edgy_u64(),
    };
    assert_eq!(SentDelivery::decode(&sent.encode()), Some(sent));
}

#[cfg(test)]
mod tests {
    use crate::checks::run_cases;

    #[test]
    fn a_lookup_reads_neither_place_only_from_a_reply_that_says_so() {
        run_cases(41, 20_000, 24, super::lookup_reply);
    }

    #[test]
    fn kept_records_read_back_as_themselves() {
        run_cases(42, 20_000, 60, super::kept_records);
        run_cases(43, 5_000, 60, |bytes| {
            for len in [40, 41, 49] {
                let mut exact = bytes.to_vec();
                exact.resize(len, 0);
                super::kept_records(&exact);
            }
        });
    }
}
