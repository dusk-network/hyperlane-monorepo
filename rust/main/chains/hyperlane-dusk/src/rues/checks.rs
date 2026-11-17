//! Properties of how the agent reads the node's replies: block pairs,
//! transaction status, finalized-event pages and cursors, hashes, scan
//! anchors, rkyv contract replies, GraphQL envelopes, and the text it
//! quotes in errors. The node is untrusted here: any reply must be refused
//! or read as what it says, and never panic.

use std::time::Duration;

use hyperlane_dusk_types::merkle::IncrementalMerkle;
use hyperlane_dusk_types::{EthAddress, GasPaymentRecord};
use rkyv::validation::validators::DefaultValidator;
use rkyv::{Archive, Deserialize, Infallible};
use serde_json::{json, Value as JsonValue};

use super::{
    bounded_body, bounded_text, canonical_event_cursor, decode_event_cursor, graphql_data,
    next_tx_lookup, parse_block_heights, parse_block_pair, parse_confirmed_transaction,
    parse_h256_hex, rkyv_deserialize, rkyv_serialize, validate_finalized_event_page,
    FinalizedContractEvent, FinalizedEventPage, ScanAnchor, MAX_ERROR_BODY_BYTES,
    MAX_EVENT_TOPIC_BYTES,
};
use crate::checks::Input;

#[cfg(feature = "fuzzing")]
pub use super::submissions::checks::{kept_records, lookup_reply};

/// A JSON value built from the input: often what a field should hold, else
/// a value of another type or an edge of its own.
fn json_number(input: &mut Input) -> JsonValue {
    match input.u8() % 10 {
        0 => JsonValue::Null,
        1 => json!(-1),
        2 => json!(1.5),
        3 => json!(input.edgy_u64().to_string()),
        4 => json!([input.edgy_u64()]),
        5 => json!(u64::MAX),
        _ => json!(input.edgy_u64() % 1_000_000),
    }
}

/// A hash as the node writes one, or something close to one.
fn json_hash(input: &mut Input) -> (JsonValue, Option<[u8; 32]>) {
    let hash: [u8; 32] = std::array::from_fn(|_| input.u8());
    let text = hex::encode(hash);
    match input.u8() % 9 {
        0 => (JsonValue::Null, None),
        1 => (json!(7), None),
        2 => (json!(&text[1..]), None),
        3 => (json!(format!("{text}0")), None),
        4 => (json!(format!("{}g", &text[1..])), None),
        5 => (json!(format!("0x{text}")), Some(hash)),
        6 => (
            json!(format!("0X{}", text.to_ascii_uppercase())),
            Some(hash),
        ),
        _ => (json!(text), Some(hash)),
    }
}

/// A `[height, hash]` block, or a malformed one.
fn json_block(input: &mut Input) -> (JsonValue, Option<u64>, Option<[u8; 32]>) {
    let height = json_number(input);
    let (hash, parsed_hash) = json_hash(input);
    let parsed_height = height.as_u64();
    match input.u8() % 8 {
        0 => (JsonValue::Null, None, None),
        1 => (json!([]), None, None),
        2 => (json!([height]), parsed_height, None),
        3 => (json!({ "height": height, "hash": hash }), None, None),
        4 => (json!([height, hash, "extra"]), parsed_height, parsed_hash),
        _ => (json!([height, hash]), parsed_height, parsed_hash),
    }
}

/// `parse_block_heights` and `parse_block_pair` on any `lastBlockPair`
/// reply: they succeed exactly when both blocks carry a u64 height and the
/// final one is at or below the tip (and, for the pair, both carry a valid
/// hash), and then return exactly those values.
pub fn block_pair(data: &[u8]) {
    let mut input = Input::new(data);
    let (latest, latest_height, latest_hash) = json_block(&mut input);
    let (finalized, finalized_height, finalized_hash) = json_block(&mut input);
    let mut pair = json!({ "last_block": latest, "last_finalized_block": finalized });
    if input.u8() % 16 == 0 {
        pair = json!([pair]);
    }
    let reply = match input.u8() % 16 {
        0 => json!({ "lastBlockPair": pair }),
        1 => json!({}),
        _ => json!({ "lastBlockPair": { "json": pair } }),
    };
    let heights = match (latest_height, finalized_height) {
        (Some(latest), Some(finalized))
            if pair.is_object() && reply["lastBlockPair"]["json"].is_object() =>
        {
            (finalized <= latest).then_some((latest, finalized))
        }
        _ => None,
    };
    assert_eq!(parse_block_heights(&reply).ok(), heights, "{reply}");
    match parse_block_pair(&reply) {
        Ok(parsed) => {
            let (latest, finalized) = heights.unwrap_or_default();
            assert_eq!(
                parsed.tip(),
                (
                    latest,
                    hyperlane_core::H256(latest_hash.unwrap_or_default())
                )
            );
            assert_eq!(parsed.finalized(), finalized);
            assert!(heights.is_some() && latest_hash.is_some() && finalized_hash.is_some());
            assert_eq!(parsed.finalized_hash.0, finalized_hash.unwrap_or_default());
        }
        Err(_) => assert!(heights.is_none() || latest_hash.is_none() || finalized_hash.is_none()),
    }
    // Any JSON at all.
    if let Ok(value) = serde_json::from_slice::<JsonValue>(data) {
        if let Ok((latest, finalized)) = parse_block_heights(&value) {
            assert!(finalized <= latest);
        }
        let _ = parse_block_pair(&value);
    }
}

/// `parse_confirmed_transaction` reads the gas a transaction spent and its
/// error exactly: a u64 `gasSpent`, and an `err` that is null (success) or
/// a string. Anything else, or a missing field, is refused.
pub fn confirmed_transaction(data: &[u8]) {
    let mut input = Input::new(data);
    let gas = json_number(&mut input);
    let (err, expected_err) = match input.u8() % 6 {
        0 => (None, None),
        1 => (Some(json!(3)), None),
        2 => (Some(json!(["x"])), None),
        3 => (Some(JsonValue::Null), Some(None)),
        _ => {
            let text = input.text(8, &["Out of gas", "Panic"]);
            (Some(json!(text.clone())), Some(Some(text)))
        }
    };
    let mut transaction = json!({ "gasSpent": gas });
    if let Some(err) = err {
        transaction["err"] = err;
    }
    let expected = gas.as_u64().zip(expected_err);
    match parse_confirmed_transaction("ab", &transaction) {
        Ok(confirmed) => {
            assert_eq!(Some((confirmed.gas_spent, confirmed.error)), expected);
        }
        Err(_) => assert!(expected.is_none(), "{transaction}"),
    }
}

/// Event cursors: every non-negative ID round-trips through its canonical
/// cursor, negative ones are refused, and any text that decodes is the
/// canonical cursor of the ID it decodes to.
pub fn event_cursor(data: &[u8]) {
    let mut input = Input::new(data);
    let id = match input.u8() % 6 {
        0 => 0,
        1 => i64::MAX,
        2 => i64::MIN,
        3 => -1,
        _ => input.u64() as i64,
    };
    let cursor = canonical_event_cursor(id);
    match decode_event_cursor(&cursor) {
        Ok(decoded) => assert_eq!(decoded, id),
        Err(_) => assert!(id < 0),
    }
    let text = String::from_utf8_lossy(input.bytes(64)).into_owned();
    if let Ok(decoded) = decode_event_cursor(&text) {
        assert!(decoded >= 0);
        assert_eq!(canonical_event_cursor(decoded), text);
    }
}

fn event(input: &mut Input, id: i64, source: &str) -> FinalizedContractEvent {
    let topic = match input.u8() % 12 {
        0 => String::new(),
        1 => "t".repeat(MAX_EVENT_TOPIC_BYTES),
        2 => "t".repeat(MAX_EVENT_TOPIC_BYTES + 1),
        _ => "dispatch_id".to_owned(),
    };
    FinalizedContractEvent {
        id,
        block_height: input.u64() % 1_000,
        block_hash: hex::encode([input.u8(); 32]),
        origin: hex::encode([input.u8(); 32]),
        topic,
        source: source.to_owned(),
        data: hex::encode(input.bytes(8)),
        reverted: input.u8() % 8 == 0,
    }
}

/// `validate_finalized_event_page` accepts a page only when it is
/// consistent with the request: the cursor names the last row read, the
/// page holds at most the rows asked for, its cursors name its own first
/// and last rows and move on, every row is the queried contract's with a
/// topic of 1 to 256 bytes, and row IDs only grow, past the last one read.
/// A page built that way is accepted.
pub fn event_page(data: &[u8]) {
    let mut input = Input::new(data);
    let contract: [u8; 32] = std::array::from_fn(|_| input.u8() % 4);
    let source = hex::encode(contract);
    let previous_id = match input.u8() % 4 {
        0 => None,
        1 => Some(-1),
        _ => Some(i64::from(input.u16())),
    };
    let requested_cursor = match input.u8() % 6 {
        0 => None,
        1 => Some("not a cursor".to_owned()),
        2 => Some(canonical_event_cursor(i64::from(input.u16()))),
        _ => previous_id.map(canonical_event_cursor),
    };
    let requested_limit = input.below(8);
    let count = input.below(10);
    let mut id = previous_id.unwrap_or(-1);
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        id = match input.u8() % 10 {
            0 => id,
            1 => id - 1,
            2 => -5,
            _ => id + 1 + i64::from(input.u8() % 3),
        };
        let row_source = match input.u8() % 10 {
            0 => hex::encode([9u8; 32]),
            1 => format!("0x{}", source.to_ascii_uppercase()),
            2 => format!("0X{source}"),
            _ => source.clone(),
        };
        events.push(event(&mut input, id, &row_source));
    }
    let cursor_of = |input: &mut Input, row: Option<&FinalizedContractEvent>| match input.u8() % 8 {
        0 => None,
        1 => Some("bogus".to_owned()),
        2 => Some(canonical_event_cursor(i64::from(input.u16()))),
        _ => row.map(|row| canonical_event_cursor(row.id)),
    };
    let start_cursor = cursor_of(&mut input, events.first());
    let end_cursor = cursor_of(&mut input, events.last());
    let page = FinalizedEventPage {
        events,
        start_cursor,
        end_cursor,
        has_next_page: input.u8() % 4 == 0,
    };
    let accepted = validate_finalized_event_page(
        &contract,
        requested_cursor.as_deref(),
        previous_id,
        requested_limit,
        &page,
    )
    .is_ok();
    let cursor_matches = match (&requested_cursor, previous_id) {
        (None, None) => true,
        (Some(cursor), Some(id)) => decode_event_cursor(cursor).ok() == Some(id),
        _ => false,
    };
    let rows_ok = page.events.iter().enumerate().all(|(i, row)| {
        let after = if i == 0 {
            previous_id
        } else {
            Some(page.events[i - 1].id)
        };
        row.id >= 0
            && after.is_none_or(|after| row.id > after)
            && !row.topic.is_empty()
            && row.topic.len() <= MAX_EVENT_TOPIC_BYTES
            && row
                .source
                .strip_prefix("0x")
                .or_else(|| row.source.strip_prefix("0X"))
                .unwrap_or(&row.source)
                .eq_ignore_ascii_case(&source)
    });
    let shape_ok = match (page.events.first(), page.events.last()) {
        (None, _) | (_, None) => {
            !page.has_next_page && page.start_cursor.is_none() && page.end_cursor.is_none()
        }
        (Some(first), Some(last)) => {
            page.start_cursor.as_deref() == Some(canonical_event_cursor(first.id).as_str())
                && page.end_cursor.as_deref() == Some(canonical_event_cursor(last.id).as_str())
                && requested_cursor != page.end_cursor
        }
    };
    let consistent = cursor_matches && page.events.len() <= requested_limit && rows_ok && shape_ok;
    assert_eq!(
        accepted, consistent,
        "{page:?} after {previous_id:?} limit {requested_limit}"
    );
}

/// `parse_h256_hex` takes exactly 32 bytes of hex after an optional `0x`.
pub fn h256_hex(data: &[u8]) {
    let text = String::from_utf8_lossy(data);
    let digits = text
        .strip_prefix("0x")
        .or_else(|| text.strip_prefix("0X"))
        .unwrap_or(&text);
    let valid = digits.len() == 64 && digits.bytes().all(|byte| byte.is_ascii_hexdigit());
    match parse_h256_hex(&text, "field") {
        Ok(hash) => {
            assert!(valid);
            assert_eq!(hex::encode(hash.0), digits.to_ascii_lowercase());
        }
        Err(_) => assert!(!valid),
    }
}

/// What the agent quotes of node text in errors and logs is a prefix of it,
/// cut on a character boundary, at most `MAX_ERROR_BODY_BYTES` of it, with
/// the original length named when anything was cut.
pub fn bounded(data: &[u8]) {
    let suffix = |len: usize| format!("... [truncated, {len} bytes]");
    let text = String::from_utf8_lossy(data).into_owned();
    let shown = bounded_text(&text);
    if text.len() <= MAX_ERROR_BODY_BYTES {
        assert_eq!(shown, text);
    } else {
        let kept = shown.strip_suffix(&suffix(text.len())).unwrap_or_default();
        assert!(text.starts_with(kept) && !kept.is_empty() || MAX_ERROR_BODY_BYTES < 4);
        assert!(kept.len() <= MAX_ERROR_BODY_BYTES && kept.len() + 4 > MAX_ERROR_BODY_BYTES);
    }
    // Raw bodies: at most the first MAX_ERROR_BODY_BYTES bytes, decoded.
    let shown = bounded_body(data);
    let head = String::from_utf8_lossy(&data[..data.len().min(MAX_ERROR_BODY_BYTES)]).into_owned();
    if data.len() <= MAX_ERROR_BODY_BYTES && head.len() <= MAX_ERROR_BODY_BYTES {
        assert_eq!(shown, head);
    } else {
        let kept = shown.strip_suffix(&suffix(data.len())).unwrap_or("\u{0}");
        assert!(head.starts_with(kept), "{shown:?}");
        assert!(kept.len() <= MAX_ERROR_BODY_BYTES);
    }
}

/// Transaction lookups go at least every block and wait at most a block;
/// the first wait ends one block after sending; and a 12-block wait takes
/// a bounded number of lookups, whatever the block time.
pub fn lookup_cadence(data: &[u8]) {
    let mut input = Input::new(data);
    let block = Duration::from_millis(1 + u64::from(input.u32() % 600_000));
    let waited = Duration::from_millis(input.u64() % 100_000_000);
    let next = next_tx_lookup(waited, block);
    assert!(next > Duration::ZERO || block < Duration::from_nanos(4));
    assert!(next <= block);
    if waited < block {
        assert_eq!(next, block - waited);
    }
    let mut at = Duration::ZERO;
    let mut lookups = 0;
    while at < block * 12 {
        at += next_tx_lookup(at, block);
        lookups += 1;
    }
    assert!(lookups <= 20, "{lookups} lookups for a block of {block:?}");
}

/// A GraphQL reply is its data unless it carries errors: a non-empty
/// error list, or any non-null `errors` that is not a list.
pub fn graphql(data: &[u8]) {
    let mut input = Input::new(data);
    let errors = match input.u8() % 7 {
        0 => None,
        1 => Some(JsonValue::Null),
        2 => Some(json!([])),
        3 => Some(json!([{ "message": "boom" }])),
        4 => Some(json!({})),
        5 => Some(json!("")),
        _ => Some(json!(0)),
    };
    let body = json!({ "tx": { "gasSpent": input.u64() } });
    let mut reply = match input.u8() % 3 {
        0 => body.clone(),
        _ => json!({ "data": body.clone() }),
    };
    if let Some(errors) = errors.clone() {
        reply["errors"] = errors;
    }
    let has_errors = match &errors {
        None | Some(JsonValue::Null) => false,
        Some(JsonValue::Array(list)) => !list.is_empty(),
        Some(_) => true,
    };
    match graphql_data(reply.clone()) {
        Ok(data) => {
            assert!(!has_errors);
            assert_eq!(data["tx"], body["tx"]);
        }
        Err(_) => assert!(has_errors, "{reply}"),
    }
}

/// Scan anchors decode only from exactly their encoding, and only when the
/// fence row is a real row (ID at least 0, a rank) below the position.
pub fn scan_anchor(data: &[u8]) {
    if let Some(anchor) = ScanAnchor::decode(data) {
        assert_eq!(anchor.encode(), data);
        assert!(anchor.fence_id >= 0 && anchor.rank > 0);
        assert!(anchor.fence_block < anchor.before_block);
    }
    let mut input = Input::new(data);
    let anchor = ScanAnchor {
        before_block: input.edgy_u64(),
        fence_id: input.u64() as i64,
        fence_digest: std::array::from_fn(|_| input.u8()),
        rank: input.edgy_u64(),
        fence_block: input.edgy_u64(),
        fence_block_hash: std::array::from_fn(|_| input.u8()),
    };
    let readable =
        anchor.fence_id >= 0 && anchor.rank > 0 && anchor.fence_block < anchor.before_block;
    assert_eq!(
        ScanAnchor::decode(&anchor.encode()),
        readable.then_some(anchor)
    );
}

fn reply<T>(bytes: &[u8])
where
    T: Archive
        + rkyv::Serialize<rkyv::ser::serializers::AllocSerializer<256>>
        + PartialEq
        + std::fmt::Debug,
    T::Archived: Deserialize<T, Infallible> + for<'b> rkyv::CheckBytes<DefaultValidator<'b>>,
{
    // Replies arrive as owned buffers; copy into one, as the agent has.
    let owned = bytes.to_vec();
    if let Ok(value) = rkyv_deserialize::<T>(&owned) {
        let again = rkyv_serialize(&value).unwrap_or_default();
        assert_eq!(rkyv_deserialize::<T>(&again).ok(), Some(value));
    }
}

/// Contract replies of every type the agent reads: any bytes are refused or
/// read as a value that serializes and reads back the same.
pub fn contract_reply(data: &[u8]) {
    let mut input = Input::new(data);
    let choice = input.u8();
    let bytes = input.bytes(4_096);
    match choice % 12 {
        0 => reply::<u32>(bytes),
        1 => reply::<u64>(bytes),
        2 => reply::<bool>(bytes),
        3 => reply::<[u8; 32]>(bytes),
        4 => reply::<Vec<[u8; 32]>>(bytes),
        5 => reply::<Vec<u64>>(bytes),
        6 => reply::<(Vec<EthAddress>, u8)>(bytes),
        7 => reply::<Option<[u8; 32]>>(bytes),
        8 => reply::<GasPaymentRecord>(bytes),
        9 => reply::<Vec<GasPaymentRecord>>(bytes),
        10 => reply::<IncrementalMerkle>(bytes),
        _ => reply::<([u8; 32], u32)>(bytes),
    }
}

#[cfg(test)]
mod tests {
    use crate::checks::run_cases;

    #[test]
    fn block_pairs_are_read_exactly_or_refused() {
        run_cases(21, 20_000, 120, super::block_pair);
    }

    #[test]
    fn transaction_status_is_read_exactly_or_refused() {
        run_cases(22, 10_000, 40, super::confirmed_transaction);
    }

    #[test]
    fn event_cursors_are_canonical() {
        run_cases(23, 20_000, 80, super::event_cursor);
    }

    #[test]
    fn event_pages_are_accepted_exactly_when_consistent() {
        run_cases(24, 40_000, 200, super::event_page);
    }

    #[test]
    fn hashes_are_exactly_32_bytes_of_hex() {
        run_cases(25, 5_000, 70, super::h256_hex);
    }

    #[test]
    fn quoted_node_text_is_bounded() {
        run_cases(26, 2_000, 3 * super::MAX_ERROR_BODY_BYTES, super::bounded);
        super::bounded("é".repeat(super::MAX_ERROR_BODY_BYTES).as_bytes());
        super::bounded(&[0xff; 2 * super::MAX_ERROR_BODY_BYTES]);
    }

    #[test]
    fn transaction_lookups_keep_their_cadence() {
        run_cases(27, 20_000, 16, super::lookup_cadence);
    }

    #[test]
    fn graphql_errors_are_never_read_as_data() {
        run_cases(28, 5_000, 16, super::graphql);
    }

    #[test]
    fn scan_anchors_decode_only_from_their_encoding() {
        run_cases(29, 20_000, 100, super::scan_anchor);
        run_cases(30, 2_000, 120, |bytes| {
            let mut exact = bytes.to_vec();
            exact.resize(super::ScanAnchor::ENCODED_LEN, 0);
            super::scan_anchor(&exact);
        });
    }

    #[test]
    fn contract_replies_are_refused_or_read_back_the_same() {
        run_cases(31, 20_000, 300, super::contract_reply);
    }
}
