//! Differential tests: the Dusk shared types (`hyperlane-dusk-types`, at the
//! commit Cargo.toml pins), which the Dusk contracts run and the agents use
//! off chain, against
//! - hyperlane-core, the Rust mirror of the Solidity contracts, on
//!   pseudo-random inputs, and
//! - what Hyperlane's Solidity libraries computed
//!   (`tests/fixtures/solidity_vectors.jsonl`, written by
//!   `tests/fixtures/solidity_vectors/DuskVectors.s.sol`).
//!
//! Covered: message bytes and IDs, checkpoint and announcement digests,
//! Merkle roots, branches and proofs (across powers of two), token message
//! amounts at the u64/u128/u256 edges with every kind of recipient, and the
//! MessageIdMultisig metadata layout.

use hyperlane_core::accumulator::incremental::IncrementalMerkle as CoreTree;
use hyperlane_core::accumulator::merkle::{verify_merkle_proof, MerkleTree};
use hyperlane_core::accumulator::{TREE_DEPTH, ZERO_HASHES};
use hyperlane_core::utils::{announcement_domain_hash, domain_hash};
use hyperlane_core::{
    Announcement, Checkpoint, CheckpointWithMessageId, Decode, Encode, HyperlaneMessage, Signable,
    H160, H256, U256,
};
use hyperlane_dusk_types::checkpoint as dusk_checkpoint;
use hyperlane_dusk_types::merkle::IncrementalMerkle as DuskTree;
use hyperlane_dusk_types::message as dusk_message;
use hyperlane_dusk_types::message::keccak256;
use hyperlane_dusk_types::token_message as dusk_token;
use serde_json::Value;

/// Deterministic generator (splitmix64), so a failure names its case.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    fn u32(&mut self) -> u32 {
        (self.next() >> 32) as u32
    }

    fn bytes32(&mut self) -> [u8; 32] {
        let mut out = [0u8; 32];
        for chunk in out.chunks_mut(8) {
            chunk.copy_from_slice(&self.next().to_le_bytes());
        }
        out
    }

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }

    /// A length, often right at an edge of the keccak rate or a word.
    fn length(&mut self, max: u64) -> usize {
        const EDGES: [usize; 9] = [0, 1, 31, 32, 33, 135, 136, 137, 272];
        if self.below(3) == 0 {
            EDGES[self.below(EDGES.len() as u64) as usize]
        } else {
            self.below(max) as usize
        }
    }
}

fn core_message(message: &dusk_message::HyperlaneMessage) -> HyperlaneMessage {
    HyperlaneMessage {
        version: message.version,
        nonce: message.nonce,
        origin: message.origin,
        sender: H256(message.sender),
        destination: message.destination,
        recipient: H256(message.recipient),
        body: message.body.clone(),
    }
}

fn random_message(rng: &mut Rng) -> dusk_message::HyperlaneMessage {
    let length = rng.length(400);
    dusk_message::HyperlaneMessage {
        version: if rng.below(8) == 0 {
            rng.next() as u8
        } else {
            hyperlane_dusk_types::VERSION
        },
        nonce: rng.u32(),
        origin: rng.u32(),
        sender: rng.bytes32(),
        destination: rng.u32(),
        recipient: rng.bytes32(),
        body: rng.bytes(length),
    }
}

#[test]
fn messages_encode_decode_and_hash_like_hyperlane_core() {
    let mut rng = Rng(1);
    for case in 0..4_000 {
        let dusk = random_message(&mut rng);
        let core = core_message(&dusk);
        let bytes = dusk.encode();
        assert_eq!(bytes, core.to_vec(), "case {case}");
        assert_eq!(dusk.id(), core.id().0, "case {case}");
        assert_eq!(dusk_message::id(&bytes), core.id().0, "case {case}");

        let read = HyperlaneMessage::read_from(&mut bytes.as_slice()).unwrap();
        assert_eq!(read, core, "case {case}");
        match dusk_message::decode(&bytes) {
            Some(decoded) => {
                assert_eq!(decoded.version, hyperlane_dusk_types::VERSION);
                assert_eq!(decoded, dusk, "case {case}");
            }
            None => assert_ne!(dusk.version, hyperlane_dusk_types::VERSION, "case {case}"),
        }
        assert_eq!(dusk_message::nonce(&bytes), core.nonce);
        assert_eq!(dusk_message::origin(&bytes), core.origin);
        assert_eq!(dusk_message::sender(&bytes), core.sender.0);
        assert_eq!(dusk_message::destination(&bytes), core.destination);
        assert_eq!(dusk_message::recipient(&bytes), core.recipient.0);
        assert_eq!(dusk_message::body(&bytes), core.body.as_slice());
    }
}

#[test]
fn arbitrary_bytes_decode_like_hyperlane_core() {
    let mut rng = Rng(2);
    for case in 0..20_000 {
        let length = rng.below(160) as usize;
        let mut bytes = rng.bytes(length);
        if !bytes.is_empty() && rng.below(2) == 0 {
            bytes[0] = hyperlane_dusk_types::VERSION;
        }
        let core = HyperlaneMessage::read_from(&mut bytes.as_slice());
        match dusk_message::decode(&bytes) {
            Some(dusk) => {
                let core = core.as_ref().unwrap();
                assert_eq!(&core_message(&dusk), core, "case {case}");
            }
            // hyperlane-core reads any version; the Dusk Mailbox only its own.
            None => assert!(
                core.is_err() || bytes[0] != hyperlane_dusk_types::VERSION,
                "case {case}"
            ),
        }
        assert_eq!(core.is_ok(), length >= dusk_message::MIN_MESSAGE_LENGTH);
    }
}

#[test]
fn checkpoint_digests_match_hyperlane_core() {
    let mut rng = Rng(3);
    for case in 0..4_000 {
        let origin = match rng.below(4) {
            0 => 0,
            1 => u32::MAX,
            _ => rng.u32(),
        };
        let index = match rng.below(4) {
            0 => 0,
            1 => u32::MAX,
            _ => rng.u32(),
        };
        let hook = rng.bytes32();
        let root = rng.bytes32();
        let message_id = rng.bytes32();
        let core = CheckpointWithMessageId {
            checkpoint: Checkpoint {
                merkle_tree_hook_address: H256(hook),
                mailbox_domain: origin,
                root: H256(root),
                index,
            },
            message_id: H256(message_id),
        };
        assert_eq!(
            dusk_checkpoint::domain_hash(origin, &hook),
            domain_hash(H256(hook), origin).0,
            "case {case}"
        );
        assert_eq!(
            dusk_checkpoint::checkpoint_digest(origin, &hook, &root, index, &message_id),
            core.eth_signed_message_hash().0,
            "case {case}"
        );
        assert_eq!(
            dusk_checkpoint::eth_signed_message_hash(&core.signing_hash().0),
            core.eth_signed_message_hash().0,
            "case {case}"
        );
    }
}

#[test]
fn announcement_digests_match_hyperlane_core() {
    let mut rng = Rng(4);
    let fixed = [
        "",
        "s3://hyperlane-testnet/us-east-1/validator-0",
        "file:///var/lib/hyperlane/checkpoints",
        "gs://bücket/✓",
    ];
    for case in 0..2_000 {
        let domain = rng.u32();
        let mailbox = rng.bytes32();
        let location = if case < fixed.len() {
            fixed[case].to_string()
        } else {
            let length = rng.length(300);
            (0..length)
                .map(|_| char::from(0x20 + rng.below(0x5f) as u8))
                .collect()
        };
        let core = Announcement {
            validator: H160::from_low_u64_be(rng.next()),
            mailbox_address: H256(mailbox),
            mailbox_domain: domain,
            storage_location: location.clone(),
        };
        assert_eq!(
            dusk_checkpoint::announcement_domain_hash(domain, &mailbox),
            announcement_domain_hash(H256(mailbox), domain).0,
            "case {case}"
        );
        assert_eq!(
            dusk_checkpoint::announcement_digest(domain, &mailbox, &location),
            core.eth_signed_message_hash().0,
            "case {case}"
        );
    }
}

fn leaf(i: u64) -> [u8; 32] {
    keccak256(&i.to_be_bytes())
}

#[test]
fn merkle_roots_match_at_every_count() {
    let mut rng = Rng(5);
    let mut dusk = DuskTree::new();
    let mut core = CoreTree::default();
    assert_eq!(dusk.root(), core.root().0);
    assert_eq!(dusk.latest_checkpoint(), None);
    for count in 1..=2_100u32 {
        // Arbitrary leaves, including repeated and zero ones.
        let leaf = match rng.below(10) {
            0 => [0u8; 32],
            1 => dusk.branch[0],
            _ => rng.bytes32(),
        };
        dusk.insert(leaf);
        core.ingest(H256(leaf));
        assert_eq!(dusk.count, count);
        assert_eq!(core.count(), count as usize);
        assert_eq!(dusk.root(), core.root().0, "count {count}");
        assert_eq!(
            dusk.latest_checkpoint(),
            Some((core.root().0, core.index())),
            "count {count}"
        );
    }
}

#[test]
fn merkle_roots_match_on_each_side_of_powers_of_two() {
    let mut dusk = DuskTree::new();
    let mut core = CoreTree::default();
    let mut checked = 0;
    for i in 0..(1u64 << 16) + 1 {
        dusk.insert(leaf(i));
        core.ingest(H256(leaf(i)));
        let count = i + 1;
        let boundary = (0..=16).any(|k| {
            let power = 1u64 << k;
            count + 1 == power || count == power || count == power + 1
        });
        if boundary {
            assert_eq!(dusk.root(), core.root().0, "count {count}");
            checked += 1;
        }
    }
    assert!(checked >= 45);
}

/// The slots of the branch that hold a subtree, those whose bit is set in
/// the count, agree with hyperlane-core's; a core tree started from zeroed
/// slots has the hook's whole branch (docs/storage.md, "MerkleTreeHook").
#[test]
fn merkle_branches_agree_where_they_hold_subtrees() {
    let mut rng = Rng(6);
    let mut dusk = DuskTree::new();
    let mut core = CoreTree::default();
    let mut zeroed = CoreTree::new([H256::zero(); TREE_DEPTH], 0);
    for count in 1..=600usize {
        let leaf = rng.bytes32();
        dusk.insert(leaf);
        core.ingest(H256(leaf));
        zeroed.ingest(H256(leaf));
        for level in 0..TREE_DEPTH {
            if count >> level & 1 == 1 {
                assert_eq!(dusk.branch[level], core.branch()[level].0, "count {count}");
            }
            assert_eq!(
                dusk.branch[level],
                zeroed.branch()[level].0,
                "count {count}"
            );
        }
        assert_eq!(zeroed.root(), core.root());
    }
}

/// Proofs hyperlane-core makes, against the current root and against an
/// earlier checkpoint's, verify against the roots the hook's tree computes.
#[test]
fn merkle_proofs_verify_against_the_hooks_roots() {
    let mut rng = Rng(7);
    for case in 0..24 {
        let count = match case {
            0 => 1,
            1 => 2,
            2 => 3,
            3 => 64,
            4 => 65,
            5 => 255,
            6 => 256,
            7 => 257,
            _ => 1 + rng.below(300) as usize,
        };
        let leaves: Vec<[u8; 32]> = (0..count).map(|_| rng.bytes32()).collect();
        let mut roots = Vec::with_capacity(count);
        let mut dusk = DuskTree::new();
        for leaf in &leaves {
            dusk.insert(*leaf);
            roots.push(dusk.root());
        }
        let core_leaves: Vec<H256> = leaves.iter().map(|leaf| H256(*leaf)).collect();
        let tree = MerkleTree::create(&core_leaves, TREE_DEPTH);
        assert_eq!(tree.hash().0, dusk.root(), "case {case}");
        for _ in 0..8 {
            let index = rng.below(count as u64) as usize;
            let (proven, path) = tree.generate_proof(index, TREE_DEPTH);
            assert_eq!(proven.0, leaves[index]);
            assert!(
                verify_merkle_proof(proven, &path, TREE_DEPTH, index, H256(dusk.root())),
                "case {case} index {index}"
            );
            let mut branch = [H256::zero(); TREE_DEPTH];
            branch.copy_from_slice(&path);
            assert_eq!(
                CoreTree::branch_root(proven, branch, index).0,
                dusk.root(),
                "case {case} index {index}"
            );
            // Against an earlier checkpoint: the root at `root_index + 1`.
            let root_index = index + rng.below((count - index) as u64) as usize;
            let proof = tree.prove_against_previous(index, root_index);
            assert_eq!(proof.leaf.0, leaves[index]);
            assert_eq!(proof.root().0, roots[root_index], "case {case}");
        }
    }
}

/// The uint256 amount Solidity's TokenMessage writes, big endian.
fn amount_word(amount: U256) -> [u8; 32] {
    let mut word = [0u8; 32];
    amount.to_big_endian(&mut word);
    word
}

#[test]
fn token_messages_match_uint256_encoding_at_every_edge() {
    let mut rng = Rng(8);
    let max64 = U256::from(u64::MAX);
    let max128 = U256::from(u128::MAX);
    let edges = [
        U256::zero(),
        U256::one(),
        max64 - 1,
        max64,
        max64 + 1,
        max128,
        max128 + 1,
        U256::one() << 255,
        U256::MAX,
        U256::from(10u64).pow(U256::from(18u64)),
    ];
    for case in 0..3_000 {
        let amount = if case < edges.len() * 5 {
            edges[case % edges.len()]
        } else {
            match rng.below(3) {
                0 => U256::from(rng.next()),
                1 => U256::from(rng.next()) << (64 + rng.below(64) as usize),
                _ => U256::from_big_endian(&rng.bytes32()),
            }
        };
        let recipient: [u8; 32] = match case % 5 {
            0 => [0u8; 32],
            // An EVM address, left-padded.
            1 => H256::from(H160::from_low_u64_be(rng.next())).0,
            // The same, right-padded (security.md, LOW-5).
            2 => {
                let mut padded = [0u8; 32];
                padded[..20].copy_from_slice(H160::from_low_u64_be(rng.next()).as_bytes());
                padded
            }
            3 => [0xff; 32],
            // A Dusk contract ID or the keccak256 of a BLS key.
            _ => rng.bytes32(),
        };
        let metadata_length = rng.length(80);
        let metadata = rng.bytes(metadata_length);
        let solidity = [&recipient[..], &amount_word(amount), &metadata].concat();
        match u64::try_from(amount) {
            Ok(fits) => {
                assert_eq!(
                    dusk_token::encode_with_metadata(recipient, fits, &metadata),
                    solidity,
                    "case {case}"
                );
                if metadata.is_empty() {
                    assert_eq!(dusk_token::encode(recipient, fits), solidity);
                }
                let decoded = dusk_token::decode(&solidity).unwrap();
                assert_eq!(decoded.recipient, recipient);
                assert_eq!(decoded.amount, fits);
                assert_eq!(decoded.metadata, metadata.as_slice());
            }
            // Refused, never truncated (security.md, "Synthetic amounts are u64").
            Err(_) => assert!(dusk_token::decode(&solidity).is_none(), "case {case}"),
        }
    }
}

fn hex_bytes(value: &Value, field: &str) -> Vec<u8> {
    let text = value[field].as_str().unwrap();
    hex::decode(text.strip_prefix("0x").unwrap()).unwrap()
}

fn hex32(value: &Value, field: &str) -> [u8; 32] {
    hex_bytes(value, field).try_into().unwrap()
}

fn number(value: &Value, field: &str) -> u64 {
    value[field].as_u64().unwrap()
}

/// The MessageIdMultisig ISM's metadata rules (contracts/ism-multisig,
/// `verify`): at least 68 bytes, then whole 65-byte signatures only.
fn dusk_ism_signature_count(metadata: &[u8]) -> Option<usize> {
    let signatures = metadata.len().checked_sub(68)?;
    (signatures % 65 == 0).then_some(signatures / 65)
}

#[test]
fn solidity_libraries_compute_what_the_dusk_types_compute() {
    let vectors = include_str!("fixtures/solidity_vectors.jsonl");
    let mut seen = std::collections::BTreeMap::<String, usize>::new();
    let mut merkle_roots = std::collections::BTreeMap::<u64, [u8; 32]>::new();
    for line in vectors.lines() {
        let value: Value = serde_json::from_str(line).unwrap();
        let kind = value["kind"].as_str().unwrap().to_string();
        match kind.as_str() {
            "message" => {
                let version = number(&value, "version") as u8;
                let sender = hex32(&value, "sender");
                let recipient = hex32(&value, "recipient");
                let body = hex_bytes(&value, "body");
                let encoded = hex_bytes(&value, "encoded");
                let id = hex32(&value, "id");
                let nonce = number(&value, "nonce") as u32;
                let origin = number(&value, "origin") as u32;
                let destination = number(&value, "destination") as u32;
                let dusk = dusk_message::encode(
                    version,
                    nonce,
                    origin,
                    sender,
                    destination,
                    recipient,
                    &body,
                );
                assert_eq!(dusk, encoded, "{line}");
                assert_eq!(dusk_message::id(&encoded), id, "{line}");
                match dusk_message::decode(&encoded) {
                    Some(message) => {
                        assert_eq!(version, hyperlane_dusk_types::VERSION);
                        assert_eq!(message.id(), id);
                        assert_eq!(
                            (message.nonce, message.origin, message.destination),
                            (nonce, origin, destination)
                        );
                        assert_eq!(message.body, body);
                    }
                    None => assert_ne!(version, hyperlane_dusk_types::VERSION),
                }
            }
            "token_message" => {
                let recipient = hex32(&value, "recipient");
                let amount = U256::from_big_endian(&hex32(&value, "amount"));
                let metadata = hex_bytes(&value, "metadata");
                let encoded = hex_bytes(&value, "encoded");
                match u64::try_from(amount) {
                    Ok(fits) => {
                        assert_eq!(
                            dusk_token::encode_with_metadata(recipient, fits, &metadata),
                            encoded,
                            "{line}"
                        );
                        let decoded = dusk_token::decode(&encoded).unwrap();
                        assert_eq!(
                            (decoded.recipient, decoded.amount, decoded.metadata),
                            (recipient, fits, metadata.as_slice())
                        );
                    }
                    Err(_) => assert!(dusk_token::decode(&encoded).is_none(), "{line}"),
                }
            }
            "checkpoint" => {
                let origin = number(&value, "origin") as u32;
                let hook = hex32(&value, "merkle_tree_hook");
                let root = hex32(&value, "root");
                let index = number(&value, "index") as u32;
                let message_id = hex32(&value, "message_id");
                assert_eq!(
                    dusk_checkpoint::domain_hash(origin, &hook),
                    hex32(&value, "domain_hash"),
                    "{line}"
                );
                assert_eq!(
                    dusk_checkpoint::checkpoint_digest(origin, &hook, &root, index, &message_id),
                    hex32(&value, "digest"),
                    "{line}"
                );
            }
            "announcement" => {
                let location = String::from_utf8(hex_bytes(&value, "location")).unwrap();
                assert_eq!(
                    dusk_checkpoint::announcement_digest(
                        number(&value, "domain") as u32,
                        &hex32(&value, "mailbox"),
                        &location,
                    ),
                    hex32(&value, "digest"),
                    "{line}"
                );
            }
            "zero_hash" => {
                let level = number(&value, "level") as usize;
                assert_eq!(
                    hyperlane_dusk_types::merkle::ZERO_HASHES[level],
                    hex32(&value, "hash")
                );
                assert_eq!(ZERO_HASHES[level].0, hex32(&value, "hash"));
            }
            "merkle_root" => {
                merkle_roots.insert(number(&value, "count"), hex32(&value, "root"));
            }
            "multisig_metadata" => {
                let metadata = hex_bytes(&value, "metadata");
                let trailing = number(&value, "trailing") as usize;
                let signatures = value["signatures"].as_array().unwrap();
                // Solidity reads the fields at the ISM's offsets.
                assert_eq!(&metadata[0..32], hex32(&value, "merkle_tree_hook"));
                assert_eq!(&metadata[32..64], hex32(&value, "root"));
                assert_eq!(
                    u32::from_be_bytes(metadata[64..68].try_into().unwrap()),
                    number(&value, "index") as u32
                );
                for (i, signature) in signatures.iter().enumerate() {
                    let signature = hex::decode(&signature.as_str().unwrap()[2..]).unwrap();
                    assert_eq!(&metadata[68 + 65 * i..68 + 65 * (i + 1)], signature);
                }
                // Whole signatures: both count them alike. Trailing bytes:
                // Solidity's `verify` still reads the first `threshold`, the
                // Dusk ISM refuses (security.md, "Multisig metadata").
                match dusk_ism_signature_count(&metadata) {
                    Some(count) => {
                        assert_eq!(trailing, 0);
                        assert_eq!(count, signatures.len());
                        assert_eq!(value["signature_count"].as_u64(), Some(count as u64));
                    }
                    None => {
                        assert!(trailing > 0);
                        assert!(value["signature_count"].is_null());
                    }
                }
            }
            other => unreachable!("unknown vector kind {other}"),
        }
        *seen.entry(kind).or_default() += 1;
    }
    // Leaf i is keccak256(abi.encode(uint256(i))).
    let mut dusk = DuskTree::new();
    assert_eq!(merkle_roots.get(&0), Some(&dusk.root()));
    let last = *merkle_roots.keys().last().unwrap();
    for i in 0..last {
        let mut word = [0u8; 32];
        word[24..].copy_from_slice(&i.to_be_bytes());
        dusk.insert(keccak256(&word));
        if let Some(root) = merkle_roots.get(&(i + 1)) {
            assert_eq!(&dusk.root(), root, "count {}", i + 1);
        }
    }
    for (kind, minimum) in [
        ("message", 40),
        ("token_message", 40),
        ("checkpoint", 40),
        ("announcement", 20),
        ("zero_hash", 32),
        ("merkle_root", 80),
        ("multisig_metadata", 20),
    ] {
        assert!(
            seen.get(kind).copied().unwrap_or(0) >= minimum,
            "{kind}: {seen:?}"
        );
    }
}
