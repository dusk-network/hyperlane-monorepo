//! A Rusk 1.7.3 node's handling of one Moonlight account's transactions,
//! for the delivery submission tests, and a `dusk-tx` stand-in that talks
//! to it over HTTP as the helper does.
//!
//! The node follows rusk-private 1.7.3 (`node/src/mempool.rs`,
//! `mempool/admission.rs`, `mempool/prequeue.rs`, `chain/acceptor.rs` and
//! `rusk/src/lib/http/chain.rs`):
//!
//! - Preverification refuses a transaction the mempool or the ledger
//!   holds, one whose nonce the mempool holds for another transaction
//!   unless it pays a higher gas price (which replaces it), one whose nonce
//!   the account has used, and one ahead of the account's next nonce whose
//!   intermediate nonces the mempool does not all hold
//!   (`Missing intermediate nonce`).
//! - Propagation admits what preverification accepts, and queues a
//!   transaction refused only for a missing intermediate nonce, at most 64
//!   per account: the queue tries it again every `RETRY_DELAY`, at most
//!   `MAX_RETRIES` times, and as soon as the nonce before it is admitted,
//!   and then drops it.
//! - A block holds the consecutive nonces, from the account's next one,
//!   that the mempool held when the block before it came, as Rusk builds a
//!   candidate when its round starts. A transaction that delivers a message
//!   a block already delivered fails and pays its whole gas limit.
//! - Finality advances on its own, `finality_lag` blocks behind the tip, or
//!   by hand; a rollback reverts the blocks above a height that is not
//!   final, restores the account's nonce and puts their transactions back
//!   into the mempool, as Rusk's revert does.
//! - Evicting a transaction drops the mempool's later nonces of the account
//!   with it, as Rusk's cascading delete does.
//!
//! The account status (`nonce`, `next_nonce`, and the transfer contract's
//! `account` query that dusk-tx reads), the `process` simulation,
//! `lastBlockPair` and the transaction lookups (`mempoolTx`, `tx`) read
//! this state. Faults can refuse, hold or lose a request at a route, and
//! the path of every request is logged.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Condvar, Mutex,
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use hyperlane_core::{
    config::OpSubmissionConfig, HyperlaneDomain, HyperlaneDomainProtocol,
    HyperlaneDomainTechnicalStack, HyperlaneMessage, Metadata, NativeToken, H256, U256,
};
use hyperlane_dusk::{
    ConnectionConf, DuskMailbox, DuskProvider, DuskSigner, QueryRateLimit, RuesClient,
    SubmissionLimits,
};
use serde_json::{json, Value};
use tempfile::TempDir;
use url::Url;

pub const MAILBOX: [u8; 32] = [2; 32];
pub const HOOK: [u8; 32] = [3; 32];
pub const DOMAIN: u32 = 4242;
/// A block every 200 ms: a Dusk block takes about 10 s.
pub const BLOCK_TIME: Duration = Duration::from_millis(200);
/// The queue of future nonces tries a transaction again this often: Rusk's
/// 500 ms, scaled like the block time.
pub const RETRY_DELAY: Duration = Duration::from_millis(10);
/// Rusk's `MAX_RETRIES` of the future-nonce queue.
pub const MAX_RETRIES: u32 = 10;
/// Rusk's `max_moonlight_future_nonce_per_account`.
pub const MAX_FUTURE_PER_ACCOUNT: usize = 64;
/// The gas a delivery that executes spends.
pub const PROCESS_GAS: u64 = 2_000_000;
/// The gas any other call that executes spends.
pub const OTHER_GAS: u64 = 300_000;
/// The gas limit deliveries are sent with in these tests.
pub const GAS_LIMIT: u64 = 5_000_000;

fn hash_hex(height: u64, fork: u64) -> String {
    let mut bytes = height.to_be_bytes().to_vec();
    bytes.extend_from_slice(&fork.to_be_bytes());
    hex::encode(hyperlane_dusk_types::message::keccak256(&bytes))
}

/// A transaction as the stand-in encodes it: `dusk-fixture-tx <id>
/// <nonce> <gas price> <gas limit> <function> <arguments>`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Tx {
    pub id: String,
    pub nonce: u64,
    pub gas_price: u64,
    pub gas_limit: u64,
    pub function: String,
    pub args: String,
}

impl Tx {
    fn parse(body: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(body).ok()?;
        let mut fields = text.trim_end().split(' ');
        if fields.next()? != "dusk-fixture-tx" {
            return None;
        }
        let id = fields.next()?.to_owned();
        let nonce = fields.next()?.parse().ok()?;
        let gas_price = fields.next()?.parse().ok()?;
        let gas_limit = fields.next()?.parse().ok()?;
        let function = fields.next()?.to_owned();
        let args = fields.next().unwrap_or_default().to_owned();
        Some(Self {
            id,
            nonce,
            gas_price,
            gas_limit,
            function,
            args,
        })
    }

    pub fn delivers(&self) -> bool {
        self.function == "process"
    }
}

/// Why preverification refuses a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Refusal {
    InMempool,
    InLedger,
    NonceHeld,
    NonceUsed,
    MissingIntermediateNonce(u64),
    QueueFull,
    /// A fault a test injected.
    Injected(String),
}

impl Refusal {
    fn reason(&self) -> String {
        match self {
            Self::InMempool => "this transaction exists in the mempool".into(),
            Self::InLedger => "this transaction exists in the ledger".into(),
            Self::NonceHeld => "this transaction's spendId exists in the mempool".into(),
            Self::NonceUsed => "this transaction is invalid Repeating nonce".into(),
            Self::MissingIntermediateNonce(nonce) => {
                format!("Missing intermediate nonce {nonce}")
            }
            Self::QueueFull => {
                "Maximum queued future Moonlight transactions per account exceeded 64".into()
            }
            Self::Injected(reason) => reason.clone(),
        }
    }
}

/// What happened to a transaction at the node, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// Preverification refused it.
    Refused(Tx, Refusal),
    /// Propagation admitted it to the mempool.
    Admitted(Tx),
    /// Propagation queued it for a missing intermediate nonce.
    Queued(Tx),
    /// The queue of future nonces dropped it.
    Expired(Tx),
    /// A block at this height holds it; whether it failed.
    Included(Tx, u64, bool),
    /// The mempool dropped it.
    Evicted(Tx),
    /// A rollback reverted its block and put it back into the mempool.
    Restored(Tx),
}

/// A transaction in a block of the node's chain.
#[derive(Clone, Debug)]
pub struct Included {
    pub tx: Tx,
    pub height: u64,
    pub gas_spent: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug)]
struct Block {
    hash: String,
    txs: Vec<Included>,
    /// The account's nonce before the block.
    nonce_before: u64,
}

#[derive(Clone, Debug)]
struct Queued {
    tx: Tx,
    retries_left: u32,
    next_retry: Instant,
}

/// The node's state of the account and its chain.
pub struct Model {
    dir: PathBuf,
    pub height: u64,
    /// Rollbacks so far: blocks built again at a height get another hash.
    pub fork: u64,
    pub finalized: u64,
    /// Finality follows the tip this many blocks behind; `None` while it
    /// moves only by hand.
    pub finality_lag: Option<u64>,
    /// The account's nonce at the tip.
    pub nonce: u64,
    /// The `process` arguments delivered at the tip, by hex.
    delivered: HashSet<String>,
    pub mempool: BTreeMap<u64, Tx>,
    /// What the mempool held when the last block came: the next block's
    /// candidate.
    candidate: HashSet<String>,
    prequeue: BTreeMap<u64, Queued>,
    blocks: BTreeMap<u64, Block>,
    ledger: HashMap<String, Included>,
    pub events: Vec<Event>,
    /// The path of every request the node got, in order.
    pub requests: Vec<String>,
    /// Blocks hold no transaction while set.
    pub paused: bool,
}

impl Model {
    pub fn new(dir: PathBuf, finality_lag: Option<u64>) -> Self {
        let model = Self {
            dir,
            height: 0,
            fork: 0,
            finalized: 0,
            finality_lag,
            nonce: 0,
            delivered: HashSet::new(),
            mempool: BTreeMap::new(),
            candidate: HashSet::new(),
            prequeue: BTreeMap::new(),
            blocks: BTreeMap::new(),
            ledger: HashMap::new(),
            events: Vec::new(),
            requests: Vec::new(),
            paused: false,
        };
        model.publish();
        model
    }

    /// The account's next nonce past those its mempool holds, as Rusk's
    /// account status reports it.
    pub fn next_nonce(&self) -> u64 {
        let mut next = self.nonce + 1;
        while self.mempool.contains_key(&next) {
            next += 1;
        }
        next
    }

    /// The block of the node's chain that holds transaction `id`.
    pub fn included(&self, id: &str) -> Option<&Included> {
        self.ledger.get(id)
    }

    fn block_hash(&self, height: u64) -> String {
        self.blocks
            .get(&height)
            .map(|block| block.hash.clone())
            .unwrap_or_else(|| hash_hex(height, 0))
    }

    /// Rusk's admission checks, in its order.
    fn check(&self, tx: &Tx) -> Result<(), Refusal> {
        if self.mempool.values().any(|held| held.id == tx.id) {
            return Err(Refusal::InMempool);
        }
        if self.ledger.contains_key(&tx.id) {
            return Err(Refusal::InLedger);
        }
        if let Some(held) = self.mempool.get(&tx.nonce) {
            if tx.gas_price <= held.gas_price {
                return Err(Refusal::NonceHeld);
            }
        }
        if tx.nonce <= self.nonce {
            return Err(Refusal::NonceUsed);
        }
        for nonce in self.nonce + 1..tx.nonce {
            if !self.mempool.contains_key(&nonce) {
                return Err(Refusal::MissingIntermediateNonce(nonce));
            }
        }
        Ok(())
    }

    fn admit(&mut self, tx: Tx) {
        if let Some(replaced) = self.mempool.insert(tx.nonce, tx.clone()) {
            self.events.push(Event::Evicted(replaced));
        }
        let next = tx.nonce + 1;
        self.events.push(Event::Admitted(tx));
        self.drain(next);
    }

    /// Admit the queued transactions that follow an admitted nonce.
    fn drain(&mut self, mut next: u64) {
        while let Some(queued) = self.prequeue.remove(&next) {
            if self.check(&queued.tx).is_err() {
                self.events.push(Event::Expired(queued.tx));
                return;
            }
            self.mempool.insert(next, queued.tx.clone());
            self.events.push(Event::Admitted(queued.tx));
            next += 1;
        }
    }

    pub fn preverify(&mut self, tx: &Tx) -> Result<(), Refusal> {
        let checked = self.check(tx);
        if let Err(refusal) = &checked {
            self.events
                .push(Event::Refused(tx.clone(), refusal.clone()));
        }
        checked
    }

    pub fn propagate(&mut self, tx: Tx) -> Result<(), Refusal> {
        match self.check(&tx) {
            Ok(()) => {
                self.admit(tx);
                Ok(())
            }
            Err(Refusal::MissingIntermediateNonce(_)) => {
                if let Some(queued) = self.prequeue.get_mut(&tx.nonce) {
                    // Rusk keeps the queued one unless this pays more.
                    if tx.gas_price > queued.tx.gas_price {
                        queued.tx = tx.clone();
                        self.events.push(Event::Queued(tx));
                    }
                    return Ok(());
                }
                if self.prequeue.len() >= MAX_FUTURE_PER_ACCOUNT {
                    self.events.push(Event::Refused(tx, Refusal::QueueFull));
                    return Err(Refusal::QueueFull);
                }
                self.prequeue.insert(
                    tx.nonce,
                    Queued {
                        tx: tx.clone(),
                        retries_left: MAX_RETRIES,
                        next_retry: Instant::now() + RETRY_DELAY,
                    },
                );
                self.events.push(Event::Queued(tx));
                Ok(())
            }
            Err(refusal) => {
                self.events.push(Event::Refused(tx, refusal.clone()));
                Err(refusal)
            }
        }
    }

    pub fn retry_queue(&mut self, now: Instant) {
        let due: Vec<u64> = self
            .prequeue
            .iter()
            .filter(|(_, queued)| queued.next_retry <= now)
            .map(|(nonce, _)| *nonce)
            .collect();
        for nonce in due {
            let Some(mut queued) = self.prequeue.remove(&nonce) else {
                continue;
            };
            match self.check(&queued.tx) {
                Ok(()) => self.admit(queued.tx),
                Err(Refusal::MissingIntermediateNonce(_)) if queued.retries_left > 1 => {
                    queued.retries_left -= 1;
                    queued.next_retry = now + RETRY_DELAY;
                    self.prequeue.insert(nonce, queued);
                }
                Err(_) => self.events.push(Event::Expired(queued.tx)),
            }
        }
    }

    fn execute(&mut self, tx: Tx, height: u64) -> Included {
        let error = if tx.delivers() && !self.delivered.insert(tx.args.clone()) {
            Some("Panic: message already delivered".to_owned())
        } else {
            None
        };
        let gas_spent = match (&error, tx.delivers()) {
            (Some(_), _) => tx.gas_limit,
            (None, true) => PROCESS_GAS,
            (None, false) => OTHER_GAS,
        };
        let included = Included {
            tx,
            height,
            gas_spent,
            error,
        };
        self.ledger.insert(included.tx.id.clone(), included.clone());
        self.events.push(Event::Included(
            included.tx.clone(),
            height,
            included.error.is_some(),
        ));
        included
    }

    /// The next block.
    pub fn produce(&mut self) {
        self.height += 1;
        let height = self.height;
        let nonce_before = self.nonce;
        let mut txs = Vec::new();
        if !self.paused {
            loop {
                let next = self.nonce + 1;
                let Some(tx) = self.mempool.get(&next) else {
                    break;
                };
                if !self.candidate.contains(&tx.id) {
                    break;
                }
                let Some(tx) = self.mempool.remove(&next) else {
                    break;
                };
                txs.push(self.execute(tx, height));
                self.nonce = next;
            }
        }
        self.blocks.insert(
            height,
            Block {
                hash: hash_hex(height, self.fork),
                txs,
                nonce_before,
            },
        );
        self.candidate = self.mempool.values().map(|tx| tx.id.clone()).collect();
        if let Some(lag) = self.finality_lag {
            self.finalized = self.finalized.max(height.saturating_sub(lag));
        }
        self.publish();
    }

    /// Revert the blocks above `height`, which must not be final: their
    /// transactions go back into the mempool.
    pub fn rollback_to(&mut self, height: u64) {
        assert!(height >= self.finalized, "a final block cannot be reverted");
        while self.height > height {
            let Some(block) = self.blocks.remove(&self.height) else {
                break;
            };
            for included in block.txs.iter().rev() {
                self.ledger.remove(&included.tx.id);
                if included.tx.delivers() && included.error.is_none() {
                    self.delivered.remove(&included.tx.args);
                }
            }
            self.nonce = block.nonce_before;
            for included in block.txs {
                self.mempool.insert(included.tx.nonce, included.tx.clone());
                self.events.push(Event::Restored(included.tx));
            }
            self.height -= 1;
        }
        self.fork += 1;
        self.candidate = self.mempool.values().map(|tx| tx.id.clone()).collect();
        self.publish();
    }

    /// Drop the mempool's transaction of `nonce` and every later nonce it
    /// holds after it, as Rusk's cascading delete does when it evicts one.
    pub fn evict_from(&mut self, nonce: u64) {
        let mut next = nonce;
        while let Some(tx) = self.mempool.remove(&next) {
            self.events.push(Event::Evicted(tx));
            next += 1;
        }
    }

    /// Write what the stand-in reads: the account's nonce, and the
    /// transactions in blocks.
    fn publish(&self) {
        std::fs::write(self.dir.join("committed"), self.nonce.to_string()).unwrap();
        let included: String = self
            .blocks
            .values()
            .flat_map(|block| block.txs.iter())
            .map(|included| format!("{}\n", included.tx.id))
            .collect();
        std::fs::write(self.dir.join("included"), included).unwrap();
    }

    fn graphql(&self, query: &str) -> Value {
        if query == "query { lastBlockPair { json } }" {
            return json!({"lastBlockPair": {"json": {
                "last_block": [self.height, self.block_hash(self.height)],
                "last_finalized_block": [self.finalized, self.block_hash(self.finalized)],
            }}});
        }
        let hash = query
            .split_once("hash: \"")
            .and_then(|(_, rest)| rest.split('"').next())
            .unwrap_or_default();
        let mut reply = serde_json::Map::new();
        if query.contains("mempoolTx(") {
            let held = self.mempool.values().any(|tx| tx.id == hash);
            reply.insert(
                "mempoolTx".into(),
                if held {
                    json!({"id": hash})
                } else {
                    Value::Null
                },
            );
        }
        if query.contains(" tx(") {
            let tx = self.ledger.get(hash).map(|included| {
                json!({
                    "blockHeight": included.height,
                    "gasSpent": included.gas_spent,
                    "err": included.error,
                })
            });
            reply.insert("tx".into(), tx.unwrap_or(Value::Null));
        }
        Value::Object(reply)
    }
}

/// A route of the node's HTTP API that faults apply to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Route {
    Preverify,
    Propagate,
    AccountStatus,
    Graphql,
    Simulation,
}

/// What an injected fault does to the next request at its route.
pub enum Fault {
    /// Answer `400` with this reason, without applying the request.
    Refuse(String),
    /// Apply the request, then close the connection without an answer.
    LoseReply,
    /// Close the connection without applying the request.
    Drop,
    /// Wait until the gate opens, then serve the request as usual.
    Hold(Arc<Gate>),
    /// Serve the request as usual, but answer it only once the gate opens:
    /// what the node says is what it held when the request came.
    HoldReply(Arc<Gate>),
    /// Answer `200`, but admit nothing, as Rusk's mempool does when the
    /// transaction it routed fails its admission there.
    AcceptAndDiscard,
}

/// A gate a held request waits at.
#[derive(Default)]
pub struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
    /// Whether a request is waiting at it.
    pub waiting: AtomicBool,
}

impl Gate {
    pub fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.opened.notify_all();
    }

    fn wait(&self) {
        self.waiting.store(true, Ordering::SeqCst);
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.opened.wait(open).unwrap();
        }
    }
}

/// Read one HTTP request: its path and body.
fn read_request(stream: &mut TcpStream) -> Option<(String, Vec<u8>)> {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut reader = BufReader::new(stream);
    let mut first = String::new();
    reader.read_line(&mut first).ok()?;
    let path = first.split_whitespace().nth(1)?.to_owned();
    let mut length = 0;
    loop {
        let mut header = String::new();
        if reader.read_line(&mut header).ok()? == 0 {
            return None;
        }
        if header == "\r\n" {
            break;
        }
        if let Some(value) = header.to_ascii_lowercase().strip_prefix("content-length:") {
            length = value.trim().parse().ok()?;
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).ok()?;
    Some((path, body))
}

fn reply(stream: &mut TcpStream, status: &str, body: &[u8]) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
}

type Faults = Arc<Mutex<Vec<(Route, Option<u64>, Fault)>>>;

/// A `dusk-tx` stand-in. `call --help` lists `--ack-before-send` unless
/// the `no-ack` file exists, and `--nonce` and `--no-wait` when the
/// `pipelines` file exists. A call reads the URL and the key from stdin,
/// logs its arguments to `invocations`, asks the endpoint for the chain ID,
/// signs with `--nonce` or the account's nonce plus one (read from
/// `committed`, as dusk-tx reads the account), and prints `Prepared TX`.
/// With `--ack-before-send` it then reads one more line from stdin, within
/// 30 s, and logs it to `words` (`<eof>` when stdin closed first): any
/// line but `send` makes it print `Not submitted:` and stop without
/// sending. Without that flag it never sends to the node: only a pipelined
/// call (`--no-wait`), whose URL is the agent's own loopback endpoint,
/// goes on without the word. It then preverifies and propagates at the URL
/// it was given, and, without `--no-wait`, waits until a block holds the
/// transaction (`included`), for `wait-tries` × 20 ms (3 s by default). A
/// preverify refusal for a missing intermediate nonce does not stop a call
/// with `--nonce`. A `mode-<nonce>` file changes the next call that signs
/// that nonce: `lose` reports an unknown outcome without sending,
/// `unknown` sends and then reports an unknown outcome, `hang <seconds>`
/// waits that long once it may send, and `gate` writes `gated-<nonce>` and
/// waits for an `open-<nonce>` file before it prints the hash, and writes
/// `printed-<nonce>` once it has.
const HELPER: &str = r#"#!/usr/bin/env bash
dir="$(cd "$(dirname "$0")" && pwd)"
if [ "$1" = "call" ] && [ "$2" = "--help" ]; then
  if [ ! -f "$dir/no-ack" ]; then printf '      --ack-before-send\n'; fi
  if [ -f "$dir/pipelines" ]; then printf '      --nonce <NONCE>\n      --no-wait\n'; fi
  exit 0
fi
printf '%s\n' "$*" >> "$dir/invocations"
IFS= read -r url || exit 89
IFS= read -r key || exit 89
[ -n "$url" ] && [ -n "$key" ] || exit 89
nonce=""; given=""; wait=1; args=""; fn=""; gas_price=1; gas_limit=0; ack=""
shift
while [ $# -gt 0 ]; do
  case "$1" in
    --nonce) nonce="$2"; given=1; shift ;;
    --no-wait) wait="" ;;
    --ack-before-send) ack=1 ;;
    --args) args="$2"; shift ;;
    --fn-name) fn="$2"; shift ;;
    --gas-price) gas_price="$2"; shift ;;
    --gas-limit) gas_limit="$2"; shift ;;
  esac
  shift
done
rest="${url#http://}"
hostport="${rest%%/*}"
prefix=""
if [ "$rest" != "$hostport" ]; then prefix="/${rest#*/}"; fi
prefix="${prefix%/}"
host="${hostport%:*}"
port="${hostport##*:}"
status=""
reply=""
post() {
  local response
  if ! exec 3<>"/dev/tcp/$host/$port"; then status=000; reply="connection refused"; return 1; fi
  printf 'POST %s%s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s' \
    "$prefix" "$1" "$hostport" "${#2}" "$2" >&3
  response="$(cat <&3 2>/dev/null)"
  exec 3<&-
  if [ -z "$response" ]; then status=000; reply="the connection closed"; return 1; fi
  status="${response#HTTP/1.1 }"
  status="${status%% *}"
  reply="${response#*$'\r\n\r\n'}"
  reply="${reply//\"/\'}"
  reply="${reply//$'\n'/ }"
  reply="${reply//$'\r'/}"
  return 0
}
fail() {
  local message="${1//\\/\\\\}"
  printf '{"success":false,"error":"%s"}\n' "${message//\"/\\\"}"
  exit 1
}
# Only a pipelined call, to the agent's own endpoint, goes without the word.
if [ -z "$ack" ] && [ -n "$wait" ]; then
  fail "fixture: a call that sends to the node needs --ack-before-send"
fi
post "/on/contracts:0100000000000000000000000000000000000000000000000000000000000000/chain_id" "" \
  || fail "HTTP error: $reply"
[ "$status" = 200 ] || fail "Query chain_id failed ($status): $reply"
if [ -z "$nonce" ]; then nonce=$(( $(cat "$dir/committed") + 1 )); fi
echo x >> "$dir/calls"
tx=$(printf '%032x%032x' "$nonce" "$(wc -l < "$dir/calls")")
mode=""
if [ -f "$dir/mode-$nonce" ]; then mode="$(cat "$dir/mode-$nonce")"; rm -f "$dir/mode-$nonce"; fi
if [ "$mode" = gate ]; then
  : > "$dir/gated-$nonce"
  while [ ! -f "$dir/open-$nonce" ]; do sleep 0.01; done
fi
printf '  Prepared TX %s; reconcile this exact hash before retrying if submission is interrupted\n' "$tx" >&2
if [ "$mode" = gate ]; then : > "$dir/printed-$nonce"; fi
if [ -n "$ack" ]; then
  # As dusk-tx: only the line `send` sends.
  reason=""
  if IFS= read -r -t 30 word; then
    [ "$word" = send ] || reason='the line on stdin was not "send"'
  elif [ $? -gt 128 ]; then
    word="<timeout>"; reason='no line "send" on stdin within 30s'
  else
    word="<eof>"; reason='stdin closed before the line "send"'
  fi
  printf '%s %s\n' "$nonce" "$word" >> "$dir/words"
  if [ -n "$reason" ]; then
    printf 'Not submitted: %s\n' "$reason" >&2
    fail "Transaction $tx submission failed: Not submitted: $reason"
  fi
fi
case "$mode" in hang*) sleep "${mode#hang }" ;; esac
if [ "$mode" = lose ]; then
  fail "Transaction $tx submission failed: Propagation outcome unknown: the connection closed; retain tx_id=$tx"
fi
body="dusk-fixture-tx $tx $nonce $gas_price $gas_limit $fn $args"
post "/on/transactions/preverify" "$body" \
  || fail "Transaction $tx submission failed: Preverify failed before propagation: $reply; retain tx_id=$tx"
if [ "$status" != 200 ]; then
  if [ -z "$given" ] || [[ "$reply" != *"Missing intermediate nonce"* ]]; then
    fail "Transaction $tx submission failed: Preverify rejected before propagation ($status): $reply; retain tx_id=$tx"
  fi
fi
post "/on/transactions/propagate" "$body" \
  || fail "Transaction $tx submission failed: Propagation outcome unknown: $reply; retain tx_id=$tx"
[ "$status" = 200 ] \
  || fail "Transaction $tx submission failed: Propagation outcome unknown ($status): $reply; retain tx_id=$tx"
if [ "$mode" = unknown ]; then
  fail "Transaction $tx submission failed: Propagation outcome unknown: the reply was lost; retain tx_id=$tx"
fi
if [ -n "$wait" ]; then
  tries=0
  limit=$(cat "$dir/wait-tries" 2>/dev/null || echo 150)
  while ! grep -q "^$tx" "$dir/included"; do
    sleep 0.02
    tries=$((tries + 1))
    if [ "$tries" -ge "$limit" ]; then
      fail "Transaction $tx confirmation outcome unknown: in no block yet; retain tx_id=$tx"
    fi
  done
fi
printf '{"success":true,"tx_id":"%s","nonce":%s}\n' "$tx" "$nonce"
"#;

/// The node, its block producer, and the stand-in's directory.
pub struct Node {
    pub directory: TempDir,
    pub url: Url,
    pub state: Arc<Mutex<Model>>,
    faults: Faults,
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<()>>,
}

impl Node {
    /// A node whose finality follows its tip one block behind. `pipelines`:
    /// the stand-in takes `--nonce` and `--no-wait`.
    pub fn start(pipelines: bool) -> Self {
        Self::start_with(pipelines, Some(1))
    }

    pub fn start_with(pipelines: bool, finality_lag: Option<u64>) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let dir = directory.path().to_path_buf();
        for file in ["calls", "invocations", "words"] {
            std::fs::write(dir.join(file), "").unwrap();
        }
        if pipelines {
            std::fs::write(dir.join("pipelines"), "").unwrap();
        }
        let helper = dir.join("dusk-tx");
        std::fs::write(&helper, HELPER).unwrap();
        std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();

        let state = Arc::new(Mutex::new(Model::new(dir.clone(), finality_lag)));
        let faults: Faults = Arc::default();
        let stop = Arc::new(AtomicBool::new(false));
        let producer = {
            let (state, stop) = (state.clone(), stop.clone());
            thread::spawn(move || {
                let mut next_block = Instant::now() + BLOCK_TIME;
                while !stop.load(Ordering::SeqCst) {
                    thread::sleep(RETRY_DELAY);
                    let now = Instant::now();
                    let mut model = state.lock().unwrap();
                    model.retry_queue(now);
                    if now >= next_block {
                        model.produce();
                        next_block = now + BLOCK_TIME;
                    }
                }
            })
        };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let server = {
            let (state, faults, stop) = (state.clone(), faults.clone(), stop.clone());
            thread::spawn(move || {
                for stream in listener.incoming() {
                    if stop.load(Ordering::SeqCst) {
                        break;
                    }
                    let Ok(mut stream) = stream else {
                        continue;
                    };
                    let (state, faults) = (state.clone(), faults.clone());
                    thread::spawn(move || {
                        if let Some((path, body)) = read_request(&mut stream) {
                            serve(&state, &faults, &mut stream, &path, body);
                        }
                    });
                }
            })
        };
        Self {
            directory,
            url,
            state,
            faults,
            stop,
            workers: vec![producer, server],
        }
    }

    pub fn helper(&self) -> PathBuf {
        self.directory.path().join("dusk-tx")
    }

    pub fn model(&self) -> std::sync::MutexGuard<'_, Model> {
        self.state.lock().unwrap()
    }

    /// Change the next request at `route`, of the transaction signed with
    /// `nonce` when one is given.
    pub fn inject(&self, route: Route, nonce: Option<u64>, fault: Fault) {
        self.faults.lock().unwrap().push((route, nonce, fault));
    }

    /// The stand-in's next call that signs `nonce`: `lose`, `unknown`,
    /// `hang <seconds>` or `gate`.
    pub fn next_submission(&self, nonce: u64, mode: &str) {
        std::fs::write(self.directory.path().join(format!("mode-{nonce}")), mode).unwrap();
    }

    /// How long the stand-in waits for a block, in tries of 20 ms.
    pub fn helper_waits(&self, tries: u32) {
        std::fs::write(self.directory.path().join("wait-tries"), tries.to_string()).unwrap();
    }

    /// Make the stand-in's `call --help` leave out `--ack-before-send`, as
    /// a helper from before it does. The agent asks once per helper.
    pub fn without_ack(&self) {
        std::fs::write(self.directory.path().join("no-ack"), "").unwrap();
    }

    /// The word the stand-in read under `--ack-before-send` at each call,
    /// after the nonce it signed: `send`, or `<eof>` when stdin closed
    /// first.
    pub fn words(&self) -> Vec<String> {
        std::fs::read_to_string(self.directory.path().join("words"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    /// Whether the stand-in's call that signs `nonce` in `gate` mode has
    /// written `marker`: `gated` once it waits at its gate, `printed` once
    /// it has printed its hash.
    pub fn helper_reached(&self, marker: &str, nonce: u64) -> bool {
        self.directory
            .path()
            .join(format!("{marker}-{nonce}"))
            .exists()
    }

    /// Let the stand-in's call that signs `nonce` in `gate` mode past its
    /// gate.
    pub fn open_gate(&self, nonce: u64) {
        std::fs::write(self.directory.path().join(format!("open-{nonce}")), "").unwrap();
    }

    /// The arguments of every call the stand-in got.
    pub fn invocations(&self) -> Vec<String> {
        std::fs::read_to_string(self.directory.path().join("invocations"))
            .unwrap()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    pub fn pause(&self, paused: bool) {
        self.model().paused = paused;
    }

    pub fn evict_from(&self, nonce: u64) {
        self.model().evict_from(nonce);
    }

    pub fn rollback_to(&self, height: u64) {
        self.model().rollback_to(height);
    }

    /// Let finality follow the tip `lag` blocks behind, or move only by
    /// hand.
    pub fn finality_lag(&self, lag: Option<u64>) {
        self.model().finality_lag = lag;
    }

    pub fn finalize_to(&self, height: u64) {
        let mut model = self.model();
        model.finalized = model.finalized.max(height.min(model.height));
    }

    pub fn height(&self) -> u64 {
        self.model().height
    }

    /// The nonce of every transaction the node admitted or queued at its
    /// propagation, in order.
    pub fn propagated(&self) -> Vec<u64> {
        self.model()
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Admitted(tx) | Event::Queued(tx) => Some(tx.nonce),
                _ => None,
            })
            .collect()
    }

    /// How many requests the node got whose path ends with `route`.
    pub fn requests_to(&self, route: &str) -> usize {
        self.model()
            .requests
            .iter()
            .filter(|path| path.ends_with(route))
            .count()
    }

    /// Every refusal, with the nonce of the transaction refused.
    pub fn refusals(&self) -> Vec<(u64, Refusal)> {
        self.model()
            .events
            .iter()
            .filter_map(|event| match event {
                Event::Refused(tx, refusal) => Some((tx.nonce, refusal.clone())),
                _ => None,
            })
            .collect()
    }

    /// The transactions in blocks of the node's chain, in nonce order.
    pub fn included(&self) -> Vec<Included> {
        let model = self.model();
        model
            .blocks
            .values()
            .flat_map(|block| block.txs.iter().cloned())
            .collect()
    }

    /// How many transactions in blocks delivered each message, failed ones
    /// included, by their `process` arguments.
    pub fn deliveries_included(&self) -> BTreeMap<String, usize> {
        let mut counts = BTreeMap::new();
        for included in self.included() {
            if included.tx.delivers() {
                *counts.entry(included.tx.args).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Wait until `done` holds of the node, for at most 10 s.
    pub async fn wait_until(&self, what: &str, done: impl Fn(&Model) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if done(&self.model()) {
                return;
            }
            assert!(Instant::now() < deadline, "timed out waiting until {what}");
            tokio::time::sleep(BLOCK_TIME / 4).await;
        }
    }

    /// Wait until a final block holds the account's transaction of
    /// `nonce`.
    pub async fn wait_until_final(&self, nonce: u64) {
        self.wait_until(&format!("nonce {nonce} is in a final block"), |model| {
            model.blocks.iter().any(|(height, block)| {
                *height <= model.finalized && block.txs.iter().any(|tx| tx.tx.nonce == nonce)
            })
        })
        .await;
    }

    /// Wait until blocks hold `count` transactions, then a few blocks more,
    /// and check that no message was delivered by more than one
    /// transaction.
    pub async fn settle(&self, count: usize) {
        self.pause(false);
        self.wait_until(&format!("{count} transactions are in blocks"), |model| {
            model
                .blocks
                .values()
                .map(|block| block.txs.len())
                .sum::<usize>()
                >= count
        })
        .await;
        // A transaction sent twice would land in the blocks after.
        tokio::time::sleep(BLOCK_TIME * 4).await;
        let included = self.deliveries_included();
        assert!(
            included.values().all(|transactions| *transactions == 1),
            "a message was delivered by more than one transaction: {included:?}"
        );
    }

    /// A Mailbox of this node's chain, submitting deliveries pipelined or
    /// one at a time, keeping its deliveries in flight in memory.
    pub fn mailbox(&self, pipelined: bool) -> DuskMailbox {
        let rues = Arc::new(RuesClient::new(self.url.clone()).unwrap());
        mailbox(rues, &self.url, self.directory.path(), pipelined)
    }

    /// A Mailbox that keeps its deliveries in flight in the event store at
    /// `store`, which survives a restart (`restart`).
    pub fn durable_mailbox(&self, store: &Path, pipelined: bool) -> DuskMailbox {
        let rues = Arc::new(
            RuesClient::new_with_event_cursor_dir(self.url.clone(), store.to_path_buf()).unwrap(),
        );
        mailbox(rues, &self.url, store, pipelined)
    }

    /// Deliver `delivery`'s message at the tip by another relayer: its
    /// simulation fails from now on, and a transaction of ours that
    /// delivers it fails.
    pub fn deliver_elsewhere(&self, delivery: &(HyperlaneMessage, Metadata, Option<U256>)) {
        self.model().delivered.insert(process_args_hex(delivery));
    }

    /// Take back `deliver_elsewhere`: the message is not delivered at the
    /// tip, as if that delivery had not counted.
    pub fn undeliver(&self, delivery: &(HyperlaneMessage, Metadata, Option<U256>)) {
        self.model().delivered.remove(&process_args_hex(delivery));
    }

    /// Have another process that signs with the agent's key send a
    /// transaction with `nonce`, as the node would take it.
    pub fn send_foreign(&self, nonce: u64) -> Result<(), Refusal> {
        let tx = Tx {
            id: format!("{:032x}{:032x}", nonce, u64::MAX),
            nonce,
            gas_price: 1,
            gas_limit: GAS_LIMIT,
            function: "transfer".into(),
            args: String::new(),
        };
        self.model().propagate(tx)
    }

    /// Make the agent read the tip and the last final block again at its
    /// next request, as if a tick had passed.
    pub async fn expire_block_pair(&self) {
        RuesClient::new(self.url.clone())
            .unwrap()
            .expire_block_pair()
            .await;
    }

    /// Close the event store at `store`, as the agent's exit does.
    pub fn restart(&self, store: &Path) {
        RuesClient::forget_shared_client(&self.url, store);
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop.
        let _ = TcpStream::connect(self.url.socket_addrs(|| None).unwrap()[0]);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn take_fault(faults: &Faults, route: Route, nonce: Option<u64>) -> Option<Fault> {
    let mut faults = faults.lock().unwrap();
    let position = faults.iter().position(|(at, of, _)| {
        *at == route && (of.is_none() || nonce.is_none() || *of == nonce)
    })?;
    Some(faults.remove(position).2)
}

fn route_of(path: &str) -> Option<Route> {
    if path.ends_with("/on/transactions/preverify") {
        Some(Route::Preverify)
    } else if path.ends_with("/on/transactions/propagate") {
        Some(Route::Propagate)
    } else if path.starts_with("/on/account:") {
        Some(Route::AccountStatus)
    } else if path == "/on/graphql/query" {
        Some(Route::Graphql)
    } else if path.ends_with("/process") {
        Some(Route::Simulation)
    } else {
        None
    }
}

fn serve(
    state: &Arc<Mutex<Model>>,
    faults: &Faults,
    stream: &mut TcpStream,
    path: &str,
    body: Vec<u8>,
) {
    state.lock().unwrap().requests.push(path.to_owned());
    if path.ends_with("/chain_id") {
        return reply(stream, "200 OK", &[0]);
    }
    // The transfer contract's `account` query, as dusk-tx reads the
    // account: its `AccountData`, the nonce and the balance, as rkyv
    // archives them.
    if path.ends_with("/account") {
        let nonce = state.lock().unwrap().nonce;
        let mut account = nonce.to_le_bytes().to_vec();
        account.extend_from_slice(&(1u64 << 50).to_le_bytes());
        return reply(stream, "200 OK", &account);
    }
    let Some(route) = route_of(path) else {
        return reply(stream, "404 Not Found", b"not served");
    };
    let tx = Tx::parse(&body);
    let mut lose_reply = false;
    let mut hold_reply = None;
    match take_fault(faults, route, tx.as_ref().map(|tx| tx.nonce)) {
        Some(Fault::Refuse(reason)) => {
            if let Some(tx) = &tx {
                state.lock().unwrap().events.push(Event::Refused(
                    tx.clone(),
                    Refusal::Injected(reason.clone()),
                ));
            }
            return reply(stream, "400 Bad Request", reason.as_bytes());
        }
        Some(Fault::Drop) => return,
        Some(Fault::Hold(gate)) => gate.wait(),
        Some(Fault::HoldReply(gate)) => hold_reply = Some(gate),
        Some(Fault::LoseReply) => lose_reply = true,
        Some(Fault::AcceptAndDiscard) => return reply(stream, "200 OK", b""),
        None => {}
    }
    let (status, answer): (&str, Vec<u8>) = {
        let mut model = state.lock().unwrap();
        match route {
            Route::Preverify | Route::Propagate => match tx {
                None => ("400 Bad Request", b"Data: not a transaction".to_vec()),
                Some(tx) => {
                    let id = tx.id.clone();
                    let checked = if route == Route::Preverify {
                        model.preverify(&tx)
                    } else {
                        model.propagate(tx)
                    };
                    match checked {
                        Ok(()) => ("200 OK", Vec::new()),
                        Err(refusal) => (
                            "400 Bad Request",
                            format!("Tx {id} not accepted: {}", refusal.reason()).into_bytes(),
                        ),
                    }
                }
            },
            Route::AccountStatus => (
                "200 OK",
                json!({
                    "balance": 1u64 << 50,
                    "nonce": model.nonce,
                    "next_nonce": model.next_nonce(),
                })
                .to_string()
                .into_bytes(),
            ),
            Route::Graphql => (
                "200 OK",
                model
                    .graphql(&String::from_utf8_lossy(&body))
                    .to_string()
                    .into_bytes(),
            ),
            Route::Simulation => {
                if model.delivered.contains(&hex::encode(&body)) {
                    ("500 Internal Server Error", b"already delivered".to_vec())
                } else {
                    // The encoding of `()`.
                    ("200 OK", Vec::new())
                }
            }
        }
    };
    if let Some(gate) = hold_reply {
        gate.wait();
    }
    if !lose_reply {
        reply(stream, status, &answer);
    }
}

pub fn domain() -> HyperlaneDomain {
    HyperlaneDomain::from_config(
        DOMAIN,
        "dusk-submission",
        HyperlaneDomainProtocol::Dusk,
        HyperlaneDomainTechnicalStack::Other,
    )
    .unwrap()
}

/// The chain's connection, with pipelined deliveries opted in or not.
pub fn connection(url: &Url, directory: &Path, pipelined: bool) -> ConnectionConf {
    ConnectionConf {
        url: url.clone(),
        chain_id: 0,
        event_cursor_dir: directory.to_path_buf(),
        gas_limit: GAS_LIMIT,
        gas_price: 1,
        native_token: NativeToken::default(),
        op_submission_config: OpSubmissionConfig {
            max_batch_size: 16,
            ..OpSubmissionConfig::default()
        },
        query_rate_limit: QueryRateLimit::default(),
        pipeline_deliveries: pipelined,
    }
}

pub fn mailbox(rues: Arc<RuesClient>, url: &Url, directory: &Path, pipelined: bool) -> DuskMailbox {
    let provider = Arc::new(DuskProvider::new(domain(), rues.clone()));
    DuskMailbox::new(
        provider,
        rues,
        H256::from(MAILBOX),
        H256::from(HOOK),
        domain(),
        // A synthetic fixture key.
        Some(DuskSigner::new(H256::repeat_byte(1)).unwrap()),
        connection(url, directory, pipelined),
    )
    .with_submission_limits(limits())
}

/// Delivery `nonce`: its message, metadata and the relayer's gas limit.
pub fn delivery(nonce: u32) -> (HyperlaneMessage, Metadata, Option<U256>) {
    let message = HyperlaneMessage {
        version: 3,
        nonce,
        origin: 7,
        sender: H256::repeat_byte(4),
        destination: DOMAIN,
        recipient: H256::repeat_byte(5),
        body: vec![nonce as u8],
    };
    (message, Metadata::new(vec![]), Some(U256::from(GAS_LIMIT)))
}

/// The hex of `delivery`'s `process` arguments, as the Mailbox sends
/// them.
pub fn process_args_hex(delivery: &(HyperlaneMessage, Metadata, Option<U256>)) -> String {
    let (message, metadata, _) = delivery;
    let encoded = hyperlane_dusk_types::message::encode(
        message.version,
        message.nonce,
        message.origin,
        message.sender.into(),
        message.destination,
        message.recipient.into(),
        &message.body,
    );
    hex::encode(rkyv::to_bytes::<_, 256>(&(metadata.to_vec(), encoded)).unwrap())
}

/// The Mailbox's limits on this chain: the default ones, with its block
/// time.
pub fn limits() -> SubmissionLimits {
    SubmissionLimits {
        block: BLOCK_TIME,
        ..SubmissionLimits::default()
    }
}

/// Limits that suit a chain that holds transactions back for a while: a
/// delivery waits 600 ms for its block, and a transaction in neither the
/// mempool nor a block counts as dropped after 300 ms.
pub fn patient(outstanding: usize) -> SubmissionLimits {
    SubmissionLimits {
        receipt: Duration::from_millis(600),
        drop_grace: Duration::from_millis(300),
        outstanding,
        ..limits()
    }
}

/// Past `patient`'s drop grace.
pub const PAST_DROP_GRACE: Duration = Duration::from_millis(400);

/// The process environment names the helper, so one test at a time uses
/// it.
pub static HELPER_ENV: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
