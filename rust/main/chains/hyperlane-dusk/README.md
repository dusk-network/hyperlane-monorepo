# hyperlane-dusk

Hyperlane agent support for Dusk. It implements the chain traits (Mailbox,
MerkleTreeHook, InterchainSecurityModule, MultisigIsm, ValidatorAnnounce,
InterchainGasPaymaster, provider and indexers) over a Rusk node's HTTP API, so
the relayer, validator and scraper can run with Dusk as an origin or
destination.

The Dusk contracts, deployment tooling and end-to-end tests live in
[`dusk-network/hyperlane-dusk`](https://github.com/dusk-network/hyperlane-dusk).
That repository's README covers how the contracts differ from EVM Hyperlane and
how to run the local bridge.

## How it works

- **Reads** go to Rusk contract queries (`POST /on/contracts:<id>/<method>`)
  with rkyv-encoded arguments, and to Rusk's GraphQL endpoint for blocks,
  transactions and finalized events. Events come from the archive, so the
  node must be built with the `archive` feature. Response bodies, the parse
  errors that quote them and the `dusk-tx` helper's output are cut to 1 KiB
  before they appear in errors or logs.
- **Pacing.** Rusk 1.7.3 allows all of a node's clients together 180
  contract queries a second, 360 in any 2 s and 12 at once, and answers the
  rest `429 too_many_requests` with `Retry-After: 1`. Every client of one
  endpoint in an agent process shares one pace for contract queries:
  `contractQueriesPerSecond` (100 by default) and `contractQueryBurst` (20),
  with at most 8 in flight, which leaves room for the node's other clients.
  A request the node refuses for its rate limit, on any route, holds back
  every request of its class from that process for the `Retry-After` plus up
  to as long again at random, and is sent again after a wait that doubles
  with each refusal in a row, also at random within it, so that indexers do
  not come back together; after 4 refusals in a row (15 to 30 s) it fails.
  The node refuses a request before it runs it, so sending it again is safe.
- **Finality is read once per tick.** Finalized data changes only when the
  node's last final block does. Every client of an endpoint in a process
  shares one `lastBlockPair` read per 5 s tick, and what the agent computes
  from final data is kept for the last final block it was computed at: the
  indexers' counts and tips, checkpoints, `delivered` and the announcement
  checks. Until the node finalizes another block they cost no request; the
  relayer's `delivered` checks every half second while it waits for
  signatures are answered from the last one. When the node does, each
  record indexer reads its contract's count once, and the heights of only
  the records past the last final one it knows, from there (see
  `final_records.rs`); a validator's indexer and checkpoint reader share
  that read. The delivery log reads the archive again only then, from where
  it stopped, and a checkpoint whose leaf count has not changed is not
  checked against the hook again. An idle relayer sends one request a tick
  plus about 6 per newly final block, and a validator one a tick plus one,
  whatever the history (48 and 18 a minute on a 10 s chain). Finality is
  seen up to one tick later than a fresh read would see it.
- **Batched reads.** A chain whose `batchContractAddress` names Multicall
  for Dusk sends reads that do not depend on each other as one `aggregate`
  query, which answers them all at one state; an aggregate is one request to
  the pace and to the node's rate limit. Before its first aggregate, a
  process checks that the multicall answers `version` with 1 and an empty
  `aggregate` with no results. One that does not, or later answers wrongly,
  is set aside with one warning, every read is one query again, and it is
  probed again 10 minutes later. Without a `batchContractAddress` every read
  is one query, as before. Through the multicall:
  - the deployment check reads its ten views in one aggregate, and an
    aggregation hook's three in one more, after the probe's two queries;
  - the reads made once per final block, the record counts and the
    delivery checks of messages the relayer is waiting on, go in one
    aggregate per final block, which also carries those asked at the two
    blocks before: an idle relayer sends 30 requests a minute instead of 48.
    A validator reads one count per final block, which gains nothing (18);
  - indexers read the heights, IDs and pages of a range of records together,
    and search record heights 256 at a time: a restart's catch-up of 1,000
    records sends 25 contract queries instead of 3,048, and of 10,000
    records 151 instead of 30,132;
  - a checkpoint's checks against the hook are one aggregate at one state
    (see Checkpoints);
  - messages prepared together read their recipient's ISM and its
    `validators_and_threshold` once, and reads asked while an aggregate is
    in flight, such as the first delivery checks of a batch of messages, go
    together in the next one.

  Each aggregate keeps within the multicall's 256 calls and 64 KiB of
  argument and of reply by the longest reply each view gives, even if every
  call fails with the longest panic message. One that fails as a whole is
  split in two and each half sent again, and a half of one read goes as a
  single query; when both halves fail, the reads go one query each. Rusk
  1.7.3 answers every failed query with `500 Internal server error`, so a
  reply past 64 KiB cannot be told from other failures. A call that fails
  gives the error its single query would, `500`, here with the reason; one
  that runs out of its share of gas is sent again as a single query, with
  the query's whole gas. A call can change what the later calls of its query
  read, so reads of contracts outside the deployment, such as an ISM a
  recipient chose, go after the deployment's own, and their failures never
  set the multicall aside. Every view the agent batches answers its caller
  alike, so its reply in an aggregate is the bytes a single query returns.
- **Indexing** uses finalized contract events only. Each event is matched to
  its block and transaction, and stored in the agent's `eventCursorDir`, which
  every agent process needs its own copy of. The Mailbox stores only the ID of
  each dispatched message (`dispatched_id`); the message itself comes from the
  finalized `Dispatch` event whose message hashes to that ID.
- **Event logs.** The Mailbox keeps no list of deliveries, so they come from
  its finalized `ProcessId` events alone: the agent keeps a log of them in
  `eventCursorDir`, every non-reverted row the archive has finalized, in
  block order and within a block in archive order (by transaction hash),
  numbered from zero. A delivery's sequence is its number there, the same for
  any endpoint whose archive holds the same blocks, but not its order of
  execution within a block. The log takes a block once it has read all of
  its rows, they all name one block hash, the block is at or below the
  node's last final block and `checkBlock(onlyFinalized)` accepted it, also
  when the block has no row of the topic, since the log then holds that it
  has none; a row that does not parse stops it before its block, and a row
  more than 64 blocks above the node's tip fails the read. It reads on from
  the closest saved scan position below the first block it lacks, at most 16
  pages per indexer call and 4 per delivery check, even inside a block: a
  block not read whole by then is staged in `eventCursorDir` with its rows of
  the topic and the position after the last row read, and the next call
  reads on from there. A block of more than 1,000,000 rows of the contract,
  or 32 MiB of rows of the topic, which no Rusk block holds, stops the log
  with an error. The log never numbers rows of a block it already holds
  again, so its numbering survives restarts and archives that only number
  their rows differently. An archive's history can change all the same: rows
  can come out of block order, and a rebuilt archive can hold rows of a block
  the old one skipped, or lack rows it had. Every block the log holds that a
  pass reads again from its first row, as it does from the contract's first
  row once the archive numbers its rows differently, must match the log's
  rows of it, and every block the log holds rows of must come again before a
  later block does. A row below a block read before it, or a block that
  differs, drops the log's rows from that block on, and the log reads them
  again; a block that `checkBlock` no longer accepts when a delivery is
  served drops them all. The delivery indexer marks as published every
  delivery below a count it reports to contract sync, and every delivery it
  serves. The log keeps the dropped rows of published deliveries, and where
  the rows it reads again would give one of those sequences another
  delivery, it halts (see [Event log halts](#event-log-halts)). The delivery
  indexer's tip is the height up to which the log holds every finalized
  delivery.
- **Announced locations.** ValidatorAnnounce keeps only the keccak256 of
  each location a validator announced, oldest first
  (`get_announced_storage_location_hashes`), and the locations are in its
  `ValidatorAnnouncement` events. A relayer keeps a log of the finalized
  announcements like the deliveries' (at most 16 pages per read) and returns,
  for each validator, the logged locations whose hash the contract lists for
  it, in the list's order, so the last is the latest. A listed hash whose
  announcement it has not read yet, such as one that is not final, is left
  out until it has. A validator's startup check has announced its location
  iff the location's hash is in its own list, and reports it only once the
  announcement is final (see below); otherwise the validator announces.
- **Archive scans** read a contract's `finalizedEvents` 200 rows at a time
  (Rusk's maximum) and match every record of a block in one pass over that
  block's rows. The agent reads at most 4 MiB of a reply, and 200 rows of
  large dispatches are more than that, so a page over the limit is asked for
  again at the same position with half the rows, down to one. A Rusk event
  carries at most 64 KiB of data, so a single row always fits; one that does
  not is reported as an endpoint fault. Each scan starts from a saved
  position just before the block, kept in `eventCursorDir` (about one per
  200 rows), so a restart or an archive that lags behind finality does not
  send the scan back to the contract's first row. Before reusing a position,
  the scan checks that the archive still has the row it was taken from,
  unchanged and below the block the position names. Rusk numbers rows in
  insertion order and never deletes one, so if the archive returns another
  row first, it numbers its rows differently: the contract's positions are
  dropped and the scan starts over. If it returns no row from there on, the
  archive may only be behind, and the positions are kept, with the lookup
  failing until it catches up, only on evidence of that: the archive has not
  finalized that row's block (`checkBlock(onlyFinalized)`, which Rusk
  records together with the block's rows), the node's ledger holds that
  block under the position's hash, and the node has not finalized the block
  yet or the archive has not finalized the node's last final block either.
  Otherwise the archive will never return the row, and the scan starts over.
- **Missing archive rows.** When contract state records an event in a
  finalized block and the archive has passed that block without a matching
  row, the archive will not add it: Rusk archives a block's rows in one
  transaction and blocks in height order, and a node whose archive write
  fails skips the block for good. The archive has passed the block once
  `checkBlock(onlyFinalized)` accepts a later block holding a row of the
  contract, or else the node's last final block, at or above this one,
  which covers a contract that has emitted nothing since. The lookup then
  fails with an error naming the block, its hash, the contract and the
  records, and the agent logs it as a critical error at most every 10
  minutes per block. Its node's archive has to be repaired or rebuilt, or
  another endpoint used; the agent never accepts a record without its row.
- **Block cache.** A scan reads the whole block it is looking in, and the
  block's rows stay in memory, so other lookups in the same block (a cursor
  with a small `index.chunk`, or one lookup per transaction) match against
  them instead of reading the archive again. Every lookup still checks
  contract state and `checkBlock` itself, from memory or not. Only blocks
  whose rows carry the hash `checkBlock(onlyFinalized)` accepted are kept,
  at most 64 blocks and 64 MiB, and 32 MiB for one block; a larger block is
  read per lookup. A block with a row of the topic whose block hash,
  transaction hash or data does not parse, which Rusk never writes, is not
  kept either. The Merkle hook, dispatch and IGP indexers look a transaction
  up by the records its own rows name, instead of every record of its block;
  when its rows name none, or the block cannot be read whole, contract state
  decides which records of the block are the transaction's.
- **Writes** are Moonlight transactions built and signed by the `dusk-tx`
  helper from the companion repository. The agent runs it as a subprocess
  (`DUSK_TX_BIN`, or `dusk-tx` on `PATH`) and passes the RPC URL and signing
  key on stdin. Before retrying an uncertain submission, the agent reconciles
  it against the transaction hash it prepared. A delivery is submitted with
  the gas limit the relayer's gas policy asks for, which follows the gas the
  sender paid for, but at most `gasLimit` and Rusk's block gas limit of
  3,000,000,000: the mempool keeps a transaction over the block gas limit
  without any block taking it, and a failed transaction pays its whole limit.
- **Pipelined deliveries.** The helper signs with the account's next nonce
  and returns once the transaction is in a block, so deliveries sent one at
  a time land one every two blocks: the next one reaches the node after the
  next block's round has started. With `maxBatchSize` above 1 in the chain's
  config and a helper that takes `call --nonce <N> --no-wait` (found from
  its `call --help`, once per process), the relayer submits a batch of
  deliveries as consecutive transactions of its account, from the node's
  `next_nonce`, each once the node holds the one before, and they share a
  block. A delivery the node refuses, or whose submission outcome is
  unknown, ends the batch: the deliveries after it are submitted one at a
  time. The batch then follows each transaction to its block; one that is
  in none by then still counts as sent, and the relayer checks that its
  message was delivered before it relays it again. At most 64 go in a batch,
  Rusk's queue of an account's future nonces. With another helper, every
  delivery is submitted one at a time.
- **Delivery simulation.** Dusk charges a failed transaction its whole gas
  limit. Before submitting `process`, the relayer runs it as an unsigned
  contract query, which Rusk executes in a throwaway session. If the query
  fails, or replies with anything other than the encoding of the `()` that
  `process` returns, nothing is submitted and the message is retried later.
  A simulation of the same arguments that passed less than 2 s before
  stands for the next one, so the relayer's preparation, its dry run before
  submitting and `process` itself, which ran three in a row, run one when
  the submission follows the preparation. Rusk's query reports no gas use,
  and its transaction simulation needs a signed transaction a node could
  include, so `gasLimit` stays the estimate of every delivery.
- **Delivery and announcement finality.** Contract queries read the node's
  tip, which Rusk can roll back until a block is final, and Rusk 1.7.3 has
  no query of contract state at the last finalized block. The relayer stops
  retrying a message once the Mailbox reports it delivered, so `delivered`
  reports a delivery the tip holds only once the agent's delivery log holds
  its `ProcessId` row in a block `checkBlock(onlyFinalized)` accepts, which
  no rollback can undo, whoever delivered the message. Until then the check
  fails as not final and the relayer tries again later with its usual
  backoff; a rolled-back delivery reads as not delivered, and the message is
  relayed again. That holds as well while the log does not reach the last
  final block (read with `lastBlockPair` before the state), cannot be read,
  or the client has no `eventCursorDir`: the blocks above the last final
  one never make a delivery count, since a check reads them up to a tip
  older than the state it read, and a block accepted and rolled back within
  the check's own requests leaves no trace there. Where the log holds every
  final block, the check reads those blocks (`blocks`, each block's events
  from `contractEvents`) only to name the one that holds the delivery while
  it is not final; when none does, the block was rolled back during the
  check or the node's archive skipped it, which it logs as a warning. When
  finality falls more than 64 blocks behind the tip, it waits for finality
  to catch up instead of reading every block. A validator's startup reports
  its own announcement only once its announcement log holds it in a block
  `checkBlock(onlyFinalized)` accepts, and waits until then. Each check
  reads at most 4 pages of the archive (16 for announcements) to bring its
  log up to date, so an agent started with an empty `eventCursorDir`
  confirms deliveries, or its announcement, only once its log has read the
  contract's history that far, a few checks for a young contract and more
  for a long history.
- **Identity checks.** Building the agent's Dusk components makes no
  request. Before its first request, each client checks, once, that the
  endpoint serves the configured deployment: the node's chain ID, the
  Mailbox and ValidatorAnnounce domains, each contract's `state_version`,
  and the ValidatorAnnounce and Merkle hook bindings to that Mailbox. Every
  client of one endpoint in a process that requires the same deployment,
  such as a validator's indexer and its checkpoint reader, shares the check.
  Until a check succeeds, every request fails without being sent and the
  next one checks again; requests that wait while a check runs share its
  result. A
  node that is unreachable, or serves another deployment, when an agent
  starts therefore delays its reads, and the relayer keeps the chain instead
  of dropping it. Submissions through the `dusk-tx` helper are checked the
  same way first. The agent accepts only the state versions it reads
  (Mailbox 5, MerkleTreeHook 2, ValidatorAnnounce 2) and refuses any other
  with an explicit error, since another version lacks queries the agent
  uses.
- **Checkpoints** are read at consensus finality, adjusted by the configured
  reorg period. The Merkle hook keeps no root per insertion, so a
  checkpoint's root is computed: the agent reads the message IDs the hook
  stores (`message_ids`, pages of 256, each as long as asked) and inserts
  them into hyperlane-core's incremental tree, which it keeps between
  checkpoints and only extends (32 branch nodes and a count, whatever the
  tree's size). Before the root is used, the tree's last ID must be the
  hook's `message_id_at` for that index, and when the hook holds exactly
  those leaves, the hook's own `tree` must have the same count, root and
  root-making branch nodes. Otherwise the read fails, the tree is dropped and
  the next read rebuilds it, so a validator never signs a root computed from
  IDs that disagree with the hook. Through a multicall, the checks
  (`count`, `root`, `tree` and `message_id_at` of the last leaf) and the
  last pages of IDs that fit beside them are one aggregate, at one state,
  instead of queries that can straddle a block; the hook's `root` must be
  the computed one too, and the state's height at or above the block the
  leaves were counted at, or the read fails and is tried again. The height
  only shows that the state is not older than that block: it does not name a
  block, and the leaves are final by their count, found as before.

## Supported

- `index.mode: sequence` only.
- Mailbox `state_version` 5, MerkleTreeHook 2 and ValidatorAnnounce 2: the
  contracts whose growing state is chunked, which keep deliveries, Merkle
  roots and announced locations out of contract state.
- Security modules: `MessageIdMultisig`, plus the companion repository's
  test-only null ISM. Routing, Aggregation and CCIP-read ISMs return an
  explicit unsupported error.
- A Merkle hook attached either directly or inside a one-level static
  AggregationHook as the Mailbox's required hook.
- One RPC endpoint for indexing and submission. Validators can list several
  endpoints for checkpoint quorum; there is no provider failover.

## Configuration

```
"dusk": {
  "name": "dusk",
  "protocol": "dusk",
  "domainId": <domain>,
  "chainId": <chain id reported by the node>,
  "rpcUrls": [{ "http": "https://<rusk-node>" }],
  "eventCursorDir": "/var/lib/hyperlane/dusk-events",
  "gasLimit": 5000000,
  "gasPrice": 2000,
  "contractQueriesPerSecond": 100,
  "contractQueryBurst": 20,
  "batchContractAddress": "0x93de287b7d349d678a0038fe7772e8960e1a84a1b5da2d0b1f62f532616ce1d4",
  "maxBatchSize": 16,
  "mailbox": "0x<contract id>",
  "interchainGasPaymaster": "0x<contract id>",
  "validatorAnnounce": "0x<contract id>",
  "merkleTreeHook": "0x<contract id>",
  "signer": { "type": "duskKey", "keyFile": "/run/secrets/dusk-signer.key" }
}
```

`gasLimit` is the gas ceiling of every transaction the agent sends, and
defaults to 5,000,000, at most Rusk's block gas limit of 3,000,000,000. A
failed Dusk transaction pays its whole limit, so keep it close to what the
agent needs: a relayed `process` costs about 1.95M gas plus 0.22M per
signature the Dusk ISM checks, and a validator announcement about 2.1M. It
is also the estimate the relayer's gas payment policy compares payments
with, since Rusk reports no gas for a simulation.

`contractQueriesPerSecond` and `contractQueryBurst` pace the agent's
contract queries to its endpoint (see Pacing above); an operator of a node
that serves only its own agents can raise them together with the node's
`[http.policy.global_limits.classes.contract_query]`.

`batchContractAddress` names the Multicall for Dusk contract
([HDauven/dusk-multicall](https://github.com/HDauven/dusk-multicall)) the
agent batches its contract reads through (see Batched reads); on EVM chains
the same setting names Multicall3. Mainnet and testnet run it at
`0x93de287b7d349d678a0038fe7772e8960e1a84a1b5da2d0b1f62f532616ce1d4`. A
contract ID follows the Wasm, the deploy nonce and the owner, not the chain,
so a local chain that deploys the published Wasm with nonce 0 and the
published owner gets the same ID. It has no default: without it, every read
is one query.

`maxBatchSize` (1 by default) is how many prepared deliveries the relayer
submits together; above 1, with a helper that takes `--nonce` and
`--no-wait`, they go as consecutive transactions (see Pipelined
deliveries).

The `duskKey` signer takes exactly one of `keyFile` (a regular file with no
group or world permissions), `keyEnv` (an environment variable name) or `key`
(inline; local development only).

## Event log halts

Contract sync indexes deliveries by their sequence in the delivery log, so
the log never numbers a delivery it has published differently. When the
endpoint's archive changes its finalized history under published
deliveries (an archive rebuilt or repaired with rows it lacked, or another
node's archive with a different history behind the same URL), every read
of the delivery log fails with an error that starts with `CRITICAL` and
names the contract, the topic, the block, the sequence and the rows, and
the agent logs it as a critical error at most every 10 minutes. The
indexer stops; the relayer's delivery checks, which never publish
sequences, read their own log again instead of halting. To recover:

1. Check the endpoint's archive, and use one whose finalized history you
   trust.
2. Stop the agent.
3. Reset its database (the store its contract sync indexed deliveries
   into, such as the scraper's) and its `eventCursorDir`, so that both
   index the archive afresh.
4. Start the agent again.

An archive that only numbers its rows differently, or changes blocks whose
deliveries no consumer has been told of, needs nothing: the log checks the
blocks it holds against the archive and reads changed ones again. The
announcement log is never read by sequence and never halts.

## Shared types

Message, metadata and token-message encoding come from the
`hyperlane-dusk-types` crate, pinned in `Cargo.toml` to an exact
`hyperlane-dusk` commit. Deploy contracts that match that pin. There is no
in-place migration between contract ABI versions.

## Testing

From `rust/main`:

```bash
cargo test -p hyperlane-dusk
cargo test -p hyperlane-base dusk_
cargo test -p hyperlane-base types::multisig
cargo test -p relayer msg::metadata::multisig
cargo test -p validator dusk
cargo test -p validator reorg
cargo clippy -p hyperlane-dusk --all-targets -- -D warnings
```

`cargo test -p hyperlane-dusk --test node_load -- --ignored --nocapture`
measures a restart's catch-up of 10,000 records, with and without a
multicall, in a few minutes. CI runs the others through
`.github/workflows/dusk-agent-gate.yml`. End-to-end tests against a live
node are in the companion repository's `demo/` directory.

## Changes outside this crate

The integration also touches shared code: Dusk connection and signer parsing
in `hyperlane-base`, the `Dusk` protocol in `hyperlane-core`, and validator
startup, checkpoint consensus and reorg reporting. The backward
sequence-aware cursor starts a range above the logs its store already holds
below the gap it fills, so a restart reads only the logs that arrived while
the agent was stopped, instead of the whole chunk below them. It also carries a fix for
upstream checkpoint quorums, which counted signatures over different message
IDs together (`hyperlane-base`'s multisig syncer, `hyperlane-core`'s
checkpoint types and the relayer's message-ID multisig metadata). The
`dusk-proposal-validation.yml` workflow lists every file the fork may change
relative to upstream.
