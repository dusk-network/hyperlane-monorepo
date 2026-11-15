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
- **Indexing** uses finalized contract events only. Each event is matched to
  its block and transaction, and stored in the agent's `eventCursorDir`, which
  every agent process needs its own copy of. The Mailbox stores only the ID of
  each dispatched message (`dispatched_id`); the message itself comes from the
  finalized `Dispatch` event whose message hashes to that ID.
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
- **Block cache.** A scan reads the whole block it is looking in, and the
  block's rows stay in memory, so other lookups in the same block (a cursor
  with a small `index.chunk`, or one lookup per transaction) match against
  them instead of reading the archive again. Every lookup still checks
  contract state and `checkBlock` itself. Only blocks whose rows carry the
  hash `checkBlock(onlyFinalized)` accepted are kept, at most 64 blocks and
  64 MiB, and 32 MiB for one block; a larger block is read per lookup. The
  Merkle hook, dispatch and IGP indexers look a transaction up by the
  records its own rows name, instead of every record of its block.
- **Writes** are Moonlight transactions built and signed by the `dusk-tx`
  helper from the companion repository. The agent runs it as a subprocess
  (`DUSK_TX_BIN`, or `dusk-tx` on `PATH`) and passes the RPC URL and signing
  key on stdin. Before retrying an uncertain submission, the agent reconciles
  it against the transaction hash it prepared.
- **Delivery simulation.** Dusk charges a failed transaction its whole gas
  limit. Before submitting `process`, the relayer runs it as an unsigned
  contract query, which Rusk executes in a throwaway session. If the query
  fails, or replies with anything other than the encoding of the `()` that
  `process` returns, nothing is submitted and the message is retried later.
  The query reports no gas use, so `gasLimit` stays the transaction ceiling.
- **Delivery and announcement finality.** Contract queries read the node's
  tip, which Rusk can roll back until a block is final, and Rusk 1.7.3 has
  no query of contract state at the last finalized block. The relayer stops
  retrying a message once the Mailbox reports it delivered, so `delivered`
  reports a delivery the tip holds only once no block above the last
  finalized one emitted its `ProcessId` event, whoever delivered it. The
  check reads `lastBlockPair` before the state and then the blocks above the
  last finalized one (`blocks`), which must still hold that tip, so a
  rollback in between is noticed; each block's events come from the archive
  (`contractEvents`). Until the delivery is final, the check fails and the
  relayer tries again later with its usual backoff; a rolled-back delivery
  reads as not delivered, and the message is relayed again. A validator's
  startup waits the same way until its announcement is final. When finality
  falls more than 64 blocks behind the tip, the checks wait for it to catch
  up instead of reading every block.
- **Identity checks.** On startup the agent checks that the node's chain ID
  and the configured contracts agree with each other: the Mailbox and
  ValidatorAnnounce domains, each contract's `state_version`, and the
  ValidatorAnnounce and Merkle hook bindings to that Mailbox. The agent
  accepts only the state versions it reads (Mailbox 5, MerkleTreeHook 2,
  ValidatorAnnounce 2) and refuses any other with an explicit error, since
  another version lacks queries the agent uses. Startup waits for the node
  instead of failing when it is unreachable.
- **Checkpoints** are read at consensus finality, adjusted by the configured
  reorg period.

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
  "mailbox": "0x<contract id>",
  "interchainGasPaymaster": "0x<contract id>",
  "validatorAnnounce": "0x<contract id>",
  "merkleTreeHook": "0x<contract id>",
  "signer": { "type": "duskKey", "keyFile": "/run/secrets/dusk-signer.key" }
}
```

`gasLimit` is the gas ceiling of every transaction the agent sends, and
defaults to 5,000,000. A failed Dusk transaction pays its whole limit, so
keep it close to what the agent needs: a relayed `process` costs about 1.95M
gas plus 0.22M per signature the Dusk ISM checks, and a validator
announcement about 2.1M.

The `duskKey` signer takes exactly one of `keyFile` (a regular file with no
group or world permissions), `keyEnv` (an environment variable name) or `key`
(inline; local development only).

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

CI runs these through `.github/workflows/dusk-agent-gate.yml`. End-to-end
tests against a live node are in the companion repository's `demo/`
directory.

## Changes outside this crate

The integration also touches shared code: Dusk connection and signer parsing
in `hyperlane-base`, the `Dusk` protocol in `hyperlane-core`, and validator
startup, checkpoint consensus and reorg reporting. It also carries a fix for
upstream checkpoint quorums, which counted signatures over different message
IDs together (`hyperlane-base`'s multisig syncer, `hyperlane-core`'s
checkpoint types and the relayer's message-ID multisig metadata). The
`dusk-proposal-validation.yml` workflow lists every file the fork may change
relative to upstream.
