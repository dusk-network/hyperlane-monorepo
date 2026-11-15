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
  node must be built with the `archive` feature. Response bodies are cut to
  1 KiB before they appear in errors or logs.
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
  not is reported as an endpoint fault. Each scan starts from a saved position just before the block,
  kept in `eventCursorDir` (about one per 200 rows), so a restart or an archive
  that lags behind finality does not send the scan back to the contract's
  first row. Before reusing a position, the scan checks that the archive still
  has the row it was taken from, unchanged and below the block the position
  names. If the archive returns another row there, it was rebuilt: the
  contract's positions are dropped and the scan starts over. If the row is
  missing, the scan asks the archive (`checkBlock(onlyFinalized)`) whether it
  has finalized that row's block, which Rusk records together with the
  block's rows. If it has, its rows are numbered differently and the scan
  starts over; if not, the archive is behind, and the lookup fails until it
  catches up, keeping the positions.
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
- **Identity checks.** On startup the agent checks that the node's chain ID
  and the configured contracts agree with each other: the Mailbox domain and
  `state_version`, and the ValidatorAnnounce and Merkle hook bindings to that
  Mailbox. A Mailbox below `state_version` 4 is refused with an explicit
  error. Startup waits for the node instead of failing when it is unreachable.
- **Checkpoints** are read at consensus finality, adjusted by the configured
  reorg period.

## Supported

- `index.mode: sequence` only.
- A Mailbox that stores dispatched message IDs (`state_version` 4 or later).
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
  "gasLimit": 30000000,
  "gasPrice": 2000,
  "mailbox": "0x<contract id>",
  "interchainGasPaymaster": "0x<contract id>",
  "validatorAnnounce": "0x<contract id>",
  "merkleTreeHook": "0x<contract id>",
  "signer": { "type": "duskKey", "keyFile": "/run/secrets/dusk-signer.key" }
}
```

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
startup, checkpoint consensus and reorg reporting. The
`dusk-proposal-validation.yml` workflow lists every file the fork may change
relative to upstream.
