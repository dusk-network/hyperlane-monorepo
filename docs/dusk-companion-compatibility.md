# Dusk companion compatibility manifest

Date: 2026-07-20

This manifest is the cross-repository authority for the reopened Hyperlane/Dusk
reassessment. Branch names and evidence from earlier heads are regression
history only; they are not release evidence for this candidate.

## Exact candidate set

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `669d966ad71582fe3c9d96b5ed1b8ea3724e07fe` | Current `hyperlane-xyz/hyperlane-monorepo` base; the fork was fetched and is not behind it |
| Dusk agent runtime | `af957a9fc814fa7533aadf997104863306eed645` | Dusk protocol adapter, indexers, signer/config parsing, relayer and validator integration |
| Dusk base tested code | `876848ecc6c671995fad3ae7b22843e68a3ce8ca` | Escrow, dispatch-credit custody/consumption, canonical DRC20 boundary, deployment and E2E harness |
| Dusk base review head | `ed98f5358d17dabc50c5d47743462df63bcd53bc` | Tested code plus the authoritative decision, immutable-evidence hygiene, proposal validation, and readiness-policy record |
| Stacked withdrawal tested code | `b28d575527421d2a67245921ce561c88f554c099` | Beneficiary-authorized dispatch-credit withdrawal and route-owner proxy methods, stacked on the exact tested base code |
| Stacked withdrawal review head | `99aef2999ece7ee632377cf714716e4bd193bb53` | Tested stack plus the synchronized base decision, proposal validation, and immutable-evidence hygiene record |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Frozen clean Dusk 1.7.1 dependency, VM, archive API, and live-node boundary |

The default `dusk-agent-gate.yml` companion checkout is the exact base-contract
commit above. A manual workflow input may deliberately select another exact
reference, but a moving branch is not accepted as compatibility evidence.

## Contract decisions

Canonical DRC20 compatibility is pinned to
`dusk-network/contracts@bc1b00ee0af059975e158b7b580b4d0c0f1bdf9f`.
The collateral route requires an exact custody increase from `transfer_from`;
fee-on-transfer or otherwise non-exact assets require a different route design.
Compatibility is tested against the pinned upstream archive bytes and against
the real upstream `drc20-roles-pausable` WASM in the Dusk VM.

Inbound escrow remains non-custodial. Invalid recipients reserve native or
collateral custody, or reserved synthetic supply, until the beneficiary proves
the appropriate Moonlight or contract identity. No owner expiry, redirect, or
global drain is introduced. The accepted tradeoff is that a permanently
invalid recipient can reserve funds indefinitely.

Dispatch credit is beneficiary-keyed prepaid native DUSK custody. Funding is
permissionless, but the funder receives no dispatch or reclaim authority.
Dispatch consumes only the encoded sender's balance. The base PR owns custody
and consumption; the stacked PR adds the only exit, authorized by the
beneficiary identity, plus owner-authorized route proxy methods for credit
owned by each route contract. There is no Mailbox-owner global drain.

Fresh-deployment versions are:

| Contract | Base PR | Withdrawal stack |
| --- | ---: | ---: |
| WarpDrc20 | 3 | 4 |
| WarpDrc20Collateral | 2 | 3 |
| WarpNative | 1 | 2 |
| IGP | 2 | 2 |
| Other deployed contracts | 1 | 1 |

Mixed base/stack route deployments are rejected. No in-place state migration
is claimed.

## Agent decisions

Every Dusk chain requires separate explicit `domainId` and native `chainId`
values plus an agent-exclusive `eventCursorDir`. Startup validates the endpoint
chain ID and both Mailbox and ValidatorAnnounce `local_domain` values before a
provider is returned. Every submission passes the configured native chain ID
to `dusk-tx`; a mismatch is rejected before signer access or transaction
construction.

Dusk signer material is parsed as a BLS scalar, not as a generic address. It
must decode from hex, base58, or bech32 to exactly 32 bytes and must be a valid
nonzero canonical scalar. EVM/Tron address padding and noncanonical scalars are
rejected.

Mailbox and MerkleTreeHook adapters share query helpers but expose their own
contract identities. Checkpoints bind to the hook address. Validator
announcements use the existing 20-byte `H160` directly, and relayer location
discovery queries each validator independently with the contract's 16-location
and 1,024-byte-per-location bounds. One bad validator cannot poison the whole
quorum lookup.

The archive path uses Rusk 1.7.1's contract-scoped
`finalizedEvents(contractId, limit, cursor)` API with pages capped at 16 rows.
Each row supplies its own event ID, height, block hash, transaction origin,
source, topic, data, and reverted flag. State-derived height/data is matched to
that row and `checkBlock(..., onlyFinalized: true)` validates its finalized
block. Whole-block event buffering is not used.

Opaque cursors, per-topic sequence counts, and row provenance are stored in an
exclusive RocksDB under `eventCursorDir`. Each page and its cursor are written
in one synchronous atomic batch. Rows are keyed by contract, topic, and
sequence, so the store does not rewrite or deserialize an ever-growing JSON
history. A caught-up page is temporary: later events continue from the last
opaque cursor.

Current Rusk simulation accepts an ordinary signed transaction whose bytes can
be replayed. The agent therefore performs bounded local payload preparation and
uses the configured conservative gas ceiling; it does not send a signed
simulation to a remote endpoint. After propagation starts, non-success is an
unknown outcome and the exact locally computed transaction hash is retained
for ledger reconciliation.

On validator root mismatch, the fail-stop reorg flag is written before any
best-effort RPC diagnostics. Diagnostics have per-endpoint timeouts and retain
requested and actually observed heights separately.

## E2E boundary

The `messageIdMultisig` case deploys a real MessageIdMultisig ISM on both Dusk
and EVM and starts one validator for each origin. Dusk-to-EVM delivery must
therefore consume a Dusk-origin checkpoint and exercise Dusk
ValidatorAnnounce, finalized-event indexing, Merkle hook checkpointing,
checkpoint sync, relayer metadata, and EVM verification.

Both E2E security modes must run the synthetic, native, and canonical
DRC20-collateral routes in both directions. The stacked run must additionally
prove payer isolation, beneficiary-authorized withdrawal, route-owned proxy
withdrawal, remaining-credit consumption, exact custody, allowance use, and
secret/archive hygiene.

The live harness assigns different pre-funded Anvil accounts to the operator,
relayer, and validator. Both EVM and Dusk multisig ISMs consume the identical
validator address and threshold. Operator withdrawal and other setup complete
before either validator starts, so the test cannot manufacture EVM or Dusk
nonce races by sharing a signer across concurrent roles.

## Current local validation

At the runtime commit above, the focused agent boundary passed:

- `cargo test -p hyperlane-dusk`: 22 passed;
- `cargo test -p hyperlane-base --lib dusk_`: 9 passed;
- `cargo test -p validator reorg`: 2 passed;
- `cargo clippy -p hyperlane-dusk --all-targets -- -D warnings`; and
- `cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander`.

The base gate passed 12 WASMs, clippy, 29 type tests, 108 VM tests, 17 helper
tests, 5 driver tests, hygiene, and the full agent compile boundary. The stack
gate passed 114 VM tests, 19 helper tests, 7 driver tests, release driver WASM,
and the same remaining boundaries. Their log SHA-256 values are respectively
`b4d3864dfb178adc283e8a3cc6f137c4c9580525b4bd1ffb07d7ef9a0bdbdedd`
and `314ff8b12204be6dcf9055ce9917013d47e6c93d4adfca88b6e53c55e0434ec6`.

At the exact stack code anchor, TestMock run `1784629402` and
MessageIdMultisig run `1784628130` each passed beneficiary withdrawal plus
synthetic, native, and canonical DRC20 routes in both directions. The multisig
logs contain only validator `0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC`
through checkpoint index 2; neither run contains `nonce too low`, leaked signer
material, an orphan agent, or a surviving test listener. The harness log hashes
are `c155747f8d49beb16e8cf005c3bca77eff62b3d3fe0c3e86fa4737a8ca3b0540`
and `d6d9100b3f306662000d5d865d849f492bf1810f88c245a53fda998843898df6`.

Fresh independent GPT-5.6 xhigh and Controlecentrum deep/xhigh reviews must
target these frozen source heads. Any source change after those reviews
invalidates the affected evidence and requires a new pin and proportionate
rerun.
