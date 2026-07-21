# Dusk companion compatibility manifest

Date: 2026-07-20

This manifest is the cross-repository authority for the reopened Hyperlane/Dusk
reassessment. Branch names and evidence from earlier heads are regression
history only; they are not release evidence for this candidate.

## Exact candidate set

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `67933966ed9c6f9e3d5ec095372e11414c82e4e7` | Current `hyperlane-xyz/hyperlane-monorepo` base; the fork is 0 commits behind it |
| Dusk agent runtime | `e95d3ea282a55ead114471ffb1dece77706ffc81` | Rebased equivalent of tested runtime `af957a9fc814fa7533aadf997104863306eed645`; Dusk protocol adapter, indexers, signer/config parsing, relayer and validator integration |
| Dusk base tested code | `9058755927473239d59ce702a8074acbae0e0a24` | Escrow, dispatch-credit custody/consumption, Mailbox reentrancy guard, canonical DRC20 boundary, deployment and E2E harness |
| Dusk base review head | Resolve live PR #1 head | Moving review head; tested runtime remains the exact immutable commit above |
| Stacked withdrawal tested code | `dc8aba07773993878edd81735d59e66beddd66a3` | Beneficiary-authorized dispatch-credit withdrawal and route-owner proxy methods, stacked on the exact tested base code |
| Stacked withdrawal review head | Resolve live PR #10 head | Moving review head; tested runtime remains the exact immutable commit above |
| Static covered monorepo checkout | `833b77b4436e146a4776a3b35db68525014b3adb` | Rebased equivalent of clean-repro checkout `b4c46ce9bdade2590018facaa51255d497a80db2` |
| Review-policy boundary anchor | `c35f86405cf8cd83927860aca8b5c38b042ee198` | Rebased equivalent of `dad14dbbea4bbd59f6c6697f89cc245d5c1cf2a0`; adds validator fail-stop source files to the fork boundary |
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
| Mailbox | 2 | 3 |
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

The base gate passed 13 WASMs, clippy, 29 type tests, 109 VM tests, 17 helper
tests, 5 driver tests, hygiene, and the full agent compile boundary. The stack
gate passed 115 VM tests, 19 helper tests, 7 driver tests, release driver WASM,
and the same remaining boundaries. Their log SHA-256 values are respectively
`16ac8e62d2d8c5952a9363c90f77e15ff756043a102302780f0f9e272a166d62`
and `03de4d4e1597c8136e9a00bbb74e7fbbe290b5b2fa3e8cb8d82e004a31f640fb`.

At the exact stack code anchor, TestMock run `1784638666` and
MessageIdMultisig run `1784639741` each passed beneficiary withdrawal plus
synthetic, native, and canonical DRC20 routes in both directions. The multisig
logs contain only validator `0x3C44CdDdB6a900fa2b585dd299e03d12FA4293BC`
through checkpoint index 2; neither run contains `nonce too low`, leaked signer
material, an orphan agent, or a surviving test listener. The harness log hashes
are `a195ea9f8c7e47e8c27c2e1ad728d83b9af0a23ff2ca79c59b52fd37fe5683bc`
and `9d7a6db2e3599591c8c364d44187a61f9bb7b30b3067c058812fa2480cef85c9`.

The sequential live fault suite also passed dirty-redeploy refusal, validator
delay, corrupt-checkpoint rejection/recovery, low-signer rejection/recovery,
both RPC outage directions, duplicate-relayer one-delivery stability, and a
five-transfer-per-direction relayer restart/backlog run. Its aggregate log is
`/tmp/hyperlane-stack-fault-e2e-dc8aba0-6ef326b.log`, SHA-256
`6841c405430020027665ba37d282cf63724b32605407e40ab68b2318a7b0378b`.

After upstream advanced by one SVM/TypeScript-only commit, the fork was rebased
onto `67933966`. `git range-diff` marked all 79 Dusk commits patch-equivalent;
the old-to-new tree delta contains only that upstream SVM/TypeScript change.
The complete affected Rust package cargo check passed again at the rebased
head.

Fresh independent GPT-5.6 xhigh and Controlecentrum deep/xhigh reviews must
target these frozen source heads. Any source change after those reviews
invalidates the affected evidence and requires a new pin and proportionate
rerun.
