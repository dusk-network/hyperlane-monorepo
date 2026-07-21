# Dusk companion compatibility manifest

Date: 2026-07-21

This manifest is the cross-repository authority for the reopened Hyperlane/Dusk
reassessment. Branch names and evidence from earlier heads are regression
history only; they are not release evidence for this candidate.

## Exact candidate set

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `3811ba9961d4110674c166ca02cfe7e6e94fc932` | Current `hyperlane-xyz/hyperlane-monorepo` base merged into this fork candidate |
| Dusk agent production source | `c4597e01418f117c4779336b70a8b9274a22c967` | Dusk protocol adapter, bounded/authenticated indexers, redacted signer/config parsing, and validator fail-stop integration; later changes are workflow, documentation, or test-assertion only |
| Dusk agent review head | Resolve live monorepo PR #1 head | Moving review head; use the exact hosted-check SHA from the PR, not this label, as release evidence |
| Dusk base tested code | `f6be24a411f2a0a247b8d1b798106c37449f7dcf` | Escrow, dispatch-credit custody/consumption, authenticated route funding, Mailbox reentrancy guard, canonical DRC20 boundary, deployment and E2E harness |
| Dusk base review head | Resolve live PR #1 head | Moving review head; tested runtime remains the exact immutable commit above |
| Stacked withdrawal tested code | `4ed5734816287a9d08bc8bdaf87d000afb38b5f9` | Beneficiary-authorized dispatch-credit withdrawal, route-owner proxy methods, and prepared-transaction hash reporting, stacked on the exact tested base code |
| Stacked withdrawal review head | Resolve live PR #10 head | Moving review head; tested runtime remains the exact immutable commit above |
| Manual reproduction policy | `da4db7e62234bf1a9a0c9033a7520fc37fab85a3` | Exact-ref dispatcher, protected environment, and distinct manual evidence check contexts; live execution still needs the provisioned runner and read token |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Frozen clean Dusk 1.7.1 dependency, VM, archive API, and live-node boundary |

The default `dusk-agent-gate.yml` companion checkout is exact base-contract
commit `f6be24a411f2a0a247b8d1b798106c37449f7dcf`. A manual workflow input may deliberately select another exact
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

Endpoint cursors and row IDs remain process-local scan hints, independently
tracked per topic; they never become durable authority. The exclusive RocksDB
under `eventCursorDir` stores only the exact requested row after its direct
contract state and finalized block have been authenticated. Durable v2 keys
bind contract, topic, and local sequence; page peers and legacy page-derived
v1 entries are ignored. On restart the scanner performs page-bounded replay to
recover its remote position. A caught-up page remains temporary and is polled
again for later events.

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

At the current review head, whose production Dusk source remains the runtime
commit above, the focused agent boundary passed:

- `cargo test -p hyperlane-dusk`: 30 passed;
- `cargo test -p hyperlane-base --lib dusk_`: 9 passed;
- `cargo test -p validator`: 27 passed;
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

The fork was first rebased onto `67933966`; `git range-diff` marked all 79 Dusk
commits patch-equivalent. During the final 2026-07-21 review, upstream advanced
again to `3811ba9961d4110674c166ca02cfe7e6e94fc932`. That two-commit delta changes
only six TypeScript warp-check files. It was merged without conflict, leaving
the Dusk Rust production tree and immutable companion types pin unchanged.
Immediately after that merge the feature branch was 89 commits ahead and zero
behind, and the focused tests, clippy, lockfile check, and complete affected
Rust package check above passed again. Hosted and adversarial evidence must use
the final live PR head containing this documentation, not the pre-documentation
merge commit.

Fresh independent GPT-5.6 xhigh and Controlecentrum deep/xhigh reviews must
target these frozen source heads. Any source change after those reviews
invalidates the affected evidence and requires a new pin and proportionate
rerun.
