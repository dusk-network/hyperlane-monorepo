# Dusk Companion Compatibility Manifest

Date: 2026-07-20

This manifest pins the cross-repository set reviewed during the Hyperlane/Dusk
reassessment. Moving branch names are not compatibility evidence.

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `6c2ca1d5514907f6875b6b6729cbffc31e97c09c` | Base of monorepo PR #1 |
| Dusk agent implementation | `37e24eed2c7ad7aed63e3fa033d1fe8a28355ec0` | Finality, bounded history, simulation, transaction reconciliation/provenance, and signer enforcement |
| Dusk base contracts | `4d8f5da013d56e5d3fa036ab924de6a6729b5f4f` | Contract/agent ABI and shared types used by the default agent gate |
| Stacked withdrawal contracts | `183b56a875e5c2962ef621937258b8e497baef2a` | Optional payer-owned dispatch-credit withdrawal layer |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Clean Dusk 1.7.1 build, VM, and live E2E anchor |

The default `dusk-agent-gate.yml` checkout is the exact Dusk base-contract
commit above. A manual workflow input may override it for deliberate
compatibility testing, but a branch-name override is not accepted as final
review evidence. The stacked withdrawal PR adds an event and withdrawal entry
points; the agent does not consume that event, so its shared-type build remains
correctly pinned to the independently mergeable base PR.

MerkleTreeHook and WarpNative expose `state_version() == 1`; WarpDrc20 exposes
version 2 for its aggregate pending-supply reserve. The stacked withdrawal
layer gives Mailbox version 1. These contracts must be freshly deployed. The
agent depends on bounded `message_ids` and `gas_payments` pages, coherent
`validators_and_threshold`, hook insertion-height/root history, and Rusk's
transaction simulation endpoint. Older serialized instances are incompatible;
this reassessment does not claim an in-place migration.

Validation at the agent implementation commit passed:

- `cargo test -p hyperlane-dusk`: 19 passed;
- `cargo test -p hyperlane-base dusk_`: 7 passed;
- `cargo clippy -p hyperlane-dusk --all-targets -- -D warnings`;
- package-scoped formatting; and
- `cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p
  scraper -p lander`.

The base contract gate passed 94 VM tests, while the combined stacked gate
passed 100. Fresh-state bidirectional E2E passed against this exact
implementation set in TestMock run `1784592169` and MessageIdMultisig run
`1784592942`. Both runs delivered the synthetic, native, and collateral routes
in both directions, asserted exact custody/allowance changes, exercised a live
one-LUX route-credit withdrawal before dispatch, and observed successful Rusk
process simulation. The multisig run additionally produced and consumed real
signed checkpoint metadata.

Any source change to the Dusk agent or the three state-versioned contracts
invalidates this manifest and requires a new exact pin plus focused and E2E
validation. Documentation-only descendants may cite the implementation commit
above without changing the tested code boundary.
