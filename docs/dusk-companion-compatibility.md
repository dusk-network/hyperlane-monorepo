# Dusk Companion Compatibility Manifest

Date: 2026-07-21

This manifest pins the cross-repository set reviewed during the Hyperlane/Dusk
reassessment. Moving branch names are not compatibility evidence.

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `669d966ad71582fe3c9d96b5ed1b8ea3724e07fe` | Synchronized base of monorepo PR #1 and `dusk-network/hyperlane-monorepo` `main` |
| Dusk agent implementation | `23df1ec7c0211b0178079f12a4a5b4057463a363` (runtime anchor `37e24eed2c7ad7aed63e3fa033d1fe8a28355ec0`) | Upstream-synchronized finality, bounded history, simulation, transaction reconciliation/provenance, signer enforcement, and exact companion-gate pin |
| Dusk base contracts | `75293b9695dbca8dd8c36024b285fb38c099e78f` (code anchor `d32c0f56c66d93be203cc44e3f48a0a7257216f0`) | Complete deployed-contract compatibility matrix, fail-closed IGP policy, contract/agent ABI, shared types, and hermetic policy guards used by the default agent gate |
| Stacked withdrawal contracts | `ab3352f7c5bb1d97ac2ec397c332fca7fd3a16f9` (code anchor `54587f9267a6f26d2a7127288f9587d877ee3b62`) | Payer-owned dispatch-credit withdrawal layer, base deep-review remediations, and fail-closed IGP policy |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Clean Dusk 1.7.1 build, VM, and live E2E anchor |

The default `dusk-agent-gate.yml` checkout is the exact Dusk base-contract
commit above. A manual workflow input may override it for deliberate
compatibility testing, but a branch-name override is not accepted as final
review evidence. The stacked withdrawal PR adds an event and withdrawal entry
points; the agent does not consume that event, so its shared-type build remains
correctly pinned to the independently mergeable base PR.

Every deployed base contract has an explicit compatibility version. Mailbox,
MerkleTreeHook, TestMock, MessageIdMultisigISM, ValidatorAnnounce, ProtocolFee,
AggregationHook, WarpNative, WarpDrc20Collateral, and TestRecipient are version
1. WarpDrc20 is version 2 for its aggregate pending-supply reserve, and IGP is
version 2 for explicit fail-closed destination pricing. The stacked withdrawal
layer additionally advances Mailbox to version 2 so a base Mailbox without
`withdraw_dispatch_credit` cannot be reused. Both reuse boundaries validate the
complete 12-contract matrix while retaining semantic kind, live default-ISM,
and exact saved/live IGP configuration checks. These contracts must be freshly
deployed. The
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

The upstream-synchronized gate rerun passed the same commands at agent/CI
anchor `23df1ec7c0211b0178079f12a4a5b4057463a363` against exact Dusk base head
`75293b9695dbca8dd8c36024b285fb38c099e78f`. Durable local log:
`/tmp/hyperlane-monorepo-agent-gate-23df1ec-20260721.log` (SHA-256
`044724c63cf1fa6c306d8619113d108f41808f211318094c807183462697e883`).

The hosted agent gate disables Cargo incremental output and dev/test debug
information. Those settings do not alter protocol behavior or test selection;
they reduce cold-build disk artifacts after an otherwise healthy hosted run
exhausted the runner filesystem while compiling the test boundary. The job
also reports filesystem capacity so a future capacity regression is explicit
rather than misclassified as a Rust assertion failure.

The refreshed base contract gate passed 99 VM tests, 5 data-driver tests, and
17 CLI tests. The combined stacked gate passed 105 VM tests, 7 data-driver
tests, and 19 CLI tests. Its log is
`/tmp/hyperlane-dusk-withdrawal-repro-54587f9.log` (SHA-256
`df5d8272b47a341473660b547a77e69c1959928cb552c0b812db52f49cb5ecdb`).

Earlier fresh-state bidirectional E2E passed in TestMock run
`1784597325` and MessageIdMultisig run `1784598195`. Both validated the full
saved topology before agent-config generation, confirmed a live one-LUX
dispatch-credit withdrawal, delivered the synthetic/native/collateral routes in
both directions, asserted exact fee/custody/allowance changes, and observed
successful Dusk process simulation. The multisig run produced and consumed a
real signed checkpoint. Combined harness log:
`/tmp/hyperlane-final-e2e-b16af0c-dbed54a.log` (SHA-256
`5d59231d77c1cce8fafa42e1527eecd9ba1d41993b18a8fc947a63257933170d`).
Those runs predate IGP compatibility version 2 and are historical evidence for
their pinned heads; the final exact heads require a fresh two-mode live run.

Any source change to the Dusk agent or any deployed versioned contract
invalidates this manifest and requires a new exact pin plus focused and E2E
validation. Documentation-only descendants may cite the implementation commit
above without changing the tested code boundary.
