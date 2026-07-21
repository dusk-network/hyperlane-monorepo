# Dusk Companion Compatibility Manifest

Date: 2026-07-21

This manifest pins the cross-repository set reviewed during the Hyperlane/Dusk
reassessment. Moving branch names are not compatibility evidence.

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `6c2ca1d5514907f6875b6b6729cbffc31e97c09c` | Base of monorepo PR #1 |
| Dusk agent implementation | `37e24eed2c7ad7aed63e3fa033d1fe8a28355ec0` | Finality, bounded history, simulation, transaction reconciliation/provenance, and signer enforcement |
| Dusk base contracts | `8a2467acd5edba5e08cd6b7954f7c3dc622340b5` | Complete deployed-contract compatibility matrix, contract/agent ABI, shared types, and hermetic policy guards used by the default agent gate |
| Stacked withdrawal contracts | `b16af0c05547a5d8e8687f47895c664b1aa93c00` | Payer-owned dispatch-credit withdrawal layer; tested code boundary `265b7e9b1e47f4feadc4e71644d23df04680661c` |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Clean Dusk 1.7.1 build, VM, and live E2E anchor |

The default `dusk-agent-gate.yml` checkout is the exact Dusk base-contract
commit above. A manual workflow input may override it for deliberate
compatibility testing, but a branch-name override is not accepted as final
review evidence. The stacked withdrawal PR adds an event and withdrawal entry
points; the agent does not consume that event, so its shared-type build remains
correctly pinned to the independently mergeable base PR.

Every deployed base contract has an explicit compatibility version. Mailbox,
MerkleTreeHook, TestMock, MessageIdMultisigISM, ValidatorAnnounce, IGP,
ProtocolFee, AggregationHook, WarpNative, WarpDrc20Collateral, and TestRecipient
are version 1; WarpDrc20 is version 2 for its aggregate pending-supply reserve.
The stacked withdrawal layer advances Mailbox to version 2 so a base Mailbox
without `withdraw_dispatch_credit` cannot be reused. Both reuse boundaries
validate the complete 12-contract matrix while retaining semantic kind and
default-ISM checks. These contracts must be freshly deployed. The
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

The post-compatibility-pin rerun passed the same commands against base head
`8a2467acd5edba5e08cd6b7954f7c3dc622340b5`. Durable local log:
`/tmp/hyperlane-monorepo-agent-gate-8a2467a-20260721.log` (SHA-256
`4f087ba4d47c96424a2f2c42d6635378612fc9a404c81e7568160724751d32b4`).

The hosted agent gate disables Cargo incremental output and dev/test debug
information. Those settings do not alter protocol behavior or test selection;
they reduce cold-build disk artifacts after an otherwise healthy hosted run
exhausted the runner filesystem while compiling the test boundary. The job
also reports filesystem capacity so a future capacity regression is explicit
rather than misclassified as a Rust assertion failure.

The refreshed base contract gate passed 95 VM tests, while the combined stacked
gate passed 101, plus 7 data-driver tests and 18 CLI tests. Fresh-state
bidirectional E2E then passed on the exact manifest heads in TestMock run
`1784597325` and MessageIdMultisig run `1784598195`. Both validated the full
saved topology before agent-config generation, confirmed a live one-LUX
dispatch-credit withdrawal, delivered the synthetic/native/collateral routes in
both directions, asserted exact fee/custody/allowance changes, and observed
successful Dusk process simulation. The multisig run produced and consumed a
real signed checkpoint. Combined harness log:
`/tmp/hyperlane-final-e2e-b16af0c-dbed54a.log` (SHA-256
`5d59231d77c1cce8fafa42e1527eecd9ba1d41993b18a8fc947a63257933170d`).

Any source change to the Dusk agent or any deployed versioned contract
invalidates this manifest and requires a new exact pin plus focused and E2E
validation. Documentation-only descendants may cite the implementation commit
above without changing the tested code boundary.
