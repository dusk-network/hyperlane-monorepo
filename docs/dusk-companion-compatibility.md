# Dusk Companion Compatibility Manifest

Date: 2026-07-20

This manifest pins the cross-repository set reviewed during the Hyperlane/Dusk
reassessment. Moving branch names are not compatibility evidence.

| Component | Exact reference | Role |
| --- | --- | --- |
| Hyperlane upstream base | `6c2ca1d5514907f6875b6b6729cbffc31e97c09c` | Base of monorepo PR #1 |
| Dusk agent implementation | `bcb68d4d4973bca212f80f8abd2cf84baa7a9afb` | Finality, hook provenance, transaction observation, and signer hardening |
| Dusk base contracts | `726040440c904ec6adf6616a1963146ee9693fe4` | Contract/agent ABI and shared types used by the default agent gate |
| Stacked withdrawal contracts | `6b3a17845a3ff206bd830383d7d353ee94a34667` | Optional payer-owned dispatch-credit withdrawal layer |
| Rusk | `5c6a0bab11c61fb4c81275afdeceb97fb942d85e` | Clean Dusk 1.7.1 build, VM, and live E2E anchor |

The default `dusk-agent-gate.yml` checkout is the exact Dusk base-contract
commit above. A manual workflow input may override it for deliberate
compatibility testing, but a branch-name override is not accepted as final
review evidence. The stacked withdrawal PR adds an event and withdrawal entry
points; the agent does not consume that event, so its shared-type build remains
correctly pinned to the independently mergeable base PR.

MerkleTreeHook, WarpDrc20, and WarpNative expose `state_version() == 1` and
must be freshly deployed. The agent depends on the v1 Merkle history queries
`message_id_at`, `inserted_block_height`, and `root_at`. Older serialized
instances are incompatible; this reassessment does not claim an in-place
migration.

Validation at the agent implementation commit passed:

- `cargo test -p hyperlane-dusk`: 15 passed;
- `cargo test -p hyperlane-base dusk_`: 7 passed;
- `cargo clippy -p hyperlane-dusk --all-targets -- -D warnings`;
- package-scoped formatting; and
- `cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p
  scraper -p lander`.

The base contract gate passed 93 VM tests, while the combined stacked gate
passed 98. Bidirectional TestMock and MessageIdMultisig agent E2E evidence is
recorded against this set after the remote heads and deployment are pinned.

Any source change to the Dusk agent or the three state-versioned contracts
invalidates this manifest and requires a new exact pin plus focused and E2E
validation. Documentation-only descendants may cite the implementation commit
above without changing the tested code boundary.
