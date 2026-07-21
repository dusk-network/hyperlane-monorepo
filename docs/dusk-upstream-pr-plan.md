# Dusk Upstream PR Plan

This branch is for internal Dusk review first. Do not open an upstream
Hyperlane PR until the companion Dusk contract/security PR is reviewed and the
remaining production decisions are accepted or changed.

Current base:

- Current Dusk monorepo branch head: see the GitHub PR header.
- Upstream Hyperlane `main`: `6c2ca1d5514907f6875b6b6729cbffc31e97c09c`
- Rebase/check evidence: use the live `git fetch upstream main`,
  `git merge-base HEAD upstream/main`, and
  `git rev-list --left-right --count HEAD...upstream/main` checks recorded in
  the companion Dusk gate reports.
- The final 2026-07-20 refresh rebased the 50-commit feature series from
  `577aa4a82e1082aed35dcde589c9b51bed787478` onto that upstream head. The three
  intervening commits change SVM/warp TypeScript, mainnet infrastructure
  configuration, and TypeScript SDK warp checks; none touches the Dusk Rust
  paths. `git range-diff` pairs all 50 commits exactly and the `rust/main` tree
  remains `18cc899741589bff06b831ff0f2904b7b0997a36` before and after the rebase.
- Post-rebase validation passes the focused Dusk gate, Dusk crate tests,
  package-scoped formatting, and the expanded affected-package cargo check
  against companion base-contract head
  `62464287dce7472f52dcb11ee92ece6631ac9368` and stacked dispatch-withdrawal
  head `b16af0c05547a5d8e8687f47895c664b1aa93c00` (tested code boundary
  `265b7e9b1e47f4feadc4e71644d23df04680661c`).
- Dusk signer test cleanup evidence commit:
  `b989bbcfbb2a427d3a538c5201f5d7214de6ba84`

## Proposed Upstream Shape

1. Agent/protocol support PR against `hyperlane-xyz/hyperlane-monorepo`.
   - Include the Dusk chain crate and Rust agent wiring only.
   - Keep Dusk contract ports, demo scripts, E2E harnesses, and audit notes in
     `dusk-network/hyperlane-dusk`.
2. Follow-up PRs only if Hyperlane reviewers request them.
   - SDK/CLI support for Dusk warp-route operations.
   - CI fixtures or docs that Hyperlane maintainers are willing to own.
   - Any production deployment config after Dusk signer custody is decided.

## Monorepo Scope

The current upstream-facing diff is intentionally limited to Rust agent support:

- `rust/main/chains/hyperlane-dusk/`
- `rust/main/Cargo.toml`
- `rust/main/Cargo.lock`
- `rust/main/hyperlane-base/Cargo.toml`
- `rust/main/hyperlane-base/src/settings/chains.rs`
- `rust/main/hyperlane-base/src/settings/parser/connection_parser.rs`
- `rust/main/hyperlane-base/src/settings/parser/mod.rs`
- `rust/main/hyperlane-base/src/settings/signers.rs`
- `rust/main/hyperlane-base/src/contract_sync/cursors/mod.rs`
- `rust/main/hyperlane-core/src/chain.rs`
- `rust/main/agents/validator/src/reorg_reporter.rs`
- `rust/main/lander/src/adapter/chains/factory.rs`

The Dusk Rust agent crate currently depends on `hyperlane-dusk-types` through
the adjacent companion Dusk repo path (`../../../../../dusk/types`). That keeps
Dusk-specific message/metadata/token encoding shared with the Dusk contract
ports during internal review. An upstream Hyperlane PR should either vendor or
publish that type crate in the shape Hyperlane maintainers prefer, or replace
the path dependency before upstream submission.

## Companion Evidence

The companion Dusk PR is the source of contract/tooling/security evidence:

- `dusk-network/hyperlane-dusk#1`
- Latest evidence includes clean-Rusk TestMock and MessageIdMultisig E2E,
  dirty redeploy guard, metadata corruption, RPC failures, low signer balance,
  duplicate relayers, a 3102-second high-volume restart/backlog soak, and the
  later 7282-second clean-Rusk soak with 280 completed transfers.
- `SECURITY_REVIEW.md` records Dusk-specific security assumptions and
  deviations from Solidity Hyperlane contracts.
- `GOAL_AUDIT.md` maps the original revival goal to concrete artifacts and
  remaining gates.
- `dusk-network/hyperlane-dusk#2` tracks the remaining production sign-off
  decisions before upstream PR preparation.
- Consolidated review entry point for the split decisions:
  https://github.com/dusk-network/hyperlane-dusk/issues/2#issuecomment-4427052555
- Split decision issues `dusk-network/hyperlane-dusk#4` through
  `dusk-network/hyperlane-dusk#9` route the remaining contract-policy,
  signer-custody, CI/repro-runner, and soak-acceptance decisions.

## Upstream Compatibility Review

`docs/dusk-upstream-compatibility-review.md` records the upstream areas checked
at the current base, including recent rate-limited ISM/hook work, nested
trusted relayer ISM fixes, relayer API exposure, and Dusk's explicit
non-support for Routing, Aggregation, CCIP-read, rate-limited hooks, and
rate-limited ISMs in this branch.

## Validation Commands

From `rust/main` in this monorepo:

```bash
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
cargo test -p hyperlane-dusk
cargo test -p hyperlane-base dusk_
cargo fmt --package hyperlane-dusk --package hyperlane-base -- --check
cargo clippy -p hyperlane-dusk --all-targets -- -D warnings
```

The package-scoped formatter is deliberate. `cargo fmt --all` traverses the
adjacent companion Dusk repository because `hyperlane-dusk-types` is a local
path dependency, which would mix formatting from a different PR into this
one. The current companion type crate also has a direct `dusk-bytes`
dependency; `rust/main/Cargo.lock` is kept current so a clean agent-gate
checkout does not rewrite the lockfile.

The fork also includes `.github/workflows/dusk-agent-gate.yml` as a narrow
pull-request status check for the Dusk agent crate. It checks out this
monorepo and the companion `dusk-network/hyperlane-dusk` repo in the same
adjacent layout used locally, scans `rust/main/chains/hyperlane-dusk` for
runtime placeholder macros, and runs:

```bash
cargo fmt --package hyperlane-dusk --package hyperlane-base -- --check
cargo test -p hyperlane-dusk
cargo test -p hyperlane-base dusk_
cargo clippy -p hyperlane-dusk --all-targets -- -D warnings
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
```

The workflow needs `DUSK_ORG_READ_TOKEN` because the companion Dusk repo is
private. It preflights that private repo access with `gh api` before checkout
so missing token provisioning fails explicitly. Its default companion checkout
is an exact reviewed Dusk commit rather than a moving branch; the manual
`workflow_dispatch` input is the only intentional override. This pin is part of
the contract/agent ABI compatibility record, not merely build reproducibility.
A separate policy-gate step
diffs the branch against live Hyperlane upstream and rejects changes outside
the reviewed Dusk integration allowlist. Together these are the fork-scoped
validation substitute for Hyperlane-owned infrastructure, not a replacement
for the companion Dusk E2E evidence.

Production indexers now require an archive-enabled Rusk endpoint. Canonical
`LogMeta` is derived by matching contract state to the archived event's source,
topic, in-block ordinal, exact serialized payload, transaction origin, and
block hash. Missing archive data fails closed; zero provenance is not emitted
because the scraper can filter it and still advance its cursor.

All sequence indexers are additionally capped at Rusk's consensus-finalized
height. Merkle insertion records and validator checkpoints come from the
configured MerkleTreeHook's persisted message/height/root history and exact
archived event; Mailbox dispatch is not used as a proxy for hook execution.
The companion contracts carrying this history require a fresh deployment and
must match the complete per-contract compatibility-version matrix recorded in
`dusk-companion-compatibility.md`.

The indexer API also supports canonical transaction-hash lookups. Dusk's
32-byte transaction ID is represented as a zero-left-padded common `H512`;
lookups reject noncanonical padding, resolve the containing block through Rusk,
binary-search the contract sequence interval for that block, and filter on the
archived transaction origin. Dusk configuration rejects block index mode and
retains shared operation-submission queue limits.

The inherited upstream Rust agent and monorepo image workflows are guarded to
run only when `github.repository_owner == 'hyperlane-xyz'`. The inherited
Depot-backed PR jobs in `rust.yml`, `test.yml`, and `rebalancer-e2e-test.yml`
use the same guard so internal Dusk PRs do not stay queued on Hyperlane-owned
runner labels. That avoids Dusk-fork PR failures or indefinite queued checks on
Hyperlane-owned GitHub App, Depot, and image-publishing infrastructure while
preserving the upstream workflows for the eventual Hyperlane PR path.

From the companion Dusk repo:

```bash
make repro-check
make repro-check-agent
make all
cargo test -p hyperlane-dusk-types
cargo test -p hyperlane-dusk-integration-tests
cargo test -p dusk-tx
make secret-hygiene
```

The companion E2E and stress commands are recorded in its `TEST_REPORT.md`
with exact run IDs and artifact paths.

`docs/dusk-companion-compatibility.md` is the authoritative exact-head manifest
for the reassessed agent, base contracts, stacked withdrawal, Rusk, and
fresh-deployment boundary.

## Blockers Before Upstream

- Dusk reviewers must accept or change the production decisions in
  `SECURITY_REVIEW.md`.
- Dusk must decide production signer custody and CI artifact policy.
- Dusk must resolve the production sign-off tracker in
  `dusk-network/hyperlane-dusk#2`.
- Dusk must resolve or replace split decision issues #4 through #9 before the
  upstream PR text can accurately describe accepted Dusk production
  assumptions.
- This fork keeps upstream `.github/CODEOWNERS` routing `rust/` to Hyperlane's
  `@tkporter`; the companion Dusk repo has no `CODEOWNERS`, `OWNERS`, or
  `MAINTAINERS` file. Internal Dusk agent/runtime review is still requested
  from `Neotamandua`, based on recent Rusk HTTP/RUES/GraphQL route ownership,
  because the remaining decisions are Dusk/Rusk-specific and not covered by
  upstream Hyperlane ownership alone.
- Dusk must decide CI/repro runner strategy for the private Rusk-dependent
  companion checks. The companion repo now has `make repro-check` and
  `make repro-check-agent` for the repeatable local non-E2E subset, plus a
  manual self-hosted workflow template for Dusk-runner reproduction. Dusk still
  needs to provide the runner and private checkout secret before this can
  replace local evidence.
- Internal Dusk PRs must be reviewed first.
- Upstream PR text should explicitly state that Dusk contract ports and E2E
  scripts live in `dusk-network/hyperlane-dusk`, not this monorepo.
