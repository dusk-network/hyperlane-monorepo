# Dusk Upstream PR Plan

This branch is for internal Dusk review first. Do not open an upstream
Hyperlane PR until the companion Dusk contract/security PR is reviewed and the
remaining production decisions are accepted or changed.

Current base:

- Current Dusk monorepo branch head: see the GitHub PR header.
- Upstream Hyperlane `main`: `67933966ed9c6f9e3d5ec095372e11414c82e4e7`
- Rebase/check evidence: use the live `git fetch upstream main`,
  `git merge-base HEAD upstream/main`, and
  `git rev-list --left-right --count HEAD...upstream/main` checks recorded in
  the companion Dusk gate reports.
- The fork was fetched against that upstream head and is not behind it. The
  exact reassessment candidate is pinned in
  `docs/dusk-companion-compatibility.md`: agent runtime
  `e95d3ea282a55ead114471ffb1dece77706ffc81`, companion base
  code `9058755927473239d59ce702a8074acbae0e0a24` (review head
  `6a2d7fda8d3f5eea52aa56af910e93c29a167d81`), and stacked withdrawal code
  `dc8aba07773993878edd81735d59e66beddd66a3` (review head
  `d1bb490c469142f154727f0a7aab5476b064eb59`). The separately validated
  review-policy boundary anchor is
  `c35f86405cf8cd83927860aca8b5c38b042ee198`.
- Focused Dusk tests, clippy, and the expanded affected-package cargo check
  pass at that runtime boundary. The exact companion base/stack gates and both
  live E2E security modes are recorded in the compatibility manifest; earlier
  heads remain regression history only.
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

The Dusk Rust agent crate depends on `hyperlane-dusk-types` at the exact public
companion commit `f6be24a411f2a0a247b8d1b798106c37449f7dcf`. This keeps
Dusk-specific message/metadata/token encoding shared with the reviewed Dusk
contract port without an adjacent checkout or moving branch. An upstream
Hyperlane PR must still decide whether to retain that immutable Git source,
publish the crate, or vendor it in the shape Hyperlane maintainers prefer.

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
cargo fmt --all -- --check
cargo clippy -p hyperlane-dusk --all-targets -- -D warnings
```

The exact Git dependency is outside this workspace, so workspace-wide format
validation no longer crosses into a different PR. `rust/main/Cargo.lock`
records the full immutable source and remains stable in clean agent and Docker
builds.

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

A repository checkout secret is deliberately not used: the companion Dusk repo
is public, and exposing an organization token to proposed workflow code would
add authority without adding checkout capability. The default companion
checkout is an exact reviewed Dusk commit rather than a moving branch; the
manual `workflow_dispatch` input is the only intentional override. This pin is
part of the contract/agent ABI compatibility record, not merely build
reproducibility.
A separate policy-gate step
diffs the branch against live Hyperlane upstream and rejects changes outside
the reviewed Dusk integration allowlist. Together these are the fork-scoped
validation substitute for Hyperlane-owned infrastructure, not a replacement
for the companion Dusk E2E evidence.

Production indexers require an archive-enabled Rusk endpoint and an exclusive
`eventCursorDir`. Canonical `LogMeta` is derived from contract-scoped,
cursor-paginated finalized-event rows by matching state height/data to each
row's source, topic, exact serialized payload, and block hash, followed by
`checkBlock(..., onlyFinalized: true)`. The archived transaction origin is
parsed into `LogMeta` but remains endpoint-supplied metadata; it is filtered
against the caller's requested hash on transaction-hash lookups, not
independently proven by direct contract state. Missing archive data fails
closed; whole blocks are not buffered below the helper transport cap.

Endpoint cursors and row IDs are independent process-local hints per topic and
are rebuilt by page-bounded replay after restart. The exclusive RocksDB stores
only an exact requested row after direct state and finalized-block validation,
under a v2 key bound to contract, topic, and local sequence. Page peers and
legacy v1 page-derived entries are never durable authority. A caught-up page is
polled again for later events rather than treated as permanent archive
exhaustion.

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
