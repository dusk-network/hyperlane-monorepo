# Dusk Upstream PR Plan

This branch is for internal Dusk review first. Do not open an upstream
Hyperlane PR until the companion Dusk contract/security PR is reviewed and the
remaining production decisions are accepted or changed.

Current base:

- Current Dusk monorepo branch head: see the GitHub PR header.
- Upstream Hyperlane `main`: `7a362a093d622b69d6c55d47992c9490ec33fb1a`
- Rebase/check evidence: use the live `git fetch upstream main`,
  `git merge-base HEAD upstream/main`, and
  `git rev-list --left-right --count HEAD...upstream/main` checks recorded in
  the companion Dusk gate reports.
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
```

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
