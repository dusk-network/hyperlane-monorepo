# Dusk Upstream PR Plan

This branch is for internal Dusk review first. Do not open an upstream
Hyperlane PR until the companion Dusk contract/security PR is reviewed and the
remaining production decisions are accepted or changed.

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

## Companion Evidence

The companion Dusk PR is the source of contract/tooling/security evidence:

- `dusk-network/hyperlane-dusk#1`
- Latest evidence includes clean-Rusk TestMock and MessageIdMultisig E2E,
  dirty redeploy guard, metadata corruption, RPC failures, low signer balance,
  duplicate relayers, and a 3102-second high-volume restart/backlog soak.
- `SECURITY_REVIEW.md` records Dusk-specific security assumptions and
  deviations from Solidity Hyperlane contracts.
- `GOAL_AUDIT.md` maps the original revival goal to concrete artifacts and
  remaining gates.
- `dusk-network/hyperlane-dusk#2` tracks the remaining production sign-off
  decisions before upstream PR preparation.

## Validation Commands

From `rust/main` in this monorepo:

```bash
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
```

From the companion Dusk repo:

```bash
make repro-check
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
- Dusk agent/runtime review has been requested from `Neotamandua`, based on
  recent Rusk HTTP/RUES/GraphQL route ownership. No `CODEOWNERS`, `OWNERS`, or
  `MAINTAINERS` file was found in this fork or the companion Dusk repo.
- Dusk must decide CI/repro runner strategy for the private Rusk-dependent
  companion checks. The companion repo now has `make repro-check` for the
  repeatable local non-E2E subset, but current evidence is still local and
  clean-Rusk documented, not automated PR CI.
- Internal Dusk PRs must be reviewed first.
- Upstream PR text should explicitly state that Dusk contract ports and E2E
  scripts live in `dusk-network/hyperlane-dusk`, not this monorepo.
