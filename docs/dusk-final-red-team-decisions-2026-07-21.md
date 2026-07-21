# Dusk final red-team decisions (2026-07-21)

This companion record covers the agent-side decisions for monorepo PR #1.
Contract, escrow, dispatch-credit, CI, and operational decisions are recorded
in `FINAL_RED_TEAM_DECISIONS_2026-07-21.md` on the companion Dusk PR heads.

## Fixed

- The trusted policy gate binds proposal and agent evidence to exact workflow
  files, the pull-request event, and the exact proposed head SHA. Display-name
  and GitHub Actions app matches are not accepted as workflow identity.
- `CheckpointSyncerConf::build_and_validate` fails when a persistent reorg flag
  cannot be read. It no longer interprets an I/O failure as proof that no reorg
  was detected.
- Local storage treats only `NotFound` as an absent reorg flag. Permission,
  directory/type, and other I/O errors remain errors across restart.
- The restart regression models a failed flag write followed by an unreadable
  flag path and proves validation fails closed.
- `Mailbox::count` uses the same finalized Merkle-tree view as the dedicated
  MerkleTreeHook adapter. The current Mailbox nonce is intentionally not used
  as a proxy for a reorg-safe leaf count.
- The hosted Dusk agent gate executes `cargo test -p validator reorg`; compiling
  the validator is not sufficient evidence for the changed fail-stop behavior.
- The unprivileged proposal gate expands its reviewed-boundary allowlist only
  for the three files introduced by this hardening: this decision record,
  `checkpoint_syncer.rs`, and `local_storage.rs`. The boundary remains an
  explicit fail-closed file list rather than a broad directory exemption.

## Deliberate evidence boundaries

- The previously hosted Dusk agent validation succeeded for the exact tested
  agent source anchor `e95d3ea282a55ead114471ffb1dece77706ffc81`.
  Later policy or documentation commits are not relabeled as having run it.
- Compatibility entries resolve the live PR heads at review time while retaining
  exact immutable tested refs for runtime, static, and policy evidence.
- A whole-package `hyperlane-base` clippy pass currently reaches an unrelated
  pre-existing `arithmetic_side_effects` lint. Focused tests, formatting,
  `hyperlane-dusk` clippy, and the complete affected-package check are the
  relevant patch evidence; the unrelated lint is not represented as fixed.

## Operational decision

Temporary branch protection requires the proposal and hosted Dusk agent checks
that can execute today, remains strict, requires one review, and leaves the
administrator bypass enabled. This permits an owner override but does not
substitute for review or production readiness.
