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
