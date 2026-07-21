# Dusk final red-team decisions (2026-07-21)

This companion record covers the agent-side decisions for monorepo PR #1.
Contract, escrow, dispatch-credit, CI, and operational decisions are recorded
in `FINAL_RED_TEAM_DECISIONS_2026-07-21.md` on the companion Dusk PR heads.

## Fixed

- Reorg detection first trips a process-wide atomic latch, then writes and
  fsyncs a mandatory tombstone adjacent to the validator database before
  attempting the secondary remote marker or RPC diagnostics. Every signing and
  publication boundary rechecks the latch. Tombstone writes serialize across
  submitter clones; an existing marker and its directory are fsynced before a
  second writer returns. Single-component relative database paths normalize to
  the current directory for directory fsync. Every startup checks the marker
  before opening the database. Only explicit operator removal can clear it.
- RUES-derived per-block transaction lookup ranges reject non-monotonic and
  empty-after-match boundaries. Accepted work is processed in 256-record
  chunks, while endpoint-derived transaction-hash lookups fail closed above a
  4,096-record aggregate budget across dispatch, Merkle insertion, delivery,
  and IGP. This bounds both per-call memory and total hostile-endpoint work;
  ordinary background sequence synchronization remains chunkable.
- Finalized-event archive cursors are decoded as canonical `v1:<row-id>`
  values and bound to the requested previous ID plus the returned page's first
  and last row IDs. Each topic owns independent process-local scan state, so
  one topic cannot skip earlier peers and no unbounded cross-topic pending
  buffer is needed. Only the exact requested row is persisted after it matches
  direct contract state and passes finalized `checkBlock`; page peers and
  prefixes never acquire durable authority. The v2 row-key migration ignores
  old page-derived v1 entries, and an exact cached mismatch deletes that row
  and resets its topic scan so a repaired endpoint can replace it on retry.
- Dusk request failures strip the reqwest URL at conversion and retain only a
  stable failure class plus optional HTTP status. Public reorg diagnostics use
  static error categories and hash only the normalized origin
  (scheme/host/port); raw error strings, URL userinfo, paths, and query
  parameters are never serialized, logged, or included in the hash oracle.
- Signer configuration implements an explicitly redacted `Debug` surface, and
  signer construction instrumentation skips `self`. Raw hex, Dusk, AWS,
  Cosmos, Radix, and Stark key material cannot enter tracing output through
  generic configuration formatting.
- The Dusk types dependency is pinned to exact companion commit
  `f6be24a411f2a0a247b8d1b798106c37449f7dcf` over Git. Release and Docker
  builds no longer require an adjacent checkout or a moving branch.
- The trusted policy gate binds proposal and agent evidence to exact workflow
  files, the pull-request event, current PR number, base ref/SHA, and exact
  proposed head SHA. Display-name, GitHub Actions app, or same-head runs from a
  different PR/base are not accepted as workflow identity. The trusted gate
  has no manual trigger, and manual agent runs use a distinct check context, so
  `workflow_dispatch` cannot satisfy either required PR context.
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

- Direct contract state and finalized `checkBlock` authenticate an event's
  source, topic, payload, height, and block hash. The archive's transaction
  origin is retained as endpoint-supplied `LogMeta` and is used as a filter
  when the caller already supplies an exact transaction hash; it is not an
  independently proven event-inclusion claim. A configured endpoint therefore
  remains a transaction-correlation trust boundary even though it cannot make
  an unauthenticated page peer durable.
- A transport write already inside the checkpoint syncer cannot be revoked by
  a later sibling reorg observation. The shared latch is checked before signer
  access, after awaited checkpoint reads, after signing, and before publication
  boundaries; the regression proves a sibling paused immediately before
  signing performs zero signing attempts after the latch trips.
- The exact Dusk dependency commit must remain reachable through a durable
  branch, merge, or tag. Deleting the only containing ref would turn future
  clean release builds into an availability failure even though Cargo pins the
  object immutably.
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
