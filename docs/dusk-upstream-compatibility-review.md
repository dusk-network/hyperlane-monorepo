# Dusk Upstream Compatibility Review

Date: 2026-07-20

This note records the upstream Hyperlane areas checked before keeping the Dusk
integration scoped to internal review. It is not an upstream PR; upstream PR
preparation remains blocked on the companion Dusk sign-off tracker.

## Base

Current Dusk branch:

- Branch: `feat/dusk-support-v2`
- Current review head: see the GitHub PR header.
- Rebase/check evidence: use the live upstream freshness commands below and
  the companion Dusk `make gate-status` report.
- Dusk signer test cleanup evidence commit:
  `b989bbcfbb2a427d3a538c5201f5d7214de6ba84`
- Upstream base: `577aa4a82e1082aed35dcde589c9b51bed787478`
- Upstream commit: `chore: release npm packages (#9070)`

Verification commands:

```bash
git rev-parse upstream/main
git merge-base HEAD upstream/main
git rev-list --left-right --count HEAD...upstream/main
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
```

Observed on 2026-07-20:

- `upstream/main` and `merge-base HEAD upstream/main` both resolve to
  `577aa4a82e1082aed35dcde589c9b51bed787478`; the rebased feature branch is 47
  commits ahead and zero commits behind that base before this evidence update.
- A local backup branch,
  `backup/feat-dusk-support-v2-pre-577aa4a`, preserves the pre-rebase head
  `a931f75b3d23d2e15e75f2e064470a1a01289abb`.
- The final upstream delta after the branch's previous base
  `197b1e0d1a7b7ee5539e9ad38a02a23a7eb0a0b3` consists of
  `31abc0b089` (CCIP server image build cleanup) and `577aa4a82e` (npm release).
  Neither changes the Dusk Rust agent crate, shared Rust settings, or Dusk-fork
  CI paths.
- The focused gate command `cargo check -p hyperlane-dusk`, the crate tests,
  and the expanded affected-package command
  `cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander`
  all pass against companion Dusk head
  `63bd80803e36bdca883d815eacea74c7575199de`.
- `cargo fmt --package hyperlane-dusk --package hyperlane-base -- --check`
  passes. Workspace-wide
  `cargo fmt --all -- --check` is not used as Dusk PR evidence because Cargo's
  `--all` also formats the adjacent companion repository through the local path
  dependency and therefore crosses the monorepo PR boundary.

## 2026-07-20 Reassessment Decisions

- Keep this PR scoped to Rust agent/protocol integration. The contract,
  deployment, CLI, escrow, and dispatch-credit implementation remains in the
  companion `dusk-network/hyperlane-dusk` repository.
- Retain the adjacent `hyperlane-dusk-types` path dependency for internal Dusk
  review. Publishing or vendoring that crate remains a prerequisite for an
  upstream Hyperlane PR, not for this fork PR.
- Update `rust/main/Cargo.lock` for the companion type crate's current direct
  `dusk-bytes` dependency. Leaving the lock stale would make a clean checkout
  mutate it during the Dusk agent gate.
- Format only the owned `hyperlane-dusk` package. This records the formatter
  change required by the current toolchain without pulling unrelated
  companion-repository formatting into this PR.
- Preserve explicit unsupported errors for Routing, Aggregation, CCIP-read,
  and rate-limited ISMs/hooks. A successful compile after the upstream sync is
  compatibility evidence, not a claim that those protocols are implemented.
- Keep the fork sync as a separate PR (`dusk-network/hyperlane-monorepo#2`) and
  the 47-commit Dusk feature series in PR #1. This makes upstream provenance
  visible while avoiding a merge commit inside the feature history.

## 2026-07-20 Deep-Review Remediation Decisions

The rebased PR was also inspected with Controlecentrum in deep, blind, staged
mode using GPT-5.6 at `xhigh` reasoning. The staged report assembler did not
ingest the validator's fenced JSON and therefore produced a misleading
zero-finding summary. The raw scout and validation artifacts were read
directly; the Dusk-attributable obligations were remediated rather than
discarded with the failed assembly. One reported Solana devnet signer-threshold
candidate was inherited unchanged from current Hyperlane upstream and is not a
Dusk feature change.

The resulting implementation decisions are:

- Treat indexer provenance as consensus-relevant data. Dispatch, delivery, and
  gas-payment records now fail closed when their state height cannot be read,
  require the queried sequence to match the decoded nonce where applicable,
  and resolve nonzero block and transaction hashes from Rusk's archive API.
  The archived event source, topic, in-block ordinal, and exact rkyv payload
  must agree with contract state before a log is returned.
- Require an archive-enabled Rusk endpoint for production indexing. Returning
  zero hashes or advancing a cursor without canonical provenance could cause
  scraper data loss, so deployments without `contractEvents(height)` and block
  hash queries now fail explicitly. This is a deliberate availability-for-
  correctness tradeoff.
- Bound RUES response buffering to 4 MiB and make transaction observation
  retry transient HTTP, GraphQL, and decode failures under one total deadline.
  The transaction ID remains immutable across retries. The external `dusk-tx`
  helper has its own 120-second deadline and is killed when that deadline
  expires.
- Preserve semantic errors across the chain adapter: signer unavailability is
  no longer collapsed into a generic retryable communication error, an ISM
  rejection remains distinct from a query failure, and unknown ISM module
  types are rejected rather than defaulted.
- Honor the relayer's process gas limit after checked `U256` to `u64`
  conversion, and reject Dusk chain IDs outside the contract's one-byte domain
  instead of truncating them.
- Replace the fork-only compile check with formatting, unit tests, targeted
  parser coverage, clippy, and checks of all affected Rust agents. A second CI
  guard compares the PR with live Hyperlane upstream and rejects paths outside
  the documented Dusk integration boundary. This is the scoped substitute for
  Hyperlane-owned Depot jobs that cannot run in the Dusk fork.

Regression evidence for these decisions includes exact GraphQL query bytes,
canonical event selection and transaction origin, transient observation
retries, oversized-response rejection, stalled-helper termination, checked
chain-ID conversion, ISM error separation, and signer error preservation.

The follow-up raw-artifact triage added these compatibility decisions:

- Dusk indexers are sequence-only. An explicit `index.mode: block` is rejected
  during settings parsing instead of being accepted into an implementation
  whose ranges are contract sequence numbers.
- Dusk participates in shared operation-submission configuration. In
  particular, `maxSubmitQueueLength` is no longer silently dropped, so the
  lander/relayer backpressure policy applies to Dusk like the other supported
  submission adapters.
- Provider metadata fails closed. A block fetched by height must return that
  same height; transaction gas limit, price, and spent values must be numeric;
  latest block height, timestamp, and 32-byte hash must be valid; and gas-price
  statistics errors remain visible rather than becoming zero/default metrics.
- Transaction confirmation treats malformed archive observations as retryable
  only within the existing absolute deadline. It never changes the submitted
  transaction ID and never converts malformed data into execution success.
- Dusk transaction hashes are reversible only in the canonical left-padded
  `H512` representation. Indexers now parse Dusk IDs, resolve the containing
  archive block, find that block's monotonic sequence range with binary search,
  and retain only records whose archived transaction origin is the requested
  transaction. Nonzero high padding and heights outside the shared `u32`
  cursor domain are rejected.
- Helper stdout and stderr are capped independently at 1 MiB in addition to
  the existing 120-second deadline. This bounds memory even if the configured
  helper is replaced or emits pathological diagnostics.
- Same-block ordinal reconstruction now uses logarithmic boundary searches
  over monotonic contract-record heights rather than scanning the entire prior
  same-block prefix for every record.

`announce_tokens_needed = 0` remains deliberate. Like Sealevel, Dusk validator
announcement has no separate contract deposit; the transaction sender still
pays normal chain execution fees. Returning zero allows the validator agent to
submit, and is not a claim that transaction execution itself is free.

Local regression evidence for this follow-up is:

- `cargo test -p hyperlane-dusk`: 12 passed, including bounded helper output,
  exact transaction-ID conversion, provider fail-closed parsing, archive-event
  provenance, and malformed-observation retry behavior;
- `cargo test -p hyperlane-base dusk_`: 6 passed, including Dusk signer,
  chain-ID, and sequence-only index-mode coverage;
- `cargo clippy -p hyperlane-dusk --all-targets -- -D warnings`: passed;
- `cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p
  scraper -p lander`: passed; and
- package-scoped formatting and `git diff --check`: passed.

`actionlint` was not installed in the local environment, so the modified
workflow received syntax/diff inspection here and still requires its GitHub
workflow parser/status run. This limitation is recorded rather than treating a
missing local tool as successful workflow validation.

The Dusk fork now also proposes
`.github/workflows/dusk-agent-gate.yml` as a narrow PR status check for the
agent crate. It checks out the companion private Dusk repo for
`hyperlane-dusk-types`, scans `rust/main/chains/hyperlane-dusk` for runtime
placeholder macros, preflights private repo access for `DUSK_ORG_READ_TOKEN`,
then runs package formatting, unit and parser tests, Dusk clippy, the expanded
affected-package check, and a stable-lockfile assertion. The workflow is
intended to give the monorepo PR a focused status check while the full
cross-repo repro and E2E evidence remain in `dusk-network/hyperlane-dusk`.

The inherited upstream `rust-docker.yml` and `monorepo-docker.yml`
image-publishing workflows have job-level
`github.repository_owner == 'hyperlane-xyz'` guards. The inherited Depot-backed
PR jobs in `rust.yml`, `test.yml`, `rebalancer-e2e-test.yml`, and
`rebalancer-sim-test.yml` use the same repository-owner guard so Dusk-fork PRs
do not stay queued on Hyperlane-owned Depot runner labels. This keeps internal
Dusk review focused on the Dusk review-policy and Dusk agent gates, and leaves
the upstream workflows active for the later Hyperlane PR path.

## Upstream Areas Checked

Recent upstream changes relevant to the current compatibility boundary include:

- `577aa4a82e chore: release npm packages (#9070)`
- `31abc0b089 fix: remove redundant prisma generate from ccip-server build (#9071)`
- `a7d9af7541 fix(relayer): bound and validate CCIP-read responses (#9047)`
- `58c5e11e1e fix(relayer): restrict CCIP-read network destinations (#9048)`
- `e22be4b14d perf(relayer): wake database loader after indexing (#9034)`
- `0a2a8fa51b perf(validator): remove final checkpoint pacing delay (#9033)`
- `c5122b8b10 perf(relayer): overlap multisig checkpoint reads (#9032)`
- `506f1ab781 fix: aggregation ism metadata building improvement (#8920)`

The release and CCIP-server build commits are TypeScript/container-only. The
Rust relayer and validator changes compile with the Dusk agent integration.
CCIP-read and Aggregation ISM changes do not expand Dusk support: the Dusk
builder still rejects those module types explicitly. The relayer performance
changes do not alter the Dusk RUES or transaction-confirmation contracts.

## Current Dusk Support

The Dusk Rust agent integration currently supports the pieces used by the
companion Dusk E2E evidence:

- Dusk chain/protocol parsing.
- `duskKey` signer construction with inline, `keyFile`, or `keyEnv` key
  sources. Inline key material remains for backwards-compatible local dev
  only; generated Dusk demo configs use `keyFile`. On Unix, `keyFile` paths
  must be regular files with no group/world permissions.
- RUES-backed provider/indexing.
- Mailbox dispatch/process flow.
- MerkleTreeHook reads and indexer wrapping.
- `InterchainSecurityModule` module-type and dry-run verify calls.
- `MessageIdMultisigISM` validator/threshold reads for relayer metadata.
- Relayer, validator, scraper, and lander compile-time integration.

The companion Dusk repository contains the Dusk contract ports, demo tooling,
fault-injection scripts, stress scripts, and security notes.

## Explicit Non-Support

The current Dusk branch intentionally returns unsupported errors for:

- Routing ISM.
- Aggregation ISM.
- CCIP-read ISM.

This is deliberate. Recent upstream fixes around nested trusted relayer ISMs,
rate-limited ISMs inside aggregation ISMs, and aggregation metadata are not
claimed as Dusk-supported behavior until Dusk implements the corresponding
contract ports, chain trait implementations, and E2E coverage.

Rate-limited hooks and rate-limited ISMs are also out of scope for this branch.
Supporting them for Dusk would require:

- Dusk contract ports.
- Dusk config/deploy tooling.
- Dusk chain trait implementations if the Rust agents need to inspect them.
- Negative tests for quota exhaustion, reset windows, wrong domains, and
  nested ISM behavior.
- Bidirectional E2E coverage through the Dusk relayer path.

## Interface Compatibility Notes

- The Dusk `DuskIsm::module_type` reads the Dusk contract `module_type` value
  and maps it into Hyperlane `ModuleType`.
- The relayer metadata builder can build metadata for `MessageIdMultisig`.
- If a Dusk deployment config points at Routing, Aggregation, or CCIP-read ISMs,
  the chain builder returns explicit unsupported errors rather than silently
  attempting partial support.
- Dusk MerkleTreeHook integration is intentionally backed by the Dusk mailbox
  wrapper plus direct MerkleTreeHook contract reads, matching the E2E-tested
  MessageIdMultisig path.

## Runtime Placeholder Scan

The original revival goal requires no `todo!`, `unimplemented!`, or direct
`panic!` paths in Dusk runtime agent code.

Commands checked:

```bash
rg -n "todo!|unimplemented!|panic!" rust/main/chains/hyperlane-dusk -g '!target'
git diff upstream/main...HEAD -- \
  rust/main/chains/hyperlane-dusk \
  rust/main/hyperlane-base/src/settings/chains.rs \
  rust/main/hyperlane-base/src/settings/parser \
  rust/main/hyperlane-base/src/settings/signers.rs \
  rust/main/lander/src/adapter/chains/factory.rs \
  rust/main/agents/validator/src/reorg_reporter.rs \
  | rg -n "^\+.*(todo!|unimplemented!|panic!)"
```

Observed:

- No matches in `rust/main/chains/hyperlane-dusk`.
- No added placeholder macros in the Dusk diff against `upstream/main`.
- A broader scan of shared Hyperlane files still finds existing non-Dusk
  placeholder macros for other chains, for example Fuel and Radix branches.
  Those are pre-existing upstream behavior and are not Dusk runtime paths.

## Fork Dependency Advisory Scope

The Dusk fork currently has open Dependabot alerts, but they are inherited npm
alerts outside the Dusk Rust integration diff. They are not remediated in this
branch because doing so would require broad upstream TypeScript/package-lock
churn unrelated to the internal Dusk agent review.

Commands checked:

```bash
gh api repos/dusk-network/hyperlane-monorepo/dependabot/alerts --paginate \
  | jq -s 'add | map(select(.state=="open")) | {total:length, by_ecosystem:(group_by(.dependency.package.ecosystem)|map({ecosystem:.[0].dependency.package.ecosystem,count:length})), by_manifest:(group_by(.dependency.manifest_path)|map({manifest:.[0].dependency.manifest_path,count:length})|sort_by(.manifest)), severities:(group_by(.security_advisory.severity)|map({severity:.[0].security_advisory.severity,count:length}))}'
gh api repos/dusk-network/hyperlane-monorepo/dependabot/alerts --paginate \
  | jq -r -s 'add | map(select(.state=="open") | .dependency.manifest_path) | unique[]'
gh api repos/dusk-network/hyperlane-monorepo/dependabot/alerts --paginate \
  --jq '.[] | select(.state=="open") | select(.dependency.package.ecosystem != "npm") | [.number,.dependency.package.ecosystem,.dependency.package.name,.dependency.manifest_path] | @tsv'
git diff --name-status upstream/main...HEAD -- \
  pnpm-lock.yaml \
  typescript/github-proxy/package.json \
  typescript/warp-widget/examples/react-app/package.json \
  typescript/warp-widget/examples/react-app/pnpm-lock.yaml \
  typescript/widgets/package.json
```

Observed on 2026-05-12:

- 173 open alerts, all ecosystem `npm`.
- Severity counts: 6 critical, 68 high, 79 medium, 20 low.
- Alert manifests: `pnpm-lock.yaml`, `typescript/github-proxy/package.json`,
  `typescript/warp-widget/examples/react-app/package.json`,
  `typescript/warp-widget/examples/react-app/pnpm-lock.yaml`, and
  `typescript/widgets/package.json`.
- No open non-npm Dependabot alerts were returned.
- The Dusk branch diff does not touch any of those npm manifests.
- The Dusk branch does touch `rust/main/Cargo.lock` for the Dusk Rust agent
  integration, but the Dependabot API reported no open Cargo/Rust alerts for
  this monorepo fork.

Upstream PR preparation should re-check this advisory scope. If Hyperlane
main still has npm advisories then, they should be handled as upstream
dependency maintenance rather than as Dusk protocol integration changes unless
Hyperlane reviewers ask for a combined remediation.

## Upstream PR Implication

An upstream Hyperlane PR should describe this as Dusk Rust agent/protocol
support only. It should not present Dusk as supporting:

- Hyperlane TypeScript SDK/CLI deployment flows.
- Rate-limited hooks.
- Rate-limited ISMs.
- Routing/Aggregation/CCIP-read ISMs.
- Production signer custody.

Those items should remain Dusk follow-up work unless Hyperlane reviewers ask
for a different split.
