# Dusk Upstream Compatibility Review

Date: 2026-05-13

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
- Upstream base: `215135227a0e47883d3581433a02c68d89986e41`
- Upstream commit: `feat(sdk)!: move ICA helpers to subpath export (#8764)`

Verification commands:

```bash
git rev-parse upstream/main
git merge-base HEAD upstream/main
git rev-list --left-right --count HEAD...upstream/main
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
```

Observed:

- `upstream/main` and `merge-base HEAD upstream/main` both resolve to
  `215135227a0e47883d3581433a02c68d89986e41`.
- `git rev-list --left-right --count HEAD...upstream/main` is reported by the
  live gate checks instead of being hard-coded here, so docs-only evidence
  refreshes do not immediately stale this compatibility note.
- The Rust agent check passed after the rebase to that base. Latest
  clean-layout validation is recorded in the companion Dusk `TEST_REPORT.md`
  for run `1778683232` on monorepo head
  `9050143c1ef12f76d117ee97effa79da8df3e334`.

The Dusk fork now also proposes
`.github/workflows/dusk-agent-gate.yml` as a narrow PR status check for the
agent crate. It checks out the companion private Dusk repo for
`hyperlane-dusk-types`, scans `rust/main/chains/hyperlane-dusk` for runtime
placeholder macros, preflights private repo access for `DUSK_ORG_READ_TOKEN`,
and runs `cargo check -p hyperlane-dusk`. The workflow is intended to give the
monorepo PR a focused status check while the full cross-repo repro and E2E
evidence remain in `dusk-network/hyperlane-dusk`.

The inherited upstream `rust-docker.yml` and `monorepo-docker.yml`
image-publishing workflows have job-level
`github.repository_owner == 'hyperlane-xyz'` guards. The inherited Depot-backed
PR jobs in `rust.yml`, `test.yml`, and `rebalancer-e2e-test.yml` use the same
repository-owner guard so Dusk-fork PRs do not stay queued on Hyperlane-owned
Depot runner labels. This keeps internal Dusk review focused on the Dusk
review-policy and Dusk agent gates, and leaves the upstream workflows active
for the later Hyperlane PR path.

## Upstream Areas Checked

Recent upstream changes around the current base include:

- `21513522 feat(sdk)!: move ICA helpers to subpath export (#8764)`
- `2b7db706 feat(infra): add Citrea/Moonpay warp route config getters (#8722)`
- `c6bce706 fix: reduce cctp interval to 10 retries (#8744)`
- `7a362a09 feat: temporarily disable relaying to/from krown (#8742)`
- `66e8c1f4 feat: whitelist moonpay route for fastpath relayer (#8725)`
- `f758a7063 feat: rate limit ism support (#8703)`
- `b8a600cc1 feat: add RateLimitedHook support to warp deploy and apply (#8715)`
- `33097209a fix: rate limit ism inside aggregation ism (#8709)`
- `8fb94f974 fix: nested trusted relayer ism (#8721)`
- `f2ba67b2 feat(infra): expose relay API via Cloudflare tunnel sidecar (#8710)`

The rate-limit ISM and rate-limited hook changes are primarily TypeScript SDK,
CLI, deploy, and TypeScript relayer metadata changes. The Dusk Rust agent
branch does not add TypeScript SDK/CLI support and does not claim rate-limited
ISM or rate-limited hook support for Dusk deployments.

The `21513522` ICA helper subpath export, `2b7db706` Citrea/Moonpay getter
change, `7a362a09` Krown relaying disablement, and `66e8c1f4` fastpath relayer
whitelist update are TypeScript SDK or infra/config-only. They do not change
the Rust agent interfaces used by the Dusk chain crate, relayer, validator,
scraper, or lander integration. The `c6bce706` CCTP retry interval change
touches Rust relayer CCIP-read metadata handling only. Dusk continues to return
explicit unsupported errors for CCIP-read ISMs, so this does not expand the
supported Dusk behavior.

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
