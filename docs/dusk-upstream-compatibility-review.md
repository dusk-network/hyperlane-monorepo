# Dusk Upstream Compatibility Review

Date: 2026-05-12

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
- Upstream base: `66e8c1f4644cea0392b33007225e6611b8f06804`
- Upstream commit: `feat: whitelist moonpay route for fastpath relayer (#8725)`

Verification commands:

```bash
git rev-parse upstream/main
git merge-base HEAD upstream/main
cargo check -p hyperlane-dusk -p hyperlane-base -p validator -p relayer -p scraper -p lander
```

Observed:

- `upstream/main` and `merge-base HEAD upstream/main` both resolve to
  `66e8c1f4644cea0392b33007225e6611b8f06804`.
- The Rust agent check passed after the rebase to that base.

## Upstream Areas Checked

Recent upstream changes around the current base include:

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

The `66e8c1f4` fastpath relayer whitelist update is also TypeScript
infra/config-only. It does not change the Rust agent interfaces used by the
Dusk chain crate, relayer, validator, scraper, or lander integration.

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
