# Dusk monorepo CI bootstrap decisions

This document records the trust boundary for the internal Dusk fork checks.
The first merge is a policy bootstrap because the default branch does not yet
contain Dusk-specific workflows.

## No private checkout token

`dusk-network/hyperlane-dusk` is public. The Dusk agent check therefore uses a
read-only `GITHUB_TOKEN` context and does not consume `DUSK_ORG_READ_TOKEN` or
any other repository secret. Proposed Rust and workflow code runs only in the
unprivileged `pull_request` context. Its companion input must be an exact
40-character commit SHA and the resolved checkout must equal it.

## Bootstrap and steady state

For monorepo PR #1, branch protection temporarily requires the two checks that
can run before the policy exists on `main`: `Dusk proposal validation` and
`Dusk agent validation`. Both run without secrets. The PR needs focused owner
review; a proposed workflow cannot provide a trusted endorsement of its own
first publication.

After that bootstrap is merged, branch protection must require `Dusk review
policy gate` and `Dusk agent validation` (and may retain `Dusk proposal
validation`). The trusted gate uses `pull_request_target`, checks out only the
base commit, fetches the event head as Git data, and waits for both unprivileged
checks on the exact head SHA. It never checks out or executes proposed code.

The trusted gate locks these files byte-for-byte against the base:

- `.github/workflows/dusk-agent-gate.yml`
- `.github/workflows/dusk-proposal-validation.yml`
- `.github/workflows/dusk-review-policy-gate.yml`

Later policy changes use a focused, owner-supervised bootstrap: review the
exact policy-only diff, temporarily select the unprivileged bootstrap checks,
merge with the documented admin bypass only after approval, and immediately
restore the trusted gate as required. Normal implementation PRs must not alter
the locked workflows.

## Supply-chain and scope controls

`actions/checkout` and `actions/github-script` use immutable commit SHAs.
Actionlint uses the immutable digest for upstream `rhysd/actionlint:1.7.12`:

`sha256:b1934ee5f1c509618f2508e6eb47ee0d3520686341fec936f3b79331f9315667`

The proposal check compares the complete fork diff with live Hyperlane
`main` and rejects paths outside the documented Dusk integration allowlist.
The agent check scans Dusk runtime sources for panic/placeholder paths, then
runs formatting, unit tests, clippy, the full affected-package cargo check, and
a lockfile-stability check.
