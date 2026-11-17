#!/usr/bin/env bash
# Run the agent's test against a real dusk-tx build:
# `a_dusk_tx_build_sends_only_on_the_agents_word` in
# tests/delivery_submission.rs.
#
# The agent drives dusk-tx through its output: the `Prepared TX` line, the
# go-ahead it writes to dusk-tx's stdin, and dusk-tx's error prefixes, which
# decide whether a delivery may have been sent. CI runs every other test
# against a shell stand-in for dusk-tx: dusk-tx builds against Dusk's
# private Rusk repository, which the public agent gate can't check out. So
# this test is a release check, run where rusk-private is at hand. It
# builds dusk-tx from the hyperlane-dusk commit the agent gate pins
# (DUSK_REF in .github/workflows/dusk-agent-gate.yml), or the one given,
# and runs the test against it.
#
# Usage, from anywhere:
#   bash rust/main/chains/hyperlane-dusk/scripts/dusk-tx-interop-check.sh [options]
#
#   --dusk-dir DIR     a hyperlane-dusk checkout that has the commit
#                      (default: ../dusk next to this repository)
#   --dusk-ref SHA     the commit to build (default: the agent gate's DUSK_REF)
#   --rusk-dir DIR     a rusk-private checkout at the commit that commit's
#                      RUSK_VERSION pins (default: ../../rusk-private next to
#                      this repository)
#   --target-dir DIR   where Cargo builds dusk-tx (default: a temporary
#                      directory, removed afterwards)
#   --dusk-tx-bin PATH test this dusk-tx build instead of building one
#
# The layout is the one the hyperlane-dusk README shows:
#   <workspace>/rusk-private, <workspace>/hyperlane/dusk and
#   <workspace>/hyperlane/hyperlane-monorepo.

set -euo pipefail

MONOREPO="$(cd "$(dirname "$0")/../../../../.." && pwd)"
RUST_MAIN="$MONOREPO/rust/main"
GATE="$MONOREPO/.github/workflows/dusk-agent-gate.yml"
CRATE_MANIFEST="$RUST_MAIN/chains/hyperlane-dusk/Cargo.toml"
TEST_FILE=delivery_submission
TEST_NAME=a_dusk_tx_build_sends_only_on_the_agents_word

DUSK_DIR="$MONOREPO/../dusk"
RUSK_DIR="$MONOREPO/../../rusk-private"
DUSK_REF=""
TARGET_DIR=""
DUSK_TX_BIN=""

fail() { echo "[FAIL] $*" >&2; exit 1; }
info() { echo "[INFO] $*" >&2; }

while [ "$#" -gt 0 ]; do
    case "$1" in
        --dusk-dir) DUSK_DIR="${2:?--dusk-dir needs a path}"; shift 2 ;;
        --dusk-ref) DUSK_REF="${2:?--dusk-ref needs a commit}"; shift 2 ;;
        --rusk-dir) RUSK_DIR="${2:?--rusk-dir needs a path}"; shift 2 ;;
        --target-dir) TARGET_DIR="${2:?--target-dir needs a path}"; shift 2 ;;
        --dusk-tx-bin) DUSK_TX_BIN="${2:?--dusk-tx-bin needs a path}"; shift 2 ;;
        -h|--help) sed -n '2,/^$/s/^# \{0,1\}//p' "$0"; exit 0 ;;
        *) fail "unknown argument: $1 (see --help)" ;;
    esac
done

command -v cargo >/dev/null || fail "cargo is required"
command -v git >/dev/null || fail "git is required"

# The commit the agent gate checks the shared types at, which the crate's
# Cargo.toml pins too.
gate_ref="$(sed -n "s/.*DUSK_REF: .*'\([0-9a-fA-F]\{40\}\)'.*/\1/p" "$GATE")"
[[ "$gate_ref" =~ ^[0-9a-fA-F]{40}$ ]] || fail "Can't read DUSK_REF from $GATE"
crate_ref="$(sed -n 's/^hyperlane-dusk-types = .*rev = "\([0-9a-fA-F]\{40\}\)".*/\1/p' "$CRATE_MANIFEST")"
[ "${crate_ref,,}" = "${gate_ref,,}" ] \
    || fail "$CRATE_MANIFEST pins hyperlane-dusk-types at ${crate_ref:-nothing}, but the agent gate checks $gate_ref: make them the same commit"

if [ -n "$DUSK_TX_BIN" ]; then
    [ -x "$DUSK_TX_BIN" ] || fail "No dusk-tx build at $DUSK_TX_BIN"
    DUSK_TX_BIN="$(cd "$(dirname "$DUSK_TX_BIN")" && pwd)/$(basename "$DUSK_TX_BIN")"
    info "Testing the dusk-tx build $DUSK_TX_BIN (not built here; the agent gate pins $gate_ref)"
else
    DUSK_REF="${DUSK_REF:-$gate_ref}"
    [[ "$DUSK_REF" =~ ^[0-9a-fA-F]{40}$ ]] || fail "--dusk-ref must be a commit as 40 hex characters, not '$DUSK_REF'"
    DUSK_REF="${DUSK_REF,,}"
    [ "$DUSK_REF" = "${gate_ref,,}" ] \
        || info "Building dusk-tx at $DUSK_REF, not the agent gate's DUSK_REF $gate_ref"
    [ -d "$DUSK_DIR/.git" ] || [ -f "$DUSK_DIR/.git" ] \
        || fail "No hyperlane-dusk checkout at $DUSK_DIR (--dusk-dir)"
    git -C "$DUSK_DIR" cat-file -e "$DUSK_REF^{commit}" 2>/dev/null \
        || fail "$DUSK_DIR doesn't have the commit $DUSK_REF: fetch it first"
    [ -d "$RUSK_DIR/core" ] || fail "No rusk-private checkout at $RUSK_DIR (--rusk-dir)"
    RUSK_DIR="$(cd "$RUSK_DIR" && pwd -P)"

    # A checkout of DUSK_REF where its `rusk` link (../../rusk-private)
    # reaches RUSK_DIR, as the README lays them out.
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/dusk-tx-interop-XXXXXXXX")"
    LAYOUT="$WORK/hyperlane/dusk"
    cleanup() {
        git -C "$DUSK_DIR" worktree remove --force "$LAYOUT" >/dev/null 2>&1 || true
        rm -rf "$WORK"
    }
    trap cleanup EXIT
    mkdir -p "$WORK/hyperlane"
    ln -s "$RUSK_DIR" "$WORK/rusk-private"
    git -C "$DUSK_DIR" worktree add --detach "$LAYOUT" "$DUSK_REF" >/dev/null 2>&1 \
        || fail "Can't check out $DUSK_REF from $DUSK_DIR"
    if [ -f "$LAYOUT/scripts/check-rusk-version.sh" ]; then
        bash "$LAYOUT/scripts/check-rusk-version.sh" \
            || fail "rusk-private at $RUSK_DIR is not the commit $DUSK_REF's RUSK_VERSION pins"
    fi
    TARGET_DIR="${TARGET_DIR:-$WORK/target}"
    info "Building dusk-tx at $DUSK_REF (rusk-private $(git -C "$RUSK_DIR" rev-parse --short HEAD 2>/dev/null || echo '?'))..."
    (cd "$LAYOUT" && CARGO_TARGET_DIR="$TARGET_DIR" cargo build -p dusk-tx --release --locked) \
        || fail "dusk-tx at $DUSK_REF doesn't build"
    DUSK_TX_BIN="$TARGET_DIR/release/dusk-tx"
    [ -x "$DUSK_TX_BIN" ] || fail "The build left no dusk-tx at $DUSK_TX_BIN"
fi

info "Running $TEST_NAME against it..."
log="$(mktemp "${TMPDIR:-/tmp}/dusk-tx-interop-test-XXXXXXXX")"
status=0
(cd "$RUST_MAIN" && DUSK_TX_REAL_BIN="$DUSK_TX_BIN" cargo test -p hyperlane-dusk --test "$TEST_FILE" \
    -- --ignored --exact "$TEST_NAME") >"$log" 2>&1 || status=$?
tail -n 20 "$log" >&2
# A filter that matched nothing would pass with no test run.
if [ "$status" -ne 0 ] || ! grep -q "^test $TEST_NAME \.\.\. ok$" "$log" \
    || ! grep -q "test result: ok\. 1 passed" "$log"; then
    rm -f "$log"
    fail "$TEST_NAME failed against $DUSK_TX_BIN"
fi
rm -f "$log"
info "dusk-tx and the agent agree: $TEST_NAME passed"
