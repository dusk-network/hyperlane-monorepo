# Fuzz targets for the Dusk agent's parsers

The crate's parsers of what a Rusk node and the `dusk-tx` helper send have
property checks in `checks` modules next to them (`src/checks.rs`,
`src/tx_sender/checks.rs`, `src/rues/checks.rs`,
`src/rues/submissions/checks.rs`, `src/final_records/checks.rs`). The
crate's tests run each check on generated inputs (`cargo test -p
hyperlane-dusk checks`; `DUSK_CHECK_CASES=<n>` runs more). This directory
runs the same checks under libFuzzer, one target per check, through the
crate's `fuzzing` feature.

```bash
cd rust/main/chains/hyperlane-dusk/fuzz
cargo +nightly fuzz run block_pair -- -max_total_time=600 -max_len=4096
```

It is a workspace of its own, with rust/main's `[patch]` entries and a
lockfile started from rust/main's. `corpus/`, `artifacts/` and `target/` are
ignored.
