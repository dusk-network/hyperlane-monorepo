#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| hyperlane_dusk::fuzzing::scan_anchor(data));
