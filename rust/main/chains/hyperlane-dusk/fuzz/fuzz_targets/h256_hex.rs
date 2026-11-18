#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| hyperlane_dusk::fuzzing::h256_hex(data));
