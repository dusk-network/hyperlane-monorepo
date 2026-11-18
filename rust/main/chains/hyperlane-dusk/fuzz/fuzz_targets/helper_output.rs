#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| hyperlane_dusk::fuzzing::helper_output(data));
