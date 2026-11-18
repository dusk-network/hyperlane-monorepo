#![no_main]

libfuzzer_sys::fuzz_target!(|data: &[u8]| hyperlane_dusk::fuzzing::event_page(data));
