//! Shared pieces of the parser property checks.
//!
//! Each parser module has a `checks` module of properties that hold for any
//! input. The checks take bytes and build their cases from them with
//! [`Input`], so the same check runs on bytes from a seeded generator in the
//! crate's tests ([`run_cases`]) and on bytes from a coverage-guided fuzzer
//! (the `fuzzing` feature and the targets in `fuzz/`).

/// Builds the parts of a case from bytes. Past the end it reads zeros, so
/// any bytes make a case.
pub struct Input<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> Input<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    pub fn is_empty(&self) -> bool {
        self.position >= self.data.len()
    }

    pub fn u8(&mut self) -> u8 {
        let byte = self.data.get(self.position).copied().unwrap_or(0);
        self.position = self.position.saturating_add(1);
        byte
    }

    pub fn bool(&mut self) -> bool {
        self.u8() & 1 == 1
    }

    pub fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.u8(), self.u8()])
    }

    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.u8(), self.u8(), self.u8(), self.u8()])
    }

    pub fn u64(&mut self) -> u64 {
        u64::from(self.u32()) | (u64::from(self.u32()) << 32)
    }

    /// A number below `bound`, which must not be zero.
    pub fn below(&mut self, bound: usize) -> usize {
        usize::from(self.u16()) % bound
    }

    /// A number, often at an edge: zero, one, the type's maximum or near it.
    pub fn edgy_u64(&mut self) -> u64 {
        match self.u8() % 8 {
            0 => 0,
            1 => 1,
            2 => u64::MAX,
            3 => u64::MAX - 1,
            4 => u64::from(u32::MAX),
            5 => i64::MAX as u64,
            _ => self.u64(),
        }
    }

    /// Up to `max` bytes of the input.
    pub fn bytes(&mut self, max: usize) -> &'a [u8] {
        let wanted = self.below(max.saturating_add(1));
        let start = self.position.min(self.data.len());
        let end = start.saturating_add(wanted).min(self.data.len());
        self.position = start.saturating_add(wanted);
        &self.data[start..end]
    }

    /// 64 hex digits in either case, as a transaction hash is written.
    pub fn hash_hex(&mut self) -> String {
        let upper = self.bool();
        (0..64)
            .map(|_| {
                let digit = char::from_digit(u32::from(self.u8() % 16), 16).unwrap_or('0');
                if upper {
                    digit.to_ascii_uppercase()
                } else {
                    digit
                }
            })
            .collect()
    }

    /// Text of up to `max` pieces: characters of every width, newlines, and
    /// `fragments`, the phrases the parser under test looks for.
    pub fn text(&mut self, max: usize, fragments: &[&str]) -> String {
        let mut text = String::new();
        for _ in 0..self.below(max.saturating_add(1)) {
            match self.u8() % 8 {
                0 | 1 if !fragments.is_empty() => {
                    text.push_str(fragments[self.below(fragments.len())]);
                }
                2 => text.push('\n'),
                3 => text.push(['é', '✓', '𝄞', '\u{0}', '\r', '"', '\\'][self.below(7)]),
                _ => text.push(char::from(0x20 + self.u8() % 0x5f)),
            }
        }
        text
    }
}

/// Runs `future` to completion on this thread. For futures that never
/// wait on anything outside themselves, such as a search over records
/// served from memory: one that stays pending is a bug in the check.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..1_000_000 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
    unreachable!("a future that waits on nothing stayed pending")
}

/// Deterministic generator (splitmix64) for the tests' cases.
#[cfg(test)]
pub struct Rng(pub u64);

#[cfg(test)]
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// How many cases each property runs: `default`, or `DUSK_CHECK_CASES` to
/// run many more locally.
#[cfg(test)]
pub fn cases(default: usize) -> usize {
    std::env::var("DUSK_CHECK_CASES")
        .ok()
        .and_then(|cases| cases.parse().ok())
        .unwrap_or(default)
}

/// Runs `check` on `cases(default)` byte strings of up to `max_len` bytes
/// from a generator seeded with `seed`.
#[cfg(test)]
pub fn run_cases(seed: u64, default: usize, max_len: usize, check: impl Fn(&[u8])) {
    let mut rng = Rng(seed);
    for _ in 0..cases(default) {
        let len = (rng.next() % (max_len as u64 + 1)) as usize;
        let bytes: Vec<u8> = (0..len).map(|_| rng.next() as u8).collect();
        check(&bytes);
    }
}
