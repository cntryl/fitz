use std::time::Duration;

pub mod midge;
pub use midge::*;

/// Scale a test deadline by `FITZ_TEST_TIMEOUT_MULTIPLIER`.
///
/// Hosted parallel suites opt into extra scheduling headroom while local tests
/// retain their literal deadlines. Invalid and zero multipliers are ignored.
#[must_use]
pub fn scaled_test_timeout(timeout: Duration) -> Duration {
    let multiplier = std::env::var("FITZ_TEST_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    timeout.saturating_mul(multiplier)
}

pub mod benchmark;
pub use benchmark::*;
