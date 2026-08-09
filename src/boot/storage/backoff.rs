use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) const BASE_BACKOFF: Duration = Duration::from_millis(250);
pub(super) const MAX_BACKOFF: Duration = Duration::from_secs(5);
pub(super) const MAX_JITTER_MS: u64 = 250;

pub(super) fn retry_delay(attempt: u32) -> Duration {
    retry_delay_with_jitter(attempt, jitter_ms(attempt))
}

fn retry_delay_with_jitter(attempt: u32, jitter_ms: u64) -> Duration {
    let multiplier = 1_u64.checked_shl(attempt).unwrap_or(u64::MAX);
    let base_ms = duration_millis(BASE_BACKOFF)
        .saturating_mul(multiplier)
        .min(duration_millis(MAX_BACKOFF));
    Duration::from_millis(base_ms.saturating_add(jitter_ms.min(MAX_JITTER_MS)))
}

fn jitter_ms(attempt: u32) -> u64 {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let salt = u64::from(now_nanos) ^ u64::from(attempt).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    salt % (MAX_JITTER_MS + 1)
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_cap_exponential_backoff_before_jitter() {
        // Arrange
        let attempt = u32::MAX;

        // Act
        let delay = retry_delay_with_jitter(attempt, MAX_JITTER_MS);

        // Assert
        assert_eq!(delay, MAX_BACKOFF + Duration::from_millis(MAX_JITTER_MS));
    }
}
