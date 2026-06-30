//! Retry logic with exponential backoff
//!
//! Implements the retry strategy specified in Phase A error handling tests:
//! - Exponential backoff: base 100ms, max 30s
//! - Max retries: 10 (configurable)
//! - Error classification: retryable vs fatal

use std::time::Duration;

/// Exponential backoff calculator.
///
/// Formula: `backoff = min(base * (2 ^ attempt), max_backoff)`.
/// Example sequence (`base=100ms`, `max=30s`):
///   attempt 1: `100ms`
///   attempt 2: `200ms`
///   attempt 3: `400ms`
///   attempt 4: `800ms`
///   attempt 5: `1600ms`
///   attempt 6: `3200ms`
///   attempt 7: `6400ms`
///   attempt 8: `12800ms`
///   attempt 9: `25600ms`
///   attempt 10: `30000ms` (capped at max)
#[derive(Clone, Debug)]
pub struct ExponentialBackoff {
    base_ms: u64,
    max_ms: u64,
}

impl ExponentialBackoff {
    fn duration_millis_saturated(duration: Duration) -> u64 {
        u64::try_from(duration.as_millis().min(u128::from(u64::MAX))).unwrap_or(u64::MAX)
    }

    /// Create new backoff with base delay and maximum cap
    #[must_use]
    pub fn new(base: Duration, max: Duration) -> Self {
        Self {
            base_ms: Self::duration_millis_saturated(base),
            max_ms: Self::duration_millis_saturated(max),
        }
    }

    /// Calculate delay for attempt number (0-indexed)
    ///
    /// # Arguments
    /// * `attempt` - Attempt number (0 for first retry, 1 for second, etc.)
    ///
    /// # Returns
    /// Duration to wait before this attempt
    #[must_use]
    pub fn delay(&self, attempt: u32) -> Duration {
        let delay_ms = self.base_ms.saturating_mul(2_u64.saturating_pow(attempt));
        let capped = delay_ms.min(self.max_ms);
        Duration::from_millis(capped)
    }
}

impl Default for ExponentialBackoff {
    /// Default: base 100ms, max 30s (as per Phase A spec)
    fn default() -> Self {
        Self {
            base_ms: 100,
            max_ms: 30_000,
        }
    }
}

/// Retry configuration
#[derive(Clone, Debug)]
pub struct RetryConfig {
    /// Backoff calculator
    pub backoff: ExponentialBackoff,

    /// Maximum number of retry attempts
    /// (e.g., 10 means: 1 initial + 10 retries = 11 total attempts)
    pub max_retries: u32,

    /// Classification function for determining if error is retryable
    pub classify: fn(&str) -> ErrorClassification,
}

impl RetryConfig {
    /// Create custom retry config
    pub fn new(
        backoff: ExponentialBackoff,
        max_retries: u32,
        classify: fn(&str) -> ErrorClassification,
    ) -> Self {
        Self {
            backoff,
            max_retries,
            classify,
        }
    }

    /// Calculate if we should retry after this attempt
    #[must_use]
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }

    /// Get delay for next attempt
    #[must_use]
    pub fn next_delay(&self, attempt: u32) -> Duration {
        self.backoff.delay(attempt)
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            backoff: ExponentialBackoff::default(),
            max_retries: 10,
            classify: default_error_classification,
        }
    }
}

/// Error classification for retry decisions
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorClassification {
    /// Should retry: connection refused, connection reset, timeout
    Retryable,

    /// Should not retry: protocol violation, authorization failure
    Fatal,
}

/// Retryable error wrapper
#[derive(Clone, Debug)]
pub struct RetryableError {
    pub message: String,
    pub classification: ErrorClassification,
}

impl RetryableError {
    #[must_use]
    pub fn new(message: String, classification: ErrorClassification) -> Self {
        Self {
            message,
            classification,
        }
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        self.classification == ErrorClassification::Retryable
    }
}

/// Default error classification logic
///
/// Retryable errors:
/// - `ECONNREFUSED`: connection refused
/// - `ECONNRESET`: connection reset
/// - `ETIMEDOUT`: operation timeout
/// - `EAGAIN`: resource temporarily unavailable
/// - `timeout`: application-level timeout
///
/// Fatal errors:
/// - `ERR_FRAME_TOO_LARGE`: protocol violation
/// - `ERR_INVALID_UTF8`: protocol violation
/// - `ERR_UNAUTHORIZED`: authorization failure
/// - `ERR_INVALID_OPERATION`: protocol violation
#[must_use]
pub fn default_error_classification(error_msg: &str) -> ErrorClassification {
    let msg_lower = error_msg.to_lowercase();

    // Retryable patterns
    if msg_lower.contains("econnrefused")
        || msg_lower.contains("connection refused")
        || msg_lower.contains("econnreset")
        || msg_lower.contains("connection reset")
        || msg_lower.contains("etimedout")
        || msg_lower.contains("timeout")
        || msg_lower.contains("eagain")
        || msg_lower.contains("resource temporarily unavailable")
    {
        return ErrorClassification::Retryable;
    }

    // Fatal patterns
    if msg_lower.contains("err_frame_too_large")
        || msg_lower.contains("frame too large")
        || msg_lower.contains("err_invalid_utf8")
        || msg_lower.contains("invalid utf-8")
        || msg_lower.contains("invalid utf8")
        || msg_lower.contains("err_unauthorized")
        || msg_lower.contains("unauthorized")
        || msg_lower.contains("err_invalid_operation")
        || msg_lower.contains("invalid operation")
    {
        return ErrorClassification::Fatal;
    }

    // Default: treat as retryable if uncertain
    ErrorClassification::Retryable
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_calculate_exponential_backoff_correctly() {
        // Arrange
        let backoff = ExponentialBackoff::default();

        // Act
        // Assert
        assert_eq!(backoff.delay(0), Duration::from_millis(100));
        assert_eq!(backoff.delay(1), Duration::from_millis(200));
        assert_eq!(backoff.delay(2), Duration::from_millis(400));
        assert_eq!(backoff.delay(3), Duration::from_millis(800));
        assert_eq!(backoff.delay(4), Duration::from_millis(1600));
        assert_eq!(backoff.delay(5), Duration::from_millis(3200));
        assert_eq!(backoff.delay(6), Duration::from_millis(6400));
        assert_eq!(backoff.delay(7), Duration::from_millis(12800));
        assert_eq!(backoff.delay(8), Duration::from_millis(25600));
        // Capped at 30000ms
        assert_eq!(backoff.delay(9), Duration::from_secs(30));
        assert_eq!(backoff.delay(10), Duration::from_secs(30));
    }

    #[test]
    fn should_classify_retryable_errors() {
        // Arrange
        // Act
        // Assert
        assert_eq!(
            default_error_classification("ECONNREFUSED"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            default_error_classification("connection refused"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            default_error_classification("ECONNRESET"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            default_error_classification("timeout"),
            ErrorClassification::Retryable
        );
    }

    #[test]
    fn should_classify_fatal_errors() {
        // Arrange
        // Act
        // Assert
        assert_eq!(
            default_error_classification("ERR_FRAME_TOO_LARGE"),
            ErrorClassification::Fatal
        );
        assert_eq!(
            default_error_classification("ERR_INVALID_UTF8"),
            ErrorClassification::Fatal
        );
        assert_eq!(
            default_error_classification("ERR_UNAUTHORIZED"),
            ErrorClassification::Fatal
        );
    }

    #[test]
    fn should_respect_max_retries() {
        // Arrange
        let config = RetryConfig::default();

        // Act
        // Assert
        assert!(config.should_retry(0));
        assert!(config.should_retry(9));
        assert!(!config.should_retry(10));
    }
}
