//! Retry logic with exponential backoff
//!
//! Retries errors classified as retryable and stops on fatal or indeterminate
//! outcomes.

use std::time::Duration;

/// Calculate an exponentially increasing delay, capped by `max_ms`.
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
    /// Default retry delay starts at 100 ms and is capped at 30 seconds.
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

    /// Check whether another attempt is within the configured retry budget.
    ///
    /// This method does not classify an error. Use [`Self::should_retry_error`]
    /// when the error message is available.
    #[must_use]
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_retries
    }

    /// Check the attempt limit and classify an error before retrying.
    ///
    /// A retry can duplicate an operation when its outcome is indeterminate.
    /// Callers must also ensure the operation is safe to repeat.
    #[must_use]
    pub fn should_retry_error(&self, attempt: u32, error: &str) -> bool {
        self.should_retry(attempt) && (self.classify)(error) == ErrorClassification::Retryable
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
    /// The failure is known to have happened before dispatch and can be retried.
    Retryable,

    /// The request is known to have failed due to a protocol or authorization error.
    Fatal,

    /// The operation may have taken effect, so retrying could duplicate it.
    Indeterminate,
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
///
/// Timeouts, connection resets, resource contention, and unknown errors are
/// indeterminate: the operation may already have taken effect.
///
/// Fatal errors:
/// - `ERR_FRAME_TOO_LARGE`: protocol violation
/// - `ERR_INVALID_UTF8`: protocol violation
/// - `ERR_UNAUTHORIZED`: authorization failure
/// - `ERR_INVALID_OPERATION`: protocol violation
#[must_use]
pub fn default_error_classification(error_msg: &str) -> ErrorClassification {
    let msg_lower = error_msg.to_lowercase();

    // A refused connection is known to have failed before request dispatch.
    if msg_lower.contains("econnrefused") || msg_lower.contains("connection refused") {
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

    // The operation may have reached the broker, so uncertainty is not safe to retry.
    ErrorClassification::Indeterminate
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
    fn should_classify_retry_outcomes() {
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
            ErrorClassification::Indeterminate
        );
        assert_eq!(
            default_error_classification("timeout"),
            ErrorClassification::Indeterminate
        );
        assert_eq!(
            default_error_classification("connection refused"),
            ErrorClassification::Retryable
        );
        assert_eq!(
            default_error_classification("connection reset after request write"),
            ErrorClassification::Indeterminate
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

    #[test]
    fn should_retry_only_classified_errors_within_attempt_limit() {
        // Arrange
        let config = RetryConfig::default();

        // Act
        // Assert
        assert!(config.should_retry_error(0, "connection refused"));
        assert!(!config.should_retry_error(0, "operation timeout"));
        assert!(!config.should_retry_error(10, "connection refused"));
        assert!(!config.should_retry_error(0, "unrecognized failure"));
    }
}
