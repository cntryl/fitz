//! Client-side error handling and retry logic
//!
//! Implements exponential backoff, error classification, and recovery strategies
//! for network operations against the Fitz broker.

pub mod frame;
pub mod retry;
pub mod timeout;
pub mod validation;

pub use frame::{validate_frame, validate_utf8, FrameLimits, FrameValidation};
pub use retry::{ErrorClassification, ExponentialBackoff, RetryConfig, RetryableError};
pub use timeout::{FrameBuffer, TimeoutConfig, TimeoutTracker};
pub use validation::{IntegrityChecker, QuotaError, ResourceQuota, SizeError, SizeLimits};

/// Configuration for client behavior
#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    /// Retry configuration
    pub retry: RetryConfig,

    /// Frame validation limits
    pub frame_limits: FrameLimits,
}
