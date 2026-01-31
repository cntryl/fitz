//! Client-side error handling and retry logic
//!
//! Implements exponential backoff, error classification, and recovery strategies
//! for network operations against the Fitz broker.

pub mod retry;
pub mod frame;
pub mod timeout;
pub mod validation;

pub use retry::{ExponentialBackoff, RetryConfig, RetryableError, ErrorClassification};
pub use frame::{FrameLimits, FrameValidation, validate_frame, validate_utf8};
pub use timeout::{TimeoutConfig, TimeoutTracker, FrameBuffer};
pub use validation::{SizeLimits, SizeError, ResourceQuota, QuotaError, IntegrityChecker};

/// Configuration for client behavior
#[derive(Clone, Debug, Default)]
pub struct ClientConfig {
    /// Retry configuration
    pub retry: RetryConfig,
    
    /// Frame validation limits
    pub frame_limits: FrameLimits,
}
