//! RPC error types
//!
//! Defines error codes and responses as specified in the RPC v2 domain spec.
//! These errors are sent back to clients via the reply inbox when RPC
//! operations fail.

use std::fmt;

/// RPC error codes as defined in the spec
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcErrorCode {
    /// Worker didn't reply within lease timeout
    Timeout,
    /// Route queue is at capacity
    Backpressure,
    /// Client lacks permission for this route
    Unauthorized,
    /// Route format is invalid
    InvalidRoute,
    /// Out-of-order chunk received in stream
    StreamGap,
    /// Client inbox vanished mid-request
    ClientDisconnected,
    /// Worker crashed or lease expired without reply
    WorkerCrashed,
}

impl RpcErrorCode {
    /// Get string code for TLV encoding
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Timeout => "RPC_TIMEOUT",
            Self::Backpressure => "RPC_BACKPRESSURE",
            Self::Unauthorized => "RPC_UNAUTHORIZED",
            Self::InvalidRoute => "RPC_INVALID_ROUTE",
            Self::StreamGap => "RPC_STREAM_GAP",
            Self::ClientDisconnected => "RPC_CLIENT_DISCONNECTED",
            Self::WorkerCrashed => "RPC_WORKER_CRASHED",
        }
    }
}

impl fmt::Display for RpcErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// RPC error with correlation ID for client matching
#[derive(Debug, Clone)]
pub struct RpcError {
    /// Correlation ID from the original request
    pub correlation_id: String,
    /// Error code
    pub code: RpcErrorCode,
    /// Human-readable error message
    pub message: String,
}

impl RpcError {
    /// Create new RPC error
    pub fn new(correlation_id: String, code: RpcErrorCode, message: String) -> Self {
        Self {
            correlation_id,
            code,
            message,
        }
    }

    /// Create timeout error
    pub fn timeout(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::Timeout,
            "Worker did not reply within lease timeout".to_string(),
        )
    }

    /// Create backpressure error
    pub fn backpressure(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::Backpressure,
            "Route queue is at capacity, retry with backoff".to_string(),
        )
    }

    /// Create unauthorized error
    pub fn unauthorized(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::Unauthorized,
            "Insufficient permissions for this route".to_string(),
        )
    }

    /// Create invalid route error
    pub fn invalid_route(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::InvalidRoute,
            "Route format is invalid".to_string(),
        )
    }

    /// Create stream gap error
    pub fn stream_gap(correlation_id: String, expected: u64, received: u64) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::StreamGap,
            format!(
                "Out-of-order chunk: expected seq {}, received {}",
                expected, received
            ),
        )
    }

    /// Create client disconnected error
    pub fn client_disconnected(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::ClientDisconnected,
            "Client inbox vanished mid-request".to_string(),
        )
    }

    /// Create worker crashed error
    pub fn worker_crashed(correlation_id: String) -> Self {
        Self::new(
            correlation_id,
            RpcErrorCode::WorkerCrashed,
            "Worker crashed or lease expired without reply".to_string(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_format_error_codes() {
        assert_eq!(RpcErrorCode::Timeout.as_str(), "RPC_TIMEOUT");
        assert_eq!(RpcErrorCode::Backpressure.as_str(), "RPC_BACKPRESSURE");
    }

    #[test]
    fn should_create_backpressure_error() {
        let err = RpcError::backpressure("req-001".to_string());
        assert_eq!(err.correlation_id, "req-001");
        assert_eq!(err.code, RpcErrorCode::Backpressure);
    }
}
