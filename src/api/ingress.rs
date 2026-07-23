// LAYER: API
//! Ingress transport configuration and utilities
//!
//! This module provides shared types and configuration for transport adapters
//! (WebSocket, TCP) to normalize connections and frames into a consistent interface.

use std::sync::Arc;
use std::time::Duration;

/// Maximum time an accepted transport may remain unauthenticated.
pub const CONNECT_DEADLINE: Duration = Duration::from_secs(10);

/// WebSocket liveness cadence and pong grace period.
pub const WEBSOCKET_PING_INTERVAL: Duration = Duration::from_secs(5);
pub const WEBSOCKET_PONG_TIMEOUT: Duration = Duration::from_secs(10);

/// Kernel-assisted TCP dead-peer detection interval.
pub const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(30);

/// Maximum time allowed to receive one HTTP request header block.
pub const HTTP_HEADER_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum lifetime for a non-upgraded HTTP connection.
pub const HTTP_CONNECTION_MAX_LIFETIME: Duration = Duration::from_mins(1);

/// Configuration for ingress transports
#[derive(Debug, Clone)]
pub struct IngressConfig {
    /// Maximum frame size in bytes
    pub max_frame_size: usize,
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Bounded channel capacity between transport and runtime
    pub channel_capacity: usize,
    /// Timeout for backpressure retry
    pub backpressure_timeout: Duration,
    connection_limiter: Arc<tokio::sync::Semaphore>,
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 1024 * 1024, // 1 MB
            max_connections: 10_000,
            channel_capacity: 10_000,
            backpressure_timeout: Duration::from_millis(1),
            connection_limiter: Arc::new(tokio::sync::Semaphore::new(10_000)),
        }
    }
}

impl IngressConfig {
    /// Create a config with custom frame size
    #[must_use]
    pub fn with_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Create a config with custom connection limit
    #[must_use]
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self.connection_limiter = Arc::new(tokio::sync::Semaphore::new(max));
        self
    }

    #[must_use]
    pub(crate) fn connection_limiter(&self) -> Arc<tokio::sync::Semaphore> {
        self.connection_limiter.clone()
    }

    /// Create a config with custom channel capacity
    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Create a config with custom backpressure timeout
    #[must_use]
    pub fn with_backpressure_timeout(mut self, timeout: Duration) -> Self {
        self.backpressure_timeout = timeout;
        self
    }
}

/// Error type for ingress operations
#[derive(Debug, Clone)]
pub enum IngressError {
    /// Frame exceeded maximum size
    FrameTooLarge { size: usize, max: usize },
    /// Connection limit reached
    TooManyConnections,
    /// Channel capacity exceeded
    BackpressureFull,
    /// Session not found
    SessionNotFound(u64),
    /// Invalid frame format
    InvalidFrame(String),
    /// Transport-specific error
    TransportError(String),
}

impl std::fmt::Display for IngressError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FrameTooLarge { size, max } => {
                write!(f, "frame too large: {size} > {max}")
            }
            Self::TooManyConnections => write!(f, "too many connections"),
            Self::BackpressureFull => write!(f, "backpressure: channel full"),
            Self::SessionNotFound(id) => write!(f, "session not found: {id}"),
            Self::InvalidFrame(msg) => write!(f, "invalid frame: {msg}"),
            Self::TransportError(msg) => write!(f, "transport error: {msg}"),
        }
    }
}

impl std::error::Error for IngressError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_default_ingress_config() {
        // Arrange

        // Act
        let config = IngressConfig::default();

        // Assert
        assert_eq!(config.max_frame_size, 1024 * 1024);
        assert_eq!(config.max_connections, 10_000);
        assert_eq!(config.channel_capacity, 10_000);
    }

    #[test]
    fn should_configure_with_builder_methods() {
        // Arrange

        // Act
        let config = IngressConfig::default()
            .with_frame_size(512 * 1024)
            .with_max_connections(5_000)
            .with_channel_capacity(500);

        // Assert
        assert_eq!(config.max_frame_size, 512 * 1024);
        assert_eq!(config.max_connections, 5_000);
        assert_eq!(config.channel_capacity, 500);
    }

    #[test]
    fn should_share_connection_limit_across_config_clones() {
        // Arrange
        let config = IngressConfig::default().with_max_connections(1);
        let cloned = config.clone();

        // Act
        let first = config
            .connection_limiter()
            .try_acquire_owned()
            .expect("first connection permit");
        let second = cloned.connection_limiter().try_acquire_owned();
        drop(first);
        let after_release = cloned.connection_limiter().try_acquire_owned();

        // Assert
        assert!(second.is_err());
        assert!(after_release.is_ok());
    }

    #[test]
    fn should_display_ingress_errors() {
        // Arrange
        let errors = vec![
            IngressError::FrameTooLarge {
                size: 2048,
                max: 1024,
            },
            IngressError::TooManyConnections,
            IngressError::BackpressureFull,
            IngressError::SessionNotFound(123),
            IngressError::InvalidFrame("missing length prefix".to_string()),
            IngressError::TransportError("connection reset".to_string()),
        ];

        // Act
        let mut outputs: Vec<String> = Vec::new();
        for error in errors {
            outputs.push(format!("{error}"));
        }

        // Assert
        assert_eq!(outputs.len(), 6);
    }
}
