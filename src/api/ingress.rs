// LAYER: API
//! Ingress transport configuration and utilities
//!
//! This module provides shared types and configuration for transport adapters
//! (WebSocket, TCP) to normalize connections and frames into a consistent interface.

use std::time::Duration;

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
}

impl Default for IngressConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 1024 * 1024, // 1 MB
            max_connections: 10_000,
            channel_capacity: 10_000,
            backpressure_timeout: Duration::from_millis(1),
        }
    }
}

impl IngressConfig {
    /// Create a config with custom frame size
    pub fn with_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = size;
        self
    }

    /// Create a config with custom connection limit
    pub fn with_max_connections(mut self, max: usize) -> Self {
        self.max_connections = max;
        self
    }

    /// Create a config with custom channel capacity
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    /// Create a config with custom backpressure timeout
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
                write!(f, "frame too large: {} > {}", size, max)
            }
            Self::TooManyConnections => write!(f, "too many connections"),
            Self::BackpressureFull => write!(f, "backpressure: channel full"),
            Self::SessionNotFound(id) => write!(f, "session not found: {}", id),
            Self::InvalidFrame(msg) => write!(f, "invalid frame: {}", msg),
            Self::TransportError(msg) => write!(f, "transport error: {}", msg),
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
        assert_eq!(config.channel_capacity, 1000);
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
            outputs.push(format!("{}", error));
        }

        // Assert
        assert_eq!(outputs.len(), 6);
    }
}
