//! Transport configuration shared by TCP and WebSocket handlers

use std::time::Duration;

/// Parameters guiding transport behavior
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub max_frame_size: usize,
    pub channel_capacity: usize,
    pub backpressure_timeout: Duration,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            max_frame_size: 1024 * 1024,
            channel_capacity: 512,
            backpressure_timeout: Duration::from_millis(50),
        }
    }
}

impl TransportConfig {
    pub fn with_frame_size(mut self, size: usize) -> Self {
        self.max_frame_size = size;
        self
    }

    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity;
        self
    }

    pub fn with_backpressure_timeout(mut self, timeout: Duration) -> Self {
        self.backpressure_timeout = timeout;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_values_are_reasonable() {
        let config = TransportConfig::default();
        assert_eq!(config.max_frame_size, 1024 * 1024);
        assert_eq!(config.channel_capacity, 512);
    }
}
