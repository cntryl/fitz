//! Timeout and partial frame handling
//!
//! Implements:
//! - Per-operation timeout configuration
//! - Timeout detection and enforcement
//! - Partial frame assembly with activity timeout
//! - Buffering for multi-packet frames

use std::time::{Duration, Instant};

/// Timeout configuration
#[derive(Clone, Debug)]
pub struct TimeoutConfig {
    /// Default timeout for individual operations (default 30s)
    pub operation_timeout: Duration,
    
    /// Timeout for partial frame assembly (default 5s idle)
    pub partial_frame_timeout: Duration,
    
    /// Transaction idle timeout (default 1 hour)
    pub transaction_timeout: Duration,
    
    /// Session idle timeout (default 1 hour)
    pub session_timeout: Duration,
}

impl TimeoutConfig {
    pub fn new(
        operation: Duration,
        partial_frame: Duration,
        transaction: Duration,
        session: Duration,
    ) -> Self {
        Self {
            operation_timeout: operation,
            partial_frame_timeout: partial_frame,
            transaction_timeout: transaction,
            session_timeout: session,
        }
    }
}

impl Default for TimeoutConfig {
    fn default() -> Self {
        Self {
            operation_timeout: Duration::from_secs(30),
            partial_frame_timeout: Duration::from_secs(5),
            transaction_timeout: Duration::from_secs(3600), // 1 hour
            session_timeout: Duration::from_secs(3600),     // 1 hour
        }
    }
}

/// Tracks timeout state
#[derive(Clone, Debug)]
pub struct TimeoutTracker {
    config: TimeoutConfig,
    deadline: Instant,
}

impl TimeoutTracker {
    /// Create a new timeout tracker with operation timeout
    pub fn new(config: TimeoutConfig) -> Self {
        let deadline = Instant::now() + config.operation_timeout;
        Self { config, deadline }
    }

    /// Create with custom timeout
    pub fn with_timeout(config: TimeoutConfig, timeout: Duration) -> Self {
        let deadline = Instant::now() + timeout;
        Self { config, deadline }
    }

    /// Check if operation has timed out
    pub fn is_expired(&self) -> bool {
        Instant::now() > self.deadline
    }

    /// Get remaining time before timeout
    pub fn remaining(&self) -> Option<Duration> {
        self.deadline.checked_duration_since(Instant::now())
    }

    /// Reset deadline for another operation
    pub fn reset(&mut self) {
        self.deadline = Instant::now() + self.config.operation_timeout;
    }

    /// Reset with custom timeout
    pub fn reset_with_timeout(&mut self, timeout: Duration) {
        self.deadline = Instant::now() + timeout;
    }
}

/// Partial frame buffer for multi-packet frames
#[derive(Clone, Debug)]
pub struct FrameBuffer {
    /// Accumulated data
    data: Vec<u8>,
    
    /// Last activity time
    last_activity: Instant,
    
    /// Maximum buffer size (DoS protection)
    max_size: usize,
    
    /// Timeout configuration
    timeout_config: TimeoutConfig,
}

impl FrameBuffer {
    /// Create new frame buffer
    pub fn new(max_size: usize, timeout_config: TimeoutConfig) -> Self {
        Self {
            data: Vec::new(),
            last_activity: Instant::now(),
            max_size,
            timeout_config,
        }
    }

    /// Add data to buffer
    /// Returns error if buffer would exceed max size
    pub fn add(&mut self, chunk: &[u8]) -> Result<(), String> {
        if self.data.len() + chunk.len() > self.max_size {
            return Err(format!(
                "Frame buffer overflow: {} + {} > {}",
                self.data.len(),
                chunk.len(),
                self.max_size
            ));
        }

        self.data.extend_from_slice(chunk);
        self.last_activity = Instant::now();
        Ok(())
    }

    /// Check if buffer has timed out (idle for too long)
    pub fn is_idle_timeout(&self) -> bool {
        Instant::now().duration_since(self.last_activity)
            > self.timeout_config.partial_frame_timeout
    }

    /// Get current buffered data
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Take ownership of buffered data
    pub fn take(self) -> Vec<u8> {
        self.data
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.data.clear();
        self.last_activity = Instant::now();
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Current buffer size
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn should_detect_operation_timeout() {
        let config = TimeoutConfig::default();
        let tracker =
            TimeoutTracker::with_timeout(config, Duration::from_millis(10));

        assert!(!tracker.is_expired());
        thread::sleep(Duration::from_millis(20));
        assert!(tracker.is_expired());
    }

    #[test]
    fn should_report_remaining_time() {
        let config = TimeoutConfig::default();
        let tracker =
            TimeoutTracker::with_timeout(config, Duration::from_secs(10));

        let remaining = tracker.remaining();
        assert!(remaining.is_some());
        // Allow full 10 seconds since test runs very quickly
        assert!(remaining.unwrap() <= Duration::from_secs(10));
        assert!(remaining.unwrap() > Duration::from_secs(8));
    }

    #[test]
    fn should_reject_oversized_buffer() {
        let config = TimeoutConfig::default();
        let mut buffer = FrameBuffer::new(100, config);

        let result = buffer.add(&[0u8; 50]);
        assert!(result.is_ok());

        let result = buffer.add(&[0u8; 60]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("overflow"));
    }

    #[test]
    fn should_detect_idle_timeout() {
        let config = TimeoutConfig {
            operation_timeout: Duration::from_secs(30),
            partial_frame_timeout: Duration::from_millis(10),
            transaction_timeout: Duration::from_secs(3600),
            session_timeout: Duration::from_secs(3600),
        };
        let mut buffer = FrameBuffer::new(1000, config);

        buffer.add(b"data").unwrap();
        assert!(!buffer.is_idle_timeout());

        thread::sleep(Duration::from_millis(20));
        assert!(buffer.is_idle_timeout());
    }
}
