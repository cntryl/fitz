use std::time::Instant;
/// Tracing helpers and utilities for Fitz.
///
/// Provides helpers for:
/// - Creating spans with automatic context linking
/// - Linking MessageId causation to span relationships
/// - Recording attributes with automatic formatting
///
/// Safe for use in both async and sync code.
use tracing::Span;

/// Guard for measuring operation latency and recording to tracing span.
/// Automatically records the duration when dropped.
pub struct LatencyGuard {
    span: Span,
    metric_name: Option<String>,
    start: Instant,
}

impl LatencyGuard {
    /// Create a new latency guard.
    /// When dropped, records the duration to the span and optionally to a metric.
    pub fn new(span: Span, metric_name: Option<String>) -> Self {
        Self {
            span,
            metric_name,
            start: Instant::now(),
        }
    }

    /// Get the elapsed time since guard creation (without consuming it).
    pub fn elapsed_secs(&self) -> f64 {
        let elapsed = self.start.elapsed();
        elapsed.as_secs_f64()
    }

    /// Get the elapsed time in milliseconds.
    pub fn elapsed_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    /// Get the elapsed time in microseconds.
    pub fn elapsed_us(&self) -> u64 {
        self.start.elapsed().as_micros() as u64
    }
}

impl Drop for LatencyGuard {
    fn drop(&mut self) {
        let elapsed_ms = self.start.elapsed().as_millis() as u64;
        self.span.record("latency_ms", elapsed_ms);

        if let Some(_metric_name) = &self.metric_name {
            // In a future implementation, record to metrics here
            // metrics.histogram_observe_ms(metric_name, elapsed_ms);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_measure_elapsed_time() {
        let span = tracing::info_span!("test");
        let guard = LatencyGuard::new(span, None);
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Should be approximately 10ms, allow some variance
        assert!(guard.elapsed_ms() >= 10);
        assert!(guard.elapsed_ms() < 100);
    }

    #[test]
    fn should_convert_units() {
        let span = tracing::info_span!("test");
        let guard = LatencyGuard::new(span, None);
        std::thread::sleep(std::time::Duration::from_millis(1));

        let ms = guard.elapsed_ms();
        let us = guard.elapsed_us();

        // us should be approximately 1000x larger
        assert!(us >= ms * 1000 - 100); // Allow some variance
        assert!(us <= ms * 1000 + 1000);
    }
}
