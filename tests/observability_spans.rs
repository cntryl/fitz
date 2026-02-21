/// Integration tests for tracing and span functionality
#[cfg(test)]
mod tests {
    use fitz::observability::tracing::LatencyGuard;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn should_measure_latency() {
        let span = tracing::info_span!("test_span");
        let guard = LatencyGuard::new(span, None);

        thread::sleep(Duration::from_millis(10));

        let elapsed_ms = guard.elapsed_ms();
        assert!((10..100).contains(&elapsed_ms));
    }

    #[test]
    fn should_convert_to_microseconds() {
        let span = tracing::info_span!("test_span");
        let guard = LatencyGuard::new(span, None);

        thread::sleep(Duration::from_millis(1));

        let elapsed_us = guard.elapsed_us();
        assert!(elapsed_us >= 1000);
    }

    #[test]
    fn should_calculate_seconds_as_float() {
        let span = tracing::info_span!("test_span");
        let guard = LatencyGuard::new(span, None);

        thread::sleep(Duration::from_millis(5));

        let elapsed_secs = guard.elapsed_secs();
        assert!((0.005..0.1).contains(&elapsed_secs));
    }

    #[test]
    fn should_support_optional_metric_name() {
        let span = tracing::info_span!("test_span");
        // Should not panic even when metric_name is Some
        let _guard = LatencyGuard::new(span, Some("test_metric".to_string()));
    }
}
