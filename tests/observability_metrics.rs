/// Integration tests for observability infrastructure
///
/// Tests:
/// - Metrics collector functionality (counters, gauges, histograms)
/// - Logging setup and configuration
/// - Metrics export in Prometheus format
/// - Tracing span creation and context
#[cfg(test)]
mod tests {
    use fitz::observability as obs;
    use fitz::observability::metrics::MetricsCollector;

    #[test]
    fn should_create_metrics_collector() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        let counter = mc.counter_get("test");

        // Assert
        assert_eq!(counter, 0);
    }

    #[test]
    fn should_increment_counters() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);

        // Assert
        assert_eq!(mc.counter_get(obs::METRIC_FRAMES_RECEIVED), 3);
    }

    #[test]
    fn should_add_to_counters() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.counter_add(obs::METRIC_CONNECTIONS_OPENED, 5);
        mc.counter_add(obs::METRIC_CONNECTIONS_OPENED, 3);

        // Assert
        assert_eq!(mc.counter_get(obs::METRIC_CONNECTIONS_OPENED), 8);
    }

    #[test]
    fn should_set_gauge_values() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 10);
        let first = mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE);
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 15);
        let second = mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE);

        // Assert
        assert_eq!(first, 10);
        assert_eq!(second, 15);
    }

    #[test]
    fn should_increment_decrement_gauges() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 5);

        // Act
        mc.gauge_inc(obs::METRIC_CONNECTIONS_ACTIVE);
        let after_inc = mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE);
        mc.gauge_dec(obs::METRIC_CONNECTIONS_ACTIVE);
        let after_dec = mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE);

        // Assert
        assert_eq!(after_inc, 6);
        assert_eq!(after_dec, 5);
    }

    #[test]
    fn should_record_histogram_observations() {
        // Arrange
        let mc = MetricsCollector::new();

        // Act
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 1);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 10);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 100);

        // Assert
        let buckets = mc
            .histogram_get_buckets(obs::METRIC_MESSAGE_LATENCY)
            .unwrap();
        assert!(buckets[0] > 0); // 1ms bucket
        assert!(buckets[2] > 0); // 10ms bucket
        assert!(buckets[4] > 0); // 100ms bucket
    }

    #[test]
    fn should_export_counters_as_map() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.counter_add(obs::METRIC_FRAMES_RECEIVED, 100);
        mc.counter_add(obs::METRIC_FRAMES_SENT, 50);

        // Act
        let counters = mc.export_counters();

        // Assert
        assert_eq!(counters.get(obs::METRIC_FRAMES_RECEIVED), Some(&100));
        assert_eq!(counters.get(obs::METRIC_FRAMES_SENT), Some(&50));
    }

    #[test]
    fn should_export_gauges_as_map() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 42);
        mc.gauge_set(obs::METRIC_SESSIONS_ACTIVE, 15);

        // Act
        let gauges = mc.export_gauges();

        // Assert
        assert_eq!(gauges.get(obs::METRIC_CONNECTIONS_ACTIVE), Some(&42));
        assert_eq!(gauges.get(obs::METRIC_SESSIONS_ACTIVE), Some(&15));
    }

    #[test]
    fn should_export_histograms_as_map() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 5);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 50);

        // Act
        let histograms = mc.export_histograms();
        let buckets = histograms.get(obs::METRIC_MESSAGE_LATENCY).unwrap();

        // Assert
        assert!(buckets.iter().sum::<u64>() >= 2);
    }

    #[test]
    fn should_generate_prometheus_format() {
        // Arrange
        let mc = MetricsCollector::new();
        mc.counter_add("test_counter", 42);
        mc.gauge_set("test_gauge", 10);
        mc.histogram_observe_ms("test_histogram", 5);

        // Act
        let prometheus_text = mc.to_prometheus_text();

        // Assert
        // Should contain counter
        assert!(prometheus_text.contains("test_counter 42"));

        // Should contain gauge
        assert!(prometheus_text.contains("test_gauge 10"));

        // Should contain histogram
        assert!(prometheus_text.contains("test_histogram"));
        assert!(prometheus_text.contains("le=\"1ms\"") || prometheus_text.contains("le="));
    }

    #[test]
    fn should_handle_concurrent_updates() {
        use std::sync::Arc;
        use std::thread;

        // Arrange
        let mc = Arc::new(MetricsCollector::new());

        // Act
        let mut handles = vec![];
        for _ in 0..10 {
            let mc_clone = mc.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    mc_clone.counter_inc("concurrent_test");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Assert
        // Should have all 1000 increments (10 threads × 100 increments)
        assert_eq!(mc.counter_get("concurrent_test"), 1000);
    }

    #[test]
    fn should_define_sampling_ratios() {
        // Arrange
        // Verify that sampling ratios are defined

        // Act
        let hot_path_ratio = obs::SAMPLING_RATIO_HOT_PATH;
        let always_ratio = obs::SAMPLING_RATIO_ALWAYS;

        // Assert
        assert!((hot_path_ratio - 0.001).abs() <= f64::EPSILON);
        assert!((always_ratio - 1.0).abs() <= f64::EPSILON);
    }
}
