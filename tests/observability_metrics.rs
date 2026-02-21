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
        let mc = MetricsCollector::new();
        assert_eq!(mc.counter_get("test"), 0);
    }

    #[test]
    fn should_increment_counters() {
        let mc = MetricsCollector::new();
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);
        mc.counter_inc(obs::METRIC_FRAMES_RECEIVED);

        assert_eq!(mc.counter_get(obs::METRIC_FRAMES_RECEIVED), 3);
    }

    #[test]
    fn should_add_to_counters() {
        let mc = MetricsCollector::new();
        mc.counter_add(obs::METRIC_CONNECTIONS_OPENED, 5);
        mc.counter_add(obs::METRIC_CONNECTIONS_OPENED, 3);

        assert_eq!(mc.counter_get(obs::METRIC_CONNECTIONS_OPENED), 8);
    }

    #[test]
    fn should_set_gauge_values() {
        let mc = MetricsCollector::new();
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 10);
        assert_eq!(mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE), 10);

        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 15);
        assert_eq!(mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE), 15);
    }

    #[test]
    fn should_increment_decrement_gauges() {
        let mc = MetricsCollector::new();
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 5);
        mc.gauge_inc(obs::METRIC_CONNECTIONS_ACTIVE);
        assert_eq!(mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE), 6);

        mc.gauge_dec(obs::METRIC_CONNECTIONS_ACTIVE);
        assert_eq!(mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE), 5);
    }

    #[test]
    fn should_record_histogram_observations() {
        let mc = MetricsCollector::new();
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 1);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 10);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 100);

        let buckets = mc
            .histogram_get_buckets(obs::METRIC_MESSAGE_LATENCY)
            .unwrap();
        assert!(buckets[0] > 0); // 1ms bucket
        assert!(buckets[2] > 0); // 10ms bucket
        assert!(buckets[4] > 0); // 100ms bucket
    }

    #[test]
    fn should_export_counters_as_map() {
        let mc = MetricsCollector::new();
        mc.counter_add(obs::METRIC_FRAMES_RECEIVED, 100);
        mc.counter_add(obs::METRIC_FRAMES_SENT, 50);

        let counters = mc.export_counters();
        assert_eq!(counters.get(obs::METRIC_FRAMES_RECEIVED), Some(&100));
        assert_eq!(counters.get(obs::METRIC_FRAMES_SENT), Some(&50));
    }

    #[test]
    fn should_export_gauges_as_map() {
        let mc = MetricsCollector::new();
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 42);
        mc.gauge_set(obs::METRIC_SESSIONS_ACTIVE, 15);

        let gauges = mc.export_gauges();
        assert_eq!(gauges.get(obs::METRIC_CONNECTIONS_ACTIVE), Some(&42));
        assert_eq!(gauges.get(obs::METRIC_SESSIONS_ACTIVE), Some(&15));
    }

    #[test]
    fn should_export_histograms_as_map() {
        let mc = MetricsCollector::new();
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 5);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 50);

        let histograms = mc.export_histograms();
        let buckets = histograms.get(obs::METRIC_MESSAGE_LATENCY).unwrap();
        assert!(buckets.iter().sum::<u64>() >= 2);
    }

    #[test]
    fn should_generate_prometheus_format() {
        let mc = MetricsCollector::new();
        mc.counter_add("test_counter", 42);
        mc.gauge_set("test_gauge", 10);
        mc.histogram_observe_ms("test_histogram", 5);

        let prometheus_text = mc.to_prometheus_text();

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

        let mc = Arc::new(MetricsCollector::new());

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

        // Should have all 1000 increments (10 threads × 100 increments)
        assert_eq!(mc.counter_get("concurrent_test"), 1000);
    }

    #[test]
    fn should_support_metric_constants() {
        let mc = MetricsCollector::new();

        // Test that constants are accessible
        mc.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
        mc.counter_add(obs::METRIC_FRAMES_RECEIVED, 5);
        mc.gauge_set(obs::METRIC_CONNECTIONS_ACTIVE, 10);
        mc.histogram_observe_ms(obs::METRIC_MESSAGE_LATENCY, 100);

        assert!(mc.counter_get(obs::METRIC_CONNECTIONS_OPENED) > 0);
        assert_eq!(mc.counter_get(obs::METRIC_FRAMES_RECEIVED), 5);
        assert_eq!(mc.gauge_get(obs::METRIC_CONNECTIONS_ACTIVE), 10);
    }

    #[test]
    fn should_include_all_required_metrics() {
        // Verify that all expected metric names are defined as constants

        // Counters
        let _ = obs::METRIC_CONNECTIONS_OPENED;
        let _ = obs::METRIC_CONNECTIONS_CLOSED;
        let _ = obs::METRIC_FRAMES_RECEIVED;
        let _ = obs::METRIC_FRAMES_SENT;
        let _ = obs::METRIC_ROUTE_MISMATCHES;
        let _ = obs::METRIC_DELIVERY_FAILURES;
        let _ = obs::METRIC_PERMISSION_DENIALS;
        let _ = obs::METRIC_DOMAIN_OPERATIONS;

        // Gauges
        let _ = obs::METRIC_CONNECTIONS_ACTIVE;
        let _ = obs::METRIC_SESSIONS_ACTIVE;
        let _ = obs::METRIC_MAILBOX_DEPTH;

        // Histograms
        let _ = obs::METRIC_MESSAGE_LATENCY;
        let _ = obs::METRIC_PERMISSION_CHECK_LATENCY;
        let _ = obs::METRIC_DOMAIN_OPERATION_LATENCY;
    }

    #[test]
    fn should_include_all_required_span_names() {
        // Verify that all expected span names are defined

        let _ = obs::SPAN_REQUEST;
        let _ = obs::SPAN_TLV_ENCODE;
        let _ = obs::SPAN_TLV_DECODE;
        let _ = obs::SPAN_ROUTE_MATCH;
        let _ = obs::SPAN_PERMISSION_CHECK;
        let _ = obs::SPAN_DOMAIN_OPERATION;
    }

    #[test]
    fn should_include_all_required_attribute_keys() {
        // Verify that all expected attribute keys are defined

        let _ = obs::ATTR_MESSAGE_ID;
        let _ = obs::ATTR_ROUTE;
        let _ = obs::ATTR_DOMAIN;
        let _ = obs::ATTR_REALM;
        let _ = obs::ATTR_SESSION_ID;
        let _ = obs::ATTR_ACTOR_ID;
        let _ = obs::ATTR_OPERATION;
        let _ = obs::ATTR_ERROR_TYPE;
    }

    #[test]
    fn should_define_sampling_ratios() {
        // Verify that sampling ratios are defined
        assert_eq!(obs::SAMPLING_RATIO_HOT_PATH, 0.001);
        assert_eq!(obs::SAMPLING_RATIO_ALWAYS, 1.0);
    }
}
