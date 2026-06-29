use crate::boot::observability;

pub(super) fn append_observability_metrics(output: &mut String) {
    match std::panic::catch_unwind(|| {
        let metrics = observability::metrics();

        let mut result = String::new();

        result.push_str("# Observability metrics from MetricsCollector\n");
        for (name, value) in metrics.export_counters() {
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        for (name, value) in metrics.export_gauges() {
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        for (name, buckets) in metrics.export_histograms() {
            let bucket_bounds = ["1ms", "5ms", "10ms", "50ms", "100ms", "500ms", "1s", "5s"];
            let mut cumsum = 0u64;
            for (i, bucket_bound) in bucket_bounds.iter().enumerate() {
                cumsum += buckets[i];
                result.push_str(&format!("{}{{le=\"{}\"}} {}\n", name, bucket_bound, cumsum));
            }
            cumsum += buckets[8];
            result.push_str(&format!("{}{{le=\"+Inf\"}} {}\n", name, cumsum));
            result.push_str(&format!("{}_count {}\n", name, cumsum));
        }

        result
    }) {
        Ok(metrics_output) => output.push_str(&metrics_output),
        Err(_) => {
            tracing::debug!("MetricsCollector not yet initialized in metrics endpoint");
        }
    }
}
