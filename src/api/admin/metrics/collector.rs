use crate::boot::observability;
use std::collections::BTreeSet;

const HISTOGRAM_BUCKET_BOUNDS: [&str; 9] = [
    "1ms", "5ms", "10ms", "50ms", "100ms", "500ms", "1s", "5s", "+Inf",
];

fn existing_metric_types(output: &str) -> BTreeSet<String> {
    output
        .lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|line| line.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

fn append_metric_metadata(
    output: &mut String,
    emitted_types: &mut BTreeSet<String>,
    name: &str,
    metric_type: &str,
    help: &str,
) {
    if !emitted_types.insert(name.to_string()) {
        return;
    }

    output.push_str(&format!("# HELP {name} {help}\n"));
    output.push_str(&format!("# TYPE {name} {metric_type}\n"));
}

pub(super) fn append_observability_metrics(output: &mut String) {
    let emitted_types = existing_metric_types(output);

    match std::panic::catch_unwind(|| {
        let metrics = observability::metrics();

        let mut result = String::new();
        let mut emitted_types = emitted_types;

        result.push_str("# Observability metrics from MetricsCollector\n");
        for (name, value) in metrics.export_counters() {
            append_metric_metadata(
                &mut result,
                &mut emitted_types,
                &name,
                "counter",
                "Fitz observability counter metric",
            );
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        for (name, value) in metrics.export_gauges() {
            append_metric_metadata(
                &mut result,
                &mut emitted_types,
                &name,
                "gauge",
                "Fitz observability gauge metric",
            );
            result.push_str(&format!("{} {}\n", name, value));
        }

        result.push('\n');

        for (name, buckets) in metrics.export_histograms() {
            append_metric_metadata(
                &mut result,
                &mut emitted_types,
                &name,
                "histogram",
                "Fitz observability histogram metric",
            );
            let mut cumsum = 0u64;
            for (i, bucket_bound) in HISTOGRAM_BUCKET_BOUNDS.iter().enumerate() {
                cumsum += buckets[i];
                result.push_str(&format!("{}{{le=\"{}\"}} {}\n", name, bucket_bound, cumsum));
            }
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
