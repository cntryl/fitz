//! Golden contract for admin Stream metrics shape.
//!
//! Metric values are process-global and vary between tests, so this pins the
//! stats JSON key paths and exported `fitz_stream_*` series names only.

use super::common::*;
use fitz::testkit::golden::assert_golden;
use std::collections::BTreeSet;
use std::fmt::Write as _;

const STREAM_COUNTERS: &[&str] = &[
    "fitz_stream_admin_projection_failures_total",
    "fitz_stream_append_conflicts_total",
    "fitz_stream_append_sessions_ended_total",
    "fitz_stream_append_sessions_started_total",
    "fitz_stream_failure_total",
    "fitz_stream_family_failed_closed_total",
    "fitz_stream_maintenance_attempts_total",
    "fitz_stream_maintenance_buckets_compacted_total",
    "fitz_stream_maintenance_failures_total",
    "fitz_stream_maintenance_retries_total",
    "fitz_stream_notify_drops_total",
    "fitz_stream_operations_total",
    "fitz_stream_publish_family_mismatch_total",
    "fitz_stream_requests_total",
    "fitz_stream_response_drops_total",
    "fitz_stream_success_total",
    "fitz_stream_watermark_coordination_drops_total",
];

fn json_key_paths(value: &serde_json::Value, prefix: &str, out: &mut BTreeSet<String>) {
    if let serde_json::Value::Object(map) = value {
        for (key, child) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            json_key_paths(child, &path, out);
            out.insert(path);
        }
    }
}

async fn get_body(runtime: &std::sync::Arc<Runtime>, cookie: &str, uri: &str) -> Bytes {
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri(uri)
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    body::to_bytes(response.into_body()).await.unwrap()
}

#[tokio::test]
#[serial]
async fn should_keep_admin_stream_metrics_shape_stable() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    seed_stream_watermark_lag_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    for counter in STREAM_COUNTERS {
        metrics.counter_add(counter, 0);
    }
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    let cookie = login_cookie(runtime.clone()).await;
    let mut stats_keys = BTreeSet::new();
    let mut actual = String::new();

    // Act
    let stats = get_body(&runtime, &cookie, "/api/v1/all/stream/stats").await;
    let stats: serde_json::Value = serde_json::from_slice(&stats).unwrap();
    json_key_paths(&stats, "", &mut stats_keys);
    let metrics = get_body(&runtime, &cookie, "/api/v1/all/metrics").await;
    let series: BTreeSet<String> = structured_metrics_text(&metrics)
        .lines()
        .filter_map(|line| line.split([' ', '{']).next())
        .filter(|name| name.starts_with("fitz_stream_"))
        .map(str::to_string)
        .collect();
    for key in &stats_keys {
        let _ = writeln!(actual, "stats {key}");
    }
    for name in &series {
        let _ = writeln!(actual, "series {name}");
    }

    // Assert
    assert_golden("admin_stream_metrics_shape", &actual);
}
