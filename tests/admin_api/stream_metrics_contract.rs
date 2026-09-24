//! Golden contract for admin Stream metrics shape.
//!
//! Metric values are process-global and vary between tests, so this pins the
//! stats JSON key paths exactly and requires the `fitz_stream_*` series the
//! admin exporter emits without traffic. Series that other tests register
//! may also appear; they are allowed but not required.

use super::common::*;
use fitz::testkit::golden::{assert_golden, assert_golden_subset};
use std::collections::BTreeSet;
use std::fmt::Write as _;

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
        let _ = writeln!(actual, "{key}");
    }
    let series = series.into_iter().collect::<Vec<_>>().join("\n");

    // Assert
    assert_golden("admin_stream_stats_keys", &actual);
    assert_golden_subset("admin_stream_series", &series);
}
