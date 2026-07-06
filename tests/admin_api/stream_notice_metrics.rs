use super::common::*;

#[tokio::test]
#[serial]
async fn should_return_stream_domain_stats_given_recorded_operations() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    seed_stream_watermark_lag_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    metrics.counter_add("fitz_stream_operations_total", 5);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 8);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 60);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["events_total"], 3);
    assert_eq!(payload["watermark_lag_buckets"]["caught_up"], 3);
    assert_eq!(payload["watermark_lag_buckets"]["under_10"], 1);
    assert_eq!(payload["watermark_lag_buckets"]["under_100"], 2);
    assert_eq!(payload["watermark_lag_buckets"]["over_100"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_10ms"],
        stream_latency_before[2] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_100ms"],
        stream_latency_before[4] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 1
    );
    assert!(payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);
}

#[tokio::test]
#[serial]
async fn should_report_stream_latency_buckets_without_pressure_given_caught_up_watermarks() {
    // Arrange
    let runtime = test_runtime();
    seed_stream_latency_pressure_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    metrics.counter_add("fitz_stream_operations_total", 5);
    for _ in 0..10 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    }
    for _ in 0..40 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["streams_active"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 10
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 40
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "healthy");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        serde_json::Value::Null
    );
    assert_eq!(payload["diagnostics"]["severity"], "informational");
    assert!(!payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("stream request latency tail")));
}

#[tokio::test]
#[serial]
async fn should_classify_stream_latency_pressure_given_recorded_latency_tail_and_lag() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_streams(vec![StreamInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "logs".to_string(),
        resource: "application".to_string(),
        offset: 5,
        watermark: 1,
        size_bytes: 1024,
        sessions_active: 0,
    }]);
    let metrics = fitz::boot::observability::metrics();
    for _ in 0..10 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    }
    for _ in 0..40 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(payload["diagnostics"]["likely_bottleneck"], "append lag");
    assert_eq!(payload["diagnostics"]["severity"], "medium");
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("stream request latency tail")));
}

#[tokio::test]
#[serial]
async fn should_return_stream_and_notice_domain_stats_given_recorded_metrics() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store.clone());
    seed_stream_watermark_lag_data(&runtime);
    seed_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    let stream_requests_before = metrics.counter_get("fitz_stream_requests_total");
    let stream_success_before = metrics.counter_get("fitz_stream_success_total");
    let stream_failure_before = metrics.counter_get("fitz_stream_failure_total");
    let stream_started_before = metrics.counter_get("fitz_stream_append_sessions_started_total");
    let stream_ended_before = metrics.counter_get("fitz_stream_append_sessions_ended_total");
    let stream_conflicts_before = metrics.counter_get("fitz_stream_append_conflicts_total");
    let stream_notify_drops_before = metrics.counter_get("fitz_stream_notify_drops_total");
    let notice_requests_before = metrics.counter_get("fitz_notice_requests_total");
    let notice_success_before = metrics.counter_get("fitz_notice_success_total");
    let notice_failure_before = metrics.counter_get("fitz_notice_failure_total");
    let notice_drops_before = metrics.counter_get("fitz_notice_delivery_drops_total");
    let notice_unsubscribes_before = metrics.counter_get("fitz_notice_unsubscribes_total");
    let notice_wildcard_before = metrics.counter_get("fitz_notice_wildcard_limit_rejects_total");

    metrics.counter_add("fitz_stream_requests_total", 4);
    metrics.counter_add("fitz_stream_success_total", 3);
    metrics.counter_add("fitz_stream_failure_total", 1);
    metrics.counter_add("fitz_stream_operations_total", 6);
    metrics.counter_add("fitz_stream_append_sessions_started_total", 5);
    metrics.counter_add("fitz_stream_append_sessions_ended_total", 3);
    metrics.counter_add("fitz_stream_append_conflicts_total", 2);
    metrics.counter_add("fitz_stream_notify_drops_total", 5);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 8);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 60);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    metrics.counter_add("fitz_notice_requests_total", 7);
    metrics.counter_add("fitz_notice_success_total", 5);
    metrics.counter_add("fitz_notice_failure_total", 2);
    metrics.counter_add("fitz_notice_delivery_drops_total", 3);
    metrics.counter_add("fitz_notice_unsubscribes_total", 6);
    metrics.counter_add("fitz_notice_wildcard_limit_rejects_total", 4);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let stream_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let notice_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/notice/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let stream_response = fitz::api::admin::handlers::handle_request(stream_req, runtime.clone())
        .await
        .unwrap();
    let notice_response = fitz::api::admin::handlers::handle_request(notice_req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(stream_response.status(), StatusCode::OK);
    let stream_body = body::to_bytes(stream_response.into_body()).await.unwrap();
    let stream_payload: serde_json::Value = serde_json::from_slice(&stream_body).unwrap();
    assert_eq!(stream_payload["streams_active"], 0);
    assert_eq!(stream_payload["append_sessions_active"], 0);
    assert_eq!(stream_payload["events_total"], 3);
    assert_eq!(stream_payload["requests_total"], stream_requests_before + 4);
    assert_eq!(stream_payload["success_total"], stream_success_before + 3);
    assert_eq!(stream_payload["failure_total"], stream_failure_before + 1);
    assert_eq!(
        stream_payload["append_sessions_started_total"],
        stream_started_before + 5
    );
    assert_eq!(
        stream_payload["append_sessions_ended_total"],
        stream_ended_before + 3
    );
    assert_eq!(
        stream_payload["append_conflicts_total"],
        stream_conflicts_before + 2
    );
    assert_eq!(
        stream_payload["notify_drops_total"],
        stream_notify_drops_before + 5
    );
    assert_eq!(stream_payload["watermark_lag_buckets"]["caught_up"], 3);
    assert_eq!(stream_payload["watermark_lag_buckets"]["under_10"], 1);
    assert_eq!(stream_payload["watermark_lag_buckets"]["under_100"], 2);
    assert_eq!(stream_payload["watermark_lag_buckets"]["over_100"], 1);
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_10ms"],
        stream_latency_before[2] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_100ms"],
        stream_latency_before[4] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 1
    );
    assert!(
        stream_payload["operations_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );

    assert_eq!(notice_response.status(), StatusCode::OK);
    let notice_body = body::to_bytes(notice_response.into_body()).await.unwrap();
    let notice_payload: serde_json::Value = serde_json::from_slice(&notice_body).unwrap();
    assert_eq!(notice_payload["subscriptions_active"], 3);
    assert_eq!(notice_payload["routes_active"], 2);
    assert_eq!(notice_payload["max_route_subscribers"], 2);
    assert_eq!(notice_payload["requests_total"], notice_requests_before + 7);
    assert_eq!(notice_payload["success_total"], notice_success_before + 5);
    assert_eq!(notice_payload["failure_total"], notice_failure_before + 2);
    assert_eq!(
        notice_payload["delivery_drops_total"],
        notice_drops_before + 3
    );
    assert_eq!(
        notice_payload["unsubscribes_total"],
        notice_unsubscribes_before + 6
    );
    assert_eq!(
        notice_payload["wildcard_limit_rejects_total"],
        notice_wildcard_before + 4
    );
    assert_eq!(notice_payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(
        notice_payload["diagnostics"]["likely_bottleneck"],
        "route concentration"
    );
    assert!(
        notice_payload["publishes_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );
}

#[tokio::test]
#[serial]
async fn should_export_notice_churn_and_concentration_metrics_given_recorded_notice_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let unsubscribes_before = metrics.counter_get("fitz_notice_unsubscribes_total");
    metrics.counter_add("fitz_notice_unsubscribes_total", 5);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_notice_subscriptions_active"));
    assert!(payload.contains("fitz_notice_routes_active"));
    assert!(payload.contains("fitz_notice_max_route_subscribers"));
    assert!(payload.contains("fitz_notice_unsubscribes_total"));
    assert!(payload.contains("fitz_notice_subscriptions_active 3"));
    assert!(payload.contains(&format!(
        "fitz_notice_unsubscribes_total {}",
        unsubscribes_before + 5
    )));
    assert!(payload.contains("fitz_notice_routes_active 2"));
    assert!(payload.contains("fitz_notice_max_route_subscribers 2"));
}

#[tokio::test]
#[serial]
async fn should_export_stream_counters_and_rates_given_recorded_stream_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_stream_watermark_lag_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let operations_before = metrics.counter_get("fitz_stream_operations_total");
    let started_before = metrics.counter_get("fitz_stream_append_sessions_started_total");
    let ended_before = metrics.counter_get("fitz_stream_append_sessions_ended_total");
    let conflicts_before = metrics.counter_get("fitz_stream_append_conflicts_total");
    let drops_before = metrics.counter_get("fitz_stream_notify_drops_total");
    metrics.counter_add("fitz_stream_operations_total", 3);
    metrics.counter_add("fitz_stream_append_sessions_started_total", 2);
    metrics.counter_add("fitz_stream_append_sessions_ended_total", 4);
    metrics.counter_add("fitz_stream_append_conflicts_total", 2);
    metrics.counter_add("fitz_stream_notify_drops_total", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_stream_events_total"));
    assert!(payload.contains("fitz_stream_append_sessions_active"));
    assert!(payload.contains("fitz_stream_append_sessions_started_total"));
    assert!(payload.contains("fitz_stream_append_sessions_ended_total"));
    assert!(payload.contains("fitz_stream_operations_per_second"));
    assert!(payload.contains("fitz_stream_subscriptions_active"));
    assert!(payload.contains("fitz_stream_latency_ms{le=\"100ms\"}"));
    assert!(payload.contains("fitz_stream_latency_ms_count"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_caught_up"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_10"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_100"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_over_100"));
    assert!(payload.contains(&format!(
        "fitz_stream_operations_total {}",
        operations_before + 3
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_sessions_started_total {}",
        started_before + 2
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_sessions_ended_total {}",
        ended_before + 4
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_conflicts_total {}",
        conflicts_before + 2
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_notify_drops_total {}",
        drops_before + 1
    )));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_caught_up 3"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_10 1"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_100 2"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_over_100 1"));
}

#[tokio::test]
#[serial]
async fn should_export_stream_watermark_series_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_stream_realm_watermark{realm=\"prod\",family=\"1\"} 2"));
    assert!(payload
        .contains("fitz_stream_area_watermark{realm=\"prod\",area=\"audit\",family=\"1\"} 0"));
    assert!(
        payload.contains("fitz_stream_area_watermark{realm=\"prod\",area=\"logs\",family=\"1\"} 1")
    );
}
