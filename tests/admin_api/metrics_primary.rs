use super::common::*;

#[tokio::test]
#[serial]
async fn should_return_queue_domain_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let notify_before = metrics.counter_get("fitz_queue_notify_drops_total");
    metrics.counter_add("fitz_queue_notify_drops_total", 6);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/queue/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""messages_ready":1"#));
    assert!(payload.contains(r#""messages_delayed":2"#));
    assert!(payload.contains(r#""messages_pending":3"#));
    assert!(payload.contains(r#""messages_dead_lettered":4"#));
    assert!(payload.contains(r#""oldest_message_age_seconds":9"#));
    assert!(payload.contains(r#""oldest_backlog_age_seconds":600"#));
    assert!(payload.contains(r#""inflight_active":0"#));
    let payload_json: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload_json["backlog_age_buckets"]["under_1m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["under_5m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["under_15m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["over_15m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["under_1m"], 1);
    assert_eq!(payload_json["delay_age_buckets"]["under_5m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["under_15m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["over_15m"], 1);
    assert_eq!(payload_json["notify_drops_total"], notify_before + 6);
}

#[tokio::test]
#[serial]
async fn should_return_queue_operation_counters_given_recorded_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let requests_before = metrics.counter_get("fitz_queue_requests_total");
    let success_before = metrics.counter_get("fitz_queue_success_total");
    let failure_before = metrics.counter_get("fitz_queue_failure_total");
    let enqueues_before = metrics.counter_get("fitz_queue_enqueue_total");
    let reserves_before = metrics.counter_get("fitz_queue_reserve_total");
    let completes_before = metrics.counter_get("fitz_queue_complete_total");
    let releases_before = metrics.counter_get("fitz_queue_release_total");
    let extends_before = metrics.counter_get("fitz_queue_extend_total");
    let notify_before = metrics.counter_get("fitz_queue_notify_drops_total");
    let redeliveries_before = metrics.counter_get("fitz_queue_redeliveries_total");
    let dead_letter_transitions_before = metrics.counter_get("fitz_queue_dlq_transitions_total");
    let complete_rejected_before = metrics.counter_get("fitz_queue_complete_rejected_total");
    metrics.counter_add("fitz_queue_requests_total", 5);
    metrics.counter_add("fitz_queue_success_total", 4);
    metrics.counter_add("fitz_queue_failure_total", 2);
    metrics.counter_add("fitz_queue_enqueue_total", 3);
    metrics.counter_add("fitz_queue_reserve_total", 7);
    metrics.counter_add("fitz_queue_complete_total", 11);
    metrics.counter_add("fitz_queue_release_total", 13);
    metrics.counter_add("fitz_queue_extend_total", 17);
    metrics.counter_add("fitz_queue_notify_drops_total", 19);
    metrics.counter_add("fitz_queue_redeliveries_total", 23);
    metrics.counter_add("fitz_queue_dlq_transitions_total", 29);
    metrics.counter_add("fitz_queue_complete_rejected_total", 31);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/queue/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["requests_total"], requests_before + 5);
    assert_eq!(payload["success_total"], success_before + 4);
    assert_eq!(payload["failure_total"], failure_before + 2);
    assert_eq!(payload["enqueues_total"], enqueues_before + 3);
    assert_eq!(payload["reserves_total"], reserves_before + 7);
    assert_eq!(payload["completes_total"], completes_before + 11);
    assert_eq!(payload["releases_total"], releases_before + 13);
    assert_eq!(payload["extends_total"], extends_before + 17);
    assert_eq!(payload["notify_drops_total"], notify_before + 19);
    assert_eq!(payload["redeliveries_total"], redeliveries_before + 23);
    assert_eq!(
        payload["dead_letter_transitions_total"],
        dead_letter_transitions_before + 29
    );
    assert_eq!(
        payload["complete_rejected_total"],
        complete_rejected_before + 31
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("dead-letter transition")));
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("queue complete rejection")));
    assert!(payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);

    let metrics_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/metrics")
        .header(COOKIE, login_cookie(runtime.clone()).await)
        .body(Body::default())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = structured_metrics_text(&metrics_body);
    assert!(metrics_payload.contains("fitz_queue_complete_total"));
    assert!(metrics_payload.contains("fitz_queue_release_total"));
    assert!(metrics_payload.contains("fitz_queue_oldest_message_age_seconds 9"));
    assert!(metrics_payload.contains("fitz_queue_oldest_backlog_age_seconds 600"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_1m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_5m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_15m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_over_15m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_1m 1"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_5m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_15m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_over_15m 1"));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_redeliveries_total {}",
        redeliveries_before + 23
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_dlq_transitions_total {}",
        dead_letter_transitions_before + 29
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_complete_rejected_total {}",
        complete_rejected_before + 31
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_notify_drops_total {}",
        notify_before + 19
    )));
}

#[tokio::test]
#[serial]
async fn should_return_kv_failure_stats_given_recorded_metrics() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    let commits_failed_before = metrics.counter_get("fitz_kv_commits_failed_total");
    let rollbacks_before = metrics.counter_get("fitz_kv_rollbacks_total");
    let invalid_transaction_rejects_before =
        metrics.counter_get("fitz_kv_invalid_transaction_rejects_total");
    metrics.counter_add("fitz_kv_commits_failed_total", 3);
    metrics.counter_add("fitz_kv_rollbacks_total", 5);
    metrics.counter_add("fitz_kv_invalid_transaction_rejects_total", 7);
    let cookie = login_cookie(runtime.clone()).await;

    let kv_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/kv/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let global_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();

    // Act
    let kv_response = fitz::api::admin::handlers::handle_request(kv_req, runtime.clone())
        .await
        .unwrap();
    let global_response = fitz::api::admin::handlers::handle_request(global_req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(kv_response.status(), StatusCode::OK);
    let kv_body = body::to_bytes(kv_response.into_body()).await.unwrap();
    let kv_payload: serde_json::Value = serde_json::from_slice(&kv_body).unwrap();
    assert_eq!(
        kv_payload["commits_failed_total"],
        commits_failed_before + 3
    );
    assert!(kv_payload.get("rollbacks_total").is_none());
    assert_eq!(
        kv_payload["invalid_transaction_rejects_total"],
        invalid_transaction_rejects_before + 7
    );

    assert_eq!(global_response.status(), StatusCode::OK);
    let global_body = body::to_bytes(global_response.into_body()).await.unwrap();
    let global_payload: serde_json::Value = serde_json::from_slice(&global_body).unwrap();
    assert_eq!(
        global_payload["domains"]["kv"]["commits_failed_total"],
        commits_failed_before + 3
    );
    assert!(global_payload["domains"]["kv"]
        .get("rollbacks_total")
        .is_none());
    assert_eq!(
        global_payload["domains"]["kv"]["invalid_transaction_rejects_total"],
        invalid_transaction_rejects_before + 7
    );

    let metrics_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = structured_metrics_text(&metrics_body);
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_kv_commits_failed_total",
        commits_failed_before + 3,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_kv_rollbacks_total",
        rollbacks_before + 5,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_kv_invalid_transaction_rejects_total",
        invalid_transaction_rejects_before + 7,
    );
}

#[tokio::test]
#[serial]
async fn should_return_rpc_and_lease_domain_stats_given_recorded_metrics() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_rpc_workers(vec![RpcWorker {
        route_family: 1,
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![
        RpcPendingRequest {
            route_family: 1,
            correlation_id: "corr-abc-123".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            submitted_at: "2026-03-14T12:00:07Z".to_string(),
            age_seconds: 7,
            worker_session_id: Some("9001".to_string()),
        },
        RpcPendingRequest {
            route_family: 1,
            correlation_id: "corr-xyz-789".to_string(),
            route: "rpc://prod/api/orders/create".to_string(),
            submitted_at: "2026-03-14T12:00:13Z".to_string(),
            age_seconds: 13,
            worker_session_id: None,
        },
    ]);
    read_model.replace_leases(vec![LeaseInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "locks".to_string(),
        resource: "cache".to_string(),
        owner_session_id: "session-1".to_string(),
        acquired_at: "2026-03-14T12:00:00Z".to_string(),
        expires_at: "2026-03-14T12:05:00Z".to_string(),
        renewals: 2,
        fencing_token: 17,
    }]);

    let metrics = fitz::boot::observability::metrics();
    let rpc_requests_before = metrics.counter_get("fitz_rpc_requests_total");
    let rpc_success_before = metrics.counter_get("fitz_rpc_success_total");
    let rpc_failure_before = metrics.counter_get("fitz_rpc_failure_total");
    let rpc_timeouts_before = metrics.counter_get("rpc_request_timeouts_total");
    let rpc_backpressure_before = metrics.counter_get("rpc_backpressure_rejects_total");
    let rpc_duplicate_before =
        metrics.counter_get("rpc_requests_rejected_duplicate_correlation_total");
    let rpc_wrong_worker_before = metrics.counter_get("rpc_responses_rejected_wrong_worker_total");
    let rpc_closed_caller_before = metrics.counter_get("rpc_responses_dropped_closed_caller_total");
    let rpc_missing_pending_before = metrics.counter_get("rpc_responses_missing_pending_total");
    let rpc_invalid_sequence_before = metrics.counter_get("rpc_response_invalid_sequence_total");
    let rpc_invalid_forwarded_before =
        metrics.counter_get("rpc_invalid_sequence_errors_forwarded_total");
    let rpc_invalid_dropped_before =
        metrics.counter_get("rpc_invalid_sequence_errors_dropped_total");
    let lease_requests_before = metrics.counter_get("fitz_lease_requests_total");
    let lease_success_before = metrics.counter_get("fitz_lease_success_total");
    let lease_failure_before = metrics.counter_get("fitz_lease_failure_total");
    let lease_timeouts_before = metrics.counter_get("fitz_lease_acquire_timeouts_total");
    let lease_forced_before = metrics.counter_get("fitz_lease_forced_releases_total");
    let lease_invalid_before = metrics.counter_get("fitz_lease_invalid_token_rejects_total");
    let lease_churn_before = metrics.counter_get("fitz_lease_ownership_churn_total");

    metrics.counter_add("fitz_rpc_requests_total", 8);
    metrics.counter_add("fitz_rpc_success_total", 5);
    metrics.counter_add("fitz_rpc_failure_total", 3);
    metrics.counter_add("rpc_request_timeouts_total", 2);
    metrics.counter_add("rpc_backpressure_rejects_total", 4);
    metrics.counter_add("rpc_requests_rejected_duplicate_correlation_total", 6);
    metrics.counter_add("rpc_responses_rejected_wrong_worker_total", 7);
    metrics.counter_add("rpc_responses_dropped_closed_caller_total", 9);
    metrics.counter_add("rpc_responses_missing_pending_total", 11);
    metrics.counter_add("rpc_response_invalid_sequence_total", 17);
    metrics.counter_add("rpc_invalid_sequence_errors_forwarded_total", 19);
    metrics.counter_add("rpc_invalid_sequence_errors_dropped_total", 23);
    metrics.counter_add("fitz_lease_requests_total", 4);
    metrics.counter_add("fitz_lease_success_total", 2);
    metrics.counter_add("fitz_lease_failure_total", 1);
    metrics.counter_add("fitz_lease_acquire_timeouts_total", 3);
    metrics.counter_add("fitz_lease_forced_releases_total", 5);
    metrics.counter_add("fitz_lease_invalid_token_rejects_total", 7);
    metrics.counter_add("fitz_lease_ownership_churn_total", 11);
    metrics.gauge_set("fitz_lease_waiters_gauge", 4);
    metrics.gauge_set("fitz_lease_waiter_depth", 4);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let rpc_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/rpc/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let lease_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/lease/stats")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let rpc_response = fitz::api::admin::handlers::handle_request(rpc_req, runtime.clone())
        .await
        .unwrap();
    let lease_response = fitz::api::admin::handlers::handle_request(lease_req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(rpc_response.status(), StatusCode::OK);
    let rpc_body = body::to_bytes(rpc_response.into_body()).await.unwrap();
    let rpc_payload: serde_json::Value = serde_json::from_slice(&rpc_body).unwrap();
    assert_eq!(rpc_payload["workers_registered"], 1);
    assert_eq!(rpc_payload["requests_pending"], 2);
    assert_eq!(rpc_payload["oldest_pending_request_age_seconds"], 13);
    assert_eq!(rpc_payload["pending_routes_active"], 2);
    assert_eq!(rpc_payload["slowest_worker_average_latency_ms"], 4.5);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_5ms"], 1);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_25ms"], 0);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_100ms"], 0);
    assert_eq!(rpc_payload["worker_latency_buckets"]["over_100ms"], 0);
    assert_eq!(rpc_payload["requests_total"], rpc_requests_before + 8);
    assert_eq!(rpc_payload["success_total"], rpc_success_before + 5);
    assert_eq!(rpc_payload["failure_total"], rpc_failure_before + 3);
    assert_eq!(
        rpc_payload["request_timeouts_total"],
        rpc_timeouts_before + 2
    );
    assert_eq!(
        rpc_payload["backpressure_rejects_total"],
        rpc_backpressure_before + 4
    );
    assert_eq!(
        rpc_payload["duplicate_correlation_rejects_total"],
        rpc_duplicate_before + 6
    );
    assert_eq!(
        rpc_payload["wrong_worker_rejects_total"],
        rpc_wrong_worker_before + 7
    );
    assert_eq!(
        rpc_payload["responses_dropped_closed_caller_total"],
        rpc_closed_caller_before + 9
    );
    assert_eq!(
        rpc_payload["responses_missing_pending_total"],
        rpc_missing_pending_before + 11
    );
    assert_eq!(
        rpc_payload["invalid_sequence_responses_total"],
        rpc_invalid_sequence_before + 17
    );
    assert_eq!(
        rpc_payload["invalid_sequence_errors_forwarded_total"],
        rpc_invalid_forwarded_before + 19
    );
    assert_eq!(
        rpc_payload["invalid_sequence_errors_dropped_total"],
        rpc_invalid_dropped_before + 23
    );
    assert!(rpc_payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);

    assert_eq!(lease_response.status(), StatusCode::OK);
    let lease_body = body::to_bytes(lease_response.into_body()).await.unwrap();
    let lease_payload: serde_json::Value = serde_json::from_slice(&lease_body).unwrap();
    assert_eq!(lease_payload["leases_active"], 1);
    assert_eq!(lease_payload["waiter_depth"], 4);
    assert!(
        lease_payload["oldest_lease_age_seconds"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert_eq!(lease_payload["requests_total"], lease_requests_before + 4);
    assert_eq!(lease_payload["success_total"], lease_success_before + 2);
    assert_eq!(lease_payload["failure_total"], lease_failure_before + 1);
    assert_eq!(
        lease_payload["acquire_timeouts_total"],
        lease_timeouts_before + 3
    );
    assert_eq!(
        lease_payload["forced_releases_total"],
        lease_forced_before + 5
    );
    assert_eq!(
        lease_payload["invalid_token_rejects_total"],
        lease_invalid_before + 7
    );
    assert_eq!(
        lease_payload["ownership_churn_total"],
        lease_churn_before + 11
    );
    assert!(
        lease_payload["operations_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );

    let metrics_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/metrics")
        .header(COOKIE, login_cookie(runtime.clone()).await)
        .body(Body::default())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = structured_metrics_text(&metrics_body);
    assert!(metrics_payload.contains("fitz_rpc_requests_pending 2"));
    assert!(metrics_payload.contains("fitz_rpc_oldest_pending_request_age_seconds 13"));
    assert!(metrics_payload.contains("fitz_rpc_pending_routes_active 2"));
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_rpc_responses_dropped_closed_caller_total",
        rpc_closed_caller_before + 9,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_rpc_responses_missing_pending_total",
        rpc_missing_pending_before + 11,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_rpc_invalid_sequence_responses_total",
        rpc_invalid_sequence_before + 17,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_rpc_invalid_sequence_errors_forwarded_total",
        rpc_invalid_forwarded_before + 19,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_rpc_invalid_sequence_errors_dropped_total",
        rpc_invalid_dropped_before + 23,
    );
    assert!(metrics_payload.contains("fitz_lease_oldest_lease_age_seconds"));
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_lease_acquire_timeouts_total",
        lease_timeouts_before + 3,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_lease_forced_releases_total",
        lease_forced_before + 5,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_lease_invalid_token_rejects_total",
        lease_invalid_before + 7,
    );
    assert!(metrics_payload.contains(&format!(
        "fitz_lease_ownership_churn_total {}",
        lease_churn_before + 11
    )));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_data_loss_risk_given_late_response_drops() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    let late_drops_before = metrics.counter_get("rpc_responses_dropped_closed_caller_total");
    let missing_before = metrics.counter_get("rpc_responses_missing_pending_total");
    metrics.counter_add("rpc_responses_dropped_closed_caller_total", 4);
    metrics.counter_add("rpc_responses_missing_pending_total", 2);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/rpc/stats")
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
    assert_eq!(
        payload["responses_dropped_closed_caller_total"],
        late_drops_before + 4
    );
    assert_eq!(
        payload["responses_missing_pending_total"],
        missing_before + 2
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "data_loss_risk");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "late response drop"
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value.as_str().unwrap_or("").contains("late response drop")));
}
