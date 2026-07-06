use super::common::*;

#[tokio::test]
#[serial]
async fn should_list_kv_realms_with_valid_cookie() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""realm":"prod""#));
}

#[tokio::test]
#[serial]
async fn should_return_area_collection_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_resource_collection_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_leaf_resource_detail() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/areas/logs/resources/application")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "logs");
    assert_eq!(payload["resource"], "application");
    assert_eq!(payload["diagnostics"]["current_stage"], "healthy");
    assert_eq!(payload["diagnostics"]["severity"], "informational");
}

#[tokio::test]
#[serial]
async fn should_return_stream_realm_watermarks_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/watermarks")
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
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area_count"], 2);
    assert_eq!(payload["resource_count"], 3);
    assert_eq!(payload["family_watermarks"][0]["family"], 1);
    assert_eq!(payload["family_watermarks"][0]["watermark"], 2);
}

#[tokio::test]
#[serial]
async fn should_return_stream_area_watermarks_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/areas/logs/watermarks")
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
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "logs");
    assert_eq!(payload["resource_count"], 2);
    assert_eq!(payload["family_watermarks"][0]["family"], 1);
    assert_eq!(payload["family_watermarks"][0]["watermark"], 1);
}

#[tokio::test]
#[serial]
async fn should_return_kv_transactions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/transactions")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""tx_id":41"#));
}

#[tokio::test]
#[serial]
async fn should_return_committed_kv_value_given_authorized_route_family() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(store, 1, "prod", "app", "users", &[(b"user:1", b"alice")]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/value?route_family=1&key=user%3A1")
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
    assert_eq!(payload["route_family"], 1);
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "app");
    assert_eq!(payload["resource"], "users");
    assert_eq!(payload["key"]["utf8"], "user:1");
    assert_eq!(payload["found"], true);
    assert_eq!(payload["value"]["utf8"], "alice");
}

#[tokio::test]
#[serial]
async fn should_scan_committed_kv_prefix_given_authorized_route_family() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(
        store,
        1,
        "prod",
        "app",
        "users",
        &[
            (b"user:1", b"alice"),
            (b"user:2", b"bob"),
            (b"order:1", b"ignored"),
        ],
    );
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/prefix?route_family=1&prefix=user%3A&limit=1")
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
    let items = payload["items"].as_array().unwrap();
    assert_eq!(payload["route_family"], 1);
    assert_eq!(payload["prefix"]["utf8"], "user:");
    assert_eq!(payload["limit"], 1);
    assert_eq!(payload["has_more"], true);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["key"]["utf8"], "user:1");
    assert_eq!(items[0]["value"]["utf8"], "alice");
}

#[tokio::test]
#[serial]
async fn should_scope_kv_inventory_given_route_family_path_segment() {
    // Arrange
    let runtime = test_runtime();
    runtime.admin_read_model().replace_kv_transactions(vec![
        KvTransaction {
            route_family: 1,
            tx_id: 41,
            realm: "prod".to_string(),
            area: "app".to_string(),
            resource: "users".to_string(),
            mode: "readwrite".to_string(),
            started_at: "2026-03-14T12:00:00Z".to_string(),
            operations_count: 3,
            idle_seconds: 1,
        },
        KvTransaction {
            route_family: 2,
            tx_id: 42,
            realm: "stage".to_string(),
            area: "app".to_string(),
            resource: "users".to_string(),
            mode: "readwrite".to_string(),
            started_at: "2026-03-14T12:00:00Z".to_string(),
            operations_count: 3,
            idle_seconds: 1,
        },
    ]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms")
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
    let realms = payload["realms"].as_array().unwrap();
    assert_eq!(realms.len(), 1);
    assert_eq!(realms[0]["realm"], "prod");
}

#[tokio::test]
#[serial]
async fn should_return_kv_inventory_metrics_given_committed_writes() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(
        store,
        1,
        "prod",
        "app",
        "users",
        &[(b"user:1", b"alice"), (b"user:2", b"bob")],
    );
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources")
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
    let resources = payload["resources"].as_array().unwrap();
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0]["resource"], "users");
    assert_eq!(resources[0]["estimated_record_count"], 2);
    assert_eq!(resources[0]["estimated_storage_bytes"], 20);
    assert_eq!(resources[0]["estimate_complete"], true);
    assert_eq!(resources[0]["transactions_active"], 0);
    assert!(resources[0]["read_latency_avg_ms"].as_f64().is_some());
    assert!(resources[0]["write_latency_avg_ms"].as_f64().is_some());
}

#[tokio::test]
#[serial]
async fn should_refresh_kv_inventory_estimate_after_delete_range() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(
        store.clone(),
        1,
        "prod",
        "app",
        "users",
        &[(b"user:1", b"alice"), (b"user:2", b"bob")],
    );
    delete_committed_kv_range(store, 1, "prod", "app", "users", b"user:", b"user;");
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users")
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
    assert_eq!(payload["estimated_record_count"], 0);
    assert_eq!(payload["estimated_storage_bytes"], 0);
    assert_eq!(payload["estimate_complete"], true);
}

#[tokio::test]
#[serial]
async fn should_page_committed_kv_rows_given_route_family_path_segment() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(
        store,
        1,
        "prod",
        "app",
        "users",
        &[
            (b"user:1", b"alice"),
            (b"user:2", b"bob"),
            (b"order:1", b"ignored"),
        ],
    );
    let cookie = login_cookie(runtime.clone()).await;

    let first_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/rows?starts_with=user%3A&limit=1")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();

    // Act
    let first_response = fitz::api::admin::handlers::handle_request(first_req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(first_response.status(), StatusCode::OK);
    let first_body = body::to_bytes(first_response.into_body()).await.unwrap();
    let first_payload: serde_json::Value = serde_json::from_slice(&first_body).unwrap();
    let first_items = first_payload["items"].as_array().unwrap();
    assert_eq!(first_payload["route_family"], 1);
    assert_eq!(first_payload["starts_with"]["utf8"], "user:");
    assert_eq!(first_payload["limit"], 1);
    assert_eq!(first_payload["has_more"], true);
    assert_eq!(first_items.len(), 1);
    assert_eq!(first_items[0]["key"]["utf8"], "user:1");
    assert_eq!(first_items[0]["value"]["utf8"], "alice");

    // Arrange
    let next_cursor = first_payload["next_cursor"].as_str().unwrap();
    let second_uri = format!(
        "/api/v1/1/kv/realms/prod/areas/app/resources/users/rows?starts_with=user%3A&limit=1&cursor={next_cursor}"
    );
    let second_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri(second_uri)
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let second_response = fitz::api::admin::handlers::handle_request(second_req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(second_response.status(), StatusCode::OK);
    let second_body = body::to_bytes(second_response.into_body()).await.unwrap();
    let second_payload: serde_json::Value = serde_json::from_slice(&second_body).unwrap();
    let second_items = second_payload["items"].as_array().unwrap();
    assert_eq!(second_payload["has_more"], false);
    assert_eq!(second_items.len(), 1);
    assert_eq!(second_items[0]["key"]["utf8"], "user:2");
    assert_eq!(second_items[0]["value"]["utf8"], "bob");
}

#[tokio::test]
#[serial]
async fn should_reject_committed_kv_read_given_unauthorized_route_family() {
    // Arrange
    let _family_guard = EnvGuard::set("FITZ_ADMIN_ROUTE_FAMILIES", "1");
    let (runtime, store) = queue_runtime_with_domains();
    seed_committed_kv_values(store, 1, "prod", "app", "users", &[(b"user:1", b"alice")]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/value?route_family=2&key=user%3A1")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn should_return_queue_inflight_under_resource() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/inflight")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_queue_detail_with_delayed_and_dead_letter_counts() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker")
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
    assert_eq!(payload["messages_ready"], 1);
    assert_eq!(payload["messages_delayed"], 2);
    assert_eq!(payload["messages_inflight"], 3);
    assert_eq!(payload["messages_dead_lettered"], 4);
    assert_eq!(payload["messages_total"], 10);
    assert_eq!(payload["delay_age_buckets"]["under_1m"], 1);
    assert_eq!(payload["delay_age_buckets"]["over_15m"], 1);
    assert_eq!(
        payload["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "dead-letter pressure"
    );
    assert_eq!(payload["diagnostics"]["age_seconds"], 600);
}

#[tokio::test]
#[serial]
async fn should_return_lease_detail_with_age_and_diagnostics() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
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
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/lease/realms/prod/areas/locks/resources/cache")
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
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "locks");
    assert_eq!(payload["resource"], "cache");
    assert_eq!(payload["active_leases"], 1);
    assert!(payload["oldest_lease_age_seconds"].as_u64().unwrap_or(0) > 0);
    assert_eq!(payload["diagnostics"]["current_stage"], "contention");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "lease ownership churn"
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint.as_str().unwrap_or("").contains("renewals recorded")));
    assert_eq!(
        payload["diagnostics"]["age_seconds"],
        payload["oldest_lease_age_seconds"]
    );
}

#[tokio::test]
#[serial]
async fn should_return_schedule_stats_with_latency_pressure() {
    // Arrange
    fitz::boot::observability::metrics().clear();
    let (runtime, store, schedule) = schedule_runtime_with_domains();
    seed_active_schedule_definition(store);
    schedule
        .preload_persisted_families()
        .expect("preload schedules");
    let metrics = fitz::boot::observability::metrics();
    let schedule_latency_before = metrics
        .histogram_get_buckets("fitz_schedule_latency_ms")
        .unwrap_or([0; 9]);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/schedule/stats")
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
    assert_eq!(payload["schedules_active"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        schedule_latency_before[0] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        schedule_latency_before[5] + 2
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "schedule latency"
    );
    assert_eq!(payload["diagnostics"]["severity"], "high");
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("schedule request latency tail")));
}

#[tokio::test]
#[serial]
async fn should_return_schedule_stats_with_pending_claim_age() {
    // Arrange
    let (runtime, store, schedule) = schedule_runtime_with_domains();
    seed_pending_schedule_claim(store);
    schedule
        .preload_persisted_families()
        .expect("preload schedules");
    let metrics = fitz::boot::observability::metrics();
    let expired_before = metrics.counter_get("fitz_schedule_pending_claims_expired_total");
    let cleanup_failures_before =
        metrics.counter_get("fitz_schedule_pending_claim_cleanup_failure_total");
    let create_persistence_before =
        metrics.counter_get("fitz_schedule_create_persistence_failures_total");
    let upsert_persistence_before =
        metrics.counter_get("fitz_schedule_upsert_persistence_failures_total");
    let cancel_persistence_before =
        metrics.counter_get("fitz_schedule_cancel_persistence_failures_total");
    metrics.counter_add("fitz_schedule_pending_claims_expired_total", 2);
    metrics.counter_add("fitz_schedule_pending_claim_cleanup_failure_total", 1);
    metrics.counter_add("fitz_schedule_create_persistence_failures_total", 3);
    metrics.counter_add("fitz_schedule_upsert_persistence_failures_total", 5);
    metrics.counter_add("fitz_schedule_cancel_persistence_failures_total", 7);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/schedule/stats")
        .header(COOKIE, cookie.clone())
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
    assert_eq!(payload["schedules_active"], 1);
    assert_eq!(payload["pending_fire_claims"], 1);
    assert_eq!(payload["pending_ack_retries"], 0);
    assert!(
        payload["oldest_pending_claim_age_seconds"]
            .as_u64()
            .unwrap_or(0)
            >= 30
    );
    assert_eq!(payload["pending_claims_expired_total"], expired_before + 2);
    assert_eq!(
        payload["pending_claim_cleanup_failures_total"],
        cleanup_failures_before + 1
    );
    assert_eq!(
        payload["create_persistence_failures_total"],
        create_persistence_before + 3
    );
    assert_eq!(
        payload["upsert_persistence_failures_total"],
        upsert_persistence_before + 5
    );
    assert_eq!(
        payload["cancel_persistence_failures_total"],
        cancel_persistence_before + 7
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "stale_handoff");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "durable handoff"
    );
    assert_eq!(
        payload["diagnostics"]["age_seconds"],
        payload["oldest_pending_claim_age_seconds"]
    );

    let metrics_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = String::from_utf8(metrics_body.to_vec()).unwrap();
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_schedule_create_persistence_failures_total",
        create_persistence_before + 3,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_schedule_upsert_persistence_failures_total",
        upsert_persistence_before + 5,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_schedule_cancel_persistence_failures_total",
        cancel_persistence_before + 7,
    );
}
