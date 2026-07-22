use super::common::*;

#[tokio::test]
#[serial]
async fn should_isolate_family_admin_read_surfaces_and_global_authority() {
    // Arrange
    let _family_access = EnvGuard::set("FITZ_ADMIN_ROUTE_FAMILIES", "1");
    let runtime = test_runtime();
    let ingress = Arc::new(
        RuntimeIngress::new(true)
            .with_router(runtime.router())
            .with_admin_read_model(runtime.admin_read_model()),
    );
    runtime.attach_ingress(ingress.clone());
    for (session_id, family) in [(101, 1), (202, 2)] {
        ingress
            .on_open(RuntimeSessionInfo {
                session_id,
                transport_kind: TransportKind::WebSocket,
                peer_addr: None,
                metadata: Arc::new(SessionMetadata::new()),
                permissions_snapshot: SessionPermissions::empty(),
                claims: None,
                authenticated: false,
                route_family: RouteFamily::new(family),
            })
            .await
            .expect("session opens");
    }
    runtime.admin_read_model().replace_queues(vec![QueueInfo {
        family: 1,
        realm: "family-one".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 3,
        messages_delayed: 0,
        messages_inflight: 0,
        messages_dead_lettered: 0,
        messages_total: 3,
        oldest_message_age_seconds: 0,
        oldest_backlog_age_seconds: 0,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    // Act
    let family_sessions = fitz::api::admin::handlers::handle_request(
        hyper::http::Request::builder()
            .method(Method::GET)
            .uri("/api/v1/1/sessions")
            .header(COOKIE, cookie.clone())
            .body(Body::default())
            .unwrap(),
        runtime.clone(),
    )
    .await
    .unwrap();
    let family_stats = fitz::api::admin::handlers::handle_request(
        hyper::http::Request::builder()
            .method(Method::GET)
            .uri("/api/v1/1/stats")
            .header(COOKIE, cookie.clone())
            .body(Body::default())
            .unwrap(),
        runtime.clone(),
    )
    .await
    .unwrap();
    let denied_family = fitz::api::admin::handlers::handle_request(
        hyper::http::Request::builder()
            .method(Method::GET)
            .uri("/api/v1/2/stats")
            .header(COOKIE, cookie.clone())
            .body(Body::default())
            .unwrap(),
        runtime.clone(),
    )
    .await
    .unwrap();
    let denied_global = fitz::api::admin::handlers::handle_request(
        hyper::http::Request::builder()
            .method(Method::GET)
            .uri("/api/v1/all/stats")
            .header(COOKIE, cookie)
            .body(Body::default())
            .unwrap(),
        runtime,
    )
    .await
    .unwrap();

    // Assert
    assert_eq!(family_sessions.status(), StatusCode::OK);
    let sessions: serde_json::Value =
        serde_json::from_slice(&body::to_bytes(family_sessions.into_body()).await.unwrap())
            .unwrap();
    assert_eq!(sessions["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(sessions["sessions"][0]["route_family"], 1);

    assert_eq!(family_stats.status(), StatusCode::OK);
    let stats: serde_json::Value =
        serde_json::from_slice(&body::to_bytes(family_stats.into_body()).await.unwrap()).unwrap();
    assert_eq!(stats["broker"]["sessions"], 1);
    assert_eq!(stats["domains"]["queue"]["messages_ready"], 3);
    assert_eq!(stats["domains"]["queue"]["requests_total"], 0);
    assert_eq!(
        stats["diagnostics"]["incident_summary"]["status"],
        "healthy"
    );
    assert_eq!(denied_family.status(), StatusCode::FORBIDDEN);
    assert_eq!(denied_global.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
#[serial]
async fn should_return_global_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 100,
        messages_total: 110,
        oldest_message_age_seconds: 9,
        oldest_backlog_age_seconds: 600,
        backlog_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 1,
            under_15m: 1,
            over_15m: 0,
        },
        delay_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 0,
            under_15m: 0,
            over_15m: 1,
        },
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stats")
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
    assert_eq!(payload["domains"]["queue"]["messages_dead_lettered"], 100);
    assert_eq!(payload["domains"]["queue"]["oldest_message_age_seconds"], 9);
    assert_eq!(
        payload["domains"]["queue"]["oldest_backlog_age_seconds"],
        600
    );
    assert_eq!(
        payload["domains"]["queue"]["backlog_age_buckets"]["under_1m"],
        1
    );
    assert_eq!(
        payload["domains"]["queue"]["delay_age_buckets"]["under_1m"],
        1
    );
    assert_eq!(
        payload["domains"]["queue"]["delay_age_buckets"]["over_15m"],
        1
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["status"],
        "stalled"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["severity"],
        "high"
    );
    assert_eq!(payload["diagnostics"]["top_bottleneck"]["domain"], "queue");
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["title"],
        "Inspect recent transitions"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["priority"],
        1
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["recommended_next_query"],
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["endpoint"]
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["remediation"],
        "Use the transition history to isolate the failure reason or retry pattern before taking any follow-up action."
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][1]["title"],
        "Inspect current resource snapshot"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][1]["priority"],
        2
    );
    assert_eq!(
        payload["domains"]["queue"]["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    let signals_matched = payload["domains"]["queue"]["diagnostics"]["confidence_justification"]
        ["signals_matched"]
        .as_array()
        .expect("queue confidence signals_matched");
    assert!(signals_matched
        .iter()
        .any(|signal| signal == "failure_signal_present"));
    assert!(
        payload["domains"]["queue"]["diagnostics"]["confidence_justification"]["rationale"]
            .as_str()
            .expect("queue confidence rationale")
            .contains("telemetry freshness")
    );
}

#[tokio::test]
#[serial]
async fn should_require_admin_for_topology() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/topology")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn should_return_messaging_topology_given_live_admin_snapshots() {
    // Arrange
    let runtime = test_runtime();
    let ingress = Arc::new(
        RuntimeIngress::new(true)
            .with_router(runtime.router())
            .with_admin_read_model(runtime.admin_read_model()),
    );
    runtime.attach_ingress(ingress.clone());

    for (session_id, family) in [(11, 41), (12, 41), (22, 7)] {
        let session = RuntimeSessionInfo {
            session_id,
            transport_kind: TransportKind::WebSocket,
            peer_addr: None,
            metadata: Arc::new(SessionMetadata::new()),
            permissions_snapshot: SessionPermissions::empty(),
            claims: None,
            authenticated: false,
            route_family: RouteFamily::new(family),
        };
        ingress.on_open(session).await.unwrap();
    }
    ingress.record_frame_received(11);
    ingress.record_frame_received(11);
    ingress.record_frame_sent(11);
    ingress.record_frame_received(12);

    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 41,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 10,
        messages_delayed: 0,
        messages_inflight: 1,
        messages_dead_lettered: 2,
        messages_total: 13,
        oldest_message_age_seconds: 30,
        oldest_backlog_age_seconds: 600,
        backlog_age_buckets: QueueAgeBuckets {
            under_1m: 0,
            under_5m: 0,
            under_15m: 1,
            over_15m: 1,
        },
        delay_age_buckets: QueueAgeBuckets {
            under_1m: 0,
            under_5m: 0,
            under_15m: 0,
            over_15m: 0,
        },
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }]);
    read_model.replace_queue_inflight(vec![QueueInflight {
        message_id: 99,
        family: 41,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        inflight_token: "token-99".to_string(),
        session_id: "11".to_string(),
        expires_at: "2026-03-14T12:05:00Z".to_string(),
        attempts: 2,
    }]);
    read_model.replace_notice_subscriptions(vec![NoticeSubscription {
        route_family: 1,
        subscription_id: 7,
        session_id: "12".to_string(),
        realm: "prod".to_string(),
        pattern: "notice://prod/events/orders".to_string(),
        created_at: "2026-03-14T12:00:00Z".to_string(),
        notifications_received: 5,
    }]);
    read_model.replace_rpc_workers(vec![RpcWorker {
        route_family: 1,
        session_id: "22".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![RpcPendingRequest {
        route_family: 1,
        correlation_id: "corr-get".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        submitted_at: "2026-03-14T12:00:07Z".to_string(),
        age_seconds: 7,
        worker_session_id: Some("22".to_string()),
    }]);
    read_model.replace_leases(vec![LeaseInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "locks".to_string(),
        resource: "leader".to_string(),
        owner_session_id: "11".to_string(),
        acquired_at: "2026-03-14T12:00:00Z".to_string(),
        expires_at: "2026-03-14T12:01:00Z".to_string(),
        renewals: 3,
        fencing_token: 8,
    }]);
    read_model.replace_streams(vec![StreamInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "events".to_string(),
        resource: "orders".to_string(),
        offset: 42,
        watermark: 40,
        size_bytes: 4096,
        sessions_active: 1,
    }]);
    read_model.replace_schedules(vec![ScheduleInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "sweeper".to_string(),
        operation: "run".to_string(),
        cron: "* * * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        next_run: "2026-03-14T12:01:00Z".to_string(),
        last_run: None,
        executions_total: 4,
        enabled: true,
    }]);
    read_model.replace_kv_transactions(vec![KvTransaction {
        route_family: 1,
        tx_id: 501,
        realm: "prod".to_string(),
        area: "state".to_string(),
        resource: "users".to_string(),
        mode: "session:12:readwrite".to_string(),
        started_at: "2026-03-14T12:00:00Z".to_string(),
        operations_count: 3,
        idle_seconds: 1,
    }]);

    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/topology")
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
    assert!(payload["generated_at"].as_str().unwrap().contains('T'));

    let lane_ids = payload["lanes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|lane| lane["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        lane_ids,
        vec!["queue", "rpc", "notice", "schedule", "stream", "lease", "kv"]
    );
    assert_eq!(payload["lanes"][0]["state"], "blocked");
    assert_eq!(
        payload["lanes"][0]["top_scoped_resources"][0]["scope"]["route_family"],
        41
    );
    assert_eq!(
        payload["lanes"][0]["top_scoped_resources"][0]["scope"]["realm"],
        "prod"
    );

    let groups = payload["session_groups"].as_array().unwrap();
    let family_41 = groups
        .iter()
        .find(|group| group["route_family"] == 41)
        .expect("route family 41 group");
    assert_eq!(family_41["sessions"], 2);
    assert_eq!(family_41["messages_received"], 3);
    assert_eq!(family_41["messages_sent"], 1);

    let connections = payload["connections"]["items"].as_array().unwrap();
    let kinds = connections
        .iter()
        .map(|connection| connection["kind"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(kinds.contains(&"queue_inflight_consumer"));
    assert!(kinds.contains(&"notice_subscription"));
    assert!(kinds.contains(&"rpc_worker"));
    assert!(kinds.contains(&"rpc_pending_assignment"));
    assert!(kinds.contains(&"lease_owner"));
    assert!(kinds.contains(&"stream_append_activity"));
    assert!(kinds.contains(&"kv_transaction_activity"));

    let notice_connection = connections
        .iter()
        .find(|connection| connection["kind"] == "notice_subscription")
        .expect("notice subscription edge");
    assert_eq!(notice_connection["scope"]["realm"], "prod");
    assert!(notice_connection["scope"].get("route_family").is_none());
}

#[tokio::test]
#[serial]
async fn should_truncate_topology_connections_given_large_snapshot() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_notice_subscriptions(
        (0..260)
            .map(|index| NoticeSubscription {
                route_family: 1,
                subscription_id: index,
                session_id: format!("session-{index}"),
                realm: "prod".to_string(),
                pattern: format!("notice://prod/events/topic-{index}"),
                created_at: "2026-03-14T12:00:00Z".to_string(),
                notifications_received: 0,
            })
            .collect(),
    );
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/topology")
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
    assert_eq!(payload["connections"]["limit"], 250);
    assert!(payload["connections"]["truncated"].as_bool().unwrap());
    assert!(payload["connections"]["total"].as_u64().unwrap() > 250);
    assert_eq!(
        payload["connections"]["items"].as_array().unwrap().len(),
        250
    );
}

#[tokio::test]
#[serial]
async fn should_surface_router_overload_counters_in_global_stats_and_metrics() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    let router_backpressure_before = metrics.counter_get("fitz_router_backpressure_total");
    let router_high_lane_before = metrics.counter_get("fitz_router_high_lane_backpressure_total");
    metrics.counter_add("fitz_router_backpressure_total", 5);
    metrics.counter_add("fitz_router_high_lane_backpressure_total", 2);
    let cookie = login_cookie(runtime.clone()).await;

    let stats_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let metrics_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/all/metrics")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let stats_response = fitz::api::admin::handlers::handle_request(stats_req, runtime.clone())
        .await
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(stats_response.status(), StatusCode::OK);
    let stats_body = body::to_bytes(stats_response.into_body()).await.unwrap();
    let stats_payload: serde_json::Value = serde_json::from_slice(&stats_body).unwrap();
    assert_eq!(
        stats_payload["broker"]["router_backpressure_total"],
        router_backpressure_before + 5
    );
    assert_eq!(
        stats_payload["broker"]["router_high_lane_backpressure_total"],
        router_high_lane_before + 2
    );

    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = structured_metrics_text(&metrics_body);
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_router_backpressure_total",
        router_backpressure_before + 5,
    );
    assert_prometheus_counter(
        &metrics_payload,
        "fitz_router_high_lane_backpressure_total",
        router_high_lane_before + 2,
    );
}

#[tokio::test]
#[serial]
async fn should_surface_router_overload_in_global_troubleshooting() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    metrics.counter_add("fitz_router_backpressure_total", 5);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/troubleshooting")
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
    assert_eq!(payload["incident_summary"]["status"], "degraded");
    assert_eq!(payload["top_bottleneck"]["domain"], "broker");
    assert_eq!(
        payload["incident_summary"]["likely_bottleneck"],
        "router saturation"
    );
    assert_eq!(
        payload["incident_summary"]["recommended_next_query"],
        "inspect /api/v1/stats"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["title"],
        "Inspect broker stats"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][1]["title"],
        "Inspect broker metrics"
    );
    assert!(payload["incident_summary"]["explanation"]
        .as_str()
        .unwrap_or("")
        .contains("router mailbox saturation"));
}

#[tokio::test]
#[serial]
async fn should_return_global_troubleshooting_guidance() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 100,
        messages_total: 110,
        oldest_message_age_seconds: 9,
        oldest_backlog_age_seconds: 600,
        backlog_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 1,
            under_15m: 1,
            over_15m: 0,
        },
        delay_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 0,
            under_15m: 0,
            over_15m: 1,
        },
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/troubleshooting")
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
    assert_eq!(payload["incident_summary"]["status"], "stalled");
    assert_eq!(payload["incident_summary"]["severity"], "high");
    assert_eq!(payload["top_bottleneck"]["domain"], "queue");
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["title"],
        "Inspect recent transitions"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["priority"],
        1
    );
    assert_eq!(
        payload["incident_summary"]["recommended_next_query"],
        payload["incident_summary"]["suggested_next_queries"][0]["endpoint"]
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["remediation"],
        "Use the transition history to isolate the failure reason or retry pattern before taking any follow-up action."
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][1]["title"],
        "Inspect current resource snapshot"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][1]["priority"],
        2
    );
}

#[tokio::test]
#[serial]
async fn should_return_exact_rpc_operation_detail_counts() {
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
            correlation_id: "corr-get".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            submitted_at: "2026-03-14T12:00:07Z".to_string(),
            age_seconds: 7,
            worker_session_id: Some("9001".to_string()),
        },
        RpcPendingRequest {
            route_family: 1,
            correlation_id: "corr-get-details".to_string(),
            route: "rpc://prod/api/users/get-details".to_string(),
            submitted_at: "2026-03-14T12:00:08Z".to_string(),
            age_seconds: 8,
            worker_session_id: Some("9002".to_string()),
        },
    ]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/rpc/realms/prod/areas/api/resources/users/operations/get")
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
    assert_eq!(payload["workers_registered"], 1);
    assert_eq!(payload["requests_pending"], 1);
    assert_eq!(payload["slowest_worker_average_latency_ms"], 4.5);
    assert_eq!(payload["worker_latency_buckets"]["under_5ms"], 1);
    assert_eq!(payload["worker_latency_buckets"]["under_25ms"], 0);
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
}

#[tokio::test]
#[serial]
async fn should_return_sessions_collection_only() {
    let runtime = test_runtime();
    let ingress = Arc::new(
        RuntimeIngress::new(true)
            .with_router(runtime.router())
            .with_admin_read_model(runtime.admin_read_model()),
    );
    runtime.attach_ingress(ingress.clone());

    let session = RuntimeSessionInfo {
        session_id: 41,
        transport_kind: TransportKind::WebSocket,
        peer_addr: None,
        metadata: Arc::new(SessionMetadata::new()),
        permissions_snapshot: SessionPermissions::empty(),
        claims: None,
        authenticated: false,
        route_family: RouteFamily::new(41),
    };
    ingress.on_open(session).await.unwrap();
    ingress.record_frame_received(41);
    ingress.record_frame_received(41);
    ingress.record_frame_sent(41);

    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let sessions = payload["sessions"].as_array().unwrap();
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0]["session_id"], "41");
    assert_eq!(sessions[0]["route_family"], 41);
    assert_eq!(sessions[0]["subject"], "");
    assert_eq!(sessions[0]["identity_claim"], "");
    assert_eq!(sessions[0]["identity_value"], "");
    assert_eq!(sessions[0]["transport"], "websocket");
    assert_eq!(sessions[0]["messages_received"], 2);
    assert_eq!(sessions[0]["messages_sent"], 1);
    assert!(sessions[0]["connected_at"].as_str().unwrap().contains('T'));
    assert!(sessions[0]["idle_seconds"].as_u64().unwrap() <= 1);
}

#[tokio::test]
#[serial]
async fn should_reject_removed_session_detail_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions/123")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
