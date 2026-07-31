use super::common::*;

#[tokio::test]
#[serial]
async fn should_return_queue_events_with_bounded_timeline() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/events?limit=3")
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
    assert_eq!(payload["domain"], "queue");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["limit"], 3);
    assert_eq!(
        payload["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    let events = payload["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().any(|event| event["kind"] == "observation"));
    assert!(events.iter().any(|event| {
        event["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("3 dead-lettered"))
    }));
    assert!(events.iter().any(|event| {
        event["summary"]
            .as_str()
            .is_some_and(|summary| summary.contains("15m+ old"))
    }));
}

#[tokio::test]
#[serial]
async fn should_return_queue_comparison_between_two_resource_snapshots() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_compare_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/compare?against_realm=prod&against_area=jobs&against_resource=backup&against_family=2")
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
    assert_eq!(payload["domain"], "queue");
    assert_eq!(payload["comparison_mode"], "snapshot_vs_snapshot");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["left"]["scope"]["family"], 1);
    assert_eq!(payload["right"]["scope"]["family"], 2);
    assert_eq!(
        payload["left"]["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    assert_eq!(payload["right"]["diagnostics"]["current_stage"], "healthy");
    assert_eq!(payload["delta"]["backlog"], 3);
    assert_eq!(payload["delta"]["dead_letters"], 4);
    assert!(payload["summary"].as_str().unwrap().contains("left side"));
}

#[tokio::test]
#[serial]
async fn should_return_queue_dead_letters_under_resource() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters")
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
    assert!(payload.contains(r#""message_id":42"#));
    assert!(payload.contains(r#""reason":"max_attempts_exceeded""#));
}

#[tokio::test]
#[serial]
async fn should_reject_legacy_dead_letter_replay_path() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}/replay"
        ))
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn should_reject_admin_mutation_given_cross_origin_request() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}/replay"
        ))
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://evil.example")
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
async fn should_reject_unprovisioned_route_family_before_dead_letter_replay() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    runtime.configure_route_families(&[1]);
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/2/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}/replay"
        ))
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn should_reject_unprovisioned_route_family_before_dead_letter_purge() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    runtime.configure_route_families(&[1]);
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/api/v1/2/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}"
        ))
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
#[serial]
async fn should_replay_dead_letter_given_family_targeted_admin_request() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let replay_req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}/replay"
        ))
        .header(COOKIE, cookie.clone())
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let replay_response = fitz::api::admin::handlers::handle_request(replay_req, runtime.clone())
        .await
        .unwrap();
    let detail_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let detail_response = fitz::api::admin::handlers::handle_request(detail_req, runtime.clone())
        .await
        .unwrap();
    let dead_letters_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();
    let dead_letters_response =
        fitz::api::admin::handlers::handle_request(dead_letters_req, runtime)
            .await
            .unwrap();

    // Assert
    assert_eq!(replay_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_body = body::to_bytes(detail_response.into_body()).await.unwrap();
    let detail_payload = String::from_utf8(detail_body.to_vec()).unwrap();
    assert!(detail_payload.contains(r#""messages_ready":1"#));
    assert!(detail_payload.contains(r#""messages_dead_lettered":0"#));
    assert!(detail_payload.contains(r#""messages_total":1"#));
    assert_eq!(dead_letters_response.status(), StatusCode::OK);
    let dead_letters_body = body::to_bytes(dead_letters_response.into_body())
        .await
        .unwrap();
    let dead_letters_payload = String::from_utf8(dead_letters_body.to_vec()).unwrap();
    assert!(dead_letters_payload.contains(r#""messages":[]"#));
}

#[tokio::test]
#[serial]
async fn should_purge_dead_letter_given_family_targeted_admin_request() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let purge_req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{message_id}"
        ))
        .header(COOKIE, cookie.clone())
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let purge_response = fitz::api::admin::handlers::handle_request(purge_req, runtime.clone())
        .await
        .unwrap();
    let detail_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker")
        .header(COOKIE, cookie.clone())
        .body(Body::default())
        .unwrap();
    let detail_response = fitz::api::admin::handlers::handle_request(detail_req, runtime.clone())
        .await
        .unwrap();
    let dead_letters_req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/queue/realms/prod/areas/jobs/resources/worker/dead-letters")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();
    let dead_letters_response =
        fitz::api::admin::handlers::handle_request(dead_letters_req, runtime)
            .await
            .unwrap();

    // Assert
    assert_eq!(purge_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_body = body::to_bytes(detail_response.into_body()).await.unwrap();
    let detail_payload = String::from_utf8(detail_body.to_vec()).unwrap();
    assert!(detail_payload.contains(r#""messages_ready":0"#));
    assert!(detail_payload.contains(r#""messages_dead_lettered":0"#));
    assert!(detail_payload.contains(r#""messages_total":0"#));
    assert_eq!(dead_letters_response.status(), StatusCode::OK);
    let dead_letters_body = body::to_bytes(dead_letters_response.into_body())
        .await
        .unwrap();
    let dead_letters_payload = String::from_utf8(dead_letters_body.to_vec()).unwrap();
    assert!(dead_letters_payload.contains(r#""messages":[]"#));
}

#[tokio::test]
#[serial]
async fn should_return_notice_subscriptions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/notice/realms/prod/areas/events/resources/orders/subscriptions")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""subscription_id":7"#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_workers_under_operation() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/rpc/realms/prod/areas/api/resources/users/operations/get/workers")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""session_id":"9001""#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_pending_requests() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/rpc/pending?realm=prod")
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
    assert!(payload.contains(r#""correlation_id":"corr-abc-123""#));
    assert!(payload.contains(r#""route":"rpc://prod/api/users/get""#));
    assert!(payload.contains(r#""worker_session_id":"9001""#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_events_with_worker_registration_and_pending_transition() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/rpc/realms/prod/areas/api/resources/users/events?limit=3")
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
    assert_eq!(payload["domain"], "rpc");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["limit"], 3);
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    let events = payload["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events
        .iter()
        .any(|event| event["kind"] == "registration" && event["worker_session"] == "9001"));
    assert!(events.iter().any(|event| {
        event["kind"] == "transition" && event["correlation_id"] == "corr-abc-123"
    }));
}
