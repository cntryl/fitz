use super::common::*;

#[tokio::test]
#[serial]
async fn should_return_kv_transactions_under_resource() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/transactions")
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
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/value?key=user%3A1")
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
        .uri("/api/v1/1/kv/realms/prod/areas/app/resources/users/prefix?prefix=user%3A&limit=1")
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
