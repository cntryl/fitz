use super::common::*;

#[tokio::test]
#[serial]
async fn should_keep_healthz_unhealthy_until_readiness_checks_pass() {
    // Arrange
    let runtime = test_runtime_not_ready();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["status"], "not_ready");
    assert_eq!(payload["checks"]["storage"], "not_ready");
    assert_eq!(payload["checks"]["storage_writer_lease"], "not_ready");
    assert_eq!(payload["checks"]["auth_configuration"], "not_ready");
    assert_eq!(payload["checks"]["startup_complete"], "not_ready");
}

#[tokio::test]
#[serial]
async fn should_keep_readyz_unhealthy_until_domains_initialize_after_storage_ready() {
    // Arrange
    let runtime = test_runtime_not_ready();
    runtime.mark_storage_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/readyz")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["status"], "not_ready");
    assert_eq!(payload["checks"]["storage"], "ok");
    assert_eq!(payload["checks"]["storage_writer_lease"], "ok");
    assert_eq!(payload["checks"]["auth_configuration"], "ok");
    assert_eq!(payload["checks"]["startup_complete"], "ok");
    assert_eq!(payload["checks"]["domains_initialized"], "not_ready");
}

#[tokio::test]
#[serial]
async fn should_keep_healthz_unhealthy_given_shutdown_when_runtime_was_ready() {
    // Arrange
    let runtime = test_runtime();
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    runtime.begin_shutdown();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["status"], "not_ready");
    assert_eq!(payload["checks"]["storage"], "ok");
    assert_eq!(payload["checks"]["storage_writer_lease"], "ok");
    assert_eq!(payload["checks"]["domains_initialized"], "ok");
    assert_eq!(payload["checks"]["auth_configuration"], "ok");
    assert_eq!(payload["checks"]["startup_complete"], "ok");
    assert_eq!(payload["checks"]["accepting_traffic"], "not_ready");
}

#[tokio::test]
#[serial]
async fn should_keep_healthz_unhealthy_given_drain_when_runtime_was_ready() {
    // Arrange
    let runtime = test_runtime();
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    runtime.begin_drain();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["status"], "not_ready");
    assert_eq!(payload["checks"]["storage"], "ok");
    assert_eq!(payload["checks"]["domains_initialized"], "ok");
    assert_eq!(payload["checks"]["auth_configuration"], "ok");
    assert_eq!(payload["checks"]["startup_complete"], "ok");
    assert_eq!(payload["checks"]["accepting_traffic"], "draining");
}

#[tokio::test]
#[serial]
async fn should_report_targetz_ready_before_data_plane_readiness() {
    // Arrange
    let runtime = test_runtime_not_ready();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/targetz")
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
    assert_eq!(payload["status"], "ready");
    assert_eq!(payload["checks"]["http_listener"], "ok");
    assert_eq!(payload["checks"]["accepting_target_traffic"], "ok");
    assert_eq!(payload["checks"]["data_plane_ready"], "not_ready");
    assert_eq!(payload["checks"]["storage_writer_lease"], "not_ready");
}

#[tokio::test]
#[serial]
async fn should_keep_targetz_unhealthy_given_drain() {
    // Arrange
    let runtime = test_runtime();
    runtime.begin_drain();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/targetz")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["status"], "not_ready");
    assert_eq!(payload["checks"]["http_listener"], "ok");
    assert_eq!(payload["checks"]["accepting_target_traffic"], "draining");
    assert_eq!(payload["checks"]["data_plane_ready"], "not_ready");
}

#[tokio::test]
#[serial]
async fn should_reject_domain_admin_api_before_data_plane_readiness() {
    // Arrange
    let runtime = test_runtime_not_ready();
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
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["error"], "data plane not ready");
}

#[tokio::test]
#[serial]
async fn should_report_livez_ok_given_runtime_not_ready() {
    // Arrange
    let runtime = test_runtime_not_ready();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/livez")
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
    assert_eq!(payload["status"], "ok");
}

#[tokio::test]
#[serial]
async fn should_begin_runtime_drain_given_authenticated_same_origin_request() {
    // Arrange
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri("/api/v1/runtime/drain")
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert!(runtime.is_draining());
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["lifecycle_state"], "draining");
    assert_eq!(payload["active_sessions"], 0);
    assert_eq!(payload["drain_grace_seconds"], 25);
    assert_eq!(payload["close_reason"], "broker draining for redeploy");
    assert!(payload["drain_started_epoch_ms"].as_u64().is_some());
    assert!(payload["drain_deadline_epoch_ms"].as_u64().is_some());
}

#[tokio::test]
#[serial]
async fn should_reject_runtime_drain_given_cross_origin_request() {
    // Arrange
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri("/api/v1/runtime/drain")
        .header(COOKIE, cookie)
        .header("host", "localhost")
        .header("origin", "http://evil.example")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert!(!runtime.is_draining());
}

#[tokio::test]
#[serial]
async fn should_create_admin_session_and_set_cookie() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::from(r#"{"username":"root","password":"pwd123"}"#))
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    let set_cookie = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.contains("fitz_admin_session="));
    assert!(set_cookie.contains("; HttpOnly"));
    assert!(set_cookie.contains("; Secure"));
    assert!(set_cookie.contains("; SameSite=Strict"));
}

#[tokio::test]
#[serial]
async fn should_reject_admin_login_given_cross_origin() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .header("host", "localhost")
        .header("origin", "http://evil.example")
        .body(Body::from(r#"{"username":"root","password":"pwd123"}"#))
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
async fn should_clear_admin_session_cookie_given_valid_logout_cookie() {
    // Arrange
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri("/api/v1/session")
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
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_clear_admin_cookie(&response);
}

#[tokio::test]
#[serial]
async fn should_clear_admin_session_cookie_given_expired_logout_cookie() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri("/api/v1/session")
        .header(COOKIE, expired_admin_cookie())
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_clear_admin_cookie(&response);
}

#[tokio::test]
#[serial]
async fn should_clear_admin_session_cookie_given_malformed_logout_cookie() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri("/api/v1/session")
        .header(COOKIE, "fitz_admin_session=not-a-jwt")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_clear_admin_cookie(&response);
}

#[tokio::test]
#[serial]
async fn should_clear_admin_session_cookie_given_missing_logout_cookie() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri("/api/v1/session")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert_clear_admin_cookie(&response);
}

#[tokio::test]
#[serial]
async fn should_reject_admin_logout_given_cross_origin() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::DELETE)
        .uri("/api/v1/session")
        .header(COOKIE, expired_admin_cookie())
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
    assert!(response.headers().get(SET_COOKIE).is_none());
}

#[tokio::test]
#[serial]
async fn should_require_auth_for_hierarchical_route() {
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/1/kv/realms")
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn should_reject_overflowed_route_family_path_segment() {
    // Arrange
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/4294967296/kv/realms")
        .header(COOKIE, cookie)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial]
async fn should_add_security_headers_to_admin_json_response() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/features")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_browser_security_headers(response.headers());
}

#[tokio::test]
#[serial]
async fn should_not_expose_route_family_grants_from_protected_features() {
    // Arrange
    let _mode_guard = EnvGuard::unset("FITZ_ADMIN_AUTH_MODE");
    let _family_guard = EnvGuard::set("FITZ_ADMIN_ROUTE_FAMILIES", "internal,partner");
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/features")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Assert
    assert_eq!(payload["admin_auth_required"], true);
    assert!(payload["route_families"].as_array().unwrap().is_empty());
    assert_eq!(payload["route_families_wildcard"], false);
}

#[tokio::test]
#[serial]
async fn should_report_provisioned_route_families_and_wildcard_access_from_open_features() {
    // Arrange
    let _mode_guard = EnvGuard::set("FITZ_ADMIN_AUTH_MODE", "open");
    let _family_guard = EnvGuard::set("FITZ_ADMIN_ROUTE_FAMILIES", "internal,partner");
    let runtime = test_runtime();
    runtime.configure_route_families(&[1, 2]);
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/features")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();

    // Assert
    assert_eq!(payload["admin_auth_required"], false);
    assert_eq!(payload["route_families"], serde_json::json!(["1", "2"]));
    assert_eq!(payload["route_families_wildcard"], true);
}

#[tokio::test]
#[serial]
async fn should_add_security_headers_to_admin_error_response() {
    // Arrange
    let runtime = test_runtime();
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_browser_security_headers(response.headers());
}

#[tokio::test]
#[serial]
async fn should_add_hsts_given_external_tls_ack() {
    // Arrange
    let _guard = EnvGuard::unset("FITZ_ASSUME_EXTERNAL_TLS");
    let runtime = test_runtime_from_boot_config(true);
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/features")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_browser_security_headers(response.headers());
    assert_eq!(
        response.headers().get("strict-transport-security").unwrap(),
        "max-age=31536000"
    );
}

#[tokio::test]
#[serial]
async fn should_omit_hsts_without_external_tls_ack() {
    // Arrange
    let _guard = EnvGuard::unset("FITZ_ASSUME_EXTERNAL_TLS");
    let runtime = test_runtime_from_boot_config(false);
    let req = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/api/v1/features")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_browser_security_headers(response.headers());
    assert!(response
        .headers()
        .get("strict-transport-security")
        .is_none());
}
