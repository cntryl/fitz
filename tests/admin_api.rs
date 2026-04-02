//! Integration test for admin REST API

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use fitz::api::admin::{KvTransaction, NoticeSubscription, RpcPendingRequest, RpcWorker};
use fitz::boot::Runtime;
use fitz::runtime::Router;
use hyper::header::{COOKIE, SET_COOKIE};
use hyper::{body, Body, Method, Request, StatusCode};
use serial_test::serial;
use std::sync::Arc;

fn password_hash_for(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn configure_admin_auth() {
    std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
    std::env::set_var(
        "FITZ_ADMIN_PASSWORD_HASH",
        password_hash_for("secret-password"),
    );
    std::env::set_var("FITZ_ADMIN_JWT_SECRET", "jwt-secret");
    std::env::set_var("FITZ_ADMIN_SESSION_TTL_SECS", "3600");
}

fn test_runtime() -> Arc<Runtime> {
    configure_admin_auth();
    let router = Arc::new(Router::new());
    Arc::new(Runtime::new(router))
}

fn seed_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_kv_transactions(vec![KvTransaction {
        tx_id: 41,
        realm: "prod".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: "readwrite".to_string(),
        started_at: "2026-03-14T12:00:00Z".to_string(),
        operations_count: 3,
        idle_seconds: 1,
    }]);
    read_model.replace_notice_subscriptions(vec![NoticeSubscription {
        subscription_id: 7,
        session_id: "123".to_string(),
        realm: "prod".to_string(),
        pattern: "notice://prod/events/orders/created".to_string(),
        created_at: "2026-03-14T12:00:00Z".to_string(),
        notifications_received: 5,
    }]);
    read_model.replace_rpc_workers(vec![RpcWorker {
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![RpcPendingRequest {
        correlation_id: "corr-abc-123".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        submitted_at: "2026-03-14T12:00:07Z".to_string(),
        age_seconds: 7,
        worker_session_id: Some("9001".to_string()),
    }]);
}

async fn login_cookie(runtime: Arc<Runtime>) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .body(Body::from(
            r#"{"username":"admin","password":"secret-password"}"#,
        ))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

#[tokio::test]
#[serial]
async fn should_create_admin_session_and_set_cookie() {
    let runtime = test_runtime();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .body(Body::from(
            r#"{"username":"admin","password":"secret-password"}"#,
        ))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("fitz_admin_session="));
}

#[tokio::test]
#[serial]
async fn should_require_auth_for_hierarchical_route() {
    let runtime = test_runtime();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn should_list_kv_realms_with_valid_cookie() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_resource_collection_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_leaf_resource_detail() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/areas/logs/resources/application")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""realm":"prod""#));
    assert!(payload.contains(r#""area":"logs""#));
    assert!(payload.contains(r#""resource":"application""#));
}

#[tokio::test]
#[serial]
async fn should_return_kv_transactions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/transactions")
        .header(COOKIE, cookie)
        .body(Body::empty())
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
async fn should_return_queue_leases_under_resource() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/leases")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_notice_subscriptions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/notice/realms/prod/areas/events/resources/orders/subscriptions")
        .header(COOKIE, cookie)
        .body(Body::empty())
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

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/realms/prod/areas/api/resources/users/operations/get/workers")
        .header(COOKIE, cookie)
        .body(Body::empty())
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

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/pending?realm=prod")
        .header(COOKIE, cookie)
        .body(Body::empty())
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
async fn should_return_exact_rpc_operation_detail_counts() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_rpc_workers(vec![RpcWorker {
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![
        RpcPendingRequest {
            correlation_id: "corr-get".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            submitted_at: "2026-03-14T12:00:07Z".to_string(),
            age_seconds: 7,
            worker_session_id: Some("9001".to_string()),
        },
        RpcPendingRequest {
            correlation_id: "corr-get-details".to_string(),
            route: "rpc://prod/api/users/get-details".to_string(),
            submitted_at: "2026-03-14T12:00:08Z".to_string(),
            age_seconds: 8,
            worker_session_id: Some("9002".to_string()),
        },
    ]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/realms/prod/areas/api/resources/users/operations/get")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""workers_registered":1"#));
    assert!(payload.contains(r#""requests_pending":1"#));
}

#[tokio::test]
#[serial]
async fn should_return_sessions_collection_only() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_reject_removed_session_detail_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions/123")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
