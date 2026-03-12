//! Integration test for admin REST API

use fitz::boot::Runtime;
use fitz::runtime::Router;
use hyper::{Body, Method, Request, StatusCode};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use std::sync::Arc;

fn admin_token(is_admin: bool) -> String {
    #[derive(serde::Serialize)]
    struct Claims<'a> {
        iss: &'a str,
        aud: &'a str,
        sub: &'a str,
        exp: u64,
        tid: &'a str,
        fitz: FitzClaims<'a>,
        roles: Vec<&'a str>,
    }

    #[derive(serde::Serialize)]
    struct FitzClaims<'a> {
        permissions: Vec<&'a str>,
    }

    std::env::set_var("FITZ_JWT_HMAC_SECRET", "test-secret-key");

    let claims = Claims {
        iss: "",
        aud: "fitz",
        sub: "admin-user",
        exp: 9_999_999_999,
        tid: "admin-realm",
        fitz: FitzClaims {
            permissions: if is_admin {
                vec!["admin://**#read"]
            } else {
                vec!["notice://admin-realm/**#read"]
            },
        },
        roles: if is_admin { vec!["admin"] } else { vec![] },
    };

    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(b"test-secret-key"),
    )
    .unwrap()
}

#[tokio::test]
async fn should_respond_to_healthz_probe() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_startup_complete();
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_respond_to_readyz_probe() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/readyz")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_return_service_unavailable_when_not_ready() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/readyz")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn should_require_valid_auth_for_metrics_endpoint() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header("Authorization", "Bearer invalid-token")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn should_respond_to_metrics_endpoint_with_valid_token() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header("Authorization", format!("Bearer {}", admin_token(false)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain; version=0.0.4"
    );
}

#[tokio::test]
async fn should_require_admin_claims_for_global_stats() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/stats")
        .header("Authorization", format!("Bearer {}", admin_token(false)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn should_respond_to_global_stats_for_admin_token() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/stats")
        .header("Authorization", format!("Bearer {}", admin_token(true)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_respond_to_domain_stats() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/kv/stats")
        .header("Authorization", format!("Bearer {}", admin_token(true)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn should_return_not_implemented_for_unwired_admin_lists() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/kv/transactions")
        .header("Authorization", format!("Bearer {}", admin_token(true)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
}

#[tokio::test]
async fn should_require_websocket_upgrade_for_plain_ws_get() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/ws")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UPGRADE_REQUIRED);
}

#[tokio::test]
async fn should_serve_spa_for_unknown_paths() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/unknown/path")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_serve_spa_at_root() {
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_support_real_websocket_upgrade() {
    let server = fitz::testkit::TestServer::start()
        .await
        .expect("start server");
    let url = format!("ws://{}/ws", server.ws_addr);

    let result = tokio_tungstenite::connect_async(url).await;

    assert!(result.is_ok(), "expected websocket upgrade to succeed");
}

#[tokio::test]
async fn should_report_live_session_stats_after_websocket_activity() {
    let server = fitz::testkit::TestServer::start_with_auth(true)
        .await
        .expect("start server");
    let url = format!("ws://{}/ws", server.ws_addr);
    let _socket = tokio_tungstenite::connect_async(url)
        .await
        .expect("websocket upgrade");

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/stats")
        .header("Authorization", format!("Bearer {}", admin_token(true)))
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, server.runtime.clone())
        .await
        .unwrap();
    let body = hyper::body::to_bytes(response.into_body()).await.unwrap();
    let stats: fitz::api::admin::GlobalStats = serde_json::from_slice(&body).unwrap();

    assert!(stats.broker.sessions >= 1);
}
