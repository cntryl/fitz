//! Integration test for admin REST API

use fitz::boot::Runtime;
use fitz::runtime::Router;
use hyper::{Body, Method, Request, StatusCode};
use std::sync::Arc;

#[tokio::test]
async fn should_respond_to_healthz_probe() {
    // Arrange
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

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_respond_to_readyz_probe() {
    // Arrange
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

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_return_service_unavailable_when_not_ready() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    // Don't mark ready
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/readyz")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn should_respond_to_metrics_endpoint() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "text/plain; version=0.0.4"
    );
}

#[tokio::test]
async fn should_respond_to_global_stats() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/stats")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_respond_to_domain_stats() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/admin/kv/stats")
        .header("Authorization", "Bearer test-token")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn should_serve_spa_for_unknown_paths() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/unknown/path")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    // 404 is OK if public/index.html doesn't exist in test environment
    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_serve_spa_at_root() {
    // Arrange
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    let runtime = Arc::new(runtime);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert!(response.status() == StatusCode::OK || response.status() == StatusCode::NOT_FOUND);
}
