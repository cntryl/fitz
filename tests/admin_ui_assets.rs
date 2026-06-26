use fitz::api::http::Body;
use fitz::boot::Runtime;
use fitz::runtime::Router;
use fitz::testkit::body;
use hyper::{Method, StatusCode};
use std::path::Path;
use std::sync::Arc;

fn test_runtime() -> Arc<Runtime> {
    fitz::boot::observability::metrics().clear();
    Arc::new(Runtime::new(Arc::new(Router::new())))
}

fn assert_browser_security_headers(headers: &hyper::HeaderMap) {
    assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(headers.get("referrer-policy").unwrap(), "no-referrer");
    assert_eq!(
        headers.get("content-security-policy").unwrap(),
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; connect-src 'self'; font-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
    );
}

#[tokio::test]
async fn should_return_not_found_given_runtime_ui_missing() {
    // Arrange
    if Path::new("/app/public/index.html").is_file() {
        return;
    }
    let runtime = test_runtime();
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();
    let status = response.status();
    assert_browser_security_headers(response.headers());
    let body = body::to_bytes(response.into_body()).await.unwrap();

    // Assert
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body.as_ref(), br#"{"error":"Not Found"}"#);
}

#[tokio::test]
async fn should_reject_static_path_traversal_with_browser_security_headers() {
    // Arrange
    let runtime = test_runtime();
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/../secret")
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    assert_browser_security_headers(response.headers());
}
