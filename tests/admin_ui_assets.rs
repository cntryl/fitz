use fitz::api::http::Body;
use fitz::boot::Runtime;
use fitz::runtime::Router;
use fitz::testkit::body;
use hyper::header::{self, ETAG};
use hyper::{Method, StatusCode};
use serial_test::serial;
use std::env;
use std::path::{Path, PathBuf};
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

struct CurrentDirGuard {
    original: PathBuf,
}

impl CurrentDirGuard {
    fn change_to(path: &Path) -> Self {
        let original = env::current_dir().expect("read current directory");
        env::set_current_dir(path).expect("change current directory");
        Self { original }
    }
}

impl Drop for CurrentDirGuard {
    fn drop(&mut self) {
        env::set_current_dir(&self.original).expect("restore current directory");
    }
}

#[tokio::test]
#[serial]
async fn should_serve_embedded_ui_without_runtime_files() {
    let runtime = test_runtime();
    let temp_dir = tempfile::tempdir().expect("create temporary working directory");
    let _cwd_guard = CurrentDirGuard::change_to(temp_dir.path());
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .clone();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let html = std::str::from_utf8(&body).expect("decode embedded html");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "text/html; charset=utf-8");
    assert!(html.contains("Fitz Admin"));
}

#[tokio::test]
#[serial]
async fn should_serve_embedded_svg_with_etag_and_compression() {
    let runtime = test_runtime();
    let asset_request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/favicon.svg")
        .header(header::ACCEPT_ENCODING, "gzip")
        .body(Body::default())
        .unwrap();

    let asset_response = fitz::api::admin::handlers::handle_request(asset_request, runtime.clone())
        .await
        .unwrap();
    let etag = asset_response
        .headers()
        .get(ETAG)
        .expect("embedded asset etag header")
        .to_str()
        .expect("etag header utf-8")
        .to_string();
    let asset_status = asset_response.status();
    let asset_content_type = asset_response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .clone();
    let asset_content_encoding = asset_response
        .headers()
        .get(header::CONTENT_ENCODING)
        .expect("compressed embedded asset")
        .clone();
    let asset_vary = asset_response.headers().get(header::VARY).unwrap().clone();
    let asset_body = body::to_bytes(asset_response.into_body()).await.unwrap();

    assert_eq!(asset_status, StatusCode::OK);
    assert_eq!(asset_content_type, "image/svg+xml");
    assert_eq!(asset_content_encoding, "gzip");
    assert_eq!(asset_vary, "Accept-Encoding");
    assert!(!asset_body.is_empty());

    let not_modified_request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/favicon.svg")
        .header(header::ACCEPT_ENCODING, "gzip")
        .header(header::IF_NONE_MATCH, etag)
        .body(Body::default())
        .unwrap();
    let not_modified_response =
        fitz::api::admin::handlers::handle_request(not_modified_request, runtime)
            .await
            .unwrap();
    let not_modified_status = not_modified_response.status();
    let not_modified_body = body::to_bytes(not_modified_response.into_body())
        .await
        .unwrap();

    assert_eq!(not_modified_status, StatusCode::NOT_MODIFIED);
    assert!(not_modified_body.is_empty());
}

#[tokio::test]
#[serial]
async fn should_add_security_headers_to_static_asset_response() {
    // Arrange
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

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_browser_security_headers(response.headers());
}

#[tokio::test]
#[serial]
async fn should_add_security_headers_to_not_modified_asset_response() {
    // Arrange
    let runtime = test_runtime();
    let asset_request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/favicon.svg")
        .body(Body::default())
        .unwrap();
    let asset_response = fitz::api::admin::handlers::handle_request(asset_request, runtime.clone())
        .await
        .unwrap();
    let etag = asset_response.headers().get(ETAG).unwrap().clone();
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/favicon.svg")
        .header(header::IF_NONE_MATCH, etag)
        .body(Body::default())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_browser_security_headers(response.headers());
}

#[tokio::test]
#[serial]
async fn should_serve_client_routes_from_embedded_index() {
    let runtime = test_runtime();
    let temp_dir = tempfile::tempdir().expect("create temporary working directory");
    let _cwd_guard = CurrentDirGuard::change_to(temp_dir.path());
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/sessions/123")
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .clone();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let html = std::str::from_utf8(&body).expect("decode embedded html");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "text/html; charset=utf-8");
    assert!(html.contains("Fitz Admin"));
}

#[tokio::test]
#[serial]
async fn should_fallback_to_embedded_index_for_missing_asset_paths() {
    let runtime = test_runtime();
    let temp_dir = tempfile::tempdir().expect("create temporary working directory");
    let _cwd_guard = CurrentDirGuard::change_to(temp_dir.path());
    let request = hyper::http::Request::builder()
        .method(Method::GET)
        .uri("/assets/does-not-exist.js")
        .body(Body::default())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .clone();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let html = std::str::from_utf8(&body).expect("decode embedded html");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "text/html; charset=utf-8");
    assert!(html.contains("Fitz Admin"));
}
