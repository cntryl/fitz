use fitz::boot::Runtime;
use fitz::runtime::Router;
use hyper::header::{self, ETAG};
use hyper::{body, Body, Method, Request, StatusCode};
use regex::Regex;
use serial_test::serial;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn test_runtime() -> Arc<Runtime> {
    fitz::boot::observability::metrics().clear();
    Arc::new(Runtime::new(Arc::new(Router::new())))
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

fn extract_js_asset_path(html: &str) -> String {
    let pattern = Regex::new(r#"/assets/[^\"']+\.js"#).expect("compile asset regex");
    pattern
        .find(html)
        .map(|match_| match_.as_str().to_string())
        .expect("embedded index.html should reference a JavaScript asset")
}

#[tokio::test]
#[serial]
async fn should_serve_embedded_ui_without_runtime_files() {
    let runtime = test_runtime();
    let temp_dir = tempfile::tempdir().expect("create temporary working directory");
    let _cwd_guard = CurrentDirGuard::change_to(temp_dir.path());
    let request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(request, runtime)
        .await
        .unwrap();
    let status = response.status();
    let content_type = response.headers().get(header::CONTENT_TYPE).unwrap().clone();
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let html = std::str::from_utf8(&body).expect("decode embedded html");

    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "text/html; charset=utf-8");
    assert!(html.contains("Fitz Admin"));
}

#[tokio::test]
#[serial]
async fn should_serve_embedded_assets_with_etag_and_compression() {
    let runtime = test_runtime();
    let index_request = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();
    let index_response = fitz::api::admin::handlers::handle_request(index_request, runtime.clone())
        .await
        .unwrap();
    let index_body = body::to_bytes(index_response.into_body()).await.unwrap();
    let index_html = std::str::from_utf8(&index_body).expect("decode embedded html");
    let asset_path = extract_js_asset_path(index_html);

    let asset_request = Request::builder()
        .method(Method::GET)
        .uri(asset_path.as_str())
        .header(header::ACCEPT_ENCODING, "br, gzip")
        .body(Body::empty())
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
    assert_eq!(asset_content_type, "application/javascript; charset=utf-8");
    assert_eq!(asset_content_encoding, "br");
    assert_eq!(asset_vary, "Accept-Encoding");
    assert!(!asset_body.is_empty());

    let not_modified_request = Request::builder()
        .method(Method::GET)
        .uri(asset_path.as_str())
        .header(header::ACCEPT_ENCODING, "br, gzip")
        .header(header::IF_NONE_MATCH, etag)
        .body(Body::empty())
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