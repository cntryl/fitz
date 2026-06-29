use crate::api::http::{Body, Response};
use hyper::{header, StatusCode};
use std::path::Path;
use tempfile::TempDir;

use super::server::AssetServer;
use super::INDEX_PATH;

const APP_JS: &[u8] = b"console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');";
const INDEX_HTML: &[u8] =
    b"<!doctype html><html><body><script src=\"/assets/app.js\"></script></body></html>";

fn test_root() -> TempDir {
    let root = tempfile::tempdir().expect("create asset root");
    write_asset(root.path(), INDEX_PATH, INDEX_HTML);
    write_asset(root.path(), "assets/app.js", APP_JS);
    write_asset(root.path(), "favicon.svg", b"<svg></svg>");
    write_asset(
        root.path(),
        "logo.png",
        &[137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3, 4, 5],
    );
    root
}

fn write_asset(root: &Path, path: &str, bytes: &[u8]) {
    let absolute_path = root.join(path);
    if let Some(parent) = absolute_path.parent() {
        std::fs::create_dir_all(parent).expect("create asset directory");
    }
    std::fs::write(absolute_path, bytes).expect("write asset");
}

async fn serve(
    root: &Path,
    path: &str,
    accept_encoding: Option<&str>,
    if_none_match: Option<&str>,
) -> Response {
    let server = AssetServer::new(root);
    serve_with_server(&server, path, accept_encoding, if_none_match).await
}

async fn serve_with_server(
    server: &AssetServer,
    path: &str,
    accept_encoding: Option<&str>,
    if_none_match: Option<&str>,
) -> Response {
    let mut request = hyper::http::Request::builder().uri(path);
    if let Some(accept_encoding) = accept_encoding {
        request = request.header(header::ACCEPT_ENCODING, accept_encoding);
    }
    if let Some(if_none_match) = if_none_match {
        request = request.header(header::IF_NONE_MATCH, if_none_match);
    }

    let request = request.body(Body::default()).unwrap();
    server.serve(request.uri().path(), request.headers())
}

#[tokio::test]
async fn should_lookup_asset_by_exact_path_given_file_exists() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/assets/app.js", None, None).await;
    let body = crate::testkit::body::to_bytes(response.into_body())
        .await
        .unwrap();

    // Assert
    assert_eq!(body.as_ref(), APP_JS);
}

#[tokio::test]
async fn should_apply_svg_mime_type() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/favicon.svg", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/svg+xml"
    );
}

#[tokio::test]
async fn should_fallback_to_index_for_client_routes() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/sessions/123", None, None).await;
    let status = response.status();
    let body = crate::testkit::body::to_bytes(response.into_body())
        .await
        .unwrap();

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert!(std::str::from_utf8(&body)
        .unwrap()
        .contains("<!doctype html>"));
}

#[tokio::test]
async fn should_preserve_missing_asset_fallback_behavior() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/assets/missing.js", None, None).await;
    let status = response.status();
    let content_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .clone();
    let body = crate::testkit::body::to_bytes(response.into_body())
        .await
        .unwrap();

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert_eq!(content_type, "text/html; charset=utf-8");
    assert!(std::str::from_utf8(&body)
        .unwrap()
        .contains("<!doctype html>"));
}

#[tokio::test]
async fn should_preserve_missing_root_file_fallback_behavior() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/missing.css", None, None).await;
    let status = response.status();
    let body = crate::testkit::body::to_bytes(response.into_body())
        .await
        .unwrap();

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert!(std::str::from_utf8(&body)
        .unwrap()
        .contains("<!doctype html>"));
}

#[tokio::test]
async fn should_return_not_found_given_index_missing_for_root_request() {
    // Arrange
    let root = tempfile::tempdir().expect("create asset root");
    write_asset(root.path(), "assets/app.js", APP_JS);

    // Act
    let response = serve(root.path(), "/", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_serve_exact_asset_given_index_missing() {
    // Arrange
    let root = tempfile::tempdir().expect("create asset root");
    write_asset(root.path(), "assets/app.js", APP_JS);

    // Act
    let response = serve(root.path(), "/assets/app.js", None, None).await;
    let status = response.status();
    let body = crate::testkit::body::to_bytes(response.into_body())
        .await
        .unwrap();

    // Assert
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), APP_JS);
}

#[tokio::test]
async fn should_reject_path_traversal() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/../secret", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[cfg(unix)]
#[tokio::test]
async fn should_reject_file_symlink_escape_from_root() {
    // Arrange
    let root = test_root();
    let outside = tempfile::tempdir().expect("create outside root");
    write_asset(outside.path(), "secret.js", b"outside");
    std::fs::remove_file(root.path().join("assets/app.js")).expect("remove real asset");
    std::os::unix::fs::symlink(
        outside.path().join("secret.js"),
        root.path().join("assets/app.js"),
    )
    .expect("create escaping file symlink");

    // Act
    let response = serve(root.path(), "/assets/app.js", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[cfg(unix)]
#[tokio::test]
async fn should_reject_directory_symlink_escape_from_root() {
    // Arrange
    let root = test_root();
    let outside = tempfile::tempdir().expect("create outside root");
    write_asset(outside.path(), "secret.js", b"outside");
    std::fs::remove_dir_all(root.path().join("assets")).expect("remove real assets directory");
    std::os::unix::fs::symlink(outside.path(), root.path().join("assets"))
        .expect("create escaping directory symlink");

    // Act
    let response = serve(root.path(), "/assets/secret.js", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn should_reuse_cached_entry_given_unchanged_asset_metadata() {
    // Arrange
    let root = test_root();
    let server = AssetServer::new(root.path());
    server.reset_entry_build_count();

    // Act
    let first = serve_with_server(&server, "/assets/app.js", Some("gzip"), None).await;
    let second = serve_with_server(&server, "/assets/app.js", Some("gzip"), None).await;

    // Assert
    assert_eq!(first.status(), StatusCode::OK);
    assert_eq!(second.status(), StatusCode::OK);
    assert_eq!(server.entry_build_count(), 1);
}

#[tokio::test]
async fn should_refresh_cached_entry_given_changed_asset_metadata() {
    // Arrange
    let root = test_root();
    let server = AssetServer::new(root.path());
    server.reset_entry_build_count();
    let initial = serve_with_server(&server, "/assets/app.js", None, None).await;
    let initial_etag = initial
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    write_asset(
        root.path(),
        "assets/app.js",
        b"console.log('fitz');console.log('changed');",
    );

    // Act
    let response = serve_with_server(&server, "/assets/app.js", None, None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_ne!(
        response
            .headers()
            .get(header::ETAG)
            .unwrap()
            .to_str()
            .unwrap(),
        initial_etag.as_str()
    );
    assert_eq!(server.entry_build_count(), 2);
}

#[tokio::test]
async fn should_prefer_brotli_when_supported() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/assets/app.js", Some("gzip, br"), None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get(header::CONTENT_ENCODING).unwrap(),
        "br"
    );
    assert_eq!(
        response.headers().get(header::VARY).unwrap(),
        "Accept-Encoding"
    );
}

#[tokio::test]
async fn should_skip_compression_for_non_compressible_assets() {
    // Arrange
    let root = test_root();

    // Act
    let response = serve(root.path(), "/logo.png", Some("br, gzip"), None).await;

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "image/png"
    );
}

#[tokio::test]
async fn should_return_not_modified_when_etag_matches_representation() {
    // Arrange
    let root = test_root();
    let initial = serve(root.path(), "/assets/app.js", Some("gzip"), None).await;
    let etag = initial
        .headers()
        .get(header::ETAG)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();

    // Act
    let response = serve(root.path(), "/assets/app.js", Some("gzip"), Some(&etag)).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(
        response.headers().get(header::CONTENT_ENCODING).unwrap(),
        "gzip"
    );
    assert_eq!(
        crate::testkit::body::to_bytes(response.into_body())
            .await
            .unwrap()
            .len(),
        0
    );
}
