//! Admin UI asset serving.
//!
//! This module owns the production static-asset contract for the admin SPA,
//! including lookup, MIME mapping, SPA fallback, compression negotiation, and
//! ETag handling. The request handler delegates here so the rest of the admin
//! API remains unchanged.

use crate::api::http::{Body, Response};
use bytes::Bytes;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::StatusCode;
use once_cell::sync::Lazy;
use parking_lot::RwLock;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::Infallible;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;

const CACHE_CONTROL: &str = "public, max-age=3600";
const INDEX_PATH: &str = "index.html";
const PUBLIC_ASSET_ROOT: &str = "/app/public";
const VARY_ACCEPT_ENCODING: &str = "Accept-Encoding";

static PUBLIC_ASSET_SERVER: Lazy<AssetServer> =
    Lazy::new(|| AssetServer::new(Path::new(PUBLIC_ASSET_ROOT)));

pub(crate) fn serve_request<B>(req: &hyper::Request<B>) -> Result<Response, Infallible> {
    PUBLIC_ASSET_SERVER.serve(req.uri().path(), req.headers())
}

#[derive(Clone)]
struct AssetRepresentation {
    body: Bytes,
    etag: String,
    content_encoding: Option<&'static str>,
}

impl AssetRepresentation {
    fn from_vec(bytes: Vec<u8>, content_encoding: Option<&'static str>) -> Self {
        Self {
            etag: strong_etag(&bytes),
            body: Bytes::from(bytes),
            content_encoding,
        }
    }
}

struct AssetEntry {
    content_type: &'static str,
    cache_control: &'static str,
    identity: AssetRepresentation,
    gzip: Option<AssetRepresentation>,
    brotli: Option<AssetRepresentation>,
}

impl AssetEntry {
    fn new(path: &str, bytes: Vec<u8>) -> Self {
        let content_type = content_type_for_path(path);
        let (gzip, brotli) = if is_compressible(path, content_type) {
            let gzip = gzip_compress(&bytes)
                .filter(|compressed| compressed.len() < bytes.len())
                .map(|compressed| AssetRepresentation::from_vec(compressed, Some("gzip")));
            let brotli = brotli_compress(&bytes)
                .filter(|compressed| compressed.len() < bytes.len())
                .map(|compressed| AssetRepresentation::from_vec(compressed, Some("br")));
            (gzip, brotli)
        } else {
            (None, None)
        };
        let identity = AssetRepresentation::from_vec(bytes, None);

        Self {
            content_type,
            cache_control: CACHE_CONTROL,
            identity,
            gzip,
            brotli,
        }
    }

    fn select_representation(&self, headers: &HeaderMap) -> &AssetRepresentation {
        match preferred_encoding(headers, self) {
            CompressionEncoding::Brotli => self.brotli.as_ref().unwrap_or(&self.identity),
            CompressionEncoding::Gzip => self.gzip.as_ref().unwrap_or(&self.identity),
            CompressionEncoding::Identity => &self.identity,
        }
    }
}

#[derive(Clone)]
struct CachedAssetEntry {
    fingerprint: AssetFingerprint,
    entry: Arc<AssetEntry>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct AssetFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

impl AssetFingerprint {
    fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }
}

struct ResolvedAsset {
    relative_path: String,
    absolute_path: PathBuf,
    fingerprint: AssetFingerprint,
}

enum AssetResolution {
    Found(ResolvedAsset),
    Missing,
    Unsafe,
}

struct AssetServer {
    canonical_root: Option<PathBuf>,
    cache: RwLock<HashMap<String, CachedAssetEntry>>,
    #[cfg(test)]
    entry_builds: std::sync::atomic::AtomicUsize,
}

impl AssetServer {
    fn new(root: &Path) -> Self {
        let canonical_root = fs::canonicalize(root).ok().filter(|path| path.is_dir());

        Self {
            canonical_root,
            cache: RwLock::new(HashMap::new()),
            #[cfg(test)]
            entry_builds: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn serve(&self, path: &str, headers: &HeaderMap) -> Result<Response, Infallible> {
        let Some(resolved_asset) = self.resolve_path(path) else {
            return Ok(super::not_found());
        };

        let Some(asset) = self.asset_entry(&resolved_asset) else {
            return Ok(super::not_found());
        };

        let representation = asset.select_representation(headers);
        if if_none_match_matches(headers.get(header::IF_NONE_MATCH), &representation.etag) {
            return Ok(response_for_asset(
                StatusCode::NOT_MODIFIED,
                &asset,
                representation,
                true,
            ));
        }

        Ok(response_for_asset(
            StatusCode::OK,
            &asset,
            representation,
            false,
        ))
    }

    fn resolve_path(&self, path: &str) -> Option<ResolvedAsset> {
        let normalized_path = normalize_request_path(path)?;
        match self.resolve_file(normalized_path.as_str()) {
            AssetResolution::Found(resolved_asset) => return Some(resolved_asset),
            AssetResolution::Unsafe => return None,
            AssetResolution::Missing => {}
        }

        match self.resolve_file(INDEX_PATH) {
            AssetResolution::Found(resolved_asset) => Some(resolved_asset),
            AssetResolution::Missing | AssetResolution::Unsafe => None,
        }
    }

    fn resolve_file(&self, relative_path: &str) -> AssetResolution {
        let Some(canonical_root) = self.canonical_root.as_ref() else {
            return AssetResolution::Missing;
        };
        let absolute_path = match fs::canonicalize(canonical_root.join(relative_path)) {
            Ok(path) => path,
            Err(_) => return AssetResolution::Missing,
        };

        if !absolute_path.starts_with(canonical_root) {
            return AssetResolution::Unsafe;
        }

        let metadata = match fs::metadata(&absolute_path) {
            Ok(metadata) => metadata,
            Err(_) => return AssetResolution::Missing,
        };
        if !metadata.is_file() {
            return AssetResolution::Missing;
        }

        AssetResolution::Found(ResolvedAsset {
            relative_path: relative_path.to_string(),
            absolute_path,
            fingerprint: AssetFingerprint::from_metadata(&metadata),
        })
    }

    fn asset_entry(&self, resolved_asset: &ResolvedAsset) -> Option<Arc<AssetEntry>> {
        if let Some(cached_asset) = self.cache.read().get(resolved_asset.relative_path.as_str()) {
            if cached_asset.fingerprint == resolved_asset.fingerprint {
                return Some(Arc::clone(&cached_asset.entry));
            }
        }

        let bytes = fs::read(&resolved_asset.absolute_path).ok()?;
        let metadata = fs::metadata(&resolved_asset.absolute_path).ok()?;
        if !metadata.is_file() {
            return None;
        }
        let fingerprint = AssetFingerprint::from_metadata(&metadata);

        #[cfg(test)]
        self.entry_builds
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let entry = Arc::new(AssetEntry::new(&resolved_asset.relative_path, bytes));
        self.cache.write().insert(
            resolved_asset.relative_path.clone(),
            CachedAssetEntry {
                fingerprint,
                entry: Arc::clone(&entry),
            },
        );

        Some(entry)
    }

    #[cfg(test)]
    fn reset_entry_build_count(&self) {
        self.entry_builds
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }

    #[cfg(test)]
    fn entry_build_count(&self) -> usize {
        self.entry_builds.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CompressionEncoding {
    Identity,
    Gzip,
    Brotli,
}

fn response_for_asset(
    status: StatusCode,
    asset: &AssetEntry,
    representation: &AssetRepresentation,
    omit_body: bool,
) -> Response {
    let mut builder = hyper::http::Response::builder()
        .status(status)
        .header(header::CACHE_CONTROL, asset.cache_control)
        .header(header::CONTENT_TYPE, asset.content_type)
        .header(header::ETAG, representation.etag.as_str())
        .header(header::VARY, VARY_ACCEPT_ENCODING)
        .header(
            header::CONTENT_LENGTH,
            if omit_body {
                "0".to_string()
            } else {
                representation.body.len().to_string()
            },
        );

    if let Some(content_encoding) = representation.content_encoding {
        builder = builder.header(header::CONTENT_ENCODING, content_encoding);
    }

    builder
        .body(if omit_body {
            Body::default()
        } else {
            Body::from(representation.body.clone())
        })
        .unwrap()
}

fn normalize_request_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        return Some(INDEX_PATH.to_string());
    }

    let mut normalized = Vec::new();
    for component in trimmed.split('/') {
        if component == "." || component == ".." || component.contains('\\') {
            return None;
        }
        if component.is_empty() {
            continue;
        }
        normalized.push(component);
    }

    if normalized.is_empty() {
        Some(INDEX_PATH.to_string())
    } else {
        Some(normalized.join("/"))
    }
}

fn content_type_for_path(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        _ => "application/octet-stream",
    }
}

fn is_compressible(path: &str, content_type: &str) -> bool {
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/javascript; charset=utf-8" | "application/json" | "image/svg+xml"
        )
        || path.ends_with(".map")
}

fn preferred_encoding(headers: &HeaderMap, asset: &AssetEntry) -> CompressionEncoding {
    let Some(header_value) = headers.get(header::ACCEPT_ENCODING) else {
        return CompressionEncoding::Identity;
    };

    let Ok(header_value) = header_value.to_str() else {
        return CompressionEncoding::Identity;
    };

    let mut brotli_q = None;
    let mut gzip_q = None;
    let mut wildcard_q = None;

    for encoding in header_value.split(',') {
        let mut parts = encoding.trim().split(';');
        let name = parts.next().unwrap_or_default().trim();
        let mut quality = 1.0_f32;

        for part in parts {
            let part = part.trim();
            if let Some(value) = part.strip_prefix("q=") {
                quality = value.parse::<f32>().unwrap_or(0.0);
            }
        }

        match name {
            "br" => brotli_q = Some(quality),
            "gzip" => gzip_q = Some(quality),
            "*" => wildcard_q = Some(quality),
            _ => {}
        }
    }

    let brotli_q = brotli_q.or(wildcard_q).unwrap_or(0.0);
    let gzip_q = gzip_q.or(wildcard_q).unwrap_or(0.0);

    if brotli_q > 0.0 && asset.brotli.is_some() && brotli_q >= gzip_q {
        CompressionEncoding::Brotli
    } else if gzip_q > 0.0 && asset.gzip.is_some() {
        CompressionEncoding::Gzip
    } else {
        CompressionEncoding::Identity
    }
}

fn if_none_match_matches(value: Option<&HeaderValue>, etag: &str) -> bool {
    let Some(value) = value else {
        return false;
    };

    let Ok(value) = value.to_str() else {
        return false;
    };

    value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate == etag)
}

fn strong_etag(bytes: &[u8]) -> String {
    format!("\"{}\"", hex::encode(Sha256::digest(bytes)))
}

fn gzip_compress(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).ok()?;
    encoder.finish().ok()
}

fn brotli_compress(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut compressed = Vec::new();
    {
        let mut compressor = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
        compressor.write_all(bytes).ok()?;
    }
    Some(compressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::TempDir;

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
        server
            .serve(request.uri().path(), request.headers())
            .unwrap()
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
}
