//! Embedded admin UI asset serving.
//!
//! This module owns the production static-asset contract for the admin SPA,
//! including lookup, MIME mapping, SPA fallback, compression negotiation, and
//! ETag handling. The request handler delegates here so the rest of the admin
//! API remains unchanged.

use bytes::Bytes;
use hyper::header::{self, HeaderMap, HeaderValue};
use hyper::{Body, Request, Response, StatusCode};
use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::convert::Infallible;
use std::io::Write;

const CACHE_CONTROL: &str = "public, max-age=3600";
const INDEX_PATH: &str = "index.html";
const VARY_ACCEPT_ENCODING: &str = "Accept-Encoding";

#[derive(Clone, Copy)]
struct EmbeddedAssetSource {
    path: &'static str,
    bytes: &'static [u8],
}

impl EmbeddedAssetSource {
    const fn new(path: &'static str, bytes: &'static [u8]) -> Self {
        Self { path, bytes }
    }
}

include!(concat!(env!("OUT_DIR"), "/embedded_assets.rs"));

static ASSET_INDEX: Lazy<AssetIndex> = Lazy::new(|| {
    AssetIndex::from_sources(EMBEDDED_ASSET_SOURCES.iter().map(|source| AssetSource {
        path: source.path,
        bytes: source.bytes,
    }))
});

pub(crate) fn serve_request(req: &Request<Body>) -> Result<Response<Body>, Infallible> {
    ASSET_INDEX.serve(req.uri().path(), req.headers())
}

#[derive(Clone, Copy)]
struct AssetSource<'a> {
    path: &'a str,
    bytes: &'a [u8],
}

#[derive(Clone)]
struct AssetRepresentation {
    body: Bytes,
    etag: String,
    content_encoding: Option<&'static str>,
}

impl AssetRepresentation {
    fn from_bytes(bytes: &[u8], content_encoding: Option<&'static str>) -> Self {
        Self {
            body: Bytes::copy_from_slice(bytes),
            etag: strong_etag(bytes),
            content_encoding,
        }
    }

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
    fn new(path: &str, bytes: &[u8]) -> Self {
        let content_type = content_type_for_path(path);
        let identity = AssetRepresentation::from_bytes(bytes, None);
        let (gzip, brotli) = if is_compressible(path, content_type) {
            let gzip = gzip_compress(bytes)
                .filter(|compressed| compressed.len() < bytes.len())
                .map(|compressed| AssetRepresentation::from_vec(compressed, Some("gzip")));
            let brotli = brotli_compress(bytes)
                .filter(|compressed| compressed.len() < bytes.len())
                .map(|compressed| AssetRepresentation::from_vec(compressed, Some("br")));
            (gzip, brotli)
        } else {
            (None, None)
        };

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

struct AssetIndex {
    assets: HashMap<String, AssetEntry>,
}

impl AssetIndex {
    fn from_sources<'a, I>(sources: I) -> Self
    where
        I: IntoIterator<Item = AssetSource<'a>>,
    {
        let mut assets = HashMap::new();

        for source in sources {
            let path = normalize_embedded_path(source.path);
            if path.is_empty() {
                continue;
            }

            assets.insert(path.clone(), AssetEntry::new(&path, source.bytes));
        }

        Self { assets }
    }

    fn serve(&self, path: &str, headers: &HeaderMap) -> Result<Response<Body>, Infallible> {
        let Some(resolved_path) = self.resolve_path(path) else {
            return Ok(super::not_found());
        };

        let Some(asset) = self.assets.get(resolved_path.as_str()) else {
            return Ok(super::not_found());
        };

        let representation = asset.select_representation(headers);
        if if_none_match_matches(headers.get(header::IF_NONE_MATCH), &representation.etag) {
            return Ok(response_for_asset(
                StatusCode::NOT_MODIFIED,
                asset,
                representation,
                true,
            ));
        }

        Ok(response_for_asset(
            StatusCode::OK,
            asset,
            representation,
            false,
        ))
    }

    fn resolve_path(&self, path: &str) -> Option<String> {
        let normalized_path = normalize_request_path(path)?;
        if self.assets.contains_key(normalized_path.as_str()) {
            return Some(normalized_path);
        }

        self.assets
            .contains_key(INDEX_PATH)
            .then(|| INDEX_PATH.to_string())
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
) -> Response<Body> {
    let mut builder = Response::builder()
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
            Body::empty()
        } else {
            Body::from(representation.body.clone())
        })
        .unwrap()
}

fn normalize_embedded_path(path: &str) -> String {
    path.trim_start_matches('/').replace('\\', "/")
}

fn normalize_request_path(path: &str) -> Option<String> {
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() || trimmed == "/" {
        return Some(INDEX_PATH.to_string());
    }

    let mut normalized = Vec::new();
    for component in trimmed.split('/') {
        if component == "." || component == ".." {
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
    use hyper::body;

    fn test_index() -> AssetIndex {
        AssetIndex::from_sources([
            AssetSource {
                path: "index.html",
                bytes: b"<!doctype html><html><body><script src=\"/assets/app.js\"></script></body></html>",
            },
            AssetSource {
                path: "assets/app.js",
                bytes: b"console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');",
            },
            AssetSource {
                path: "favicon.svg",
                bytes: b"<svg></svg>",
            },
            AssetSource {
                path: "logo.png",
                bytes: &[137, 80, 78, 71, 13, 10, 26, 10, 0, 1, 2, 3, 4, 5],
            },
        ])
    }

    async fn serve(
        index: &AssetIndex,
        path: &str,
        accept_encoding: Option<&str>,
        if_none_match: Option<&str>,
    ) -> Response<Body> {
        let mut request = Request::builder().uri(path);
        if let Some(accept_encoding) = accept_encoding {
            request = request.header(header::ACCEPT_ENCODING, accept_encoding);
        }
        if let Some(if_none_match) = if_none_match {
            request = request.header(header::IF_NONE_MATCH, if_none_match);
        }

        let request = request.body(Body::empty()).unwrap();
        index.serve(request.uri().path(), request.headers()).unwrap()
    }

    #[tokio::test]
    async fn should_lookup_asset_by_exact_path() {
        let response = serve(&test_index(), "/assets/app.js", None, None).await;
        let body = body::to_bytes(response.into_body()).await.unwrap();

        assert_eq!(
            body.as_ref(),
            b"console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');console.log('fitz');"
        );
    }

    #[tokio::test]
    async fn should_apply_svg_mime_type() {
        let response = serve(&test_index(), "/favicon.svg", None, None).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE).unwrap(),
            "image/svg+xml"
        );
    }

    #[tokio::test]
    async fn should_fallback_to_index_for_client_routes() {
        let response = serve(&test_index(), "/sessions/123", None, None).await;
        let status = response.status();
        let body = body::to_bytes(response.into_body()).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(std::str::from_utf8(&body).unwrap().contains("<!doctype html>"));
    }

    #[tokio::test]
    async fn should_preserve_missing_asset_fallback_behavior() {
        let response = serve(&test_index(), "/assets/missing.js", None, None).await;
        let status = response.status();
        let content_type = response.headers().get(header::CONTENT_TYPE).unwrap().clone();
        let body = body::to_bytes(response.into_body()).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert_eq!(content_type, "text/html; charset=utf-8");
        assert!(std::str::from_utf8(&body).unwrap().contains("<!doctype html>"));
    }

    #[tokio::test]
    async fn should_preserve_missing_root_file_fallback_behavior() {
        let response = serve(&test_index(), "/missing.css", None, None).await;
        let status = response.status();
        let body = body::to_bytes(response.into_body()).await.unwrap();

        assert_eq!(status, StatusCode::OK);
        assert!(std::str::from_utf8(&body).unwrap().contains("<!doctype html>"));
    }

    #[tokio::test]
    async fn should_reject_path_traversal() {
        let response = serve(&test_index(), "/../secret", None, None).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn should_prefer_brotli_when_supported() {
        let response = serve(&test_index(), "/assets/app.js", Some("gzip, br"), None).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "br"
        );
        assert_eq!(response.headers().get(header::VARY).unwrap(), "Accept-Encoding");
    }

    #[tokio::test]
    async fn should_skip_compression_for_non_compressible_assets() {
        let response = serve(&test_index(), "/logo.png", Some("br, gzip"), None).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert!(response.headers().get(header::CONTENT_ENCODING).is_none());
        assert_eq!(response.headers().get(header::CONTENT_TYPE).unwrap(), "image/png");
    }

    #[tokio::test]
    async fn should_return_not_modified_when_etag_matches_representation() {
        let initial = serve(&test_index(), "/assets/app.js", Some("gzip"), None).await;
        let etag = initial.headers().get(header::ETAG).unwrap().to_str().unwrap().to_string();

        let response = serve(&test_index(), "/assets/app.js", Some("gzip"), Some(&etag)).await;

        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(
            response.headers().get(header::CONTENT_ENCODING).unwrap(),
            "gzip"
        );
        assert_eq!(
            body::to_bytes(response.into_body()).await.unwrap().len(),
            0
        );
    }
}