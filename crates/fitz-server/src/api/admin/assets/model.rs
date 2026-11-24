use bytes::Bytes;
use hyper::HeaderMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;

use super::cache::strong_etag;
use super::compression::{
    brotli_compress, gzip_compress, is_compressible, preferred_encoding, CompressionEncoding,
};
use super::{CACHE_CONTROL, HTML_CACHE_CONTROL};

#[derive(Clone)]
pub(super) struct AssetRepresentation {
    pub(super) body: Bytes,
    pub(super) etag: String,
    pub(super) content_encoding: Option<&'static str>,
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

pub(super) struct AssetEntry {
    pub(super) content_type: &'static str,
    pub(super) cache_control: &'static str,
    pub(super) identity: AssetRepresentation,
    pub(super) gzip: Option<AssetRepresentation>,
    pub(super) brotli: Option<AssetRepresentation>,
}

impl AssetEntry {
    pub(super) fn new(path: &str, bytes: Vec<u8>) -> Self {
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
            cache_control: if content_type == "text/html; charset=utf-8" {
                HTML_CACHE_CONTROL
            } else {
                CACHE_CONTROL
            },
            identity,
            gzip,
            brotli,
        }
    }

    pub(super) fn select_representation(&self, headers: &HeaderMap) -> &AssetRepresentation {
        match preferred_encoding(headers, self) {
            CompressionEncoding::Brotli => self.brotli.as_ref().unwrap_or(&self.identity),
            CompressionEncoding::Gzip => self.gzip.as_ref().unwrap_or(&self.identity),
            CompressionEncoding::Identity => &self.identity,
        }
    }
}

#[derive(Clone)]
pub(super) struct CachedAssetEntry {
    pub(super) fingerprint: AssetFingerprint,
    pub(super) entry: Arc<AssetEntry>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) struct AssetFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}

impl AssetFingerprint {
    pub(super) fn from_metadata(metadata: &fs::Metadata) -> Self {
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
        }
    }
}

pub(super) struct ResolvedAsset {
    pub(super) relative_path: String,
    pub(super) absolute_path: PathBuf,
    pub(super) fingerprint: AssetFingerprint,
}

pub(super) enum AssetResolution {
    Found(ResolvedAsset),
    Missing,
    Unsafe,
}

fn content_type_for_path(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("js") => "application/javascript; charset=utf-8",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("svg") => "image/svg+xml",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("ttf") => "font/ttf",
        _ => "application/octet-stream",
    }
}
