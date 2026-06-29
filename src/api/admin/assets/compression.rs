use hyper::header::{self, HeaderMap};
use std::io::Write;

use super::model::AssetEntry;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum CompressionEncoding {
    Identity,
    Gzip,
    Brotli,
}

pub(super) fn is_compressible(path: &str, content_type: &str) -> bool {
    content_type.starts_with("text/")
        || matches!(
            content_type,
            "application/javascript; charset=utf-8" | "application/json" | "image/svg+xml"
        )
        || path.ends_with(".map")
}

pub(super) fn preferred_encoding(headers: &HeaderMap, asset: &AssetEntry) -> CompressionEncoding {
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

pub(super) fn gzip_compress(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).ok()?;
    encoder.finish().ok()
}

pub(super) fn brotli_compress(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut compressed = Vec::new();
    {
        let mut compressor = brotli::CompressorWriter::new(&mut compressed, 4096, 5, 22);
        compressor.write_all(bytes).ok()?;
    }
    Some(compressed)
}
