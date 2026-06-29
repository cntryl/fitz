use crate::api::http::{Body, Response};
use hyper::header::{self, HeaderValue};
use hyper::StatusCode;

use super::model::{AssetEntry, AssetRepresentation};
use super::VARY_ACCEPT_ENCODING;

pub(super) fn response_for_asset(
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

pub(super) fn if_none_match_matches(value: Option<&HeaderValue>, etag: &str) -> bool {
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
