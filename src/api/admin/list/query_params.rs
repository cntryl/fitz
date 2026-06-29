use super::{
    DEFAULT_ADMIN_RECORD_LIMIT, DEFAULT_KV_SCAN_LIMIT, MAX_ADMIN_RECORD_LIMIT, MAX_KV_SCAN_LIMIT,
};
use base64::Engine;
use std::collections::HashMap;

pub fn parse_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect()
        })
        .unwrap_or_default()
}

/// Parses an optional unsigned integer query parameter.
///
/// # Errors
///
/// Returns an error when the parameter is present but not a valid `u64`.
pub fn parse_optional_u64_query_param(uri: &hyper::Uri, key: &str) -> Result<Option<u64>, String> {
    let params = parse_query_params(uri);
    match params.get(key) {
        Some(value) => value
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("Invalid {key} query parameter")),
        None => Ok(None),
    }
}

/// Parses the common `limit` query parameter with bounds.
///
/// # Errors
///
/// Returns an error when `limit` is present but invalid or zero.
pub fn parse_limit_query_param(
    uri: &hyper::Uri,
    default: usize,
    max: usize,
) -> Result<usize, String> {
    let params = parse_query_params(uri);
    match params.get("limit") {
        Some(value) => {
            let limit = value
                .parse::<usize>()
                .map_err(|_| "Invalid limit query parameter".to_string())?;
            if limit == 0 {
                Err("limit query parameter must be greater than zero".to_string())
            } else {
                Ok(limit.min(max))
            }
        }
        None => Ok(default.min(max)),
    }
}

/// Parses a required KV key-style query parameter as bytes.
///
/// # Errors
///
/// Returns an error when the parameter is missing, the encoding is invalid, or
/// base64 decoding fails.
pub fn parse_kv_query_bytes(uri: &hyper::Uri, key: &str) -> Result<Vec<u8>, String> {
    let params = parse_query_params(uri);
    let value = params
        .get(key)
        .ok_or_else(|| format!("Missing {key} query parameter"))?;
    let encoding = params.get("key_encoding").map_or("utf8", String::as_str);

    match encoding {
        "utf8" => Ok(value.as_bytes().to_vec()),
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(value)
            .map_err(|_| format!("Invalid base64 {key} query parameter")),
        _ => Err("Invalid key_encoding query parameter".to_string()),
    }
}

/// Parses an optional KV key-style query parameter as bytes.
///
/// # Errors
///
/// Returns an error when the encoding is invalid or base64 decoding fails.
pub fn parse_optional_kv_query_bytes(
    uri: &hyper::Uri,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    let params = parse_query_params(uri);
    let Some(value) = params.get(key) else {
        return Ok(None);
    };
    let encoding = params.get("key_encoding").map_or("utf8", String::as_str);

    match encoding {
        "utf8" => Ok(Some(value.as_bytes().to_vec())),
        "base64" => base64::engine::general_purpose::STANDARD
            .decode(value)
            .map(Some)
            .map_err(|_| format!("Invalid base64 {key} query parameter")),
        _ => Err("Invalid key_encoding query parameter".to_string()),
    }
}

/// Parses the optional KV scan cursor.
///
/// # Errors
///
/// Returns an error when the cursor is present but not valid base64.
pub fn parse_optional_kv_cursor(uri: &hyper::Uri) -> Result<Option<Vec<u8>>, String> {
    let params = parse_query_params(uri);
    let Some(value) = params.get("cursor") else {
        return Ok(None);
    };

    base64::engine::general_purpose::STANDARD
        .decode(value)
        .map(Some)
        .map_err(|_| "Invalid cursor query parameter".to_string())
}

/// Parses the bounded KV scan limit.
///
/// # Errors
///
/// Returns an error when `limit` is present but invalid or zero.
pub fn parse_kv_scan_limit(uri: &hyper::Uri) -> Result<usize, String> {
    parse_limit_query_param(uri, DEFAULT_KV_SCAN_LIMIT, MAX_KV_SCAN_LIMIT)
}

/// Parses the bounded admin record limit.
///
/// # Errors
///
/// Returns an error when `limit` is present but invalid or zero.
pub fn parse_admin_record_limit(uri: &hyper::Uri) -> Result<usize, String> {
    parse_limit_query_param(uri, DEFAULT_ADMIN_RECORD_LIMIT, MAX_ADMIN_RECORD_LIMIT)
}

pub fn parse_optional_string_query_param(uri: &hyper::Uri, key: &str) -> Option<String> {
    parse_query_params(uri)
        .get(key)
        .cloned()
        .filter(|value| !value.is_empty())
}
