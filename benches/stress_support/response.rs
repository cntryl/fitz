//! Common response envelope handling for domain benchmark drivers.

use super::types::BenchFailure;
use fitz::protocol::payload_codec::PayloadDecoder;

pub(super) fn require_ok(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    if body.first() == Some(&0) {
        return Ok(());
    }
    let (code, message) = fitz::protocol::error_codes::decode_error_body(body)
        .map_err(|error| malformed_error(body, operation, &error))?;
    Err(BenchFailure::domain_error(format!(
        "{operation} failed: code {code}: {message}"
    )))
}

pub(super) fn error_code(body: &[u8], operation: &str) -> Result<u32, BenchFailure> {
    fitz::protocol::error_codes::decode_error_body(body)
        .map(|(code, _)| u32::from(code))
        .map_err(|error| malformed_error(body, operation, &error))
}

fn malformed_error(body: &[u8], operation: &str, error: &str) -> BenchFailure {
    BenchFailure::validation(format!(
        "malformed {operation} error response ({} bytes, prefix {:02X?}): {error}",
        body.len(),
        &body[..body.len().min(128)]
    ))
}

pub(super) fn complete(decoder: &PayloadDecoder<'_>) -> Result<(), BenchFailure> {
    if decoder.is_complete() {
        Ok(())
    } else {
        Err(BenchFailure::validation("trailing response payload bytes"))
    }
}

pub(super) fn empty_success(body: &[u8], operation: &str) -> Result<(), BenchFailure> {
    require_ok(body, operation)?;
    if body == [0] {
        Ok(())
    } else {
        Err(BenchFailure::validation("expected only the success status"))
    }
}
