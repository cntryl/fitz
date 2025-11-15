// Domain utilities for common TLV parsing, response building, and operation routing patterns
//
// This module provides standardized utilities to keep domain implementations
// clean, idiomatic, and DRY (Don't Repeat Yourself).

use crate::protocol::frame::{build_tlv, find_tlv};
use crate::protocol::tags::*;

/// Common TLV parsing utilities for domain handlers
pub mod tlv {
    use super::*;

    /// Parse a UTF-8 string from a TLV tag
    pub fn parse_string(payload: &[u8], tag: u8) -> Option<&str> {
        find_tlv(payload, tag).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Parse a u32 from a TLV tag (big-endian)
    pub fn parse_u32(payload: &[u8], tag: u8) -> Option<u32> {
        find_tlv(payload, tag).and_then(|b| {
            if b.len() == 4 {
                Some(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
            } else {
                None
            }
        })
    }

    /// Parse a u64 from a TLV tag (big-endian)
    pub fn parse_u64(payload: &[u8], tag: u8) -> Option<u64> {
        find_tlv(payload, tag).and_then(|b| {
            if b.len() == 8 {
                Some(u64::from_be_bytes([
                    b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                ]))
            } else {
                None
            }
        })
    }

    /// Parse a byte vector from a TLV tag
    pub fn parse_bytes(payload: &[u8], tag: u8) -> Option<Vec<u8>> {
        find_tlv(payload, tag).map(|b| b.to_vec())
    }

    /// Parse multiple TLV values in a single pass, returning a result struct
    /// This is more efficient than multiple find_tlv calls for the same payload
    pub fn parse_multi<'a, F, R>(payload: &'a [u8], parser: F) -> R
    where
        F: FnOnce(&'a [u8]) -> R,
    {
        parser(payload)
    }

    /// Check if a TLV tag is present (without extracting value)
    pub fn has_tag(payload: &[u8], tag: u8) -> bool {
        find_tlv(payload, tag).is_some()
    }
}

/// Common response building utilities for domain handlers
pub mod response {
    use super::*;

    /// Build a simple success response with optional body
    pub fn success(body: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(body) = body {
            build_tlv(TAG_BODY, body, &mut out);
        }
        out
    }

    /// Build an error response
    pub fn error(message: &str) -> Vec<u8> {
        let mut out = Vec::new();
        build_tlv(TAG_ERR_MSG, message.as_bytes(), &mut out);
        out
    }

    /// Build a response with ID and body
    pub fn with_id(id: &str, body: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        build_tlv(TAG_ID, id.as_bytes(), &mut out);
        if let Some(body) = body {
            build_tlv(TAG_BODY, body, &mut out);
        }
        out
    }

    /// Build a response with route and optional body
    pub fn with_route(route: &str, body: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        build_tlv(TAG_ROUTE, route.as_bytes(), &mut out);
        if let Some(body) = body {
            build_tlv(TAG_BODY, body, &mut out);
        }
        out
    }

    /// Build a response with sequence number
    pub fn with_sequence(seq: u64, body: Option<&[u8]>) -> Vec<u8> {
        let mut out = Vec::new();
        build_tlv(TAG_SEQ, &seq.to_be_bytes(), &mut out);
        if let Some(body) = body {
            build_tlv(TAG_BODY, body, &mut out);
        }
        out
    }
}

/// Operation routing utilities for consistent operation determination
pub mod routing {
    use crate::protocol::route::Route;

    /// Trait for operation types that can be parsed from routes
    pub trait OperationFromRoute {
        fn from_route(route: &Route) -> Result<Self, String>
        where
            Self: Sized;
    }

    /// Helper to extract operation from route with consistent error handling
    pub fn extract_operation<T: OperationFromRoute>(route: &Route) -> Result<T, String> {
        T::from_route(route)
    }

    /// Helper to extract operation from route string with fallback
    pub fn extract_operation_string<'a>(route: &'a Route, default: &'a str) -> &'a str {
        route.operation.as_deref().unwrap_or(default)
    }
}

/// Common validation utilities
pub mod validation {
    use crate::protocol::frame::find_tlv;

    /// Validate that required TLV tags are present
    pub fn require_tags(payload: &[u8], tags: &[u8]) -> Result<(), String> {
        for &tag in tags {
            if find_tlv(payload, tag).is_none() {
                return Err(format!("Missing required TLV tag: {}", tag));
            }
        }
        Ok(())
    }

    /// Validate string length
    pub fn validate_string_len(s: &str, max_len: usize, field: &str) -> Result<(), String> {
        if s.len() > max_len {
            return Err(format!("{} too long: {} > {}", field, s.len(), max_len));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_string_from_tlv() {
        let mut payload = vec![];
        build_tlv(TAG_ID, b"test-id", &mut payload);

        let result = tlv::parse_string(&payload, TAG_ID);
        assert_eq!(result, Some("test-id"));
    }

    #[test]
    fn should_parse_u32_from_tlv() {
        let mut payload = vec![];
        build_tlv(TAG_LEASE, &123u32.to_be_bytes(), &mut payload);

        let result = tlv::parse_u32(&payload, TAG_LEASE);
        assert_eq!(result, Some(123));
    }

    #[test]
    fn should_build_success_response() {
        let body = b"test body";
        let response = response::success(Some(body));

        let parsed = tlv::parse_bytes(&response, TAG_BODY);
        assert_eq!(parsed, Some(body.to_vec()));
    }

    #[test]
    fn should_build_error_response() {
        let message = "test error";
        let response = response::error(message);

        let parsed = tlv::parse_string(&response, TAG_ERR_MSG);
        assert_eq!(parsed, Some(message));
    }
}