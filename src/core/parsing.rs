// Domain utilities for common TLV parsing, response building, and operation routing patterns
//
// This module provides standardized utilities to keep domain implementations
// clean, idiomatic, and DRY (Don't Repeat Yourself).

use crate::protocol::frame::{build_tlv, find_tlv};
use crate::protocol::tags::*;

/// Parse segments from a Fitz route string.
///
/// All domain routes follow the pattern: `scheme://realm/area/resource[/operation]`
/// This function extracts the segments after the `://` delimiter.
///
/// Returns `(realm, area, resource)` where each is a borrowed string slice.
///
/// # Examples
/// ```
/// use fitz::core::parsing::parse_route_segments;
///
/// let (realm, area, resource) = parse_route_segments("lease://r1/a1/res1").unwrap();
/// assert_eq!(realm, "r1");
/// assert_eq!(area, "a1");
/// assert_eq!(resource, "res1");
/// ```
#[inline]
pub fn parse_route_segments(route: &str) -> Result<(&str, &str, &str), String> {
    // Fast path: locate "://"
    let bytes = route.as_bytes();
    let mut i = 0;

    // Find "://"
    while i + 2 < bytes.len() {
        if bytes[i] == b':' && bytes[i + 1] == b'/' && bytes[i + 2] == b'/' {
            i += 3; // skip past "://"
            break;
        }
        i += 1;
    }

    if i == 0 || i + 2 >= bytes.len() {
        return Err("invalid_route".into());
    }

    // Now parse: realm/area/resource
    let start_realm = i;

    // realm
    while i < bytes.len() && bytes[i] != b'/' {
        i += 1;
    }
    if i == bytes.len() {
        return Err("missing_area".into());
    }
    let realm = &route[start_realm..i];
    i += 1;

    // area
    let start_area = i;
    while i < bytes.len() && bytes[i] != b'/' {
        i += 1;
    }
    if i == bytes.len() {
        return Err("missing_resource".into());
    }
    let area = &route[start_area..i];
    i += 1;

    // resource
    let start_res = i;
    while i < bytes.len() && bytes[i] != b'/' {
        i += 1;
    }
    // We intentionally don't care if operation exists
    let resource = &route[start_res..i];

    if realm.is_empty() || area.is_empty() || resource.is_empty() {
        return Err("empty_segment".into());
    }

    Ok((realm, area, resource))
}

/// Common TLV parsing utilities for domain handlers
pub mod tlv {
    use super::*;

    /// Parse a UTF-8 string from a TLV tag
    pub fn parse_string(payload: &[u8], tag: u8) -> Option<&str> {
        find_tlv(payload, tag).and_then(|b| std::str::from_utf8(b).ok())
    }

    /// Parse a String (owned) from a TLV tag
    pub fn parse_string_owned(payload: &[u8], tag: u8) -> Option<String> {
        parse_string(payload, tag).map(|s| s.to_string())
    }

    /// Parse bytes from a TLV tag
    pub fn parse_bytes(payload: &[u8], tag: u8) -> Option<&[u8]> {
        find_tlv(payload, tag)
    }

    /// Parse bytes (owned) from a TLV tag
    pub fn parse_bytes_owned(payload: &[u8], tag: u8) -> Option<Vec<u8>> {
        find_tlv(payload, tag).map(|b| b.to_vec())
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

    /// Parse all occurrences of a tag (for repeated tags)
    pub fn parse_all_bytes(payload: &[u8], tag: u8) -> Vec<Vec<u8>> {
        let mut results = Vec::new();
        let mut offset = 0;

        while offset + 2 <= payload.len() {
            let current_tag = payload[offset];
            let len_byte = payload[offset + 1];
            let mut value_start = offset + 2;

            // Handle extended length encoding
            let len = if len_byte == 255 {
                if value_start + 4 > payload.len() {
                    break;
                }
                let len_bytes = [
                    payload[value_start],
                    payload[value_start + 1],
                    payload[value_start + 2],
                    payload[value_start + 3],
                ];
                value_start += 4;
                u32::from_be_bytes(len_bytes) as usize
            } else {
                len_byte as usize
            };

            if value_start + len > payload.len() {
                break;
            }

            if current_tag == tag {
                results.push(payload[value_start..value_start + len].to_vec());
            }

            offset = value_start + len;
        }

        results
    }
}

/// Common response building utilities for domain handlers
pub mod response {
    use super::*;

    /// ResponseBuilder provides a fluent API for building TLV responses
    /// Avoids repetitive code in domain handlers
    pub struct ResponseBuilder {
        buffer: Vec<u8>,
    }

    impl ResponseBuilder {
        /// Create a new response builder
        pub fn new() -> Self {
            Self { buffer: Vec::new() }
        }

        /// Create with pre-allocated capacity
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                buffer: Vec::with_capacity(capacity),
            }
        }

        /// Add a string tag
        pub fn add_string(mut self, tag: u8, value: &str) -> Self {
            build_tlv(tag, value.as_bytes(), &mut self.buffer);
            self
        }

        /// Add a bytes tag
        pub fn add_bytes(mut self, tag: u8, value: &[u8]) -> Self {
            build_tlv(tag, value, &mut self.buffer);
            self
        }

        /// Add a u32 tag (big-endian)
        pub fn add_u32(mut self, tag: u8, value: u32) -> Self {
            build_tlv(tag, &value.to_be_bytes(), &mut self.buffer);
            self
        }

        /// Add a u64 tag (big-endian)
        pub fn add_u64(mut self, tag: u8, value: u64) -> Self {
            build_tlv(tag, &value.to_be_bytes(), &mut self.buffer);
            self
        }

        /// Add a boolean flag tag (no value)
        pub fn add_flag(mut self, tag: u8) -> Self {
            build_tlv(tag, &[], &mut self.buffer);
            self
        }

        /// Add an optional string tag
        pub fn add_optional_string(self, tag: u8, value: Option<&str>) -> Self {
            if let Some(v) = value {
                self.add_string(tag, v)
            } else {
                self
            }
        }

        /// Add an optional bytes tag
        pub fn add_optional_bytes(self, tag: u8, value: Option<&[u8]>) -> Self {
            if let Some(v) = value {
                self.add_bytes(tag, v)
            } else {
                self
            }
        }

        /// Add an optional u32 tag
        pub fn add_optional_u32(self, tag: u8, value: Option<u32>) -> Self {
            if let Some(v) = value {
                self.add_u32(tag, v)
            } else {
                self
            }
        }

        /// Add an optional u64 tag
        pub fn add_optional_u64(self, tag: u8, value: Option<u64>) -> Self {
            if let Some(v) = value {
                self.add_u64(tag, v)
            } else {
                self
            }
        }

        /// Build the final response and consume the builder
        pub fn build(self) -> Vec<u8> {
            self.buffer
        }

        /// Build into a PooledFrame
        pub fn build_frame(self) -> crate::protocol::frame::PooledFrame {
            crate::protocol::frame::PooledFrame::from_vec(self.buffer)
        }
    }

    impl Default for ResponseBuilder {
        fn default() -> Self {
            Self::new()
        }
    }

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
    fn should_parse_route_segments_correctly() {
        // Arrange
        let route = "lease://realm1/area1/resource1";

        // Act
        let result = parse_route_segments(route);

        // Assert
        assert!(result.is_ok());
        let (realm, area, resource) = result.unwrap();
        assert_eq!(realm, "realm1");
        assert_eq!(area, "area1");
        assert_eq!(resource, "resource1");
    }

    #[test]
    fn should_parse_route_segments_with_operation() {
        // Arrange
        let route = "kv://realm1/area1/resource1/get";

        // Act
        let result = parse_route_segments(route);

        // Assert
        assert!(result.is_ok());
        let (realm, area, resource) = result.unwrap();
        assert_eq!(realm, "realm1");
        assert_eq!(area, "area1");
        assert_eq!(resource, "resource1"); // operation is ignored
    }

    #[test]
    fn should_reject_route_without_scheme() {
        // Arrange
        let route = "realm1/area1/resource1";

        // Act
        let result = parse_route_segments(route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "invalid_route");
    }

    #[test]
    fn should_reject_route_missing_segments() {
        // Arrange
        let route = "lease://realm1/area1";

        // Act
        let result = parse_route_segments(route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "missing_resource");
    }

    #[test]
    fn should_reject_route_with_empty_segments() {
        // Arrange
        let route = "lease://realm1//resource1";

        // Act
        let result = parse_route_segments(route);

        // Assert
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "empty_segment");
    }

    #[test]
    fn should_parse_string_from_tlv() {
        // Arrange
        let mut payload = vec![];
        build_tlv(TAG_ID, b"test-id", &mut payload);

        // Act
        let result = tlv::parse_string(&payload, TAG_ID);

        // Assert
        assert_eq!(result, Some("test-id"));
    }

    #[test]
    fn should_parse_u32_from_tlv() {
        // Arrange
        let mut payload = vec![];
        build_tlv(TAG_LEASE, &123u32.to_be_bytes(), &mut payload);

        // Act
        let result = tlv::parse_u32(&payload, TAG_LEASE);

        // Assert
        assert_eq!(result, Some(123));
    }

    #[test]
    fn should_build_success_response() {
        // Arrange
        let body = b"test body";

        // Act
        let response = response::success(Some(body));

        // Assert
        let parsed = find_tlv(&response, TAG_BODY);
        assert_eq!(parsed, Some(body as &[u8]));
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let message = "test error";

        // Act
        let response = response::error(message);

        // Assert
        let parsed = tlv::parse_string(&response, TAG_ERR_MSG);
        assert_eq!(parsed, Some(message));
    }
}
