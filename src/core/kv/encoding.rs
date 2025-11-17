//! KV domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for KV operations.

use crate::protocol::tags::{TAG_BODY, TAG_ERR_MSG, TAG_ID};

/// Parsed TLV payload for KV operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct KvTlvPayload {
    pub key: Option<String>,
    pub value: Option<Vec<u8>>,
}

/// Parse TLV body to extract key (TAG_ID) and value (TAG_BODY)
/// Supports extended length encoding (255 = 4-byte length follows)
#[allow(dead_code)]
pub fn parse_tlv_payload(body: &[u8]) -> KvTlvPayload {
    let mut key = None;
    let mut value = None;
    let mut i = 0;

    while i < body.len() {
        if i + 2 > body.len() {
            break;
        }

        let tag = body[i];
        let len_byte = body[i + 1];
        i += 2;

        // Handle extended length encoding
        let len = if len_byte == 255 {
            // Extended length: next 4 bytes contain the actual length
            if i + 4 > body.len() {
                break;
            }
            let len_bytes = [body[i], body[i + 1], body[i + 2], body[i + 3]];
            i += 4;
            u32::from_be_bytes(len_bytes) as usize
        } else {
            len_byte as usize
        };

        if i + len > body.len() {
            break;
        }

        let data = &body[i..i + len];
        i += len;

        match tag {
            TAG_ID => {
                if let Ok(s) = String::from_utf8(data.to_vec()) {
                    key = Some(s);
                }
            }
            TAG_BODY => {
                value = Some(data.to_vec());
            }
            _ => {} // Ignore unknown tags
        }
    }

    KvTlvPayload { key, value }
}

/// Build TLV response with body or error
/// For larger bodies, uses extended length encoding (255 = 4-byte length follows)
#[allow(dead_code)]
pub fn build_tlv_response(result: Result<Option<Vec<u8>>, String>) -> Vec<u8> {
    let mut response = Vec::new();

    match result {
        Ok(Some(body)) => {
            // Success with body - handle extended length encoding
            response.push(TAG_BODY);

            if body.len() <= 254 {
                // Single byte length for small bodies
                response.push(body.len() as u8);
                response.extend_from_slice(&body);
            } else {
                // Extended TLV encoding for larger bodies
                // Use 255 as marker for extended length, followed by 4-byte length
                response.push(255);
                let len = body.len() as u32;
                response.extend_from_slice(&len.to_be_bytes());
                response.extend_from_slice(&body);
            }
        }
        Ok(None) => {
            // Success with no body
            response.push(TAG_BODY);
            response.push(0);
        }
        Err(err_msg) => {
            // Error
            response.push(TAG_ERR_MSG);
            let msg_bytes = err_msg.as_bytes();
            if msg_bytes.len() <= 254 {
                response.push(msg_bytes.len() as u8);
                response.extend_from_slice(msg_bytes);
            } else {
                // Extended TLV encoding for longer error messages
                response.push(255);
                let len = msg_bytes.len().min(65535) as u32; // Cap at 64KB
                response.extend_from_slice(&len.to_be_bytes());
                response.extend_from_slice(&msg_bytes[..len as usize]);
            }
        }
    }

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_key_and_value_from_tlv() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(4);
        payload.extend_from_slice(b"test");
        payload.push(TAG_BODY);
        payload.push(5);
        payload.extend_from_slice(b"hello");

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.key, Some("test".to_string()));
        assert_eq!(parsed.value, Some(b"hello".to_vec()));
    }

    #[test]
    fn should_parse_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.key, None);
        assert_eq!(parsed.value, None);
    }

    #[test]
    fn should_parse_extended_length_encoding() {
        // Arrange
        let large_value = vec![0u8; 300];
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(255); // Extended length marker
        payload.extend_from_slice(&(300u32).to_be_bytes());
        payload.extend_from_slice(&large_value);

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.value, Some(large_value));
    }

    #[test]
    fn should_build_success_response_with_body() {
        // Arrange
        let body = b"test_value".to_vec();

        // Act
        let response = build_tlv_response(Ok(Some(body.clone())));

        // Assert
        assert_eq!(response[0], TAG_BODY);
        assert_eq!(response[1], 10); // length
        assert_eq!(&response[2..], &body[..]);
    }

    #[test]
    fn should_build_success_response_without_body() {
        // Arrange
        let input = Ok(None);

        // Act
        let response = build_tlv_response(input);

        // Assert
        assert_eq!(response[0], TAG_BODY);
        assert_eq!(response[1], 0); // empty length
        assert_eq!(response.len(), 2);
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error = "Not found";

        // Act
        let response = build_tlv_response(Err(error.to_string()));

        // Assert
        assert_eq!(response[0], TAG_ERR_MSG);
        assert_eq!(response[1], 9); // length of "Not found"
        assert_eq!(&response[2..], b"Not found");
    }

    #[test]
    fn should_build_extended_length_response() {
        // Arrange
        let large_body = vec![0xAB; 300];

        // Act
        let response = build_tlv_response(Ok(Some(large_body.clone())));

        // Assert
        assert_eq!(response[0], TAG_BODY);
        assert_eq!(response[1], 255); // Extended length marker
        assert_eq!(
            u32::from_be_bytes([response[2], response[3], response[4], response[5]]),
            300
        );
        assert_eq!(&response[6..], &large_body[..]);
    }

    #[test]
    fn should_handle_malformed_tlv_gracefully() {
        // Arrange
        let payload = vec![TAG_ID, 100]; // Claims 100 bytes but no data follows

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.key, None);
        assert_eq!(parsed.value, None);
    }
}
