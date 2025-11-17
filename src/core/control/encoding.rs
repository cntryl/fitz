//! Control domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for control operations (heartbeat, shutdown, config updates).

use crate::protocol::tags::{TAG_BODY, TAG_ERR_MSG, TAG_ID, TAG_ROUTE};

/// Parsed TLV payload for control operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ControlTlvPayload {
    pub body: Option<Vec<u8>>,
    pub msg_id: Option<String>,
}

/// Parse TLV payload to extract body and optional message ID
#[allow(dead_code)]
pub fn parse_tlv_payload(payload: &[u8]) -> ControlTlvPayload {
    let mut body = None;
    let mut msg_id = None;
    let mut i = 0;

    while i < payload.len() {
        if i + 2 > payload.len() {
            break;
        }

        let tag = payload[i];
        let len = payload[i + 1] as usize;
        i += 2;

        if i + len > payload.len() {
            break;
        }

        let data = &payload[i..i + len];
        i += len;

        match tag {
            TAG_BODY => {
                body = Some(data.to_vec());
            }
            TAG_ID => {
                if let Ok(s) = std::str::from_utf8(data) {
                    msg_id = Some(s.to_string());
                }
            }
            _ => {} // Ignore unknown tags
        }
    }

    ControlTlvPayload { body, msg_id }
}

/// Build TLV-encoded response with route, optional message ID, and body
#[allow(dead_code)]
pub fn build_response(route: &str, msg_id: Option<&str>, body: &[u8]) -> Vec<u8> {
    let mut response = Vec::new();

    // TAG_ROUTE
    let route_bytes = route.as_bytes();
    response.push(TAG_ROUTE);
    response.push(route_bytes.len() as u8);
    response.extend_from_slice(route_bytes);

    // TAG_ID (if present)
    if let Some(id) = msg_id {
        let id_bytes = id.as_bytes();
        response.push(TAG_ID);
        response.push(id_bytes.len() as u8);
        response.extend_from_slice(id_bytes);
    }

    // TAG_BODY
    response.push(TAG_BODY);
    response.push(body.len() as u8);
    response.extend_from_slice(body);

    response
}

/// Build TLV-encoded error response
#[allow(dead_code)]
pub fn build_error_response(error_msg: &str) -> Vec<u8> {
    let mut response = Vec::new();

    response.push(TAG_ERR_MSG);
    let msg_bytes = error_msg.as_bytes();
    response.push(msg_bytes.len() as u8);
    response.extend_from_slice(msg_bytes);

    response
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_body_from_payload() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(11);
        payload.extend_from_slice(b"hello world");

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, Some(b"hello world".to_vec()));
        assert_eq!(parsed.msg_id, None);
    }

    #[test]
    fn should_parse_body_and_message_id() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(6);
        payload.extend_from_slice(b"ctrl-1");
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"data");

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, Some(b"data".to_vec()));
        assert_eq!(parsed.msg_id, Some("ctrl-1".to_string()));
    }

    #[test]
    fn should_parse_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, None);
        assert_eq!(parsed.msg_id, None);
    }

    #[test]
    fn should_build_response_without_message_id() {
        // Arrange
        let route = "control://heartbeat";
        let body = b"{\"status\":\"ok\"}";

        // Act
        let response = build_response(route, None, body);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        assert!(response.contains(&TAG_BODY));
        assert!(!response.contains(&TAG_ID));
    }

    #[test]
    fn should_build_response_with_message_id() {
        // Arrange
        let route = "control://shutdown";
        let msg_id = "shutdown-123";
        let body = b"{\"acknowledged\":true}";

        // Act
        let response = build_response(route, Some(msg_id), body);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        assert!(response.contains(&TAG_ID));
        assert!(response.contains(&TAG_BODY));

        // Verify message ID is in response
        let id_bytes = msg_id.as_bytes();
        let windows = response.windows(id_bytes.len());
        assert!(windows.into_iter().any(|w| w == id_bytes));
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error_msg = "Operation not supported";

        // Act
        let response = build_error_response(error_msg);

        // Assert
        assert_eq!(response[0], TAG_ERR_MSG);
        assert_eq!(response[1], error_msg.len() as u8);
        assert_eq!(&response[2..], error_msg.as_bytes());
    }

    #[test]
    fn should_handle_json_body() {
        // Arrange
        let json_body = br#"{"nodeId":"node-01","uptime":3600}"#;
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(json_body.len() as u8);
        payload.extend_from_slice(json_body);

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, Some(json_body.to_vec()));
    }

    #[test]
    fn should_handle_malformed_payload_gracefully() {
        // Arrange
        let payload = vec![TAG_BODY, 200]; // Claims 200 bytes but none follow

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, None); // Should not panic
    }

    #[test]
    fn should_ignore_unknown_tags() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(0xFF); // Unknown tag
        payload.push(3);
        payload.extend_from_slice(b"xyz");
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"test");

        // Act
        let parsed = parse_tlv_payload(&payload);

        // Assert
        assert_eq!(parsed.body, Some(b"test".to_vec()));
    }

    #[test]
    fn should_build_response_with_large_body() {
        // Arrange
        let route = "control://metrics";
        let body = vec![0xAB; 200];

        // Act
        let response = build_response(route, None, &body);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        // Should contain all body data
        let body_in_response = &response[response.len() - 200..];
        assert_eq!(body_in_response, &body[..]);
    }
}
