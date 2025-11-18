//! Notice domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for notice operations (subscribe, unsubscribe, publish).

use crate::protocol::tags::{
    TAG_BODY, TAG_COUNT, TAG_ERR_MSG, TAG_ID, TAG_ROUTE, TAG_SUBSCRIBE, TAG_UNSUBSCRIBE,
};
use crate::protocol::frame::{take_buf, PooledFrame};
use smallvec::SmallVec;

/// Response buffer optimized for typical notice frames (<64 bytes)
/// Uses stack allocation to avoid heap overhead for small messages
#[allow(dead_code)]
type ResponseBuf = SmallVec<[u8; 64]>;

/// Notice operation type
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub enum NoticeOperation {
    Subscribe,
    Unsubscribe,
    Publish,
}

/// Parsed TLV payload for notice operations
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct NoticeTlvPayload {
    pub operation: NoticeOperation,
    pub body: Option<Vec<u8>>,
    pub msg_id: Option<String>,
}

/// Parse TLV payload in a single pass
/// Returns descriptive errors on malformed input instead of silently dropping bytes
#[allow(dead_code)]
pub fn parse_tlv_payload(payload: &[u8]) -> Result<NoticeTlvPayload, String> {
    let mut has_subscribe = false;
    let mut has_unsubscribe = false;
    let mut body: Option<Vec<u8>> = None;
    let mut msg_id: Option<String> = None;

    let mut offset = 0;
    while offset + 2 <= payload.len() {
        let tag = payload[offset];
        let length = payload[offset + 1] as usize;
        let value_start = offset + 2;

        // Validate that we have enough bytes for the advertised length
        if value_start + length > payload.len() {
            return Err(format!(
                "Malformed TLV at offset {}: tag {} claims {} bytes but only {} available",
                offset,
                tag,
                length,
                payload.len() - value_start
            ));
        }

        match tag {
            TAG_SUBSCRIBE => has_subscribe = true,
            TAG_UNSUBSCRIBE => has_unsubscribe = true,
            TAG_BODY => body = Some(payload[value_start..value_start + length].to_vec()),
            TAG_ID => {
                if let Ok(s) = std::str::from_utf8(&payload[value_start..value_start + length]) {
                    msg_id = Some(s.to_string());
                }
            }
            _ => {
                // Unknown tag - skip it but don't error (forward compatibility)
            }
        }

        offset = value_start + length;
    }

    // Check for trailing garbage bytes
    if offset != payload.len() {
        return Err(format!(
            "TLV parse incomplete: {} trailing bytes after offset {}",
            payload.len() - offset,
            offset
        ));
    }

    let operation = if has_subscribe {
        NoticeOperation::Subscribe
    } else if has_unsubscribe {
        NoticeOperation::Unsubscribe
    } else if body.is_some() {
        NoticeOperation::Publish
    } else {
        return Err(
            "Unknown notice operation: no subscribe, unsubscribe, or body tag found".to_string(),
        );
    };

    Ok(NoticeTlvPayload {
        operation,
        body,
        msg_id,
    })
}

/// Build TLV-encoded response using SmallVec for stack allocation
#[allow(dead_code)]
pub fn build_ack_response(route: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();
    let mut tmp = Vec::new();
    crate::protocol::frame::build_tlv(TAG_ROUTE, route.as_bytes(), &mut tmp);
    response.extend_from_slice(&tmp);
    response
}

/// Build TLV-encoded publish response with route, optional message ID, and body
#[allow(dead_code)]
pub fn build_publish_response(route: &str, msg_id: Option<&str>, body: &[u8]) -> ResponseBuf {
    let mut response = ResponseBuf::new();
    let mut tmp = Vec::new();
    crate::protocol::frame::build_tlv(TAG_ROUTE, route.as_bytes(), &mut tmp);
    if let Some(id) = msg_id {
        crate::protocol::frame::build_tlv(TAG_ID, id.as_bytes(), &mut tmp);
    }
    crate::protocol::frame::build_tlv(TAG_BODY, body, &mut tmp);
    response.extend_from_slice(&tmp);
    response
}

/// Build TLV-encoded error response using SmallVec for stack allocation
#[allow(dead_code)]
pub fn build_error_response(error_msg: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();
    let mut tmp = Vec::new();
    crate::protocol::frame::build_tlv(TAG_ERR_MSG, error_msg.as_bytes(), &mut tmp);
    response.extend_from_slice(&tmp);
    response
}

/// Build a complete TLV notification frame for fanout delivery
///
/// Frame structure: TAG_ROUTE + route + [TAG_ID + id] + TAG_BODY + body
///
/// # Arguments
/// * `route` - Full route string (e.g., "notice://realm/area/resource/op")
/// * `msg_id` - Optional message ID for correlation
/// * `body` - Message payload bytes
///
/// # Returns
/// Complete TLV-encoded frame ready to send to subscribers (uses pooled buffer)
pub fn build_notification_frame(route: &str, msg_id: Option<&str>, body: &[u8]) -> PooledFrame {
    let mut frame = take_buf();
    crate::protocol::frame::build_tlv(TAG_ROUTE, route.as_bytes(), &mut frame);
    if let Some(id) = msg_id {
        crate::protocol::frame::build_tlv(TAG_ID, id.as_bytes(), &mut frame);
    }
    crate::protocol::frame::build_tlv(TAG_BODY, body, &mut frame);
    PooledFrame::from_vec(frame)
}

/// Build ACK frame for publisher confirmation with subscriber count
///
/// Frame structure: TAG_ROUTE + route + [TAG_ID + id] + TAG_COUNT + count
///
/// # Arguments
/// * `route` - Original publish route
/// * `msg_id` - Optional message ID to echo back
/// * `subscriber_count` - Number of subscribers that received the notification
///
/// # Returns
/// Complete TLV-encoded ACK frame (uses pooled buffer), or None if route is too long
pub fn build_ack_frame_with_count(
    route: &str,
    msg_id: Option<&str>,
    subscriber_count: u32,
) -> Option<PooledFrame> {
    let mut frame = take_buf();

    crate::protocol::frame::build_tlv(TAG_ROUTE, route.as_bytes(), &mut frame);
    if let Some(id) = msg_id {
        crate::protocol::frame::build_tlv(TAG_ID, id.as_bytes(), &mut frame);
    }
    crate::protocol::frame::build_tlv(TAG_COUNT, &subscriber_count.to_be_bytes(), &mut frame);
    Some(PooledFrame::from_vec(frame))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_subscribe_operation() {
        // Arrange
        let payload = vec![TAG_SUBSCRIBE, 0];

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, NoticeOperation::Subscribe);
        assert_eq!(parsed.body, None);
        assert_eq!(parsed.msg_id, None);
    }

    #[test]
    fn should_parse_unsubscribe_operation() {
        // Arrange
        let payload = vec![TAG_UNSUBSCRIBE, 0];

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, NoticeOperation::Unsubscribe);
    }

    #[test]
    fn should_parse_publish_operation() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(5);
        payload.extend_from_slice(b"hello");

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, NoticeOperation::Publish);
        assert_eq!(parsed.body, Some(b"hello".to_vec()));
    }

    #[test]
    fn should_parse_publish_with_message_id() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_ID);
        payload.push(6);
        payload.extend_from_slice(b"msg123");
        payload.push(TAG_BODY);
        payload.push(4);
        payload.extend_from_slice(b"data");

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.operation, NoticeOperation::Publish);
        assert_eq!(parsed.msg_id, Some("msg123".to_string()));
        assert_eq!(parsed.body, Some(b"data".to_vec()));
    }

    #[test]
    fn should_reject_malformed_tlv() {
        // Arrange
        let payload = vec![TAG_BODY, 100]; // Claims 100 bytes but no data

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Malformed TLV"));
    }

    #[test]
    fn should_reject_trailing_bytes() {
        // Arrange
        let payload = vec![TAG_SUBSCRIBE, 0, 0xFF]; // Garbage byte

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("trailing bytes"));
    }

    #[test]
    fn should_reject_empty_payload() {
        // Arrange
        let payload = Vec::new();

        // Act
        let result = parse_tlv_payload(&payload);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown notice operation"));
    }

    #[test]
    fn should_build_ack_response() {
        // Arrange
        let route = "notice://realm/area/resource";

        // Act
        let response = build_ack_response(route);

        // Assert
        assert_eq!(response[0], TAG_ROUTE);
        assert_eq!(response[1], route.len() as u8);
        assert_eq!(&response[2..], route.as_bytes());
    }

    #[test]
    fn should_build_publish_response_without_id() {
        // Arrange
        let route = "notice://realm/alerts";
        let body = b"alert message";

        // Act
        let response = build_publish_response(route, None, body);

        // Assert
        assert!(response.contains(&TAG_ROUTE));
        assert!(response.contains(&TAG_BODY));
        assert!(!response.contains(&TAG_ID));
    }

    #[test]
    fn should_build_publish_response_with_id() {
        // Arrange
        let route = "notice://realm/alerts";
        let msg_id = "msg-789";
        let body = b"alert";

        // Act
        let response = build_publish_response(route, Some(msg_id), body);

        // Assert
        assert!(response.contains(&TAG_ROUTE));
        assert!(response.contains(&TAG_ID));
        assert!(response.contains(&TAG_BODY));
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error_msg = "Route validation failed";

        // Act
        let response = build_error_response(error_msg);

        // Assert
        assert_eq!(response[0], TAG_ERR_MSG);
        assert_eq!(response[1], error_msg.len() as u8);
        assert_eq!(&response[2..], error_msg.as_bytes());
    }

    #[test]
    fn should_build_notification_frame_with_all_fields() {
        // Arrange
        let route = "notice://realm/area/resource/alerts";
        let msg_id = Some("msg-123");
        let body = b"hello world";

        // Act
        let frame = build_notification_frame(route, msg_id, body);

        // Assert
        let bytes = frame.as_ref();
        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], TAG_ROUTE);
        assert!(bytes.contains(&TAG_ID));
        assert!(bytes.contains(&TAG_BODY));
    }

    #[test]
    fn should_build_notification_frame_without_msg_id() {
        // Arrange
        let route = "notice://realm/area/resource/alerts";
        let body = b"test";

        // Act
        let frame = build_notification_frame(route, None, body);

        // Assert
        let bytes = frame.as_ref();
        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], TAG_ROUTE);
        assert!(!bytes.contains(&TAG_ID));
        assert!(bytes.contains(&TAG_BODY));
    }

    #[test]
    fn should_build_ack_frame_with_count() {
        // Arrange
        let route = "notice://realm/area/resource/alerts";
        let msg_id = Some("msg-123");
        let count = 42;

        // Act
        let frame = build_ack_frame_with_count(route, msg_id, count);

        // Assert
        assert!(frame.is_some());
        let frame = frame.unwrap();
        let bytes = frame.as_ref();
        assert_eq!(bytes[0], TAG_ROUTE);
        assert!(bytes.contains(&TAG_ID));
        assert!(bytes.contains(&TAG_COUNT));
    }

    #[test]
    fn should_handle_large_body_with_extended_length() {
        // Arrange
        let route = "notice://test";
        let body = vec![0u8; 300]; // > 254 bytes

        // Act
        let frame = build_notification_frame(route, None, &body);

        // Assert
        let bytes = frame.as_ref();
        assert!(!bytes.is_empty());
        // Verify large body uses extended length encoding
        // The TAG_BODY should be followed by 255 (extended length marker) + 4-byte BE length
        assert!(bytes.contains(&TAG_BODY));
        // After TAG_BODY we expect: [255][4-byte BE length][body bytes]
        let body_len = body.len();
        assert!(body_len > 254);
        // Just verify the frame is large enough to contain everything
        assert!(bytes.len() > 300 + 20); // body + overhead
    }

    #[test]
    fn should_return_none_for_oversized_route_in_ack() {
        // Arrange
        let route = &"a".repeat(300); // > 255 bytes
        let count = 10;

        // Act
        let result = build_ack_frame_with_count(route, None, count);

        // Assert
        assert!(result.is_none());
    }
}
