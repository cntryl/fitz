//! Queue domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for queue operations.

use super::types::QueueConfig;
use crate::protocol::tags::{
    TAG_BODY, TAG_DELIVERY_TOKEN, TAG_ERR_MSG, TAG_ID, TAG_LEASE, TAG_TTL_SECS,
};

/// Parse TLV payload to extract queue operation parameters
pub fn parse_tlv_payload(
    payload: &[u8],
) -> (
    Option<String>,      // message_id
    Option<Vec<u8>>,     // body
    Option<u32>,         // lease_secs
    Option<String>,      // delivery_token
    Option<u64>,         // ttl_secs
    Option<QueueConfig>, // config
) {
    let mut message_id = None;
    let mut body = None;
    let mut lease_secs = None;
    let mut delivery_token = None;
    let mut ttl_secs = None;
    let config = None;
    let mut i = 0;

    while i < payload.len() {
        if i + 2 > payload.len() {
            break;
        }

        let tag = payload[i];
        let len_byte = payload[i + 1];
        i += 2;

        // Handle extended length encoding
        let len = if len_byte == 255 {
            if i + 4 > payload.len() {
                break;
            }
            let len_bytes = [payload[i], payload[i + 1], payload[i + 2], payload[i + 3]];
            i += 4;
            u32::from_be_bytes(len_bytes) as usize
        } else {
            len_byte as usize
        };

        if i + len > payload.len() {
            break;
        }

        let data = &payload[i..i + len];
        i += len;

        match tag {
            TAG_ID => {
                if let Ok(id_str) = std::str::from_utf8(data) {
                    message_id = Some(id_str.to_string());
                }
            }
            TAG_BODY => {
                body = Some(data.to_vec());
            }
            TAG_LEASE => {
                if len == 4 {
                    let bytes = [data[0], data[1], data[2], data[3]];
                    lease_secs = Some(u32::from_be_bytes(bytes));
                }
            }
            TAG_DELIVERY_TOKEN => {
                if let Ok(token_str) = std::str::from_utf8(data) {
                    delivery_token = Some(token_str.to_string());
                }
            }
            TAG_TTL_SECS => {
                if len == 8 {
                    let bytes = [
                        data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
                    ];
                    ttl_secs = Some(u64::from_be_bytes(bytes));
                }
            }
            _ => {} // Ignore unknown tags
        }
    }

    (
        message_id,
        body,
        lease_secs,
        delivery_token,
        ttl_secs,
        config,
    )
}

/// Build TLV response for enqueue operation
pub fn build_enqueue_response(message_ids: &[String]) -> Vec<u8> {
    let mut response = Vec::new();

    for message_id in message_ids {
        // TAG_ID with the assigned message ID
        let id_bytes = message_id.as_bytes();
        response.push(TAG_ID);
        response.push(id_bytes.len() as u8);
        response.extend_from_slice(id_bytes);
    }

    response
}

/// Build TLV response for reserve operation
pub fn build_reserve_response(messages: &[(String, Vec<u8>, String)]) -> Vec<u8> {
    let mut response = Vec::new();

    for (id, body, token) in messages {
        // TAG_ID
        let id_bytes = id.as_bytes();
        response.push(TAG_ID);
        response.push(id_bytes.len() as u8);
        response.extend_from_slice(id_bytes);

        // TAG_BODY
        response.push(TAG_BODY);
        response.push(body.len() as u8);
        response.extend_from_slice(body);

        // TAG_DELIVERY_TOKEN
        let token_bytes = token.as_bytes();
        response.push(TAG_DELIVERY_TOKEN);
        response.push(token_bytes.len() as u8);
        response.extend_from_slice(token_bytes);
    }

    response
}

/// Build TLV response for list operation
pub fn build_list_response(queues: &[String]) -> Vec<u8> {
    let mut response = Vec::new();

    for queue in queues {
        // TAG_ID with queue route name
        let queue_bytes = queue.as_bytes();
        response.push(TAG_ID);
        response.push(queue_bytes.len() as u8);
        response.extend_from_slice(queue_bytes);
    }

    response
}

/// Build TLV response for successful operations (consume, extend-lease, config)
pub fn build_success_response() -> Vec<u8> {
    Vec::new() // Empty response indicates success
}

/// Build TLV error response
pub fn build_error_response(error_msg: &str) -> Vec<u8> {
    let mut response = Vec::new();

    // TAG_ERR_MSG
    let msg_bytes = error_msg.as_bytes();
    response.push(TAG_ERR_MSG);
    response.push(msg_bytes.len() as u8);
    response.extend_from_slice(msg_bytes);

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::tags::*;

    #[test]
    fn should_parse_empty_tlv_payload() {
        // Arrange
        let payload = vec![];

        // Act
        let (message_id, body, lease_secs, delivery_token, ttl_secs, config) =
            parse_tlv_payload(&payload);

        // Assert
        assert!(message_id.is_none());
        assert!(body.is_none());
        assert!(lease_secs.is_none());
        assert!(delivery_token.is_none());
        assert!(ttl_secs.is_none());
        assert!(config.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_body() {
        // Arrange
        let mut payload = Vec::new();
        payload.push(TAG_BODY);
        payload.push(5); // length
        payload.extend_from_slice(b"hello");

        // Act
        let (message_id, body, lease_secs, delivery_token, ttl_secs, config) =
            parse_tlv_payload(&payload);

        // Assert
        assert!(message_id.is_none());
        assert_eq!(body, Some(b"hello".to_vec()));
        assert!(lease_secs.is_none());
        assert!(delivery_token.is_none());
        assert!(ttl_secs.is_none());
        assert!(config.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_id_and_lease() {
        // Arrange
        let mut payload = Vec::new();

        // TAG_ID
        payload.push(TAG_ID);
        payload.push(4); // length
        payload.extend_from_slice(b"msg1");

        // TAG_LEASE
        payload.push(TAG_LEASE);
        payload.push(4); // length
        payload.extend_from_slice(&30u32.to_be_bytes());

        // Act
        let (message_id, body, lease_secs, delivery_token, ttl_secs, config) =
            parse_tlv_payload(&payload);

        // Assert
        assert_eq!(message_id, Some("msg1".to_string()));
        assert!(body.is_none());
        assert_eq!(lease_secs, Some(30));
        assert!(delivery_token.is_none());
        assert!(ttl_secs.is_none());
        assert!(config.is_none());
    }

    #[test]
    fn should_parse_tlv_payload_with_delivery_token_and_ttl() {
        // Arrange
        let mut payload = Vec::new();

        // TAG_DELIVERY_TOKEN
        payload.push(TAG_DELIVERY_TOKEN);
        payload.push(6); // length
        payload.extend_from_slice(b"token1");

        // TAG_TTL_SECS
        payload.push(TAG_TTL_SECS);
        payload.push(8); // length
        payload.extend_from_slice(&3600u64.to_be_bytes());

        // Act
        let (message_id, body, lease_secs, delivery_token, ttl_secs, config) =
            parse_tlv_payload(&payload);

        // Assert
        assert!(message_id.is_none());
        assert!(body.is_none());
        assert!(lease_secs.is_none());
        assert_eq!(delivery_token, Some("token1".to_string()));
        assert_eq!(ttl_secs, Some(3600));
        assert!(config.is_none());
    }

    #[test]
    fn should_build_enqueue_response() {
        // Arrange
        let message_ids = vec!["msg_123".to_string()];

        // Act
        let response = build_enqueue_response(&message_ids);

        // Assert
        assert!(!response.is_empty());
        // Should contain TAG_ID followed by the message ID
        assert_eq!(response[0], TAG_ID);
        assert_eq!(response[1] as usize, message_ids[0].len());
        assert_eq!(&response[2..], message_ids[0].as_bytes());
    }

    #[test]
    fn should_build_reserve_response() {
        // Arrange
        let messages = vec![
            ("msg1".to_string(), b"body1".to_vec(), "token1".to_string()),
            ("msg2".to_string(), b"body2".to_vec(), "token2".to_string()),
        ];

        // Act
        let response = build_reserve_response(&messages);

        // Assert
        assert!(!response.is_empty());
        // Response should contain TLVs for each message
        // This is a basic check - more detailed parsing would be needed for full validation
    }

    #[test]
    fn should_build_list_response() {
        // Arrange
        let queues = vec!["queue1".to_string(), "queue2".to_string()];

        // Act
        let response = build_list_response(&queues);

        // Assert
        assert!(!response.is_empty());
        // Should contain TAG_ID for each queue
        assert_eq!(response[0], TAG_ID);
        assert_eq!(response[1] as usize, queues[0].len());
        assert_eq!(&response[2..(2 + queues[0].len())], queues[0].as_bytes());
    }

    #[test]
    fn should_build_success_response() {
        // Arrange

        // Act
        let response = build_success_response();

        // Assert
        assert!(response.is_empty());
    }

    #[test]
    fn should_build_error_response() {
        // Arrange
        let error_msg = "Test error message";

        // Act
        let response = build_error_response(error_msg);

        // Assert
        assert!(!response.is_empty());
        assert_eq!(response[0], TAG_ERR_MSG);
        assert_eq!(response[1] as usize, error_msg.len());
        assert_eq!(&response[2..], error_msg.as_bytes());
    }
}
