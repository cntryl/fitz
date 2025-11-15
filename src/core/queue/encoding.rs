//! Queue domain TLV encoding/decoding utilities
//!
//! Handles parsing of TLV-encoded request payloads and building TLV-encoded responses
//! for queue operations.

use super::types::{LeaseInfo, QueueConfig, QueueMessage, StoredQueueMessage};
use crate::protocol::tags::{
    TAG_BODY, TAG_DELIVERY_TOKEN, TAG_ERR_MSG, TAG_ID, TAG_LEASE, TAG_TIMESTAMP, TAG_TTL_SECS,
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

/// Encode a QueueMessage to TLV format for storage
pub fn encode_queue_message(message: &QueueMessage) -> Vec<u8> {
    let mut data = Vec::new();

    // TAG_ID - message ID
    let id_bytes = message.id.as_bytes();
    data.push(TAG_ID);
    data.push(id_bytes.len() as u8);
    data.extend_from_slice(id_bytes);

    // TAG_BODY - message body
    data.push(TAG_BODY);
    data.push(message.body.len() as u8);
    data.extend_from_slice(&message.body);

    // TAG_TIMESTAMP - created_at
    data.push(TAG_TIMESTAMP);
    data.push(8); // u64 is 8 bytes
    data.extend_from_slice(&message.created_at.to_be_bytes());

    // TAG_LEASE - lease_expiry (optional)
    if let Some(lease_expiry) = message.lease_expiry {
        data.push(TAG_LEASE);
        data.push(8); // u64 is 8 bytes
        data.extend_from_slice(&lease_expiry.to_be_bytes());
    }

    // TAG_DELIVERY_TOKEN - lease_owner (optional)
    if let Some(ref lease_owner) = message.lease_owner {
        let token_bytes = lease_owner.as_bytes();
        data.push(TAG_DELIVERY_TOKEN);
        data.push(token_bytes.len() as u8);
        data.extend_from_slice(token_bytes);
    }

    // TAG_TTL_SECS - ttl_secs (optional)
    if let Some(ttl_secs) = message.ttl_secs {
        data.push(TAG_TTL_SECS);
        data.push(8); // u64 is 8 bytes
        data.extend_from_slice(&ttl_secs.to_be_bytes());
    }

    // For delivery_count, we'll use a custom tag since it's not in the protocol
    // TAG 0x78 - delivery count (u32)
    data.push(0x78);
    data.push(4); // u32 is 4 bytes
    data.extend_from_slice(&message.delivery_count.to_be_bytes());

    data
}

/// Encode a StoredQueueMessage to TLV format for storage
pub fn encode_stored_queue_message(message: &StoredQueueMessage) -> Vec<u8> {
    let mut data = Vec::new();

    // TAG_ID - message ID
    let id_bytes = message.id.as_bytes();
    data.push(TAG_ID);
    data.push(id_bytes.len() as u8);
    data.extend_from_slice(id_bytes);

    // TAG_BODY - message body
    data.push(TAG_BODY);
    data.push(message.body.len() as u8);
    data.extend_from_slice(&message.body);

    // TAG_TIMESTAMP - created_at
    data.push(TAG_TIMESTAMP);
    data.push(8); // u64 is 8 bytes
    data.extend_from_slice(&message.created_at.to_be_bytes());

    // TAG_TTL_SECS - ttl_secs (optional)
    if let Some(ttl_secs) = message.ttl_secs {
        data.push(TAG_TTL_SECS);
        data.push(8); // u64 is 8 bytes
        data.extend_from_slice(&ttl_secs.to_be_bytes());
    }

    data
}

/// Encode a LeaseInfo to TLV format for storage
pub fn encode_lease_info(lease: &LeaseInfo) -> Vec<u8> {
    let mut data = Vec::new();

    // TAG_LEASE - lease_expiry (optional)
    if let Some(lease_expiry) = lease.lease_expiry {
        data.push(TAG_LEASE);
        data.push(8); // u64 is 8 bytes
        data.extend_from_slice(&lease_expiry.to_be_bytes());
    }

    // TAG_DELIVERY_TOKEN - lease_owner (optional)
    if let Some(ref lease_owner) = lease.lease_owner {
        let token_bytes = lease_owner.as_bytes();
        data.push(TAG_DELIVERY_TOKEN);
        data.push(token_bytes.len() as u8);
        data.extend_from_slice(token_bytes);
    }

    // TAG 0x78 - delivery count (u32)
    data.push(0x78);
    data.push(4); // u32 is 4 bytes
    data.extend_from_slice(&lease.delivery_count.to_be_bytes());

    data
}

/// Decode a StoredQueueMessage from TLV format
pub fn decode_stored_queue_message(data: &[u8]) -> Result<StoredQueueMessage, String> {
    let mut id = None;
    let mut body = None;
    let mut created_at = None;
    let mut ttl_secs = None;

    let mut i = 0;
    while i < data.len() {
        if i + 2 > data.len() {
            break;
        }

        let tag = data[i];
        let len_byte = data[i + 1];
        i += 2;

        // Handle extended length encoding
        let len = if len_byte == 255 {
            if i + 4 > data.len() {
                break;
            }
            let len_bytes = [data[i], data[i + 1], data[i + 2], data[i + 3]];
            i += 4;
            u32::from_be_bytes(len_bytes) as usize
        } else {
            len_byte as usize
        };

        if i + len > data.len() {
            break;
        }

        match tag {
            TAG_ID => {
                id = Some(String::from_utf8_lossy(&data[i..i + len]).to_string());
            }
            TAG_BODY => {
                body = Some(data[i..i + len].to_vec());
            }
            TAG_TIMESTAMP => {
                if len == 8 {
                    created_at = Some(u64::from_be_bytes(data[i..i + 8].try_into().unwrap()));
                }
            }
            TAG_TTL_SECS => {
                if len == 8 {
                    ttl_secs = Some(u64::from_be_bytes(data[i..i + 8].try_into().unwrap()));
                }
            }
            _ => {} // Ignore unknown tags
        }

        i += len;
    }

    let id = id.ok_or_else(|| "Missing message ID".to_string())?;
    let body = body.ok_or_else(|| "Missing message body".to_string())?;
    let created_at = created_at.ok_or_else(|| "Missing created_at".to_string())?;

    Ok(StoredQueueMessage {
        id,
        route: "".to_string(), // Route is derived from key, not stored
        body,
        created_at,
        ttl_secs,
    })
}

/// Decode a LeaseInfo from TLV format
pub fn decode_lease_info(data: &[u8]) -> Result<LeaseInfo, String> {
    let mut lease_expiry = None;
    let mut lease_owner = None;
    let mut delivery_count = 0u32;

    let mut i = 0;
    while i < data.len() {
        if i + 2 > data.len() {
            break;
        }

        let tag = data[i];
        let len_byte = data[i + 1];
        i += 2;

        // Handle extended length encoding
        let len = if len_byte == 255 {
            if i + 4 > data.len() {
                break;
            }
            let len_bytes = [data[i], data[i + 1], data[i + 2], data[i + 3]];
            i += 4;
            u32::from_be_bytes(len_bytes) as usize
        } else {
            len_byte as usize
        };

        if i + len > data.len() {
            break;
        }

        match tag {
            TAG_LEASE => {
                if len == 8 {
                    lease_expiry = Some(u64::from_be_bytes(data[i..i + 8].try_into().unwrap()));
                }
            }
            TAG_DELIVERY_TOKEN => {
                lease_owner = Some(String::from_utf8_lossy(&data[i..i + len]).to_string());
            }
            0x78 => { // delivery count
                if len == 4 {
                    delivery_count = u32::from_be_bytes(data[i..i + 4].try_into().unwrap());
                }
            }
            _ => {} // Ignore unknown tags
        }

        i += len;
    }

    Ok(LeaseInfo {
        lease_expiry,
        lease_owner,
        delivery_count,
    })
}
pub fn decode_queue_message(data: &[u8]) -> Result<QueueMessage, String> {
    let mut id = None;
    let mut body = None;
    let mut created_at = None;
    let mut lease_expiry = None;
    let mut lease_owner = None;
    let mut ttl_secs = None;
    let mut delivery_count = 0u32;

    let mut i = 0;
    while i < data.len() {
        if i + 2 > data.len() {
            break;
        }

        let tag = data[i];
        let len_byte = data[i + 1];
        i += 2;

        // Handle extended length encoding
        let len = if len_byte == 255 {
            if i + 4 > data.len() {
                break;
            }
            let len_bytes = [data[i], data[i + 1], data[i + 2], data[i + 3]];
            i += 4;
            u32::from_be_bytes(len_bytes) as usize
        } else {
            len_byte as usize
        };

        if i + len > data.len() {
            break;
        }

        let value = &data[i..i + len];
        i += len;

        match tag {
            TAG_ID => {
                if let Ok(id_str) = std::str::from_utf8(value) {
                    id = Some(id_str.to_string());
                }
            }
            TAG_BODY => {
                body = Some(value.to_vec());
            }
            TAG_TIMESTAMP => {
                if value.len() == 8 {
                    let bytes = [value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]];
                    created_at = Some(u64::from_be_bytes(bytes));
                }
            }
            TAG_LEASE => {
                if value.len() == 8 {
                    let bytes = [value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]];
                    lease_expiry = Some(u64::from_be_bytes(bytes));
                }
            }
            TAG_DELIVERY_TOKEN => {
                if let Ok(token_str) = std::str::from_utf8(value) {
                    lease_owner = Some(token_str.to_string());
                }
            }
            TAG_TTL_SECS => {
                if value.len() == 8 {
                    let bytes = [value[0], value[1], value[2], value[3], value[4], value[5], value[6], value[7]];
                    ttl_secs = Some(u64::from_be_bytes(bytes));
                }
            }
            0x78 => { // delivery_count
                if value.len() == 4 {
                    let bytes = [value[0], value[1], value[2], value[3]];
                    delivery_count = u32::from_be_bytes(bytes);
                }
            }
            _ => {} // Ignore unknown tags
        }
    }

    let id = id.ok_or_else(|| "Missing message ID".to_string())?;
    let body = body.ok_or_else(|| "Missing message body".to_string())?;
    let created_at = created_at.ok_or_else(|| "Missing created_at timestamp".to_string())?;

    Ok(QueueMessage {
        id,
        route: String::new(), // This will be set from the key when loading
        body,
        lease_expiry,
        lease_owner,
        delivery_count,
        created_at,
        ttl_secs,
    })
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
