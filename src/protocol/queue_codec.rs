//! Queue domain TLV message types and codec

use crate::domains::queue::{MessageId, QueueMessage, QueueResponse};
use crate::runtime::routing::{Route, RouteFamily};
use bytes::Bytes;

/// Queue domain message type IDs
pub mod msg_type {
    pub const ENQUEUE: u16 = 200;
    pub const RESERVE: u16 = 202;
    pub const EXTEND: u16 = 203;
    pub const COMPLETE: u16 = 204;
    pub const SUBSCRIBE: u16 = 207;
    pub const UNSUBSCRIBE: u16 = 208;
    pub const QUEUE_NOTIFY: u16 = 209;
}

/// Parse Queue request from bytes
/// Per CLIENT_SPEC: All operations now include full route on wire
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    match msg_type {
        msg_type::ENQUEUE => parse_enqueue(route_family, payload),
        msg_type::RESERVE => parse_reserve(route_family, payload),
        msg_type::EXTEND => parse_extend(route_family, payload),
        msg_type::COMPLETE => parse_complete(route_family, payload),
        _ => Err(format!("Unknown Queue message type: {}", msg_type)),
    }
}

/// Encode Queue response to bytes
pub fn encode_response(response: &QueueResponse) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    match response {
        QueueResponse::Sent { id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(id.as_u64());
        }
        QueueResponse::SentBatch { ids } => {
            buf.put_u8(0); // status: success
            buf.put_u32(ids.len() as u32);
            for id in ids {
                buf.put_u64(id.as_u64());
            }
        }
        QueueResponse::Received { messages } => {
            buf.put_u8(0); // status: success
            buf.put_u32(messages.len() as u32);
            for msg in messages {
                buf.put_u64(msg.id.as_u64());
                buf.put_u64(msg.token);
                buf.put_u32(msg.body.len() as u32);
                buf.put_slice(&msg.body);
            }
        }
        QueueResponse::Extended => {
            buf.put_u8(0); // status: success
                           // Empty response
        }
        QueueResponse::Acked => {
            buf.put_u8(0); // status: success
                           // Empty response
        }
        QueueResponse::SubscribeOk { subscription_id } => {
            buf.put_u8(0); // status: success
            buf.put_u8(1); // has_subscription_id
            buf.put_u64(*subscription_id);
        }
        QueueResponse::UnsubscribeOk => {
            buf.put_u8(0); // status: success
                           // Empty response
        }
        QueueResponse::InvalidToken => {
            buf.put_u8(1); // status: error
            buf.put_u8(1); // error_code: InvalidToken
        }
        QueueResponse::LeaseExpired => {
            buf.put_u8(1); // status: error
            buf.put_u8(2); // error_code: LeaseExpired
        }
        QueueResponse::NotFound => {
            buf.put_u8(1); // status: error
            buf.put_u8(3); // error_code: NotFound
        }
        QueueResponse::BadRequest { reason } => {
            buf.put_u8(1); // status: error
            buf.put_u32(reason.len() as u32);
            buf.put_slice(reason.as_bytes());
        }
        QueueResponse::QueueNotFound => {
            buf.put_u8(1); // status: error
            buf.put_u8(4); // error_code: QueueNotFound
        }
        QueueResponse::Error { message } => {
            buf.put_u8(1); // status: error
            buf.put_u32(message.len() as u32);
            buf.put_slice(message.as_bytes());
        }
    }
    buf
}

// ===== Parsers =====

/// Parse route string (used for QueueMessage construction)
/// Expected format: "queue://realm/area/resource" or just "realm/area/resource"  
/// Returns full route string without decomposition
fn parse_route_string(payload: &[u8], offset: &mut usize) -> Result<String, String> {
    parse_route_str_ref(payload, offset).map(str::to_string)
}

fn parse_route_str_ref<'a>(payload: &'a [u8], offset: &mut usize) -> Result<&'a str, String> {
    // Read route length (u32)
    if *offset + 4 > payload.len() {
        return Err("Route length overflow".to_string());
    }
    let route_len = u32::from_be_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
    ]) as usize;
    *offset += 4;

    // Read route
    if *offset + route_len > payload.len() {
        return Err("Route string overflow".to_string());
    }
    let route_str = std::str::from_utf8(&payload[*offset..*offset + route_len])
        .map_err(|_| "Invalid UTF-8 in route".to_string())?;
    *offset += route_len;

    Ok(route_str)
}

/// Extract the queue route or pattern used for authorization without constructing a full message.
pub fn extract_auth_route(msg_type: u16, payload: &[u8]) -> Result<Option<&str>, String> {
    match msg_type {
        msg_type::ENQUEUE
        | msg_type::RESERVE
        | msg_type::EXTEND
        | msg_type::COMPLETE
        | msg_type::SUBSCRIBE
        | msg_type::UNSUBSCRIBE => {
            let mut offset = 0;
            parse_route_str_ref(payload, &mut offset).map(Some)
        }
        _ => Ok(None),
    }
}

fn parse_enqueue(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u32 body_len][body][u8 has_delay][u64 delay?]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_string(payload, &mut offset)?;

    // Parse body length
    if offset + 4 > payload.len() {
        return Err("Incomplete body length".to_string());
    }
    let body_len = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    // Parse body
    if offset + body_len > payload.len() {
        return Err("Incomplete body".to_string());
    }
    let body = Bytes::copy_from_slice(&payload[offset..offset + body_len]);
    offset += body_len;

    // Parse optional delay (1 byte flag, then u64 if present)
    let delay_seconds = if offset < payload.len() {
        if payload[offset] == 1 {
            offset += 1;
            if offset + 8 > payload.len() {
                return Err("Incomplete delay_seconds".to_string());
            }
            let delay = u64::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
                payload[offset + 4],
                payload[offset + 5],
                payload[offset + 6],
                payload[offset + 7],
            ]);
            Some(delay)
        } else {
            None
        }
    } else {
        None
    };

    Ok(QueueMessage::Send {
        family_id,
        route: Route::new(route_str),
        body,
        delay_seconds,
    })
}

fn parse_reserve(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 lease_seconds][u8 has_batch_size][u32 batch?][u8 has_wait][u64 wait?]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_string(payload, &mut offset)?;

    // Parse lease_seconds (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete lease_seconds".to_string());
    }
    let lease_seconds = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);
    offset += 8;

    // Parse batch_size (1 byte flag, then u32 if present)
    let batch_size = if offset < payload.len() {
        if payload[offset] == 1 {
            offset += 1;
            if offset + 4 > payload.len() {
                return Err("Incomplete batch_size".to_string());
            }
            let size = u32::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]) as usize;
            Some(size)
        } else {
            None
        }
    } else {
        None
    };

    // Parse wait_seconds (1 byte flag, then u64 if present)
    let wait_seconds = if offset < payload.len() {
        if payload[offset] == 1 {
            offset += 1;
            if offset + 8 > payload.len() {
                return Err("Incomplete wait_seconds".to_string());
            }
            let wait = u64::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
                payload[offset + 4],
                payload[offset + 5],
                payload[offset + 6],
                payload[offset + 7],
            ]);
            Some(wait)
        } else {
            None
        }
    } else {
        None
    };

    Ok(QueueMessage::Receive {
        family_id,
        route: Route::new(route_str),
        lease_seconds,
        batch_size,
        wait_seconds,
    })
}

fn parse_extend(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 message_id][u64 lease_token][u64 lease_seconds]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_string(payload, &mut offset)?;

    // Parse id (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete message id".to_string());
    }
    let id = MessageId::new(u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]));
    offset += 8;

    // Parse token (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete token".to_string());
    }
    let token = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);
    offset += 8;

    // Parse lease_seconds (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete lease_seconds".to_string());
    }
    let lease_seconds = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);

    Ok(QueueMessage::Extend {
        family_id,
        route: Route::new(route_str),
        id,
        token,
        lease_seconds,
    })
}

fn parse_complete(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 message_id][u64 lease_token]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_string(payload, &mut offset)?;

    // Parse id (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete message id".to_string());
    }
    let id = MessageId::new(u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]));
    offset += 8;

    // Parse token (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete token".to_string());
    }
    let token = u64::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
        payload[offset + 4],
        payload[offset + 5],
        payload[offset + 6],
        payload[offset + 7],
    ]);

    Ok(QueueMessage::Ack {
        family_id,
        route: Route::new(route_str),
        id,
        token,
    })
}

/// Parse Subscribe request (wire format: [string pattern])
///
/// `session_id` and `subscriber` are injected by the session layer.
pub fn parse_subscribe(
    route_family: RouteFamily,
    payload: &[u8],
    session_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
) -> Result<QueueMessage, String> {
    let mut offset = 0;

    // Parse pattern string
    let pattern_str = parse_route_string(payload, &mut offset)?;

    Ok(QueueMessage::Subscribe {
        family_id: route_family,
        pattern: Route::new(&pattern_str),
        session_id,
        subscriber,
    })
}

/// Parse Unsubscribe request (wire format: [string pattern])
///
/// `session_id` and `subscriber` are injected by the session layer.
pub fn parse_unsubscribe(
    route_family: RouteFamily,
    payload: &[u8],
    session_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
) -> Result<QueueMessage, String> {
    let mut offset = 0;

    // Parse pattern string
    let pattern_str = parse_route_string(payload, &mut offset)?;

    Ok(QueueMessage::Unsubscribe {
        family_id: route_family,
        pattern: Route::new(&pattern_str),
        session_id,
        subscriber,
    })
}

/// Parse UnsubscribeAll request (no wire payload, session-scoped)
///
/// `session_id` and `subscriber` are injected by the session layer.
pub fn parse_unsubscribe_all(
    session_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
) -> QueueMessage {
    QueueMessage::UnsubscribeAll {
        session_id,
        subscriber,
    }
}

/// Encode a QUEUE_NOTIFY (209) payload.
///
/// Wire format: `[u64 subscription_id][string route][bytes payload]`
pub fn encode_notify(subscription_id: u64, route: &Route, payload: &[u8]) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);

    let route_str = route.as_str();
    buf.put_u32(route_str.len() as u32);
    buf.put_slice(route_str.as_bytes());

    buf.put_u32(payload.len() as u32);
    buf.put_slice(payload);

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn should_parse_enqueue_message() {
        // Arrange
        let route = "queue://realm/area/test";
        let body = b"test message";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
        payload.push(0); // No delay

        // Act
        let result = parse_request(msg_type::ENQUEUE, RouteFamily::new(2), &payload);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_parse_reserve_message() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&30u64.to_be_bytes()); // lease_seconds
        payload.push(1); // batch_size present
        payload.extend_from_slice(&5u32.to_be_bytes()); // batch_size = 5
        payload.push(0); // No wait_seconds

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_parse_complete_message() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&123u64.to_be_bytes()); // id
        payload.extend_from_slice(&456u64.to_be_bytes()); // token

        // Act
        let result = parse_request(msg_type::COMPLETE, RouteFamily::new(2), &payload);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_encode_enqueued_response() {
        // Arrange
        let response = QueueResponse::Sent {
            id: MessageId::new(42),
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert_eq!(encoded.len(), 9); // 1 status byte + 8 bytes for u64
        assert_eq!(encoded[0], 0); // status: success
        assert_eq!(u64::from_be_bytes(encoded[1..9].try_into().unwrap()), 42);
    }

    #[test]
    fn should_encode_reserved_response() {
        use crate::domains::queue::ReservedMessage;

        // Arrange
        let response = QueueResponse::Received {
            messages: vec![ReservedMessage {
                id: MessageId::new(1),
                token: 999,
                body: Bytes::from("test"),
                lease_seconds: 30,
                attempts: 1,
            }],
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
    }
}
