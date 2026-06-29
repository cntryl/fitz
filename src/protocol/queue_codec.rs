//! Queue domain TLV message types and codec

use crate::domains::queue::{
    MessageId, QueueMessage, QueueNotification, QueueResponse, QueueSubscriptionMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;

/// Queue domain message type IDs
pub mod msg_type {
    pub const ENQUEUE: u16 = 200;
    pub const RESERVE: u16 = 202;
    pub const EXTEND: u16 = 203;
    pub const COMPLETE: u16 = 204;
    pub const WATCH: u16 = 207;
    pub const UNWATCH: u16 = 208;
    pub const NOTIFY: u16 = 209;
}

#[derive(Debug, Clone)]
pub enum ParsedQueueFrame {
    Op(QueueMessage),
    Sub(QueueSubscriptionMessage),
}

pub fn parse_frame(
    ctx: &FrameContext,
    payload: &[u8],
    route_family: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
) -> Result<ParsedQueueFrame, String> {
    match ctx.msg_type.0 {
        msg_type::ENQUEUE => parse_enqueue(route_family, payload).map(ParsedQueueFrame::Op),
        msg_type::RESERVE => parse_reserve(route_family, payload).map(ParsedQueueFrame::Op),
        msg_type::EXTEND => parse_extend(route_family, payload).map(ParsedQueueFrame::Op),
        msg_type::COMPLETE => parse_complete(route_family, payload).map(ParsedQueueFrame::Op),
        msg_type::WATCH => {
            parse_watch(route_family, session_id, subscriber, payload).map(ParsedQueueFrame::Sub)
        }
        msg_type::UNWATCH => {
            parse_unwatch(route_family, session_id, subscriber, payload).map(ParsedQueueFrame::Sub)
        }
        msg_type::NOTIFY => Err("QUEUE_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown Queue message type: {}", ctx.msg_type.0)),
    }
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
        _ => Err(format!("Unknown Queue message type: {msg_type}")),
    }
}

/// Encode Queue response to bytes
#[must_use]
pub fn encode_response(response: &QueueResponse) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    match response {
        QueueResponse::Sent { id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(id.as_u64());
        }
        QueueResponse::WatchOk { subscription_id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(*subscription_id);
        }
        QueueResponse::UnwatchOk => {
            buf.put_u8(0); // status: success
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
        QueueResponse::InvalidToken
        | QueueResponse::InflightExpired
        | QueueResponse::NotFound
        | QueueResponse::BadRequest { .. }
        | QueueResponse::QueueNotFound
        | QueueResponse::Error { .. } => {
            let (code, message) = queue_error_code_and_message(response);
            return crate::protocol::error_codes::encode_error_body(code, &message);
        }
    }
    buf
}

fn queue_error_code_and_message(response: &QueueResponse) -> (u16, String) {
    use crate::protocol::error_codes::queue;

    match response {
        QueueResponse::InvalidToken => (queue::ERR_INVALID_TOKEN, "InvalidToken".to_string()),
        QueueResponse::InflightExpired => {
            (queue::ERR_INFLIGHT_EXPIRED, "InflightExpired".to_string())
        }
        QueueResponse::NotFound => (queue::ERR_MESSAGE_NOT_FOUND, "NotFound".to_string()),
        QueueResponse::BadRequest { reason } => (queue::ERR_BAD_REQUEST, reason.clone()),
        QueueResponse::QueueNotFound => (queue::ERR_QUEUE_NOT_FOUND, "QueueNotFound".to_string()),
        QueueResponse::Error { message } => (queue::ERR_BACKEND_ERROR, message.clone()),
        _ => unreachable!("queue_error_code_and_message called for success response"),
    }
}

// ===== Parsers =====

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
        | msg_type::WATCH
        | msg_type::UNWATCH => {
            let mut offset = 0;
            parse_route_str_ref(payload, &mut offset).map(Some)
        }
        msg_type::NOTIFY => Err("QUEUE_NOTIFY is server-to-client only".to_string()),
        _ => Err(format!("Unknown Queue message type: {msg_type}")),
    }
}

fn parse_enqueue(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u32 body_len][body][u8 has_delay][u64 delay?]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_str_ref(payload, &mut offset)?;

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
        let has_delay = payload[offset];
        offset += 1;
        if has_delay == 1 {
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
            offset += 8;
            Some(delay)
        } else if has_delay == 0 {
            None
        } else {
            return Err("Invalid delay flag".to_string());
        }
    } else {
        None
    };

    if offset != payload.len() {
        return Err("Trailing data in enqueue request".to_string());
    }

    Ok(QueueMessage::Send {
        family_id,
        route: Route::from_ref(route_str),
        body,
        delay_seconds,
    })
}

fn parse_reserve(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 inflight_seconds][u8 has_batch_size][u32 batch?]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_str_ref(payload, &mut offset)?;

    // Parse inflight_seconds (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete inflight_seconds".to_string());
    }
    let inflight_seconds = u64::from_be_bytes([
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
        let has_batch_size = payload[offset];
        offset += 1;
        if has_batch_size == 1 {
            if offset + 4 > payload.len() {
                return Err("Incomplete batch_size".to_string());
            }
            let size = u32::from_be_bytes([
                payload[offset],
                payload[offset + 1],
                payload[offset + 2],
                payload[offset + 3],
            ]) as usize;
            offset += 4;
            Some(size)
        } else if has_batch_size == 0 {
            None
        } else {
            return Err("Invalid batch_size flag".to_string());
        }
    } else {
        None
    };

    if offset != payload.len() {
        return Err("Trailing data in reserve request".to_string());
    }

    Ok(QueueMessage::Receive {
        family_id,
        route: Route::from_ref(route_str),
        inflight_seconds,
        batch_size,
    })
}

fn parse_watch(
    family_id: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
    payload: &[u8],
) -> Result<QueueSubscriptionMessage, String> {
    let mut offset = 0;
    let pattern_str = parse_route_str_ref(payload, &mut offset)?;
    if offset != payload.len() {
        return Err("Trailing data in watch request".to_string());
    }

    Ok(QueueSubscriptionMessage::Watch {
        family_id,
        pattern: Route::from_ref(pattern_str),
        session_id,
        subscriber,
    })
}

fn parse_unwatch(
    family_id: RouteFamily,
    session_id: u64,
    subscriber: RouteAddress,
    payload: &[u8],
) -> Result<QueueSubscriptionMessage, String> {
    let mut offset = 0;
    let pattern_str = parse_route_str_ref(payload, &mut offset)?;
    if offset != payload.len() {
        return Err("Trailing data in unwatch request".to_string());
    }

    Ok(QueueSubscriptionMessage::Unwatch {
        family_id,
        pattern: Route::from_ref(pattern_str),
        session_id,
        subscriber,
    })
}

#[must_use]
pub fn encode_notify(
    subscription_id: u64,
    route: &Route,
    notification: QueueNotification,
) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    buf.put_u64(subscription_id);
    buf.put_u32(route.as_str().len() as u32);
    buf.put_slice(route.as_str().as_bytes());
    buf.put_u64(notification.ready_messages);
    buf.put_u64(notification.delayed_messages);
    buf.put_u64(notification.inflight_messages);
    buf
}

fn parse_extend(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 message_id][u64 inflight_token][u64 inflight_seconds]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_str_ref(payload, &mut offset)?;

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

    // Parse inflight_seconds (u64)
    if offset + 8 > payload.len() {
        return Err("Incomplete inflight_seconds".to_string());
    }
    let inflight_seconds = u64::from_be_bytes([
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

    if offset != payload.len() {
        return Err("Trailing data in extend request".to_string());
    }

    Ok(QueueMessage::Extend {
        family_id,
        route: Route::from_ref(route_str),
        id,
        token,
        inflight_seconds,
    })
}

fn parse_complete(family_id: RouteFamily, payload: &[u8]) -> Result<QueueMessage, String> {
    // Wire format per CLIENT_SPEC: [u32 route_len][route][u64 message_id][u64 inflight_token]
    let mut offset = 0;

    // Parse route
    let route_str = parse_route_str_ref(payload, &mut offset)?;

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

    if offset != payload.len() {
        return Err("Trailing data in complete request".to_string());
    }

    Ok(QueueMessage::Ack {
        family_id,
        route: Route::from_ref(route_str),
        id,
        token,
    })
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
        payload.extend_from_slice(&30u64.to_be_bytes()); // inflight_seconds
        payload.push(1); // batch_size present
        payload.extend_from_slice(&5u32.to_be_bytes()); // batch_size = 5

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_reserve_message_with_trailing_wait_seconds() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&30u64.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&5u64.to_be_bytes());

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        match result {
            Err(error) => assert_eq!(error, "Trailing data in reserve request"),
            Ok(message) => panic!("expected trailing-data error, got {message:?}"),
        }
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
                inflight_seconds: 30,
                attempts: 1,
            }],
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_encode_watch_response() {
        // Arrange
        let response = QueueResponse::WatchOk {
            subscription_id: 42,
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert_eq!(encoded.len(), 9);
        assert_eq!(encoded[0], 0);
        assert_eq!(u64::from_be_bytes(encoded[1..9].try_into().unwrap()), 42);
    }
}
