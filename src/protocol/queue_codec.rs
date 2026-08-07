//! Queue domain TLV message types and codec

use crate::dispatch::wire::queue::{
    MessageId, QueueMessage, QueueNotification, QueueResponse, QueueSubscriptionMessage,
};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;

fn usize_to_u32_saturating(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

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

/// # Errors
///
/// Returns an error when the queue message type is unsupported, server-only, or
/// the payload cannot be decoded as the expected queue frame.
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
/// Per `CLIENT_SPEC`: all operations now include full route on wire.
///
/// # Errors
///
/// Returns an error when the queue message type is unsupported or the payload
/// cannot be decoded as the requested queue operation.
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
pub fn encode_response(message_type: u16, response: &QueueResponse) -> Vec<u8> {
    use bytes::BufMut;

    let mut buf = Vec::new();
    match response {
        QueueResponse::Sent { id } => {
            buf.put_u8(0); // status: success
            buf.put_u64(id.as_u64());
        }
        QueueResponse::WatchOk { subscription_id } => {
            buf.put_u8(0); // status: success
            buf.put_u8(1); // has_subscription_id
            buf.put_u64(*subscription_id);
        }
        QueueResponse::UnwatchOk => {
            buf.put_u8(0); // status: success
        }
        QueueResponse::SentBatch { ids } => {
            buf.put_u8(0); // status: success
            buf.put_u32(usize_to_u32_saturating(ids.len()));
            for id in ids {
                buf.put_u64(id.as_u64());
            }
        }
        QueueResponse::Received { messages } => {
            buf.put_u8(0); // status: success
            buf.put_u32(usize_to_u32_saturating(messages.len()));
            for msg in messages {
                buf.put_u64(msg.id.as_u64());
                buf.put_u64(msg.token);
                buf.put_u32(usize_to_u32_saturating(msg.body.len()));
                buf.put_slice(&msg.body);
            }
        }
        QueueResponse::ReceivedRouted { messages } => {
            buf.put_u8(0); // status: success
            buf.put_u32(usize_to_u32_saturating(messages.len()));
            for routed in messages {
                buf.put_u32(usize_to_u32_saturating(routed.route.as_str().len()));
                buf.put_slice(routed.route.as_str().as_bytes());
                buf.put_u64(routed.message.id.as_u64());
                buf.put_u64(routed.message.token);
                buf.put_u32(usize_to_u32_saturating(routed.message.body.len()));
                buf.put_slice(&routed.message.body);
            }
        }
        QueueResponse::Extended | QueueResponse::Acked => {
            buf.put_u8(0); // status: success
                           // Empty response
        }
        QueueResponse::InvalidToken
        | QueueResponse::InflightExpired
        | QueueResponse::NotFound
        | QueueResponse::BadRequest { .. }
        | QueueResponse::InvalidSubscriptionPattern { .. }
        | QueueResponse::SubscriptionLimit
        | QueueResponse::QueueNotFound
        | QueueResponse::Error { .. } => {
            let (code, message) = queue_error_code_and_message(response);
            if matches!(message_type, msg_type::ENQUEUE | msg_type::RESERVE) {
                return crate::protocol::error_codes::encode_error_body(code, &message);
            }
            buf.put_u8(1);
            buf.put_u32(usize_to_u32_saturating(message.len()));
            buf.put_slice(message.as_bytes());
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
        QueueResponse::InvalidSubscriptionPattern { reason } => {
            (queue::ERR_INVALID_SUBSCRIPTION_PATTERN, reason.clone())
        }
        QueueResponse::SubscriptionLimit => (
            queue::ERR_SUBSCRIPTION_LIMIT,
            "wildcard subscription limit exceeded".to_string(),
        ),
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
///
/// # Errors
///
/// Returns an error when the message type is unsupported, server-only, or the
/// route/pattern prefix cannot be decoded from the payload.
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
    // Wire format per CLIENT_SPEC: [string route][u64 inflight_seconds]
    // [u8 has_batch_size][u32 batch?][u8 has_wait_seconds][u64 wait_seconds?]
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
    if offset >= payload.len() {
        return Err("Missing batch_size presence flag".to_string());
    }
    let batch_size = {
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
            if size > crate::dispatch::wire::queue::MAX_RESERVE_BATCH_SIZE {
                return Err(format!(
                    "Queue reserve batch_size must be <= {}",
                    crate::dispatch::wire::queue::MAX_RESERVE_BATCH_SIZE
                ));
            }
            Some(size)
        } else if has_batch_size == 0 {
            None
        } else {
            return Err("Invalid batch_size flag".to_string());
        }
    };

    if offset >= payload.len() {
        return Err("Missing wait_seconds presence flag".to_string());
    }
    let wait_seconds = {
        let has_wait_seconds = payload[offset];
        offset += 1;
        if has_wait_seconds == 1 {
            if offset + 8 > payload.len() {
                return Err("Incomplete wait_seconds".to_string());
            }
            let wait = u64::from_be_bytes(
                payload[offset..offset + 8]
                    .try_into()
                    .expect("validated wait_seconds width"),
            );
            offset += 8;
            Some(wait)
        } else if has_wait_seconds == 0 {
            None
        } else {
            return Err("Invalid wait_seconds flag".to_string());
        }
    };

    if offset != payload.len() {
        return Err("Trailing data in reserve request".to_string());
    }

    Ok(QueueMessage::Receive {
        family_id,
        route: Route::from_ref(route_str),
        inflight_seconds,
        batch_size,
        wait_seconds,
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
    buf.put_u32(usize_to_u32_saturating(route.as_str().len()));
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

    fn len_to_u32(len: usize) -> u32 {
        u32::try_from(len).expect("test payload length should fit in u32")
    }

    #[test]
    fn should_parse_enqueue_message() {
        // Arrange
        let route = "queue://realm/area/test";
        let body = b"test message";
        let mut payload = Vec::new();
        payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&len_to_u32(body.len()).to_be_bytes());
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
        payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&30u64.to_be_bytes()); // inflight_seconds
        payload.push(1); // batch_size present
        payload.extend_from_slice(&5u32.to_be_bytes()); // batch_size = 5
        payload.push(0); // wait_seconds absent

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        assert!(result.is_ok());
    }

    #[test]
    fn should_reject_reserve_batch_above_maximum() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&30u64.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&1025u32.to_be_bytes());

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        assert_eq!(
            result.expect_err("oversized reserve batch should fail"),
            "Queue reserve batch_size must be <= 1024"
        );
    }

    #[test]
    fn should_parse_reserve_message_with_wait_seconds() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
        payload.extend_from_slice(route.as_bytes());
        payload.extend_from_slice(&30u64.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.push(1);
        payload.extend_from_slice(&5u64.to_be_bytes());

        // Act
        let result = parse_request(msg_type::RESERVE, RouteFamily::new(2), &payload);

        // Assert
        let QueueMessage::Receive { wait_seconds, .. } = result.expect("reserve should parse")
        else {
            panic!("expected reserve message");
        };
        assert_eq!(wait_seconds, Some(5));
    }

    #[test]
    fn should_parse_complete_message() {
        // Arrange
        let route = "queue://realm/area/test";
        let mut payload = Vec::new();
        payload.extend_from_slice(&len_to_u32(route.len()).to_be_bytes());
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
        let encoded = encode_response(msg_type::ENQUEUE, &response);

        // Assert
        assert_eq!(encoded.len(), 9); // 1 status byte + 8 bytes for u64
        assert_eq!(encoded[0], 0); // status: success
        assert_eq!(u64::from_be_bytes(encoded[1..9].try_into().unwrap()), 42);
    }

    #[test]
    fn should_encode_reserved_response() {
        use crate::dispatch::wire::queue::ReservedMessage;

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
        let encoded = encode_response(msg_type::RESERVE, &response);

        // Assert
        assert_eq!(
            encoded,
            vec![
                0, 0, 0, 0, 1, // status and count
                0, 0, 0, 0, 0, 0, 0, 1, // message ID
                0, 0, 0, 0, 0, 0, 3, 231, // lease token
                0, 0, 0, 4, b't', b'e', b's', b't', // body
            ]
        );
    }

    #[test]
    fn should_encode_concrete_route_in_routed_reserved_response() {
        use crate::dispatch::wire::queue::ReservedMessage;

        // Arrange
        let response = QueueResponse::ReceivedRouted {
            messages: vec![crate::dispatch::wire::queue::RoutedReservedMessage {
                route: Route::new("queue://acme/jobs/email"),
                message: ReservedMessage {
                    id: MessageId::new(1),
                    token: 999,
                    body: Bytes::from_static(b"test"),
                    inflight_seconds: 30,
                    attempts: 1,
                },
            }],
        };

        // Act
        let encoded = encode_response(msg_type::RESERVE, &response);
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(&encoded);

        // Assert
        assert_eq!(decoder.get_u8().expect("status"), 0);
        assert_eq!(decoder.get_u32().expect("lease count"), 1);
        assert_eq!(
            decoder.get_string().expect("concrete route"),
            "queue://acme/jobs/email"
        );
        assert_eq!(decoder.get_u64().expect("message id"), 1);
        assert_eq!(decoder.get_u64().expect("lease token"), 999);
        assert_eq!(decoder.get_bytes().expect("body").as_ref(), b"test");
        assert!(decoder.is_complete());
    }

    #[test]
    fn should_encode_watch_response() {
        // Arrange
        let response = QueueResponse::WatchOk {
            subscription_id: 42,
        };

        // Act
        let encoded = encode_response(msg_type::WATCH, &response);

        // Assert
        assert_eq!(encoded.len(), 10);
        assert_eq!(encoded[0], 0);
        assert_eq!(encoded[1], 1);
        assert_eq!(u64::from_be_bytes(encoded[2..10].try_into().unwrap()), 42);
    }
}
