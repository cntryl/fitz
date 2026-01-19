//! Queue domain TLV message types and codec

use bytes::Bytes;
use crate::domains::queue::{QueueMessage, QueueResponse, MessageId};
use crate::runtime::routing::{Route, RouteFamily};

/// Queue domain message type IDs
pub mod msg_type {
    pub const ENQUEUE: u16 = 200;
    pub const ENQUEUE_BATCH: u16 = 201;
    pub const RESERVE: u16 = 202;
    pub const EXTEND: u16 = 203;
    pub const COMPLETE: u16 = 204;
}

/// Parse Queue request from bytes
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    match msg_type {
        msg_type::ENQUEUE => parse_enqueue(route_family, realm, area, route, payload),
        msg_type::ENQUEUE_BATCH => parse_enqueue_batch(route_family, realm, area, route, payload),
        msg_type::RESERVE => parse_reserve(route_family, realm, area, route, payload),
        msg_type::EXTEND => parse_extend(route_family, realm, area, route, payload),
        msg_type::COMPLETE => parse_complete(route_family, realm, area, route, payload),
        _ => Err(format!("Unknown Queue message type: {}", msg_type)),
    }
}

/// Encode Queue response to bytes
pub fn encode_response(response: &QueueResponse) -> Vec<u8> {
    use bytes::BufMut;
    
    let mut buf = Vec::new();
    match response {
        QueueResponse::Enqueued { id } => {
            buf.put_u64(id.as_u64());
        }
        QueueResponse::EnqueuedBatch { ids } => {
            buf.put_u32(ids.len() as u32);
            for id in ids {
                buf.put_u64(id.as_u64());
            }
        }
        QueueResponse::Reserved { messages } => {
            buf.put_u32(messages.len() as u32);
            for msg in messages {
                buf.put_u64(msg.id.as_u64());
                buf.put_u64(msg.token);
                buf.put_u32(msg.body.len() as u32);
                buf.put_slice(&msg.body);
            }
        }
        QueueResponse::Extended => {
            // Empty response
        }
        QueueResponse::Completed => {
            // Empty response
        }
        QueueResponse::InvalidToken => {
            buf.put_u8(1);
        }
        QueueResponse::LeaseExpired => {
            buf.put_u8(2);
        }
        QueueResponse::NotFound => {
            buf.put_u8(3);
        }
        QueueResponse::BadRequest { reason } => {
            buf.put_u32(reason.len() as u32);
            buf.put_slice(reason.as_bytes());
        }
        QueueResponse::QueueNotFound => {
            buf.put_u8(4);
        }
        QueueResponse::Error { message } => {
            buf.put_u32(message.len() as u32);
            buf.put_slice(message.as_bytes());
        }
    }
    buf
}

// ===== Parsers =====

fn parse_enqueue(
    family_id: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    let mut offset = 0;

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
            offset += 8;
            Some(delay)
        } else {
            offset += 1;
            None
        }
    } else {
        None
    };

    Ok(QueueMessage::Enqueue {
        family_id,
        route: Route::new(&route),
        body,
        delay_seconds,
    })
}

fn parse_enqueue_batch(
    family_id: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    let mut offset = 0;

    // Parse message count
    if offset + 4 > payload.len() {
        return Err("Incomplete message count".to_string());
    }
    let msg_count = u32::from_be_bytes([
        payload[offset],
        payload[offset + 1],
        payload[offset + 2],
        payload[offset + 3],
    ]) as usize;
    offset += 4;

    let mut messages = Vec::new();
    for _ in 0..msg_count {
        // Parse message body length
        if offset + 4 > payload.len() {
            return Err("Incomplete message body length".to_string());
        }
        let msg_len = u32::from_be_bytes([
            payload[offset],
            payload[offset + 1],
            payload[offset + 2],
            payload[offset + 3],
        ]) as usize;
        offset += 4;

        // Parse message body
        if offset + msg_len > payload.len() {
            return Err("Incomplete message body".to_string());
        }
        messages.push(Bytes::copy_from_slice(&payload[offset..offset + msg_len]));
        offset += msg_len;
    }

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
            offset += 8;
            Some(delay)
        } else {
            offset += 1;
            None
        }
    } else {
        None
    };

    Ok(QueueMessage::EnqueueBatch {
        family_id,
        route: Route::new(&route),
        messages,
        delay_seconds,
    })
}

fn parse_reserve(
    family_id: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    let mut offset = 0;

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
            offset += 4;
            Some(size)
        } else {
            offset += 1;
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
            offset += 8;
            Some(wait)
        } else {
            offset += 1;
            None
        }
    } else {
        None
    };

    Ok(QueueMessage::Reserve {
        family_id,
        route: Route::new(&route),
        lease_seconds,
        batch_size,
        wait_seconds,
    })
}

fn parse_extend(
    family_id: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    let mut offset = 0;

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
    offset += 8;

    Ok(QueueMessage::Extend {
        family_id,
        route: Route::new(&route),
        id,
        token,
        lease_seconds,
    })
}

fn parse_complete(
    family_id: RouteFamily,
    realm: String,
    area: String,
    route: String,
    payload: &[u8],
) -> Result<QueueMessage, String> {
    let mut offset = 0;

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

    Ok(QueueMessage::Complete {
        family_id,
        route: Route::new(&route),
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
        let body = b"test message";
        let mut payload = Vec::new();
        payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        payload.extend_from_slice(body);
        payload.push(0); // No delay

        let result = parse_request(
            msg_type::ENQUEUE,
            RouteFamily::new(2),
            "realm".to_string(),
            "area".to_string(),
            "queue://realm/area/test".to_string(),
            &payload,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn should_parse_reserve_message() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&30u64.to_be_bytes()); // lease_seconds
        payload.push(1); // batch_size present
        payload.extend_from_slice(&5u32.to_be_bytes()); // batch_size = 5
        payload.push(0); // No wait_seconds

        let result = parse_request(
            msg_type::RESERVE,
            RouteFamily::new(2),
            "realm".to_string(),
            "area".to_string(),
            "queue://realm/area/test".to_string(),
            &payload,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn should_parse_complete_message() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&123u64.to_be_bytes()); // id
        payload.extend_from_slice(&456u64.to_be_bytes()); // token

        let result = parse_request(
            msg_type::COMPLETE,
            RouteFamily::new(2),
            "realm".to_string(),
            "area".to_string(),
            "queue://realm/area/test".to_string(),
            &payload,
        );

        assert!(result.is_ok());
    }

    #[test]
    fn should_encode_enqueued_response() {
        let response = QueueResponse::Enqueued {
            id: MessageId::new(42),
        };

        let encoded = encode_response(&response);
        assert_eq!(encoded.len(), 8);
        assert_eq!(u64::from_be_bytes(encoded[0..8].try_into().unwrap()), 42);
    }

    #[test]
    fn should_encode_reserved_response() {
        use crate::domains::queue::ReservedMessage;

        let response = QueueResponse::Reserved {
            messages: vec![ReservedMessage {
                id: MessageId::new(1),
                token: 999,
                body: Bytes::from("test"),
                lease_seconds: 30,
                attempts: 1,
            }],
        };

        let encoded = encode_response(&response);
        assert!(!encoded.is_empty());
    }
}
