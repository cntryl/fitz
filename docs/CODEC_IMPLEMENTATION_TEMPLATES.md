# Codec Implementation Template - Copy & Adapt for Each Domain

This file provides exact templates to copy and adapt when implementing each domain codec.

## Template 1: Simple Direct Pattern (KV/Queue Style)

Use this when message variants map directly to operation codes.

```rust
// src/protocol/example_codec.rs

use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};

/// Message types for example domain
pub enum ExampleMessage {
    Operation1 { field1: String, field2: u32 },
    Operation2 { field1: u64 },
}

/// Response types for example domain
pub enum ExampleResponse {
    Ok { data: Vec<u8> },
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<ExampleMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type {
        100 => parse_operation1(&mut dec),
        101 => parse_operation2(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &ExampleResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        ExampleResponse::Ok { data } => {
            enc.put_u8(0); // success flag
            enc.put_bytes(data);
        }
        ExampleResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Functions =====

fn parse_operation1(dec: &mut TlvDecoder) -> Result<ExampleMessage, String> {
    let field1 = dec.get_string()?;
    let field2 = dec.get_u32()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ExampleMessage::Operation1 { field1, field2 })
}

fn parse_operation2(dec: &mut TlvDecoder) -> Result<ExampleMessage, String> {
    let field1 = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(ExampleMessage::Operation2 { field1 })
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_operation1_when_valid_input() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_string("test");
        enc.put_u32(42);
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 100,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(ExampleMessage::Operation1 { .. })));
    }

    #[test]
    fn should_parse_operation2_when_valid_input() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(9999);
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 101,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(ExampleMessage::Operation2 { .. })));
    }

    #[test]
    fn should_error_on_incomplete_operation1() {
        // Arrange
        let payload = vec![]; // missing fields
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 100,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_error_on_unknown_operation_code() {
        // Arrange
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 999,
            payload: vec![],
        };

        // Act
        let result = parse_request(&ctx, &[]);

        // Assert
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Unknown operation"));
    }

    #[test]
    fn should_encode_ok_response() {
        // Arrange
        let response = ExampleResponse::Ok {
            data: vec![1, 2, 3],
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0); // success flag
    }

    #[test]
    fn should_encode_error_response() {
        // Arrange
        let response = ExampleResponse::Error("test error".to_string());

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 1); // error flag
    }
}
```

---

## Template 2: Wrapped Message Struct Pattern (Notice Style)

Use this when enum variants wrap dedicated message struct types.

```rust
// src/protocol/notice_codec.rs

use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};

// ===== Message Structs (defined in src/domains/notice/protocol.rs) =====
// These are already defined in the domain - just reference them

/// Wrapper enum for notice operations
pub enum NotificationMessage {
    Publish(PublishMessage),
    Subscribe(SubscribeMessage),
    Unsubscribe(UnsubscribeMessage),
    UnsubscribeAll(UnsubscribeAllMessage),
    Notify(NotifyMessage),
}

/// Response from notice operations
pub enum NoticeResponse {
    Ok { subscription_id: Option<u64> },
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<NotificationMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type {
        100 => parse_publish(&mut dec).map(NotificationMessage::Publish),
        101 => parse_subscribe(&mut dec).map(NotificationMessage::Subscribe),
        102 => parse_unsubscribe(&mut dec).map(NotificationMessage::Unsubscribe),
        103 => parse_unsubscribe_all(&mut dec).map(NotificationMessage::UnsubscribeAll),
        104 => parse_notify(&mut dec).map(NotificationMessage::Notify),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &NoticeResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        NoticeResponse::Ok { subscription_id } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*subscription_id);
        }
        NoticeResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

fn parse_publish(dec: &mut TlvDecoder) -> Result<PublishMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(PublishMessage {
        family_id,
        route,
        payload,
    })
}

fn parse_subscribe(dec: &mut TlvDecoder) -> Result<SubscribeMessage, String> {
    let family_id = dec.get_u64()?;
    let pattern = dec.get_string()?;
    let session_id = dec.get_u64()?;
    let subscriber = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(SubscribeMessage {
        family_id,
        pattern,
        session_id,
        subscriber,
    })
}

fn parse_unsubscribe(dec: &mut TlvDecoder) -> Result<UnsubscribeMessage, String> {
    let subscription_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeMessage { subscription_id })
}

fn parse_unsubscribe_all(dec: &mut TlvDecoder) -> Result<UnsubscribeAllMessage, String> {
    let session_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(UnsubscribeAllMessage { session_id })
}

fn parse_notify(dec: &mut TlvDecoder) -> Result<NotifyMessage, String> {
    let route = dec.get_string()?;
    let payload = dec.get_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(NotifyMessage { route, payload })
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_publish_when_valid_input() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(123);
        enc.put_string("route://realm/area/resource");
        enc.put_bytes(&[1, 2, 3]);
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 100,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(NotificationMessage::Publish(_))));
    }

    #[test]
    fn should_parse_subscribe_when_valid_input() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(123);
        enc.put_string("route://realm/area/*");
        enc.put_u64(456);
        enc.put_string("subscriber1");
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 101,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(NotificationMessage::Subscribe(_))));
    }

    #[test]
    fn should_error_on_incomplete_publish() {
        // Arrange
        let payload = vec![]; // missing fields
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 100,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_ok_response() {
        // Arrange
        let response = NoticeResponse::Ok {
            subscription_id: Some(42),
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0); // success flag
    }

    #[test]
    fn should_encode_error_response() {
        // Arrange
        let response = NoticeResponse::Error("subscription failed".to_string());

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 1); // error flag
    }
}
```

---

## Template 3: Session-Based Pattern (Stream Style)

Use this when operations use different addressing modes (family_id/route vs session_id).

```rust
// src/protocol/stream_codec.rs

use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};

/// Message types for stream operations
pub enum StreamMessage {
    Begin {
        family_id: u64,
        route: String,
        expected_offset: u64,
        ingest_metadata: Option<Vec<u8>>,
    },
    Append {
        session_id: u64,
        body: Vec<u8>,
        metadata: Option<Vec<u8>>,
    },
    Commit {
        session_id: u64,
        mode: String,
    },
    Rollback {
        session_id: u64,
    },
    Read {
        family_id: u64,
        route: String,
        from_offset: u64,
        limit: u32,
        max_bytes: u32,
    },
    Last {
        family_id: u64,
        route: String,
    },
    GetMetadata {
        family_id: u64,
        route: String,
    },
}

/// Response from stream operations
pub enum StreamResponse {
    Ok { session_id: Option<u64>, data: Vec<u8> },
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<StreamMessage, String> {
    let mut dec = TlvDecoder::new(payload);

    match ctx.msg_type {
        200 => parse_begin(&mut dec),
        201 => parse_append(&mut dec),
        202 => parse_commit(&mut dec),
        203 => parse_rollback(&mut dec),
        204 => parse_read(&mut dec),
        205 => parse_last(&mut dec),
        206 => parse_get_metadata(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &StreamResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();

    match response {
        StreamResponse::Ok { session_id, data } => {
            enc.put_u8(0); // success flag
            enc.put_optional_u64(*session_id);
            enc.put_bytes(data);
        }
        StreamResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }

    enc.finish()
}

// ===== Helper Parsers =====

fn parse_begin(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let expected_offset = dec.get_u64()?;
    let ingest_metadata = dec.get_optional_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Begin {
        family_id,
        route,
        expected_offset,
        ingest_metadata: ingest_metadata.map(|b| b.to_vec()),
    })
}

fn parse_append(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    // NOTE: Append uses session_id, not family_id/route
    let session_id = dec.get_u64()?;
    let body = dec.get_bytes()?;
    let metadata = dec.get_optional_bytes()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Append {
        session_id,
        body: body.to_vec(),
        metadata: metadata.map(|b| b.to_vec()),
    })
}

fn parse_commit(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    // NOTE: Commit uses session_id, not family_id/route
    let session_id = dec.get_u64()?;
    let mode = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Commit { session_id, mode })
}

fn parse_rollback(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    // NOTE: Rollback uses session_id, not family_id/route
    let session_id = dec.get_u64()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Rollback { session_id })
}

fn parse_read(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let from_offset = dec.get_u64()?;
    let limit = dec.get_u32()?;
    let max_bytes = dec.get_u32()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Read {
        family_id,
        route,
        from_offset,
        limit,
        max_bytes,
    })
}

fn parse_last(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::Last { family_id, route })
}

fn parse_get_metadata(dec: &mut TlvDecoder) -> Result<StreamMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;

    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }

    Ok(StreamMessage::GetMetadata { family_id, route })
}

// ===== Tests =====

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_begin_when_valid_input() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(123);
        enc.put_string("route://realm/area/stream1");
        enc.put_u64(0);
        enc.put_optional_bytes(None);
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 200,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(StreamMessage::Begin { .. })));
    }

    #[test]
    fn should_parse_append_with_session_id() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(456); // session_id, not family_id
        enc.put_bytes(&[1, 2, 3, 4, 5]);
        enc.put_optional_bytes(None);
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 201,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(StreamMessage::Append { session_id: 456, .. })));
    }

    #[test]
    fn should_parse_commit_with_session_id() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(456); // session_id, not family_id
        enc.put_string("sync");
        let payload = enc.finish();
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 202,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(StreamMessage::Commit { session_id: 456, .. })));
    }

    #[test]
    fn should_error_on_incomplete_begin() {
        // Arrange
        let payload = vec![]; // missing fields
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 200,
            payload: payload.clone(),
        };

        // Act
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(result.is_err());
    }

    #[test]
    fn should_encode_ok_response_with_session_id() {
        // Arrange
        let response = StreamResponse::Ok {
            session_id: Some(456),
            data: vec![1, 2, 3],
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0); // success flag
    }
}
```

---

## How to Use These Templates

1. **Copy the appropriate template** based on your domain's message structure
2. **Replace "example" with your domain name** (notice, stream, rpc, etc.)
3. **Update the enum variants** to match your domain's actual operations
4. **Update the operation codes** (100-199 for Notice, 200-299 for Stream, etc.)
5. **Update the helper parsers** to extract correct fields
6. **Run `cargo test --lib`** to verify compilation and tests pass
7. **Add 2-3 more tests** for edge cases and error conditions

## Key Checkpoints

- ✅ Enum covers all operations from protocol
- ✅ Parse function handles all operation codes
- ✅ Parse validates complete payload consumption (`is_complete()`)
- ✅ Encode function returns valid TLV bytes
- ✅ Error messages are clear and specific
- ✅ Minimum 5 unit tests per codec
- ✅ All tests use AAA (Arrange-Act-Assert) structure
- ✅ `cargo test --lib` passes with zero errors

---

**Ready to implement? Copy the template that matches your domain, update the details, and start coding!**
