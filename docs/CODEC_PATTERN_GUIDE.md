# Domain Codec Pattern Guide

## Overview

All Fitz domain codecs follow a unified pattern to ensure consistency, maintainability, and testability. This document describes the standard approach and documents domain-specific variations.

## Core Pattern

Every domain codec consists of:

1. **Message Type** - An enum or struct representing parsed domain messages
2. **Parse Function** - Decode TLV bytes → domain message
3. **Encode Function** - Domain response → TLV bytes
4. **Error Handling** - Clear, consistent error messages
5. **Unit Tests** - Minimum 5 tests per operation

### Template Structure

```rust
// src/protocol/domain_codec.rs

use crate::frame_context::FrameContext;
use crate::protocol::tlv_codec::{TlvDecoder, TlvEncoder};

/// Message type for this domain
pub enum DomainMessage {
    Operation1 { field1: String, field2: u32 },
    Operation2 { field1: u64 },
    // ...
}

/// Response type for this domain
pub enum DomainResponse {
    Ok { data: Vec<u8> },
    Error(String),
}

/// Parse incoming message from TLV-encoded bytes
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<DomainMessage, String> {
    let mut dec = TlvDecoder::new(payload);
    
    match ctx.msg_type {
        100 => parse_operation1(&mut dec),
        101 => parse_operation2(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}

/// Encode domain response to TLV-encoded bytes
pub fn encode_response(response: &DomainResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    
    match response {
        DomainResponse::Ok { data } => {
            enc.put_bytes(data);
        }
        DomainResponse::Error(e) => {
            enc.put_u8(1); // error flag
            enc.put_string(e);
        }
    }
    
    enc.finish()
}

// Helper parsers
fn parse_operation1(dec: &mut TlvDecoder) -> Result<DomainMessage, String> {
    let field1 = dec.get_string()?;
    let field2 = dec.get_u32()?;
    
    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }
    
    Ok(DomainMessage::Operation1 { field1, field2 })
}

fn parse_operation2(dec: &mut TlvDecoder) -> Result<DomainMessage, String> {
    let field1 = dec.get_u64()?;
    
    if !dec.is_complete() {
        return Err("Trailing data in message".to_string());
    }
    
    Ok(DomainMessage::Operation2 { field1 })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_parse_operation1() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_string("test");
        enc.put_u32(42);
        let payload = enc.finish();

        // Act
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 100,
            payload: payload.clone(),
        };
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(DomainMessage::Operation1 { .. })));
    }

    #[test]
    fn should_parse_operation2() {
        // Arrange
        let mut enc = TlvEncoder::new();
        enc.put_u64(9999);
        let payload = enc.finish();

        // Act
        let ctx = FrameContext {
            session_id: 1,
            channel_id: 1,
            msg_type: 101,
            payload: payload.clone(),
        };
        let result = parse_request(&ctx, &payload);

        // Assert
        assert!(matches!(result, Ok(DomainMessage::Operation2 { .. })));
    }

    #[test]
    fn should_encode_ok_response() {
        // Arrange
        let response = DomainResponse::Ok {
            data: vec![1, 2, 3],
        };

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_encode_error_response() {
        // Arrange
        let response = DomainResponse::Error("test error".to_string());

        // Act
        let encoded = encode_response(&response);

        // Assert
        assert!(!encoded.is_empty());
    }

    #[test]
    fn should_reject_unknown_operation() {
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
    }
}
```

## Domain-Specific Variations

### KV Domain (Reference Implementation)

**Pattern**: Direct enum variants with operation codes
**File**: `src/protocol/kv_codec.rs` (664 lines)
**Operations**: 9 (BEGIN, COMMIT, ROLLBACK, GET, PUT, INSERT, DELETE, DELETE_RANGE, SCAN)
**Characteristics**:
- Operation code → direct message variant
- Simple field extraction (resource, key, value)
- Straightforward response encoding
- Status: ✅ Complete

### Queue Domain (Proven Pattern)

**Pattern**: Message enum with optional fields and batch operations
**File**: `src/protocol/queue_codec.rs` (370 lines)
**Operations**: 5 (ENQUEUE, ENQUEUE_BATCH, RESERVE, EXTEND, COMPLETE)
**Characteristics**:
- Operation variants with optional delay_seconds
- Token-based dequeue tracking
- Batch parameter support
- Status: ✅ Complete

### Notice Domain (Enum-Wrapped Message Structs)

**Pattern**: Enum wrapping dedicated message type structs
**Protocol**: `src/domains/notice/protocol.rs`
**Message Structure**:
```rust
pub enum NotificationMessage {
    Publish(PublishMessage),           // family_id, route, payload
    Subscribe(SubscribeMessage),       // family_id, pattern, session_id, subscriber
    Unsubscribe(UnsubscribeMessage),   // subscription_id
    UnsubscribeAll(UnsubscribeAllMessage), // session_id
    Notify(NotifyMessage),             // route, payload
}
```
**Codec Adaptation**:
1. Decode operation type (100-104)
2. Match variant and instantiate wrapper
3. Return `NotificationMessage::Publish(PublishMessage { ... })`
4. Encoding: Match variant and serialize inner message
**Status**: ⏳ Pending implementation

### Stream Domain (Session-Based State Machine)

**Pattern**: Enum with session-based state management
**Protocol**: `src/domains/stream/protocol.rs`
**Message Structure**:
```rust
pub enum StreamMessage {
    Begin { family_id, route, expected_offset, ingest_metadata },
    Append { session_id, body, metadata },        // session-based
    Commit { session_id, mode },                  // session-based
    Rollback { session_id },                      // session-based
    Read { family_id, route, from_offset, limit, max_bytes },
    Last { family_id, route },
    GetMetadata { family_id, route },
}
```
**Codec Adaptation**:
1. Decode operation type (200-207)
2. For Begin: Create new session, parse fields
3. For Append/Commit/Rollback: Use session_id from context
4. Parse session-specific parameters
5. Return appropriate `StreamMessage` variant
**Key Difference**: Session ID replaces family_id/route in certain operations
**Status**: ⏳ Pending implementation

### RPC Domain (Request/Response Correlation)

**Pattern**: Request ID-based message pairing
**Protocol**: `src/domains/rpc/protocol.rs`
**Expected Structure**: (Analysis pending)
**Codec Adaptation** (Expected):
1. Decode operation type
2. Extract request_id for correlation
3. Parse parameters from payload
4. Responses will use request_id to match reply
**Status**: ⏳ Protocol analysis required

### Lease Domain (Lock-Based with Tokens)

**Pattern**: Lock acquisition, renewal, surrender
**Protocol**: `src/domains/lease/protocol.rs`
**Expected Structure**: (Analysis pending)
**Codec Adaptation** (Expected):
1. Decode operation type
2. Handle acquire: create token, set TTL
3. Handle renew: validate token, extend TTL
4. Handle surrender: release token
**Status**: ⏳ Protocol analysis required

### Schedule Domain (Timer/Cron Scheduling)

**Pattern**: Delayed or recurring execution
**Protocol**: `src/domains/schedule/protocol.rs`
**Expected Structure**: (Analysis pending)
**Codec Adaptation** (Expected):
1. Decode operation type
2. Parse delay_seconds or cron expression
3. Map to internal scheduler
4. Return confirmation with schedule ID
**Status**: ⏳ Protocol analysis required

## Shared Utilities

### TlvEncoder

Encode messages to binary:

```rust
let mut enc = TlvEncoder::new();
enc.put_u8(42);
enc.put_string("hello");
enc.put_bytes(&[1, 2, 3]);
enc.put_optional_u64(Some(100));
let bytes = enc.finish();
```

### TlvDecoder

Decode binary to messages:

```rust
let mut dec = TlvDecoder::new(payload);
let num = dec.get_u8()?;
let s = dec.get_string()?;
let data = dec.get_bytes()?;
let opt = dec.get_optional_u64()?;
assert!(dec.is_complete());
```

## Testing Pattern

All codecs follow the **AAA (Arrange-Act-Assert)** pattern:

```rust
#[test]
fn should_parse_operation_when_valid_input() {
    // Arrange
    let mut enc = TlvEncoder::new();
    enc.put_u32(42);
    let payload = enc.finish();

    // Act
    let result = parse_request(&ctx, &payload);

    // Assert
    assert!(result.is_ok());
}
```

**Mandatory Structure for Tests >5 Lines**:
- Exactly 3 sections: `// Arrange`, `// Act`, `// Assert`
- One behavior per test
- Clear variable names
- Use `assert_eq!` for equality, `assert!` for boolean conditions

## Validation Checklist

Before committing a codec:

- [ ] Message enum covers all operations from protocol
- [ ] Parse function handles all variants
- [ ] Parse validates complete payload consumption
- [ ] Encode function returns valid TLV bytes
- [ ] Error messages are clear and specific
- [ ] Minimum 5 unit tests per operation
- [ ] All tests follow AAA pattern
- [ ] Tests pass with `cargo test --lib`
- [ ] E2E test demonstrates roundtrip (parse + encode)
- [ ] Documentation updated with operation codes

## Operation Codes

Reserved operation type ranges:

- **1-99**: KV domain (reserved)
- **100-199**: Notice domain (reserved)
- **200-299**: Stream domain (reserved)
- **300-399**: RPC domain (reserved)
- **400-499**: Lease domain (reserved)
- **500-599**: Schedule domain (reserved)
- **600-699**: Queue domain (reserved)
- **700-799**: Other domains (expansion room)

## Integration with DomainSink

All codecs are used by DomainSink pattern:

```rust
// In src/boot/domains.rs
impl DomainSink for MyDomainSink {
    fn handle(&self, envelope: Envelope) -> Result<Envelope, String> {
        // Extract context
        let ctx = FrameContext::from_envelope(&envelope)?;
        
        // Parse message
        let msg = my_codec::parse_request(&ctx, &ctx.payload)?;
        
        // Handle message (sync domain logic)
        let response = self.process(msg)?;
        
        // Encode response
        let response_bytes = my_codec::encode_response(&response);
        
        // Return via envelope
        envelope.reply_to(response_bytes)
    }
}
```

## Next Steps

1. **Analyze RPC protocol** → Understand request/response structure
2. **Analyze Lease protocol** → Understand token/TTL management
3. **Analyze Schedule protocol** → Understand timing expressions
4. **Implement remaining 5 codecs** using adapted pattern
5. **Test each codec** with minimum 5 unit tests + E2E roundtrip
6. **Verify compilation** and all tests passing

## References

- [KV Codec Implementation](../src/protocol/kv_codec.rs) - Reference implementation
- [Queue Codec Implementation](../src/protocol/queue_codec.rs) - Proven pattern
- [TLV Utilities](../src/protocol/tlv_codec.rs) - Shared encoding/decoding
- [Codec Trait](../src/protocol/codec_trait.rs) - Interface definition
- [Frame Context](../src/protocol/frame_context.rs) - Message metadata carrier
