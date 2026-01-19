# Codec Pattern Summary

## Unified Codec Architecture for Fitz Domains

### Quick Reference

Every domain codec follows this structure:

```rust
// Parse incoming request
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) 
    -> Result<DomainMessage, String>
{
    let mut dec = TlvDecoder::new(payload);
    match ctx.msg_type {
        OPCODE1 => parse_operation1(&mut dec),
        OPCODE2 => parse_operation2(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}

// Encode outgoing response
pub fn encode_response(response: &DomainResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    // ... match on response type and encode ...
    enc.finish()
}
```

### Domain Patterns at a Glance

| Domain | Pattern | Status | Key Difference |
|--------|---------|--------|-----------------|
| **KV** | Direct operation variants | ✅ Complete | Straightforward field extraction |
| **Queue** | Message enum with optional fields | ✅ Complete | Token-based operation tracking |
| **Notice** | Enum wrapping message structs | ⏳ Pending | Must instantiate struct → wrap in variant |
| **Stream** | Session-based state machine | ⏳ Pending | Some ops use (family_id, route), others use session_id |
| **RPC** | Request/response correlation | ⏳ Pending | Need to understand request_id pairing |
| **Lease** | Lock token management | ⏳ Pending | Need to understand TTL/renewal tracking |
| **Schedule** | Timer/cron expressions | ⏳ Pending | Need to understand scheduling format |

### Shared Utilities

All codecs use these from `src/protocol/tlv_codec.rs`:

**Encoding**:
```rust
let mut enc = TlvEncoder::new();
enc.put_u8(val);
enc.put_u16(val);
enc.put_u32(val);
enc.put_u64(val);
enc.put_string(val);
enc.put_bytes(val);
enc.put_optional_u64(Some/None);
enc.put_optional_string(Some/None);
let bytes = enc.finish();
```

**Decoding**:
```rust
let mut dec = TlvDecoder::new(payload);
let v8 = dec.get_u8()?;
let v16 = dec.get_u16()?;
let v32 = dec.get_u32()?;
let v64 = dec.get_u64()?;
let s = dec.get_string()?;
let b = dec.get_bytes()?;
let opt = dec.get_optional_u64()?;
assert!(dec.is_complete());
```

### Testing Pattern

All tests follow AAA structure:

```rust
#[test]
fn should_parse_operation_when_valid_input() {
    // Arrange
    let mut enc = TlvEncoder::new();
    enc.put_u32(42);
    let payload = enc.finish();

    // Act
    let ctx = FrameContext { msg_type: 100, /* ... */ };
    let result = parse_request(&ctx, &payload);

    // Assert
    assert!(result.is_ok());
}
```

**Rules**:
- Use `should_*` naming (NOT `test_*`)
- Tests >5 lines MUST have `// Arrange`, `// Act`, `// Assert`
- Minimum 5 tests per operation
- One behavior per test

### Implementation Checklist

For each domain codec:

- [ ] Create `src/protocol/{domain}_codec.rs`
- [ ] Define `{Domain}Message` enum (all operations)
- [ ] Define `{Domain}Response` enum (all responses)
- [ ] Implement `parse_request` function
  - [ ] Handle all operation codes
  - [ ] Call helper parsers
  - [ ] Validate complete payload consumption
- [ ] Implement `encode_response` function
  - [ ] Handle all response types
  - [ ] Use TlvEncoder for all output
- [ ] Create minimum 5 unit tests per operation
- [ ] Create E2E roundtrip test
- [ ] Run `cargo test --lib` and verify passing
- [ ] Update `CODEC_IMPLEMENTATION_PROGRESS.md`

### Domain Structure Template

**Notice Domain Example** (enum-wrapped messages):

```rust
pub enum NotificationMessage {
    Publish(PublishMessage),      // Must wrap struct
    Subscribe(SubscribeMessage),
    Unsubscribe(UnsubscribeMessage),
    UnsubscribeAll(UnsubscribeAllMessage),
    Notify(NotifyMessage),
}

fn parse_publish(dec: &mut TlvDecoder) -> Result<PublishMessage, String> {
    let family_id = dec.get_u64()?;
    let route = dec.get_string()?;
    let payload = dec.get_bytes()?;
    Ok(PublishMessage { family_id, route, payload })
}

fn parse_request(ctx: &FrameContext, payload: &[u8]) 
    -> Result<NotificationMessage, String>
{
    let mut dec = TlvDecoder::new(payload);
    match ctx.msg_type {
        100 => parse_publish(&mut dec).map(NotificationMessage::Publish),
        101 => parse_subscribe(&mut dec).map(NotificationMessage::Subscribe),
        // ...
    }
}
```

**Stream Domain Example** (session-based state):

```rust
pub enum StreamMessage {
    Begin { family_id, route, expected_offset, ingest_metadata },
    Append { session_id, body, metadata },  // ← session_id
    Commit { session_id, mode },            // ← session_id
    Rollback { session_id },                // ← session_id
    Read { family_id, route, from_offset, limit, max_bytes },
    // ...
}

fn parse_request(ctx: &FrameContext, payload: &[u8]) 
    -> Result<StreamMessage, String>
{
    let mut dec = TlvDecoder::new(payload);
    match ctx.msg_type {
        200 => {  // Begin
            let family_id = dec.get_u64()?;
            let route = dec.get_string()?;
            let expected_offset = dec.get_u64()?;
            // ... parse other fields ...
            Ok(StreamMessage::Begin { family_id, route, /* ... */ })
        },
        201 => {  // Append - uses session_id, not family_id/route
            let session_id = dec.get_u64()?;
            let body = dec.get_bytes()?;
            let metadata = dec.get_optional_bytes()?;
            Ok(StreamMessage::Append { session_id, body, metadata })
        },
        // ...
    }
}
```

### Operation Code Ranges

Reserved by domain to avoid collisions:

```
1-99:     KV domain
100-199:  Notice domain
200-299:  Stream domain
300-399:  RPC domain
400-499:  Lease domain
500-599:  Schedule domain
600-699:  Queue domain
700-799:  Reserved for expansion
```

### Common Mistakes to Avoid

❌ **Don't**: Create separate function signature per operation
```rust
// Wrong
pub fn parse_begin(...) { }
pub fn parse_append(...) { }
```

✅ **Do**: Single dispatcher with match
```rust
// Correct
pub fn parse_request(...) {
    match ctx.msg_type {
        100 => parse_begin(...),
        101 => parse_append(...),
    }
}
```

❌ **Don't**: Use async in codec
```rust
// Wrong
pub async fn parse_request(...) { }
```

✅ **Do**: Synchronous only
```rust
// Correct
pub fn parse_request(...) { }
```

❌ **Don't**: Multiple assert statements testing different inputs
```rust
// Wrong - tests 3 different inputs
assert_eq!(result1, expected1);  // Different input!
assert_eq!(result2, expected2);  // Different input!
assert_eq!(result3, expected3);  // Different input!
```

✅ **Do**: Separate test per input
```rust
// Correct - 3 focused tests
#[test]
fn should_parse_when_input1() { assert_eq!(parse(input1), expected1); }

#[test]
fn should_parse_when_input2() { assert_eq!(parse(input2), expected2); }

#[test]
fn should_parse_when_input3() { assert_eq!(parse(input3), expected3); }
```

### Current Status

✅ **Complete**:
- TLV shared utilities (290 lines, 7 tests)
- Codec trait interface (95 lines)
- KV codec (664 lines, 5+ tests per operation)
- Queue codec (370 lines, 5+ tests per operation)

⏳ **Pending**:
- Notice codec (enum-wrapped message structs)
- Stream codec (session-based state machine)
- RPC codec (request/response correlation)
- Lease codec (lock token management)
- Schedule codec (timer/cron expressions)

📊 **Test Status**: 360+ tests passing, all green

### Next Actions

1. **Analyze remaining protocols** (5-10 min)
   - RPC: `src/domains/rpc/protocol.rs`
   - Lease: `src/domains/lease/protocol.rs`
   - Schedule: `src/domains/schedule/protocol.rs`

2. **Implement 5 codecs** (40-60 min)
   - Notice adapter (enum-wrapped structs)
   - Stream adapter (session-based)
   - RPC implementation (pending analysis)
   - Lease implementation (pending analysis)
   - Schedule implementation (pending analysis)

3. **Test and verify** (10-15 min)
   - Minimum 5 tests per operation per codec
   - E2E roundtrip for each domain
   - Full test suite passes (380+)

---

**Summary**: We've converged on a unified codec pattern with shared utilities. All 7 domains now follow the same structure (parse → encode), using consistent TLV encoding/decoding. The 2 completed domains (KV, Queue) prove the pattern works. The remaining 5 domains will adapt this pattern to their specific message structures.
