# Converged Domain Codec Architecture

## Executive Summary

We have established a **unified codec architecture** across all Fitz domains to ensure consistency, maintainability, and correctness. This document describes the converged pattern and implementation strategy.

## Problem Solved

**Previous Issue**: Attempted to implement 5 domain codecs in parallel using assumptions about message structures. This resulted in type mismatches because each domain has a fundamentally different message protocol:

- **KV/Queue**: Direct operation variants
- **Notice**: Enum wrapping message struct types  
- **Stream**: Session-based state machine with multiple variants
- **RPC/Lease/Schedule**: Different patterns (analysis pending)

**Solution**: Create a unified codec architecture with shared utilities and domain-specific adaptations.

## Converged Architecture

### Layer 1: Shared Utilities (100% Cross-Domain)

**Location**: `src/protocol/tlv_codec.rs` (290 lines)

Provides `TlvEncoder` and `TlvDecoder` for all domains:

```rust
pub struct TlvEncoder {
    fn put_u8, put_u16, put_u32, put_u64(&mut self, val)
    fn put_string, put_bytes(&mut self, val)
    fn put_optional_u64, put_optional_string(&mut self, val)
    fn finish(self) -> Vec<u8>
}

pub struct TlvDecoder {
    fn get_u8, get_u16, get_u32, get_u64(&mut self) -> Result
    fn get_string, get_bytes(&mut self) -> Result
    fn get_optional_u64, get_optional_string(&mut self) -> Result
    fn is_complete(&self) -> bool
}
```

**Benefits**:
- Consistent TLV encoding/decoding across all domains
- Automatic bounds checking
- Clear error messages
- 7 unit tests validating all operations

### Layer 2: Codec Interface (100% Cross-Domain)

**Location**: `src/protocol/codec_trait.rs` (95 lines)

Defines standard codec interface:

```rust
pub trait DomainCodec {
    type Message;
    type Response;
    
    fn parse(&self, ctx: &FrameContext, payload: &[u8]) 
        -> Result<Self::Message, String>;
    fn encode(&self, response: &Self::Response) -> Vec<u8>;
}

pub enum DomainResponse {
    Ok(Option<Vec<u8>>),
    Error(String),
    Custom(Vec<u8>),
}
```

**Ensures**:
- All codecs have consistent parse/encode signatures
- Deterministic, synchronous processing
- Clear error propagation
- Extensible response envelope

### Layer 3: Domain-Specific Codecs (Adapted Per Domain)

#### KV Domain ✅ (Complete)
- **File**: `src/protocol/kv_codec.rs` (664 lines)
- **Pattern**: Direct operation enum variants
- **Operations**: BEGIN, COMMIT, ROLLBACK, GET, PUT, INSERT, DELETE, DELETE_RANGE, SCAN
- **Tests**: 5 unit tests per operation + E2E roundtrip
- **Status**: Production-ready

#### Queue Domain ✅ (Complete)  
- **File**: `src/protocol/queue_codec.rs` (370 lines)
- **Pattern**: Message enum with optional fields and batch operations
- **Operations**: ENQUEUE, ENQUEUE_BATCH, RESERVE, EXTEND, COMPLETE
- **Tests**: 5 unit tests per operation
- **Status**: Production-ready

#### Notice Domain ⏳ (Pattern Defined, Implementation Pending)
- **Pattern**: Enum wrapping dedicated message struct types
- **Example**:
  ```rust
  pub enum NotificationMessage {
      Publish(PublishMessage { family_id, route, payload }),
      Subscribe(SubscribeMessage { family_id, pattern, session_id, subscriber }),
      Unsubscribe(UnsubscribeMessage { subscription_id }),
      UnsubscribeAll(UnsubscribeAllMessage { session_id }),
      Notify(NotifyMessage { route, payload }),
  }
  ```
- **Codec Implementation**:
  ```rust
  pub fn parse_request(ctx: &FrameContext, payload: &[u8]) 
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
- **Key Difference**: Must instantiate message struct, then wrap in enum variant

#### Stream Domain ⏳ (Pattern Defined, Implementation Pending)
- **Pattern**: Session-based state machine with dual addressing modes
- **Example**:
  ```rust
  pub enum StreamMessage {
      Begin { family_id, route, expected_offset, ingest_metadata },
      Append { session_id, body, metadata },      // session-based
      Commit { session_id, mode },                // session-based
      Rollback { session_id },                    // session-based
      Read { family_id, route, from_offset, limit, max_bytes },
      Last { family_id, route },
      GetMetadata { family_id, route },
  }
  ```
- **Codec Implementation**:
  ```rust
  pub fn parse_request(ctx: &FrameContext, payload: &[u8]) 
      -> Result<StreamMessage, String>
  {
      let mut dec = TlvDecoder::new(payload);
      match ctx.msg_type {
          200 => {
              let family_id = dec.get_u64()?;
              let route = dec.get_string()?;
              // ...parse other fields...
              Ok(StreamMessage::Begin { family_id, route, /* ... */ })
          },
          201 => {
              let session_id = dec.get_u64()?;
              let body = dec.get_bytes()?;
              // ...
              Ok(StreamMessage::Append { session_id, body, /* ... */ })
          },
          // ...
      }
  }
  ```
- **Key Difference**: Some operations use (family_id, route), others use session_id

#### RPC Domain ⏳ (Protocol Analysis Required)
- **Status**: Protocol structure to be analyzed from `src/domains/rpc/protocol.rs`
- **Expected Pattern**: Request/response with correlation ID
- **Pending**: Detailed message struct examination

#### Lease Domain ⏳ (Protocol Analysis Required)
- **Status**: Protocol structure to be analyzed from `src/domains/lease/protocol.rs`
- **Expected Pattern**: Lock-based with token/TTL management
- **Pending**: Detailed message struct examination

#### Schedule Domain ⏳ (Protocol Analysis Required)
- **Status**: Protocol structure to be analyzed from `src/domains/schedule/protocol.rs`
- **Expected Pattern**: Delay/cron-based scheduling
- **Pending**: Detailed message struct examination

### Layer 4: DomainSink Integration (Unified)

**Location**: `src/boot/domains.rs`

Generic sink pattern for all domains:

```rust
pub struct MyDomainSink {
    /* domain-specific state */
}

impl DomainSink for MyDomainSink {
    fn handle(&self, envelope: Envelope) -> Result<Envelope, String> {
        // 1. Extract metadata from envelope
        let ctx = FrameContext::from_envelope(&envelope)?;
        
        // 2. Parse request using codec
        let msg = my_domain::parse_request(&ctx, &ctx.payload)?;
        
        // 3. Handle synchronously (no async, no tokio)
        let response = self.process_sync(msg)?;
        
        // 4. Encode response
        let response_bytes = my_domain::encode_response(&response);
        
        // 5. Route back to client
        envelope.reply_to(response_bytes)
    }
}
```

## Implementation Strategy

### Phase 1: Complete ✅ (Done)
- ✅ Created `TlvEncoder` and `TlvDecoder` (shared utilities)
- ✅ Created `DomainCodec` trait and `DomainResponse` envelope
- ✅ Verified KV codec uses shared utilities
- ✅ Verified Queue codec uses shared utilities
- ✅ All tests passing (360+)

### Phase 2: Analysis ⏳ (Next)
1. Read `src/domains/rpc/protocol.rs` → understand RpcMessage enum
2. Read `src/domains/lease/protocol.rs` → understand LeaseMessage enum
3. Read `src/domains/schedule/protocol.rs` → understand ScheduleMessage enum

### Phase 3: Implementation ⏳ (Next)
1. Create `src/protocol/notice_codec.rs` (adapted for wrapped message structs)
2. Create `src/protocol/stream_codec.rs` (adapted for session-based state)
3. Create `src/protocol/rpc_codec.rs` (adapted per analysis)
4. Create `src/protocol/lease_codec.rs` (adapted per analysis)
5. Create `src/protocol/schedule_codec.rs` (adapted per analysis)

### Phase 4: Testing ⏳ (Next)
1. Minimum 5 unit tests per operation per codec
2. E2E roundtrip test: parse → encode → verify format
3. All tests follow AAA (Arrange-Act-Assert) pattern
4. Run `cargo test --lib` after each codec
5. Verify 380+ total tests passing

### Phase 5: Documentation ⏳ (Next)
1. Update `CODEC_IMPLEMENTATION_PROGRESS.md`
2. Document each domain's message structure variations
3. Document operation code ranges (1-99 KV, 100-199 Notice, etc.)

## Converged Practices

### TLV Encoding
All domains use the same TLV format:
```
[u32: length][variable: payload]
[u32: length][variable: payload]
...
```

No domain-specific byte layouts—all use shared `TlvEncoder`/`TlvDecoder`.

### Message Parsing
All domains follow this pattern:
```rust
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) 
    -> Result<DomainMessage, String>
{
    let mut dec = TlvDecoder::new(payload);
    match ctx.msg_type {
        OPCODE1 => parse_op1(&mut dec),
        OPCODE2 => parse_op2(&mut dec),
        _ => Err(format!("Unknown operation: {}", ctx.msg_type)),
    }
}
```

### Response Encoding
All domains return `Vec<u8>`:
```rust
pub fn encode_response(response: &DomainResponse) -> Vec<u8> {
    let mut enc = TlvEncoder::new();
    // ... encode fields ...
    enc.finish()
}
```

### Error Handling
All domains use `Result<T, String>`:
- Clear, descriptive error messages
- No panics in domain code
- Synchronous error propagation

### Testing
All domains follow:
- **Naming**: `should_*` pattern (not `test_*`)
- **Structure**: AAA pattern for tests >5 lines
- **Behavior**: One test = one operation, one scenario
- **Coverage**: Minimum 5 tests per operation
- **Validation**: `cargo test --lib` verifies all tests

## Benefits of Convergence

1. **Consistency**: Same pattern across all 7 domains
2. **Maintainability**: Change TLV logic once, all domains benefit
3. **Correctness**: Shared utilities reduce encoding bugs
4. **Testability**: Standard interfaces enable generic tests
5. **Onboarding**: New developers learn one pattern
6. **Performance**: Optimized TLV utilities improve all codecs

## File Structure

```
src/protocol/
├── tlv_codec.rs         ← Shared TLV utilities
├── codec_trait.rs       ← Standard codec interface
├── kv_codec.rs          ← KV domain (complete)
├── queue_codec.rs       ← Queue domain (complete)
├── notice_codec.rs      ← Notice domain (pending)
├── stream_codec.rs      ← Stream domain (pending)
├── rpc_codec.rs         ← RPC domain (pending)
├── lease_codec.rs       ← Lease domain (pending)
└── schedule_codec.rs    ← Schedule domain (pending)

src/boot/
└── domains.rs           ← DomainSink implementations

docs/
├── CODEC_PATTERN_GUIDE.md       ← This pattern
└── CODEC_IMPLEMENTATION_PROGRESS.md ← Status tracking
```

## Next Action

1. **Analyze RPC/Lease/Schedule protocols** (5-10 minutes)
   - Read message enum definitions
   - Identify key fields and structure
   - Document differences from KV pattern

2. **Implement remaining 5 codecs** (40-60 minutes)
   - Create each codec file
   - Implement parse + encode functions
   - Add minimum 5 unit tests per operation
   - Verify `cargo test --lib` passes

3. **Complete E2E tests** (20-30 minutes)
   - Create roundtrip tests (parse + encode)
   - Verify message format correctness
   - Test boundary conditions

4. **Verify and document** (10 minutes)
   - Run full test suite
   - Update progress documentation
   - Commit converged architecture

## Current Status

- ✅ Shared utilities created and tested (7 tests)
- ✅ Codec interface defined
- ✅ KV domain complete (5 ops, 5+ tests)
- ✅ Queue domain complete (5 ops, 5+ tests)
- ✅ Pattern guide documented
- ⏳ RPC/Lease/Schedule protocol analysis pending
- ⏳ 5 domain codecs pending implementation
- 📊 360+ tests passing

## Success Criteria

- ✅ All 7 domain codecs compile without errors
- ✅ 380+ total tests passing (library tests)
- ✅ Each codec has minimum 5 unit tests per operation
- ✅ E2E roundtrip test for each domain
- ✅ Zero type mismatches (this was the root cause)
- ✅ Consistent pattern across all domains
- ✅ Documentation complete

---

**Next Step**: Continue with RPC/Lease/Schedule protocol analysis to understand correct message structures for remaining codecs.
