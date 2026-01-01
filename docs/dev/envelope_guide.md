# Transport Layer: Envelope

## Overview

The `Envelope` type provides domain-agnostic message routing, tracing, and observability metadata without changing actor semantics.

## Location

```
src/transport/envelope.rs
```

## Key Types

### `Envelope`

Immutable wrapper around actor messages with routing metadata:

```rust
pub struct Envelope {
    id: MessageId,                              // Unique identifier
    source: Option<ActorId>,                    // Sender (if known)
    destination: ActorId,                       // Recipient
    causation: Option<MessageId>,               // Parent message ID
    deadline: Option<Instant>,                  // Processing deadline
    payload: Box<dyn Any + Send + Sync>,        // Type-erased message
}
```

### `MessageId`

Unique message identifier for correlation and tracing:

```rust
pub struct MessageId(u64);  // Atomically generated
```

## Design Principles

1. **Type Erasure** - Payload is `Box<dyn Any>` so envelopes can carry any message type
2. **Immutability** - Envelopes cannot be modified after creation
3. **Actor Transparency** - Actors receive typed messages; envelope handling is in the runtime
4. **In-Process Only** - No networking concerns yet (future work)

## Constructor Methods

```rust
// Basic envelope
Envelope::new(destination, payload)

// With known source
Envelope::from_actor(source, destination, payload)

// Add deadline
envelope.with_deadline(instant)

// Add causation
envelope.with_causation(parent_id)

// Create reply (reverses source/dest, inherits deadline)
envelope.reply_to(response_payload)
```

## Usage Patterns

### Request/Reply

```rust
// Service receives request
let request = Envelope::from_actor(client_id, service_id, "Get data");

// Service sends reply
let reply = request.reply_to("Here's your data");
// reply.source() == service_id
// reply.destination() == client_id
// reply.causation() == request.id()
```

### Deadline Enforcement

```rust
let deadline = Instant::now() + Duration::from_secs(5);
let urgent = Envelope::new(actor_id, msg).with_deadline(deadline);

if urgent.is_expired() {
    // Drop or log warning
}
```

### Distributed Tracing

```rust
let parent = Envelope::new(actor_id, "Start work");
let child = Envelope::new(actor_id, "Sub-task")
    .with_causation(parent.id());
// Now we can trace parent -> child relationships
```

### Payload Extraction

```rust
// Borrow payload
if let Some(msg) = envelope.payload::<MyMessage>() {
    // Process msg
}

// Consume envelope
if let Some(msg) = envelope.into_payload::<MyMessage>() {
    // Take ownership of msg
}
```

## Integration Points

### Runtime (Future)

The scheduler will:
1. Wrap outgoing messages in envelopes
2. Check deadlines before delivery
3. Track causation chains for debugging
4. Route based on destination ActorId

### Domains (Future)

Domain handlers will:
1. Receive envelopes (not raw messages)
2. Extract typed payloads
3. Create reply envelopes
4. Propagate deadlines and causation

### Observability (Future)

Metrics/tracing systems will:
1. Log message IDs for correlation
2. Track causation chains
3. Measure message latency
4. Alert on deadline violations

## Test Coverage

**11 tests** (100% guideline compliant):

- ✅ Envelope creation (with/without source)
- ✅ Deadline setting and expiration
- ✅ Causation tracking
- ✅ Reply envelope generation
- ✅ Payload extraction and type safety
- ✅ MessageId uniqueness and formatting
- ✅ Deadline inheritance in replies

Run tests:
```bash
cargo test transport::envelope
```

Run example:
```bash
cargo run --example envelope_basics
```

## Future Work

1. **Remote Envelopes** - Serialization for network transport
2. **Routing Integration** - Connect to RouterActor
3. **Observability** - Metrics and tracing hooks
4. **Priority Levels** - Message prioritization
5. **TTL** - Time-to-live separate from deadline
6. **Retry Metadata** - Attempt counts, backoff info

## Status

✅ **Complete** - Ready for domain integration

- Core envelope type implemented
- Request/reply pattern supported
- Deadline tracking functional
- Causation chains working
- Type-safe payload extraction
- Comprehensive test coverage
