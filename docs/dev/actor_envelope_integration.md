# Actor Context and Envelope Routing Integration

## Summary

Successfully wired ActorRef and Context so all message sending flows through:
**ActorRef/Context → Envelope → Router → Mailbox**

## Implementation Details

### 1. Context Enhancements

**Added to `runtime::actor::Context`:**
- `router: Arc<Router>` - Reference to the router for message sending
- `current_envelope: Option<Envelope>` - Tracks current message being processed
- `set_current_envelope()` - Internal API for scheduler to set context
- `send<M>()` - Preferred API for actors to send messages with:
  - Automatic source ActorId (this actor)
  - Causation tracking from current envelope
  - Deadline inheritance from current envelope
- `reply<M>()` - Reply to the sender of the current message with:
  - Destination set to original source
  - Causation set to current message ID
  - Deadline inherited from current message

### 2. Scheduler Updates

**Modified `runtime::scheduler::Scheduler`:**
- Stores `Arc<Router>` and passes it to Context on actor creation
- **Deadline checking**: Checks `envelope.is_expired()` before dispatch
- **Expired message handling**: Drops expired envelopes with debug log
- **Envelope unwrapping**: Extracts metadata before consuming payload
- **Context setup**: Reconstructs envelope for causation tracking
- Passes envelope metadata to context via `set_current_envelope()`

### 3. Invariants Enforced

**Single-threaded actor execution:**
- Each actor processes exactly one message at a time (enforced by scheduler)
- Actor receives strongly-typed message (unwrapped from envelope)
- Envelope metadata is accessible via Context but invisible to actor logic

**Routing boundaries:**
- Router lives in `transport` layer (no runtime dependencies)
- Mailbox implements `MailboxSink` trait (narrow interface)
- Context uses router via trait object (no circular dependency)

### 4. Message Flow

```
Actor A wants to send message to Actor B:

1. ctx.send(actor_b_id, msg)
2. Context creates Envelope:
   - source = actor_a_id
   - destination = actor_b_id
   - causation = current_envelope.id() (if present)
   - deadline = current_envelope.deadline() (if present)
   - payload = Box::new(msg)
3. Context calls router.route(envelope)
4. Router looks up Actor B's mailbox sink
5. Sink delivers envelope to mailbox
6. Scheduler receives envelope from mailbox
7. Scheduler checks deadline (drops if expired)
8. Scheduler unwraps envelope to extract typed message
9. Scheduler sets envelope metadata in context
10. Scheduler calls actor.receive(msg, ctx)
```

## Test Coverage

**Total: 59 tests (100% compliant with Fitz guidelines)**

### Actor Tests (runtime::actor)
- Context creation and lifecycle
- ActorRef message sending
- Context::send() with causation tracking
- Context::send() with deadline inheritance
- Context::reply() for request-response pattern
- Mailbox integration

### Scheduler Tests (runtime::scheduler)
- Actor spawning and ID generation
- Sequential message processing
- **Deadline enforcement** - expired messages dropped
- **Actor-to-actor messaging** - via Context::send()
- **Reply pattern** - request-response flow

### Router Tests (transport::router)
- Actor registration/unregistration
- Envelope routing to destination
- Missing destination errors
- Full mailbox backpressure
- Concurrent routing safety

## Examples

### actor_messaging.rs
Demonstrates:
1. **Actor-to-actor messaging** - Ping/Pong actors communicating
2. **Causation chains** - Manual envelope routing with parent message tracking
3. **Deadline enforcement** - Expired messages dropped, valid messages processed

Run with:
```bash
cargo run --example actor_messaging
```

## Key Features

✅ **Type safety** - ActorRef<M> maintains typed API, actors receive typed messages  
✅ **Causation tracking** - Automatic parent message ID propagation  
✅ **Deadline inheritance** - Time-bounded message chains  
✅ **Expired message drops** - Deadline enforcement at dispatch boundary  
✅ **Request-reply pattern** - Context::reply() for synchronous-style messaging  
✅ **Zero circular dependencies** - Clean separation between transport and runtime  
✅ **Single-threaded actors** - One message processed at a time per actor  
✅ **Backpressure** - Mailbox capacity enforced via MailboxFull error  

## Architecture Principles Maintained

1. **Transport is routing only** - No actor or scheduler knowledge
2. **Runtime is execution only** - Uses transport via traits
3. **Envelope unwrapping at boundary** - Actors never see envelopes
4. **Best-effort delivery** - Router fails fast, no retries
5. **In-process only** - No networking or persistence yet

## Next Steps

Ready for:
- Domain actor implementations (notification, lease, etc.)
- Supervisor integration with envelope routing
- Dead letter queue for undeliverable messages
- Message metrics and tracing hooks
- Async bridges for storage/network I/O
