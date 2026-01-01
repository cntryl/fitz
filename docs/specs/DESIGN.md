# Fitz Design Document

## Data Flow Architecture: Transport → Engine → Domain

### Overview

Fitz uses a **strict async-at-the-edges, sync-in-the-core** architecture to maximize throughput, minimize jitter, and maintain deterministic ordering. This document describes how data flows from WebSocket connections through the synchronous engine to domain handlers and back.

### Terminology

Before diving into architecture, let's establish precise terminology:

- **Connection** (`conn_id: u64`): A physical WebSocket TCP connection. One client socket = one connection. Assigned atomically at accept time. Immutable for the connection's lifetime.

- **Channel** (`channel_id: u32`): A logical multiplexed stream within a connection. Each frame header contains a `channel_id`. One connection can carry multiple channels. Channels enable request/response correlation, subscription isolation, and route-family isolation over a single connection.

- **Session**: Authentication and authorization state bound to a connection at upgrade time. Contains JWT claims, route-family identity (`route_family`), and permission grants. One session per connection.

- **Route Family** (`route_family: String`): Isolation boundary identifier extracted from JWT or connection context. Used for shard selection and isolation. Maps to storage partitions. Example: `"acme-corp"`, `"family-123"`.

- **Shard**: An engine instance (one OS thread) processing events for a subset of connections. Shard assignment is deterministic by `route_family`. All connections for a route family go to the same shard.

- **Inbox**: A temporary RPC reply route allocated per request (e.g., `inbox://realm/uuid`). Owned by the requesting channel. Replies to this route are delivered back to the owner's channel on the owner's connection.

- **Subscription** (`sub_id: u64`): A notice pattern registration. Identified by subscriber's `channel_id` + pattern. When a publish matches, notifications go to the subscriber's channel on their connection.

**Key Insight**: `conn_id` identifies the transport pipe; `channel_id` identifies the logical conversation within that pipe. Engine routes responses by `channel_id`, which requires a `channel_id → conn_id` mapping to locate the physical connection for delivery.

---

### Key Principles (Target Implementation)

1. **Async Transport Layer**: WebSocket I/O is async (tokio + tungstenite).
2. **Sync Engine Core**: Message routing and domain dispatch is 100% synchronous; no futures, no `.await`, no tokio primitives in the engine.
3. **Sync Domain Logic**: All domain and service code runs synchronously; domains never perform I/O or spawn tasks.
4. **Queue-Based Boundaries**: Bounded queues (channels or SPSC ring buffers) bridge async→sync (WS → engine) and sync→async (engine → WS) with explicit backpressure.
5. **SPSC for Hot Paths**: Per-connection outbound and per-shard inbound use bounded SPSC ring buffers for predictable latency and cache friendliness.
6. **Zero-Copy Where Practical**: Pooled buffers and stack-allocated SmallVecs minimize allocations, but clarity is preferred over premature micro-optimizations.
7. **Shardable Core**: The engine runs as one or many shards; routing to shards is explicit and deterministic.

---

## Transport Layer Options

Fitz supports multiple transport protocols, all following the same async-edge/sync-core pattern:

### WebSocket Transport (`ws.rs`)
- **Protocol**: HTTP upgrade to WebSocket with FTZ binary frames
- **Authentication**: JWT in `Authorization` header or `Sec-WebSocket-Protocol` field
- **Multiplexing**: Manual via FTZ channel_id in frame headers
- **Use Case**: Web browsers, HTTP-compatible environments
- **Port**: Configurable via `FITZ_WS_PORT` (default: 8080)

### TCP Transport (`tcp.rs`)
- **Protocol**: Direct TCP with FTZ framing (length-prefixed)
- **Authentication**: NO_AUTH mode or first-frame auth (TLS optional)
- **Multiplexing**: Manual via FTZ channel_id in frame headers
- **Use Case**: Native clients, low-overhead backend services
- **Port**: Configurable via `FITZ_TCP_PORT` (default: 7070)

### Transport Comparison

| Feature | WebSocket | TCP | QUIC |
|---------|-----------|-----|------|
| Browser support | ✅ Native | ❌ No | ⚠️ WebTransport |
| Multiplexing | Manual (FTZ) | Manual (FTZ) | **Native** |
| Head-of-line blocking | Yes (TCP) | Yes | **No** |
| Connection migration | No | No | **Yes** |
| 0-RTT reconnection | No | No | **Yes** |
| Encryption | TLS upgrade | Optional | **Built-in** |
| NAT traversal | Good | Good | **Better (UDP)** |

### All Transports Share:
- Same `EngineHandle::on_frame(conn_id, bytes)` interface
- Identical session registration flow
- Per-frame authorization in engine core
- Transport-agnostic domain handlers

---

## Inbound Data Flow (Client → Server)

```
┌─────────────┐
│ WebSocket   │  ← Async (tokio-tungstenite)
│ Connection  │
└──────┬──────┘
       │ Binary frame arrives
       ↓
┌──────────────────────────────────────────────┐
│ ws.rs: handle_connection()                   │
│ - Async task per connection                  │
│ - Reads frames via StreamExt::next()         │
│ - Extracts binary payload                    │
└──────┬───────────────────────────────────────┘
    │ engine.on_frame(conn_id, bytes)
    ↓
┌──────────────────────────────────────────────┐
│ EngineHandle::on_frame()                     │
│ - Wraps bytes in EngineEvent::Frame          │
│ - Enqueues into shard SPSC inbox             │
└──────┬───────────────────────────────────────┘
    │ Bounded SPSC inbox (async → sync boundary)
    ↓
┌──────────────────────────────────────────────┐
│ Engine Event Loop (SYNC, per shard)          │
│ - Runs in dedicated thread per shard         │
│ - Dequeues from shard-local SPSC inbox       │
│ - Deterministic, single-threaded processing  │
└──────┬───────────────────────────────────────┘
       │ Parse route + build DomainContext
       ↓
┌──────────────────────────────────────────────┐
│ DomainRegistry::dispatch()                   │
│ - Route parsing (kv://, rpc://, etc.)        │
│ - Domain lookup by scheme                    │
│ - Authorization check                        │
└──────┬───────────────────────────────────────┘
       │ Call domain.handle(context)
       ↓
┌──────────────────────────────────────────────┐
│ Domain Handler (SYNC)                        │
│ - Parse TLV payload                          │
│ - Call service methods                       │
│ - Build TLV response                         │
│ - Return DomainResponse                      │
└──────┬───────────────────────────────────────┘
       │ DomainResponse enum
       ↓
       Continue to Outbound Flow...
```

### Detailed Inbound Steps

#### 1. WebSocket Layer (Async)
**File**: `src/transport/ws.rs`

- Tokio task spawned per connection
- `ws_stream.next().await` yields incoming frames
- Only `Message::Binary` frames are processed
- Connection ID assigned via atomic counter

```rust
// In handle_connection()
msg = ws_stream.next() => {
    match msg {
        Some(Ok(Message::Binary(bytes))) => {
            engine.on_frame(conn_id, bytes);  // ← Handoff to sync world
        }
        // ... handle close, errors
    }
}
```

#### 2. Engine Handle Boundary (Async → Sync)
**File**: `src/core/engine.rs`

- `EngineHandle` is held by each WebSocket task.
- `EngineHandle` is logically tied to **one engine shard**. All frames for a given connection go to exactly one shard.
- `on_frame()` wraps bytes in `EngineEvent::Frame` and enqueues into the shard-local **bounded SPSC inbox**.
- **This is the async→sync boundary**.

```rust
pub fn on_frame(&self, conn_id: ConnectionId, bytes: Vec<u8>) {
    if self.inbox.push(EngineEvent::Frame { conn_id, bytes }).is_err() {
        // Inbox full: apply backpressure policy (e.g. close connection)
        self.handle_backpressure(conn_id);
    }
}
```

#### 3. Engine Event Loop (Sync, per shard)
**File**: `src/core/engine.rs` (conceptual)

- One OS thread per engine shard.
- Each shard has its own `Receiver<EngineEvent>` and connection registry.
- Shard processes events in strict FIFO order per inbox.
- No async, no `.await`, no tokio primitives in the loop.

```rust
// Conceptual engine loop
loop {
    match event_rx.recv() {
        EngineEvent::Frame { conn_id, bytes } => {
            // Parse, authorize, dispatch synchronously
            let response = handle_frame_sync(conn_id, bytes);
            // Send response back...
        }
        EngineEvent::Disconnect { conn_id } => {
            cleanup_connection(conn_id);
        }
    }
}
```

#### 4. Route Parsing & Authorization (Sync)
**File**: `src/core/engine.rs`, `src/protocol/route.rs`

- Extract route string from frame header
- Parse route: `kv://realm/area/resource/operation`
- Extract channel_id, route_family for multi-tenancy
- Check JWT claims against route permissions

```rust
let parsed = parse_route(&route)?;  // Returns Route struct
// Authorization check happens here in engine (before domain dispatch)
let ctx = DomainContext {
    route: parsed,
    route_str,
    payload,
    channel_id,        // Logical channel from frame header
    route_family,      // From session
};
// Note: conn_id is NOT passed to domains; it's transport-layer only.
// Domains work with channel_id for correlation and routing decisions.
```

#### 5. Domain Dispatch (Sync)
**File**: `src/core/registry.rs`, `src/core/domain.rs`

- Look up domain by scheme: `kv://` → `KvDomain`
- Call `domain.handle(context)` **synchronously**
- Domain returns `DomainResponse` enum

```rust
pub trait Domain {
    fn handle(&self, context: DomainContext) -> DomainResponse;
}
```

#### 6. Domain Handler (Sync)
**Files**: `src/core/*/handler.rs` (kv, queue, lease, rpc, stream, notice, control)

**Handler responsibilities**:
1. Parse TLV payload to extract operation parameters
2. Call service methods (business logic)
3. Build TLV-encoded response using encoding module
4. Return `DomainResponse::Frame(pooled_frame)`

**Example** (`KvDomain`):
```rust
fn handle(&self, request: DomainContext) -> DomainResponse {
    // Parse TLV
    let key = tlv::parse_string_owned(&request.payload, TAG_KEY)?;
    let value = tlv::parse_bytes_owned(&request.payload, TAG_VALUE)?;
    
    // Business logic (sync)
    let result = self.service.put(key, value)?;
    
    // Build response
    let frame = ResponseBuilder::new()
        .add_string(TAG_KEY, &key)
        .build_frame();
    
    DomainResponse::Frame(frame)
}
```

---

## Outbound Data Flow (Server → Client)

```
┌──────────────────────────────────────────────┐
│ Domain Handler                               │
│ Returns DomainResponse                       │
└──────┬───────────────────────────────────────┘
       │ DomainResponse::Frame(pooled_frame)
       │ DomainResponse::RpcDelivery {...}
       │ DomainResponse::NoticeDelivery {...}
       ↓
┌──────────────────────────────────────────────┐
│ Engine (SYNC, per shard)                     │
│ - Handle DomainResponse variants             │
│ - Extract bytes from PooledFrame             │
│ - Lookup outbound queue for conn_id          │
└──────┬───────────────────────────────────────┘
    │ outbound_spsc.push(bytes)
    ↓
┌──────────────────────────────────────────────┐
│ Per-Connection SPSC Queue (sync → async)     │
│ - Bounded, single-producer/single-consumer   │
│ - Producer: engine shard thread              │
│ - Consumer: WebSocket task                   │
└──────┬───────────────────────────────────────┘
    │ outbound_spsc.pop()
       ↓
┌──────────────────────────────────────────────┐
│ ws.rs: handle_connection()                   │
│ - Async task receives from channel           │
│ - Wraps bytes in Message::Binary             │
│ - Calls ws_sink.send(frame).await            │
└──────┬───────────────────────────────────────┘
       │ Network I/O (async)
       ↓
┌─────────────┐
│ WebSocket   │
│ Connection  │
└─────────────┘
```

### Detailed Outbound Steps

#### 1. Domain Response Construction
**Files**: `src/core/*/handler.rs`

Domains build responses using pooled buffers:
- **DomainResponse::Frame**: Direct response to requester
- **DomainResponse::RpcDelivery**: Deliver to specific inbox
- **DomainResponse::NoticeDelivery**: Fanout to subscribers
- **DomainResponse::Ok**: No-op (e.g., fire-and-forget)
- **DomainResponse::Error**: Error string converted to error frame

```rust
// Simple response
DomainResponse::Frame(response_builder.build_frame())

// RPC delivery (request → handler's inbox)
DomainResponse::RpcDelivery {
    target_channel_id: inbox_owner_channel_id,  // Domain specifies target channel
    message: RpcMessage { /* ... */ },
    ack_frame: ack_to_requester,
}
// Engine will look up: channel_id → conn_id → outbound_tx for delivery

// Notice fanout (publish → N subscribers)
DomainResponse::NoticeDelivery {
    subscribers: vec![(ch1, sub1), (ch2, sub2)],
    notification_frame: pooled_frame,
    ack_frame: Some(ack_to_publisher),
}
```

#### 2. Engine Response Handling (Sync)
**File**: `src/core/engine.rs`

Engine processes `DomainResponse` and routes bytes:

**DomainResponse::Frame** (simple case):
```rust
DomainResponse::Frame(frame) => {
    let bytes = frame.into_vec();
    let outbound = self.get_outbound_channel(conn_id)?;
    outbound.try_send(bytes)?;
}
```

**DomainResponse::RpcDelivery** (routing case):
```rust
DomainResponse::RpcDelivery { target_channel_id, message, ack_frame } => {
    // 1. Send RPC message to target inbox owner
    //    Lookup: channel_id → conn_id → outbound queue
    if let Some(target_conn_id) = self.channel_to_conn.get(&target_channel_id) {
        if let Some(target_outbound) = self.connections.get(target_conn_id) {
            let _ = target_outbound.try_send(message.into_bytes());
        }
    }
    
    // 2. Send ack back to requester (on their connection)
    if let Some(requester_outbound) = self.connections.get(&requester_conn_id) {
        let _ = requester_outbound.try_send(ack_frame.into_vec());
    }
}
```

**DomainResponse::NoticeDelivery** (fanout case):
```rust
DomainResponse::NoticeDelivery { subscribers, notification_frame, ack_frame } => {
    let bytes = notification_frame.into_vec();
    
    // Fanout to all subscribers
    // Each subscriber is identified by their channel_id
    for (sub_channel_id, _sub_id) in subscribers {
        // Lookup: channel_id → conn_id → outbound queue
        if let Some(sub_conn_id) = self.channel_to_conn.get(&sub_channel_id) {
            if let Some(tx) = self.connections.get(sub_conn_id) {
                let _ = tx.try_send(bytes.clone());  // Clone for fanout
            }
        }
    }
    
    // Ack back to publisher (on their connection)
    if let Some(ack) = ack_frame {
        if let Some(tx) = self.connections.get(&publisher_conn_id) {
            let _ = tx.try_send(ack.into_vec());
        }
    }
}
```

#### 3. Outbound Queue (Sync → Async Boundary)
**File**: `src/transport/ws.rs`

Each WebSocket connection has a dedicated **bounded SPSC queue** for outbound frames:
- Created in `handle_connection()`: `let (producer, consumer) = SpscQueue::with_capacity(N);`.
- `producer` handle is registered with engine via `engine.register_connection(conn_id, producer)`.
- `consumer` handle is held by the WebSocket task.

```rust
// In handle_connection()
let (outbound_prod, mut outbound_cons) = SpscQueue::with_capacity(OUTBOUND_CAP);
engine.register_connection(conn_id, outbound_prod);

// Later in select! loop:
if let Some(frame) = outbound_cons.pop() {
    ws_sink.send(Message::Binary(frame)).await?;
}
```

#### 4. WebSocket Write (Async)
**File**: `src/transport/ws.rs`

- WebSocket task selects on both inbound (ws_stream) and outbound (outbound_rx)
- Outbound frames written via `SinkExt::send()`
- Backpressure handled by tokio's async machinery
- Write errors break connection loop

---

## Special Cases & Coordination

### 1. RPC Request/Reply Flow

**Request** (client → handler):
```
Client WS → Engine → RpcDomain.handle()
  ├─ Allocate inbox route (inbox://realm/uuid)
  ├─ Register correlation_id → inbox mapping
  ├─ Route request to handler (via RpcDelivery)
  └─ Send ack to client with inbox route
```

**Reply** (handler → client):
```
Handler WS → Engine → RpcDomain.handle()
  ├─ Parse inbox:// route
  ├─ Lookup correlation_id → client channel_id
  ├─ Validate inbox ownership
  └─ Deliver reply via RpcDelivery to client's channel_id
```

**Engine coordination**:
- Each shard stores a `conn_id → outbound_tx` mapping for physical connections.
- Each shard stores a `channel_id → conn_id` mapping to locate which connection a channel belongs to.
- RPC service stores `inbox_id → owner_channel_id` mapping for routing replies.
- When delivering an RPC reply:
  1. Domain returns `target_channel_id` (the inbox owner's channel).
  2. Engine looks up `channel_id → conn_id` to find the physical connection.
  3. Engine looks up `conn_id → outbound_tx` to get the outbound queue.
  4. Engine pushes the reply frame to that queue.
- Authorization checks inbox ownership on reply; replies must be routed within the same shard as the owning channel.

### 2. Notice Publish/Subscribe Flow

**Subscribe** (client → server):
```
Client WS → Engine → NoticeDomain.handle()
  ├─ Parse notice:// route pattern (with wildcards)
  ├─ NoticeService.subscribe(route_family, pattern, channel_id)
  ├─ Store in routing trie
  └─ Send ack to client
```

**Publish** (client → server → subscribers):
```
Publisher WS → Engine → NoticeDomain.handle()
  ├─ Parse notice:// route + body
  ├─ NoticeService.publish() returns matched subscribers
  ├─ Domain builds notification_frame (route + body + msg_id)
  └─ Return DomainResponse::NoticeDelivery {
        subscribers: [(ch1, sub1), (ch2, sub2), ...],
        notification_frame,
        ack_frame
      }
```

**Engine fanout (within a shard)**:
```rust
// Engine handles NoticeDelivery
for (subscriber_channel_id, _) in subscribers {
    // Look up which connection this channel belongs to
    if let Some(subscriber_conn_id) = self.channel_to_conn.get(&subscriber_channel_id) {
        if let Some(outbound) = self.connections.get(subscriber_conn_id) {
            let _ = outbound.try_send(notification_frame.clone());
        }
    }
}
// Send ack back to publisher (publisher's conn_id is the current frame's conn_id)
if let Some(publisher_outbound) = self.connections.get(publisher_conn_id) {
    let _ = publisher_outbound.try_send(ack_frame.into_vec());
}
```

### 3. Control Domain Cross-Domain Coordination

Control domain can trigger operations in other domains (e.g., publish notices):

```rust
// In ControlDomain.handle()
match operation {
    ControlOp::PublishNotice => {
        // Return coordination instruction to engine
        DomainResponse::NoticeDelivery {
            subscribers,  // From notice service
            notification_frame,
            ack_frame: None,
        }
    }
}
```

**Design principle**: Domains return routing decisions, engine performs I/O coordination.

---

## Authentication & Authorization

### Session Identity at Transport Connect

#### 1. JWT Extraction on WebSocket Upgrade
**Files**: `src/transport/http.rs`, `src/transport/ws.rs`

- Clients MUST present a bearer token during the HTTP → WebSocket upgrade:
    - Preferred: `Authorization: Bearer <jwt>` request header.
    - Fallback: `Sec-WebSocket-Protocol: bearer,<jwt>` or similar subprotocol.
- Upgrade handler flow:
    1. Extract the raw JWT from the request.
    2. Call `authn::verify_token(jwt)` to:
         - Validate signature, issuer, audience, expiry.
         - Extract identity and route-family claims: `sub`, `route_family`, `scopes`/`roles`.
    3. If verification fails:
         - Reject the upgrade with 4xx and DO NOT create a WebSocket or engine session.

#### 2. Session Creation and Shard Selection

- On successful token verification, the transport layer creates a `SessionAuth` record:

```rust
struct SessionAuth {
        subject: String,
        route_family: String,
        scopes: Vec<String>,
        grants: PermissionGrants, // resolved from scopes/claims
}
```

- `PermissionGrants` is built once at connect time using `authz::permissions`:

```rust
struct PermissionGrants {
        // Example shape; implementation-specific
        allowed_routes: RouteMatcher,
        // domain/operation flags, etc.
}
```

- Shard selection:
    - Determine the engine shard for the connection based on `route_family`.
    - Obtain an `EngineHandle` for that shard.
    - Before spawning the WebSocket task:
        - Register the session and outbound queue with the shard:

```rust
engine.register_session(conn_id, SessionAuth { subject, route_family, scopes, grants });
engine.register_connection(conn_id, outbound_producer);
```

    - Only after successful registration do we spawn `handle_connection` with `conn_id`, the `EngineHandle`, and the outbound SPSC consumer.

### Authorization in the Engine

#### 1. Shard-Local Session and Routing Tables
**File**: `src/core/engine.rs`

- Each shard maintains multiple tables for routing and authorization:

```rust
struct EngineShard {
    // Session state: one per connection
    sessions: HashMap<ConnectionId, SessionAuth>,
    
    // Outbound queues: conn_id → SPSC producer for that connection
    connections: HashMap<ConnectionId, OutboundProducer>,
    
    // Channel routing: channel_id → conn_id (for RPC/Notice delivery)
    // Updated on first frame from each channel_id
    channel_to_conn: HashMap<ChannelId, ConnectionId>,
    
    // Event inbox, domain registry, etc.
}
```

- All tables are shard-local and never shared across shards.
- The `channel_to_conn` mapping is built dynamically as frames arrive and is essential for routing replies and notifications to the correct physical connection.

#### 2. Per-Frame Authorization Step

When an engine shard dequeues `EngineEvent::Frame { conn_id, bytes }` from its SPSC inbox:

1. Parse the frame header to extract `channel_id`, route string, and operation.

2. Register the channel → connection mapping (if first frame from this channel):

```rust
self.channel_to_conn.entry(channel_id).or_insert(conn_id);
```

3. Look up the session (keyed by connection, not channel):

```rust
let session = self.sessions.get(&conn_id).ok_or(AuthError::UnknownSession)?;
```

4. Call a single authorization function in the engine, before any domain code runs:

```rust
authz::check_route_authorization(
    &session.grants,
    &route,        // parsed Route
    &session.route_family,
    &operation,    // derived from route or TLV
)?;
```

5. On **deny**:
     - Do NOT call the domain.
     - Build an error frame (e.g. using a shared encoding helper).
     - Wrap it in `DomainResponse::Frame` or a dedicated error variant.
     - Send it to the connection's outbound SPSC queue and return.

6. On **allow**:
     - Construct `DomainContext` with channel_id (NOT conn_id):

```rust
let ctx = DomainContext {
    route,
    route_str,
    payload,
    channel_id,                              // Logical channel from frame
    route_family: session.route_family.clone(),
    // Optional: subject, grants, or other session hints for audit/logging
};
```

     - Call `domain.handle(ctx)` synchronously.
     - Domain works with `channel_id` for all routing decisions (RPC inboxes, subscriptions).

### Domain-Level Rules

- Domains **do not perform primary authorization checks**. All route- and operation-level checks happen in the engine before domain dispatch.
- Domains may:
    - Enforce additional invariants based on `route_family`.
    - Use `subject`/identity in business logic (e.g. attaching owner IDs, audit fields).
    - Perform data-level checks (e.g. ACLs stored in KV) inside services, but these build on the already-authorized session.
- The control domain MAY expose operations that adjust grants or tokens, but any such operation must itself be protected by the engine-level authz step.

### Connection Teardown and Authz Cleanup

- On disconnect, the engine shard receives `EngineEvent::Disconnect { conn_id }` and performs:
    1. `sessions.remove(&conn_id);` — Remove session/auth state
    2. `connections.remove(&conn_id);` — Remove outbound queue
    3. Collect all `channel_ids` that belong to this `conn_id`:
       ```rust
       let orphaned_channels: Vec<ChannelId> = self.channel_to_conn
           .iter()
           .filter(|(_, &cid)| cid == conn_id)
           .map(|(&ch, _)| ch)
           .collect();
       ```
    4. For each orphaned channel, call `domain.cleanup_channel(route_family, channel_id)` to remove:
       - Notice subscriptions for that channel
       - RPC inboxes owned by that channel
       - Queue leases held by that channel
       - Lease domain resources owned by that channel
    5. Remove all `channel_id → conn_id` mappings for this connection:
       ```rust
       for ch in orphaned_channels {
           self.channel_to_conn.remove(&ch);
       }
       ```
- This guarantees that no stale session, channel, or permission state survives after the transport is gone.

---

## Engine Public Interface

The engine exposes a small, stable API to the transport layer and tests; all other interactions remain internal to the engine thread(s).

**Files**: `src/core/engine.rs`, `src/transport/ws.rs`

- `EngineHandle::register_session(conn_id, SessionAuth)`
    - Called once during WebSocket setup after JWT verification.
    - Binds identity and permission state to a connection in the owning shard.

- `EngineHandle::register_connection(conn_id, outbound: OutboundProducer)`
    - Called once during WebSocket setup.
    - Registers the outbound SPSC producer for the connection.

- `EngineHandle::on_frame(conn_id, bytes: Vec<u8>)`
    - Called for every inbound binary WebSocket frame.
    - Enqueues an `EngineEvent::Frame` into the shard's SPSC inbox.

- `EngineHandle::on_disconnect(conn_id)`
    - Called when the WebSocket task terminates (close frame, error, or remote disconnect).
    - Enqueues an `EngineEvent::Disconnect` so the shard can clean up session and domain state.

No other direct calls into the engine are permitted from transport; control-plane or admin operations should use existing domains (e.g. control, notice) rather than adding ad-hoc engine entry points.

---

## Sharding Strategy

### Shard Selection Function

**Files**: `src/core/engine.rs`, `src/transport/ws.rs`

- The system runs with a fixed `NUM_SHARDS` configured at startup.
- Shard ownership is decided at connection time and never changes for the lifetime of a connection.
- Shard selection is based on route family to preserve isolation:

```rust
fn choose_shard(route_family: &str, num_shards: usize) -> usize {
        let hash = fxhash::hash64(route_family.as_bytes());
        (hash as usize) % num_shards
}
```

- All frames for a given `conn_id` must be routed through the `EngineHandle` associated with the chosen shard.
- RPC inbox ownership and Notice subscriptions are shard-local; cross-shard delivery is not supported in this design and must be modeled explicitly (e.g., via domains and inter-shard messaging) if ever needed.

### Shard Invariants

- For any connection:
    - `conn_id` → exactly one shard for its entire lifetime.
    - All `EngineHandle::on_frame` and `on_disconnect` calls for that `conn_id` go to that shard.
- For any inbox or subscription:
    - Owner channel and its derived routes live in the same shard.
    - Engine never reaches into another shard's state; all coordination occurs within a shard.

---

## Error Semantics

### Error Classes

To keep behavior predictable and observable, all errors are grouped into a small set of classes:

1. **Authn Errors** (authentication): invalid/missing JWT during upgrade.
2. **Authz Errors** (authorization): session is valid but lacks permission for the requested route/operation.
3. **Protocol Errors**: malformed frames, invalid TLV, or unknown routes.
4. **Domain Errors**: business-level failures inside domain services.
5. **Engine Errors**: internal failures (e.g., unknown session for a frame, backpressure-triggered drops).

### Where Errors Are Generated

- **Transport layer** (`http.rs` / `ws.rs`):
    - Authn errors → HTTP 4xx during upgrade, no WebSocket, no engine session.
    - Protocol-level WebSocket errors → close frame or connection drop, followed by `on_disconnect`.

- **Engine** (`engine.rs`):
    - Unknown session (`conn_id` not found in `sessions`) → engine error; converted to an error frame and pushed outbound if possible, then connection closed.
    - Authz deny from `authz::check_route_authorization` → authz error frame.
    - Backpressure-induced conditions (inbox full, outbound saturation) → engine error; may trigger drops and/or disconnect according to backpressure policy.

- **Domains** (`src/core/*/handler.rs`):
    - TLV parsing failures → protocol error frame.
    - Business failures (e.g. resource not found, conflict) → domain error frame.

### Wire Format for Error Frames

- Error responses are encoded using a shared pattern across domains:
    - `TAG_ERR_CODE`: numeric or symbolic error code.
    - `TAG_ERR_MSG`: human-readable message.
    - Optional tags (e.g., `TAG_ROUTE`) to aid debugging.

- Example categories and codes (illustrative):

    - Authn/Authz:
        - `AUTHN_FAILED`, `AUTHZ_DENIED`.
    - Protocol:
        - `ROUTE_INVALID`, `TLV_INVALID`.
    - Domain:
        - `NOT_FOUND`, `CONFLICT`, `LIMIT_EXCEEDED`.
    - Engine:
        - `ENGINE_BACKPRESSURED`, `ENGINE_INTERNAL`.

Domains and the engine should use a common helper (e.g. `encoding::build_error_frame(code, message, maybe_route)`) to avoid divergence in encoding.

### Mapping Errors to Behavior

- **Authn error**: reject upgrade; client must obtain a new token.
- **Authz deny**: send error frame and continue processing future frames; the connection remains open.
- **Protocol error**: send error frame; depending on severity, either continue (minor TLV error) or close connection (frame-level corruption).
- **Domain error**: send error frame; connection remains open.
- **Engine error**:
    - Unknown session → send error frame (if possible) then close connection.
    - Backpressure-related → may result in frame drops or connection close as described in backpressure section.

---

## Connection Lifecycle

### Connection Establishment
```
1. TCP accept (tokio)
2. WebSocket handshake + JWT extraction (tungstenite + authn)
3. Verify JWT and build SessionAuth (subject, route_family, grants)
4. Assign conn_id (atomic counter) and choose engine shard for this connection
   (deterministic by hashing route_family from JWT claims)
5. Obtain EngineHandle for that shard
6. Create outbound SPSC queue (bounded, size = OUTBOUND_CAP)
7. Register session + connection with engine shard:
   - engine.register_session(conn_id, session_auth)
   - engine.register_connection(conn_id, outbound_producer)
8. Spawn connection task
9. Start select! loop (read WS + write outbound)

Note: channel_id values are NOT known at connection time.
They are extracted from frame headers as frames arrive.
The engine builds the channel_to_conn mapping dynamically.
```

### Connection Teardown
```
1. WS close frame OR read/write error OR remote disconnect
2. Break select! loop
3. Call engine.on_disconnect(conn_id) on the shard that owns this connection
4. Engine cleanup in that shard:
   ├─ Remove session: sessions.remove(&conn_id)
   ├─ Remove outbound queue: connections.remove(&conn_id)
   ├─ Identify orphaned channels: collect all channel_ids mapped to this conn_id
   ├─ For each orphaned channel_id, call domain.cleanup_channel(route_family, channel_id):
   │  ├─ Notice: remove all subscriptions for that channel
   │  ├─ RPC: release inboxes owned by that channel
   │  ├─ Queue: release leases held by that channel
   │  └─ Lease: release resources owned by that channel
   └─ Remove channel mappings: channel_to_conn.remove(&channel_id) for all orphaned channels
5. Drop WebSocket stream
6. Decrement connection counter
```

---

## Buffer Management & Zero-Copy Optimization

### Pooled Buffers
**File**: `src/protocol/frame.rs`

- Global `BUF_POOL: Mutex<Vec<Vec<u8>>>`.
- `take_buf()` → reuses cleared buffers.
- `PooledFrame::Drop` → returns buffer to pool.
- **Critical invariants**:
    - Drop impl **must** call `.clear()` before returning.
    - No code may hold a reference/slice into a `PooledFrame` after it is converted back into a `Vec<u8>` or dropped.

```rust
impl Drop for PooledFrame {
    fn drop(&mut self) {
        if let Some(mut b) = self.buf.take() {
            b.clear();  // ← MUST clear to prevent data leaks
            if b.capacity() <= 8 * 1024 {
                BUF_POOL.lock().unwrap().push(b);
            }
        }
    }
}
```

### SmallVec Optimization
**Files**: `src/core/rpc/encoding.rs`, `src/core/notice/encoding.rs`

For small responses (<64 bytes):
```rust
type ResponseBuf = SmallVec<[u8; 64]>;

pub fn build_ack_response(route: &str) -> ResponseBuf {
    let mut response = ResponseBuf::new();  // Stack-allocated if ≤64 bytes
    response.push(TAG_ROUTE);
    response.push(route.len() as u8);
    response.extend_from_slice(route.as_bytes());
    response
}
```

**Tradeoff**: SmallVec avoids allocations for small messages; `PooledFrame` provides better reuse for variable-sized payloads. Prefer SmallVec for tiny control frames and `PooledFrame` for larger or frequently reused buffers.

---

## Concurrency Model

### Async Layer (Per-Connection)
- One tokio task per WebSocket connection
- Task sleeps when no I/O (efficient)
- Backpressure via bounded SPSC queues (outbound) and inbox capacity (inbound)

### Sync Engine (Sharded)
- **Baseline**: The implementation **SHOULD** run with multiple engine shards for production.
- **Sharding key**: Typically `route_family`; all connections for a given route family go to the same shard.
- **Per-shard invariants**:
    - One thread per shard, single-threaded event loop.
    - Shard-local inbox (`Receiver<EngineEvent>`) and connection registry.
    - No cross-shard state mutation.
- **Cross-family isolation**: Different `route_family` values should be mapped to different shards where practical.

### Domain Services
- **Stateless domains** (Control): No locking needed
- **Stateful domains** (Notice, RPC): `Arc<RwLock<Service>>`
  - Read-heavy operations use `.read()`
  - Write operations (subscribe, allocate) use `.write()`
- **Lease domain**: `Arc<LeaseService>` with internal `DashMap` (lock-free)

---

## Performance Characteristics

### Latency Profile (Targets)
1. **WebSocket → Engine**: ~1-5 μs (crossbeam channel send)
2. **Engine dispatch**: ~2-10 μs (route parse + domain lookup)
3. **Domain handler**: ~5-50 μs (depends on operation)
4. **Engine → WebSocket**: ~1-5 μs (tokio channel send)
5. **WebSocket write**: ~50-500 μs (kernel + network)

**Total application latency**: ~10-70 μs (excluding network)

### Throughput Optimization
- **Pooled buffers**: Eliminate allocation overhead
- **Sync domain logic**: No task scheduling overhead
- **Per-shard SPSC inbox**: Bounded, cache-friendly handoff from WS tasks to engine threads
- **Per-connection SPSC outbound**: Eliminates contention and provides cheap backpressure
- **Domain sharding**: Scale engine across multiple threads/realms

### Backpressure Handling (Target Behavior)

In the target implementation, **all queues are bounded**, and backpressure behavior is deterministic and observable.

- **Outbound SPSC (engine → WS)**
    - Each connection has an `OUTBOUND_CAP`-sized SPSC queue.
    - On push failure (queue full):
        - Increment `connection_backpressure_drops_total{conn_id}`.
        - Option A (preferred): drop the oldest frame (`pop_front()` + `push()`), preserving the most recent state for that connection.
        - Option B: increment a consecutive failure counter; if it exceeds a threshold, mark the connection as backpressured and schedule a disconnect.

- **Inbound SPSC inbox (WS → engine)**
    - Each shard has an `INBOX_CAP`-sized SPSC queue.
    - `on_frame` attempts `inbox.push(event)` without blocking.
    - On push failure (queue full):
        - Increment `inbox_drops_total{shard}`.
        - Immediately call `handle_backpressure(conn_id)` to close or soft-fail the offending connection.
    - This keeps engine memory bounded and prevents slow domains from being overwhelmed by faster producers.

- **Fanout semantics (Notice/RPC)**
    - Fanout is **at-most-once per subscriber queue**: if a subscriber's outbound SPSC is full, the notification for that subscriber may be dropped.
    - Drops are recorded via `fanout_drops_total{subscriber_conn_id}`.
    - Domain-level specs (notice, RPC) must document that delivery is at-most-once under backpressure and suggest application-level retries or leases if stronger guarantees are required.

---

## Testing & Observability

### Unit Testing
- Domains tested with mock contexts: `DomainContext { route, payload, ... }`
- Service logic tested independently (no engine required)
- TLV encoding/decoding tested with golden bytes

### Integration Testing
- Spin up engine with test domains
- Send `EngineEvent::Frame` via channel
- Assert on outbound channel contents

### Observability
- **Metrics**: Connection count, message rate, domain dispatch latency
- **Tracing**: Structured logs at WS layer, engine, domains
- **Error paths**: All parsing errors return descriptive strings

---

## Summary

Fitz's data flow architecture achieves:

✅ **Low latency**: Sync hot path, minimal overhead  
✅ **High throughput**: Pooled buffers, zero-copy where it makes sense  
✅ **Deterministic**: Single-threaded engine, FIFO ordering  
✅ **Scalable**: Shard by route_family/realm for multi-tenancy  
✅ **Maintainable**: Clear async/sync boundaries, domain isolation  

**Key insight**: By keeping the engine and domain logic 100% synchronous, we eliminate async task scheduling jitter and maintain predictable performance under load.
