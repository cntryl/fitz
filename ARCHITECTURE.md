# Fitz Architecture

## Overview

The system follows a clean layered architecture with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                     CLIENT                                   │
│              (WebSocket / HTTP / TCP)                        │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ TLS (optional)
                         │ 
┌────────────────────────▼────────────────────────────────────┐
│              TRANSPORT LAYER                                 │
│     (ws.rs / http.rs / tcp.rs)                              │
│  • Accept connections                                        │
│  • TLS termination                                           │
│  • Spawn per-connection handler                              │
└────────────────────────┬────────────────────────────────────┘
                         │
                    ┌────▼─────┐
                    │ Muxer    │
                    │ (mux.rs) │
                    └────┬─────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
    ┌───▼─────────────────▼───────────────▼────┐
    │     SESSION HANDLER (session/mod.rs)      │
    │ • Establish channel_id                    │
    │ • AUTH: Validate client identity          │
    │ • Store tenant credentials in session     │
    │ • Parse incoming frames                   │
    │ • Check permissions against tenant        │
    │ • Forward requests to engine              │
    └───┬──────────────────────────────────────┘
        │ dispatch(route, payload, channel_id)
        │
┌───────▼──────────────────────────────────────────┐
│           ENGINE (engine.rs)                      │
│ • Actor-based message dispatcher (mpsc channel) │
│ • Parse route → determine scheme/domain         │
│ • Route message to appropriate domain handler   │
│ • Manage pub/sub subscriptions                  │
│ • Handle channel lifecycle (cleanup)            │
└───┬─────────────────────────────────────────────┘
    │
    ├──────────────────────────────────────────────────────┐
    │                                                       │
    │ (Route scheme determines which domain handles it)    │
    │                                                       │
    ├──────────────────────────────────────────────────────┤
    │
    ├─ notice://    ─→ Notice Domain (Publish/Subscribe)
    │
    ├─ rpc://       ─→ RPC Domain (Request/Reply)
    │
    ├─ queue://     ─→ Queue Domain (FIFO Messages)
    │
    ├─ lease://     ─→ Lease Domain (Lease Management)
    │
    ├─ kv://        ─→ KV Domain (Key/Value Store)
    │
    ├─ stream://    ─→ Stream Domain (Event Streams)
    │
    └─ control://   ─→ Control Domain (via Notice Service)

┌────────────────────────────────────────┐
│    DOMAIN HANDLER (e.g., Notice)       │
│ • handler.rs: Parse TLV, build request│
│ • service.rs: Business logic           │
│ • types.rs: Data structures            │
│                                        │
│ Flow: Parse payload → Call service →   │
│       Build response TLV → Return      │
└────────┬─────────────────────────────┘
         │
    ┌────▼──────────────────────────────────┐
    │   STORAGE LAYER (midge_adapter.rs)    │
    │ • KvStore trait (midge)               │
    │ • Memory / Local / Cloud backends      │
    └───────────────────────────────────────┘
```

## Request Flow

### 1. Client connects via WebSocket
```
Client → WS Transport → TLS → accept_async() → process_ws_stream()
```

### 2. Mux frame-based protocol
```
Muxer (demultiplexes frames by channel_id)
  ↓
Each channel has its own handler (session handler)
```

### 3. Session establishes auth
```
Session Handler receives FRAME_AUTH
  → validate credentials
  → store tenant/permissions in SessionState
  → reply with AUTH ACK
```

### 4. Client sends request
```
Client → FRAME_PUB/FRAME_REQ (notice://topic, payload with TAG_*)
  ↓
Session Handler (session/mod.rs)
  • Parse frame → route string + payload
  • Check permissions in SessionState
  • Call engine.dispatch(route, payload, channel_id)
```

### 5. Engine routes to domain
```
Engine (engine.rs)
  • Parse route string → scheme (notice, rpc, queue, etc.)
  • Find domain handler for scheme
  • Create DomainContext { route, payload, channel_id }
  • Call domain.handle(request)
```

### 6. Domain processes request
```
Domain Handler
  • Parse TLV payload (TAG_ID, TAG_BODY, etc.)
  • Call service method
  • Service accesses storage (midge)
  • Build TLV response
  • Return DomainResponse::Frame(bytes)
```

### 7. Response sent to client
```
Engine → Session Handler → Mux → WS → Client
```

### 8. Channel cleanup
```
Client disconnects
  ↓
Mux removes channel
  ↓
Engine.cleanup_channel() called
  ↓
Domain cleanup subscriptions for channel_id
```

## Key Design Principles

### 1. TLV is Transport Concern
- **Session Layer** (not Engine) builds TLV payloads from frame data
- **Engine** just routes bytes, never builds TLV
- **Domain Handlers** parse incoming TLV and build outgoing TLV

### 2. channel_id is Correlation ID
- Assigned by Mux when client first connects
- Used for:
  - Routing responses back to correct client
  - Subscription management (pub/sub)
  - Channel-specific cleanup

### 3. Authentication happens once per session
- SessionState stores tenant credentials once
- All subsequent requests use cached auth
- Permissions checked per-request against stored tenant

### 4. Domain-Owned Business Logic
- Each domain (Notice, RPC, Queue, etc.) owns its logic
- handler.rs: Protocol parsing (TLV ↔ domain types)
- service.rs: Business operations
- storage is accessed via domain-specific methods

### 5. Pub/Sub is Domain-Owned
- Notice domain manages subscriptions (not generic engine)
- RPC domain manages inbox subscriptions
- Engine routes Subscribe/Unsubscribe commands to correct domain

## File Structure

```
transport/
  ws.rs           - WebSocket acceptor
  tcp.rs          - TCP acceptor
  http.rs         - HTTP acceptor
  mux.rs          - Frame multiplexer (by channel_id)
  session/
    mod.rs        - Session handler (auth, permissions, dispatch)
    state.rs      - SessionState (channel lifecycle)

core/
  engine.rs       - Message dispatcher (route → domain)
  domain.rs       - Domain trait (handle, subscribe, etc.)
  router.rs       - Legacy fallback router (for non-domain pub/sub)
  
  notice/
    handler.rs    - Parse TLV, call service
    service.rs    - Publish/Subscribe operations
    types.rs      - Operations, topic structures
  
  rpc/
    handler.rs    - Parse RPC frames
    service.rs    - Request/Reply operations
    client.rs     - In-process RPC client helper
  
  queue/
    handler.rs    - Parse queue frames
    service.rs    - Lease, reserve, consume
  
  ... (similar for lease, kv, stream, control)

storage/
  midge_adapter.rs - KvStore trait implementation
  traits.rs        - KvStore abstraction
```

## State Management

### SessionState (per channel_id)
- `auth_state`: Arc<Mutex<Option<Tenant>>>  - authenticated identity
- `channel_id`: u32 - the channel number
- `subs`: HashMap<String, SubId> - active subscriptions
- `inflight`: Arc<Semaphore> - flow control window
- `engine`, `mux` - shared references to engine and mux

### Per-Domain Subscriptions
- **Notice**: Topics → subscribers, per tenant
- **RPC**: Inbox routes → handlers, per channel
- These are domain-specific, not engine-generic

## Concurrency Model

- **Per-connection**: One async task handles all frames for that channel_id
- **Per-domain**: Each domain may have internal concurrency (Arc<Mutex<...>>)
- **Engine**: Actor pattern via mpsc channel (single consumer, multiple producers)
- **No locks**: Response is await'd, not stored

## Error Handling

Errors flow back through the same channel_id:
```
Domain Handler
  → Err(string)
  → Engine: DomainResponse::Error(err)
  → Session: sends FRAME_ERR on channel_id
  → Client receives error response
```

## Future Extensibility

1. **New Domain**: Create `src/core/newdomain/`, implement Domain trait
2. **New Transport**: Create transport handler, spawn session per connection
3. **New Storage**: Implement midge KvStore trait, plug into midge_adapter
4. **New Permission**: Add check in session/mod.rs permissions module
