# Architecture Verification

This document maps your architectural description to the actual codebase.

## Your Description ↔ Code

### 1. "Client establishes a websocket connection"
**You said:**
> client establishes a websocket connection, we validate who they are, and establish a session

**Code:**
- `src/transport/ws.rs` - accepts WebSocket connections
- `src/transport/session/mod.rs:register_default_channel()` - establishes session per channel
- `src/transport/session/state.rs:SessionState` - stores auth_state, channel_id, subscriptions

```rust
// Session auth validation happens here:
// src/transport/session/mod.rs ~line 100+ (FRAME_AUTH handler)
if let Ok(_) = crate::authz::authn::validate_bearer_token(token_str) {
    *auth_task.lock().await = Some(tenant);  // Store identity
    // Session now established with channel_id
}
```

### 2. "Client makes requests to any domain"
**You said:**
> the client can make subsequent requests to any of the domains

**Code:**
- Transport layer receives FRAME_PUB (notice/rpc), FRAME_REQ (queue/lease)
- Session extracts route string and payload
- Session calls `engine.dispatch(route, payload, channel_id)`
- Engine routes based on scheme: `notice://`, `rpc://`, `queue://`, `lease://`, `kv://`, `stream://`, `control://`

```rust
// src/transport/session/mod.rs
let route_str = String::from_utf8_lossy(route_bytes).to_string();
let payload = /* extract from frame */;
engine_task.dispatch(route_str, payload, channel_id).await?;

// src/core/engine.rs
let parsed = parse_route(&route);  // "notice://topic" → scheme="notice"
let domain = domains.get(scheme_str);  // finds NoticeDomain
let response = domain.handle(request).await;
```

### 3. "Validate request against permissions in session"
**You said:**
> we validate the request against the permissions in the session

**Code:**
- Session stores tenant identity in `auth_state`
- For each request, Session checks permissions before dispatch
- SessionState holds the authenticated tenant

```rust
// src/transport/session/mod.rs ~line 700+
let tenant = {
    let g = auth_task.lock().await;
    g.clone()
};
if !permissions::has_permission(&tenant, &route, Action::Write) {
    send_err_chan(..., 1002, "forbidden", ...).await;
    return;
}
```

### 4. "Maintain a channel_id for bi-directional data"
**You said:**
> while active we maintain a "channel_id" that we use for bi directional data between the domain and the client

**Code:**
- `SessionState.channel_id: u32` - the bi-directional channel identifier
- `Muxer` (src/transport/mux.rs) multiplexes by channel_id
- Responses are routed back to same channel_id

```rust
// src/transport/session/state.rs
pub struct SessionState {
    pub channel_id: u32,  // ← The bi-directional channel ID
    pub mux: Arc<Muxer>,  // ← Mux routes responses to this channel
    pub engine: EngineHandle,
    pub auth_state: Arc<Mutex<Option<String>>>,
    ...
}

// Responses sent back:
// src/transport/mux.rs
pub async fn send_on_channel(&self, frame: PooledFrame) {
    // Frame includes channel_id, Mux routes it
}
```

### 5. "Engine maintains channels, broker data between client and domain"
**You said:**
> the engines job is to maintain the channels broker the data between the client and the domain

**Code:**
- Engine receives dispatch commands via mpsc channel
- Engine routes to domain, gets response
- Response sent back through same mux/channel_id

```rust
// src/core/engine.rs - Actor loop
let jh = tokio::spawn(async move {
    while let Some(cmd) = rx.recv().await {  // Receive from mpsc
        match cmd {
            EngineCommand::Dispatch { route, payload, channel_id, resp } => {
                let domain = domains.get(scheme_str)?;
                let response = domain.handle(request).await;  // Call domain
                let _ = resp.send(Ok(response));  // Send back to session
            }
        }
    }
});

// Session waits for response and sends on channel_id
let response = engine.dispatch(route, payload, channel_id).await?;
mux.send_on_channel(response_frame).await;  // Response goes to channel_id
```

### 6. "Domains do underlying work via handler → service → handler"
**You said:**
> the domains job is to do the underlying thing it is suppose to do, and it does this by structuring the chatter handler -> service -> handler

**Code:**
Each domain (Notice, RPC, Queue, Lease, KV, Stream) follows this pattern:

```
handler.rs:  Parse TLV payload from request
              ↓
service.rs:  Execute business logic (access storage)
              ↓
handler.rs:  Build TLV response from service result
```

**Example - Notice domain:**
```rust
// src/core/notice/handler.rs - top level
impl Domain for NoticeDomain {
    async fn handle(&self, request: DomainContext) -> DomainResponse {
        // Parse incoming TLV payload
        let operation = parse_operation(&request.payload)?;
        
        // Call service based on operation
        match operation {
            NoticeOp::Publish => {
                let result = self.service.publish(&route, id, body);
                // ↑ service.rs handles actual work
            }
            NoticeOp::Subscribe => {
                let id = self.service.subscribe(route, channel_id, sender);
                // ↑ service.rs handles subscriptions
            }
        }
        
        // Build response TLV
        let mut response_payload = Vec::new();
        build_tlv(TAG_ID, ...);
        build_tlv(TAG_BODY, ...);
        DomainResponse::Frame(PooledFrame::from_vec(response_payload))
    }
}

// src/core/notice/service.rs - business logic
impl NoticeService {
    pub fn publish(&self, route: &str, msg_id: String, body: Vec<u8>) -> (u32, u32) {
        // Actual publish logic
        let (delivered, failed) = self.route_table.notify(...);
        (delivered, failed)
    }
}
```

Similar patterns in:
- `src/core/queue/{handler.rs,service.rs,types.rs}`
- `src/core/rpc/{handler.rs,service.rs,client.rs}`
- `src/core/lease/{handler.rs,service.rs,types.rs}`
- `src/core/kv/{handler.rs,service.rs,store.rs}`
- `src/core/stream/{handler.rs,service.rs,types.rs}`
- `src/core/control/{handler.rs,service.rs,types.rs}`

## End-to-End Request Flow

```
1. Client sends: notice://topic (PUB frame)
   
2. WS Transport accepts, passes to Muxer
   
3. Muxer routes to channel_id's handler
   
4. Session Handler:
   - Validates auth from SessionState
   - Checks permissions
   - Extracts route + payload
   - Calls engine.dispatch()
   
5. Engine (Actor):
   - Parses route → scheme="notice"
   - Finds NoticeDomain
   - Calls domain.handle(request)
   
6. Notice Handler:
   - Parses TLV payload
   - Calls service.publish()
   
7. Notice Service:
   - Updates internal state
   - Notifies subscribers
   - Returns result
   
8. Notice Handler:
   - Builds response TLV
   - Returns DomainResponse::Frame
   
9. Engine:
   - Sends response through mpsc back to Session
   
10. Session Handler:
    - Sends response frame on channel_id
    
11. Muxer:
    - Routes frame to channel_id's WS connection
    
12. WebSocket:
    - Sends frame to client
```

## Key Verification Points

✅ **Channel ID is maintained throughout**
- SessionState.channel_id
- Passed to engine.dispatch()
- Used in mux.send_on_channel()
- Responses routed back to same channel

✅ **Auth happens once, stored in session**
- auth_state: Arc<Mutex<Option<String>>>
- Checked per-request against permissions
- Multiple requests reuse same identity

✅ **Engine is pure broker**
- No TLV building
- No business logic
- Just routes dispatch → domain

✅ **Domains own their logic**
- handler.rs: TLV parsing/building
- service.rs: Business operations
- No cross-domain dependencies

✅ **Bi-directional data flow**
- Request: Client → Transport → Session → Engine → Domain
- Response: Domain → Engine → Session → Muxer → WS → Client
- Both use channel_id for routing
