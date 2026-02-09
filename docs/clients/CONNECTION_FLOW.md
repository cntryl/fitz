# Client Connection & Domain Interaction Flow

**End-to-end walkthrough: Client connects, sends request, receives response**

This document traces a concrete example through all seven layers of the Fitz architecture.

## Architecture Layers (TOP to BOTTOM)

```
┌─────────────────────────────────────────────────────────┐
│ CLIENT (Application Layer)                               │
│  - User calls: tx = client.kv_begin(route, mode)        │
│  - Client SDK generates: CONNECT (JWT), BEGIN request    │
└──────────────────────┬──────────────────────────────────┘
                       │ Binary frames
                       │ (WebSocket/TCP)
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 1: API (Transport Edge)                           │
│  - src/api/ws.rs, src/api/tcp.rs                        │
│  - Tokio async accept(), read WebSocket/TCP frames      │
│  - Length-prefixed parsing (TCP) or frame boundaries (WS)│
│  - Forwards raw bytes to Session ingress                │
└──────────────────────┬──────────────────────────────────┘
                       │ Raw frame bytes
                       │
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 2: SESSION (Middleware)                           │
│  - src/session/session.rs, src/session/manager.rs       │
│  - TLV decode: [MessageType][Length][Payload]           │
│  - On CONNECT: authenticate JWT, extract RouteFamily    │
│  - Create SessionPermissions from JWT claims            │
│  - Demux frame to logical channel (KV, RPC, Notice...) │
│  - Route to Runtime ingress                             │
└──────────────────────┬──────────────────────────────────┘
                       │ ChannelMessage {
                       │   channel: ChannelId,
                       │   msg_type: u16,
                       │   payload: Bytes
                       │ }
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 3: MUX (Logical Channels)                         │
│  - src/protocol/mux.rs                                  │
│  - Route by MessageType range:                          │
│    - 100-108 → KV channel                               │
│    - 300-304 → RPC channel                              │
│    - 500-504 → Notice channel                           │
│    - etc.                                               │
│  - Per-channel backpressure enforcement                 │
└──────────────────────┬──────────────────────────────────┘
                       │ DomainMessage {
                       │   msg_type,
                       │   payload
                       │ }
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 4: PROTOCOL (Codecs)                              │
│  - src/protocol/*_codec.rs                              │
│  - Domain-specific TLV decoding:                        │
│    - KvCodec::parse_request(100, payload)               │
│    - Parse: route_len, route, mode, durability          │
│    - Returns: KvMessage::Begin { route, mode, ... }     │
└──────────────────────┬──────────────────────────────────┘
                       │ Typed message
                       │ KvMessage::Begin
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 5: RUNTIME (Engine)                               │
│  - src/runtime/routing.rs, router.rs, actor.rs          │
│  - 100% SYNCHRONOUS, deterministic                      │
│  - RouteFamily from session → shard key                 │
│  - Parse route, extract realm/area/resource             │
│  - Router.dispatch_durable() routes to KV actor         │
│  - Actor mailbox queues message                         │
│  - Scheduler pulls actor, calls actor.receive()         │
└──────────────────────┬──────────────────────────────────┘
                       │ KvMessage::Begin
                       │ (in actor mailbox)
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 6: DOMAINS (Business Logic)                       │
│  - src/domains/kv/mod.rs                                │
│ impl Actor for KvActor {                                │
│   fn receive(&mut self, msg, ctx) -> Response           │
│ }                                                        │
│ - Synchronous state machine                             │
│ - Begin: allocate tx_id, lock resource                  │
│ - Returns: KvResponse::BeginOk { tx_id }                │
└──────────────────────┬──────────────────────────────────┘
                       │ KvResponse::BeginOk
                       │ (in actor's local context)
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 7: PROTOCOL (Encode Response)                     │
│  - src/protocol/kv_codec.rs                             │
│  - KvCodec::encode_response(KvResponse::BeginOk)        │
│  - Build buffer: [status=0][tx_id=u64]                  │
│  - Returns: Vec<u8> (encoded bytes)                     │
└──────────────────────┬──────────────────────────────────┘
                       │ Binary response
                       │
┌──────────────────────▼──────────────────────────────────┐
│ LAYER 2: SESSION (Encode to Transport)                  │
│  - Wrap response bytes in TLV: [MessageType][Length]... │
│  - Queue to outbound sender                             │
└──────────────────────┬──────────────────────────────────┘
                       │ TLV frame bytes
                       │
┌──────────────────────▼──────────────────────────────────┘
│ LAYER 1: API (Send on Transport)                        │
│  - WebSocket: send binary frame                         │
│  - TCP: send [u32 len][frame bytes]                     │
│  - Tokio async write to socket                          │
└──────────────────────┬──────────────────────────────────┘
                       │ Network
                       │
┌──────────────────────▼──────────────────────────────────┐
│ CLIENT (Receive & Decode)                               │
│  - Read frame from WebSocket/TCP                        │
│  - Parse TLV: [MessageType][Length][Payload]            │
│  - Codec::decode_response(payload)                      │
│  - Returns: tx_id to user                               │
│  - User now owns transaction, can call tx.put(), etc.   │
└─────────────────────────────────────────────────────────┘
```

---

## Step-by-Step Walkthrough: KV BEGIN Request

### PHASE 1: CONNECTION & AUTHENTICATION

#### CLIENT SIDE
```python
# User code
client = FitzClient.connect("wss://broker:4090", jwt="eyJ...")

# Under the hood:
# 1. Establish WebSocket connection to wss://broker:4090
# 2. Upgrade to binary WebSocket
# 3. Generate CONNECT frame:
#    [MessageType=1][Length=99][99 bytes of JWT...]
# 4. Send frame
# 5. Wait for broker to respond (no explicit ACK, just silent acceptance or immediate close)
```

#### LAYER 1: API (WebSocket Accept)
```rust
// src/api/ws.rs
async fn accept_connection(socket: WebSocket) {
    // Accept WebSocket upgrade
    // Spawn task: websocket_read_loop(socket_rx, engine_tx)
}

async fn websocket_read_loop(mut rx: SplitStream, tx: Sender<EngineEvent>) {
    // Read binary frames
    while let Some(Message::Binary(frame_bytes)) = rx.next().await {
        // Forward raw bytes to session ingress
        tx.send(EngineEvent::Frame(connection_id, frame_bytes)).await
    }
}
```

#### LAYER 2: SESSION (CONNECT Handling)
```rust
// src/session/manager.rs
async fn process_frame(frame_bytes: &[u8]) -> Result<(), Error> {
    // 1. Parse TLV: [type=u16][len=u16][payload]
    let (msg_type, payload) = tlv_decode(frame_bytes)?;
    
    // 2. CONNECT frame (type=1)?
    if msg_type == 1 {
        // Extract JWT from payload
        let jwt_string = String::from_utf8(payload)?;
        
        // Authenticate JWT
        let claims = authenticate_jwt(&jwt_string)?;  // Validates signature, expiry
        
        // Extract RouteFamily from JWT claims
        let route_family = RouteFamily::from_jwt(claims)?;  // Maps tenant -> shard
        
        // Create session permissions from JWT
        let perms = SessionPermissions {
            realm: claims.realm,
            areas: claims.areas,
            scopes: claims.scopes,
        };
        
        // Create session
        let session = SessionInfo {
            id: SessionId(atomic_counter.fetch_add(1)),
            route_family,
            permissions: perms,
            transport: TransportKind::WebSocket,
        };
        
        // Store session in per-connection state
        connection.session = Some(session);
        
        // Send no response (silent acceptance)
        // On error: close connection immediately
        return Ok(());
    }
    
    // 3. Domain request? Must have authenticated session
    if connection.session.is_none() {
        return Err(Error::NotAuthenticated);
    }
    
    Ok(())
}
```

### PHASE 2: DOMAIN REQUEST (KV BEGIN)

#### CLIENT SIDE
```python
# User code: Begin transaction
tx = client.kv_begin(
    route="kv://prod/app/users",
    mode=TxMode.ReadWrite,
    durability=Durability.Sync
)

# Under the hood:
# 1. Encode BEGIN request:
#    [MessageType=100][Length=X][
#      route_len=21][route="kv://prod/app/users"]
#      [mode=1][durability=1]
#    ]
# 2. Send frame over connected WebSocket
# 3. Wait for response
```

#### LAYER 1: API (Frame Reception)
```rust
// src/api/ws.rs
websocket_read_loop(rx, tx) {
    // Receive frame bytes: [0x00 0x64][0x00 0x23][...rest of payload...]
    // Forward to session ingress
    tx.send(EngineEvent::Frame(conn_id, frame_bytes)).await
}
```

#### LAYER 2: SESSION (TLV Decode + Permission Check)
```rust
// src/session/session.rs
fn handle_frame(&mut self, frame_bytes: &[u8]) -> Result<IngressDecision, Error> {
    // 1. Parse TLV header
    let (msg_type, payload) = tlv_decode(frame_bytes)?;
    // msg_type = 100 (KV BEGIN)
    
    // 2. Get session (already authenticated from CONNECT)
    let session = self.session.as_ref().ok_or(Error::NotAuthenticated)?;
    
    // 3. Permission check (before processing)
    // Extract route from payload (peek at it for auth decision)
    let route = peek_route_from_payload(payload)?;  // "kv://prod/app/users"
    
    // Check realm match
    if route.realm != session.permissions.realm {
        return Err(Error::RealmMismatch);
    }
    
    // Check area match
    if !session.permissions.areas.contains(&route.area) {
        return Err(Error::AreaNotInScope);
    }
    
    // Check scope
    if !session.permissions.scopes.contains("kv:begin") {
        return Err(Error::ScopeNotPermitted);
    }
    
    // 4. Route frame to mux (which routes to channel)
    let channel_msg = ChannelMessage {
        channel: ChannelId::from_msg_type(msg_type)?,  // KV = channel 0
        msg_type,
        payload,
    };
    
    // 5. Enqueue to channel (with backpressure)
    self.mux.enqueue(channel_msg)?;
    
    Ok(IngressDecision::Routed)
}
```

#### LAYER 3: MUX (Channel Routing)
```rust
// src/protocol/mux.rs
impl Mux {
    pub fn enqueue(&self, msg: ChannelMessage) -> Result<(), MuxError> {
        // 1. Route msg_type to channel
        let channel_id = self.get_channel(msg.msg_type)?;
        // msg_type 100 → ChannelId::KV
        
        // 2. Check per-channel backpressure counter
        let count = self.channel_counts[channel_id.idx()].load(Ordering::Relaxed);
        if count >= CHANNEL_CAPACITY {
            return Err(MuxError::ChannelFull(channel_id));
        }
        
        // 3. Increment counter (fast atomic ops)
        self.channel_counts[channel_id.idx()].fetch_add(1, Ordering::Relaxed);
        
        // 4. Queue message to channel
        self.channels[channel_id.idx()].try_send(msg)?;
        
        Ok(())
    }
}
```

#### LAYER 4: PROTOCOL (KV Codec - Decode)
```rust
// src/protocol/kv_codec.rs
pub fn parse_request(
    msg_type: u16,
    route_family: RouteFamily,
    payload: &[u8],
) -> Result<KvMessage, String> {
    match msg_type {
        100 => parse_begin(route_family, payload),
        // ... other KV operations
    }
}

fn parse_begin(route_family: RouteFamily, payload: &[u8]) -> Result<KvMessage, String> {
    let mut decoder = TlvDecoder::new(payload);
    
    // Parse fields in order (wire format per CLIENT_SPEC)
    let route_len = decoder.get_u32()?;          // 21
    let route = decoder.get_string(route_len)?;   // "kv://prod/app/users"
    let mode = decoder.get_u8()?;                 // 1 (ReadWrite)
    let durability = decoder.get_u8()?;           // 1 (Sync)
    
    // Verify full consumption
    if decoder.remaining() != 0 {
        return Err("Trailing data in BEGIN payload".to_string());
    }
    
    Ok(KvMessage::Begin {
        route_family,
        route,
        mode: TxMode::from_u8(mode)?,
        durability: Durability::from_u8(durability)?,
    })
}
```

#### LAYER 5: RUNTIME (Dispatch to Actor)
```rust
// src/runtime/router.rs
pub fn dispatch_durable(
    &self,
    route_family: RouteFamily,
    msg: KvMessage,
) -> Result<KvResponse, String> {
    let route_family_idx = route_family.shard_idx();  // Which shard handles this?
    
    // Get domain shards for this route_family
    let kv_shard = &self.kv_shards[route_family_idx];
    
    // Extract route components
    let route_components = msg.route.parse()?;  // realm, area, resource
    
    // Create actor addressing key: (realm, area, resource)
    let actor_key = (route_components.realm.clone(), 
                     route_components.area.clone(),
                     route_components.resource.clone());
    
    // Get or create KvActor for this resource
    let actor_ref = kv_shard.get_or_create_actor(actor_key)?;
    
    // Queue message to actor mailbox
    actor_ref.mailbox.send(msg)?;
    
    // Actor scheduler picks it up and processes
    // (scheduler is the synchronous dispatcher loop)
    
    // Eventually returns KvResponse
    Ok(response)
}
```

#### LAYER 6: DOMAIN (KV Actor)
```rust
// src/domains/kv/mod.rs
impl Actor for KvActor {
    type Message = KvMessage;
    type Response = KvResponse;
    
    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) -> Self::Response {
        match msg {
            KvMessage::Begin { route_family, route, mode, durability } => {
                // 1. Allocate transaction ID (atomic counter)
                let tx_id = self.tx_counter.fetch_add(1, Ordering::Relaxed);
                
                // 2. Check isolation mode
                if mode == TxMode::ReadWrite {
                    // Try to acquire exclusive lock on resource
                    if !self.resource_locks[route.resource_hash()].compare_exchange(...) {
                        // Already locked
                        return KvResponse::Error {
                            error: "Resource locked by another transaction".into()
                        };
                    }
                }
                
                // 3. Create transaction state
                let tx_state = TransactionState {
                    id: tx_id,
                    mode,
                    durability,
                    route: route.clone(),
                    writes: Vec::new(),
                    start_snapshot: self.db.snapshot(),
                };
                self.active_transactions.insert(tx_id, tx_state);
                
                // 4. Return success
                KvResponse::BeginOk { tx_id }
                // ← Response object created here (actor local state)
            }
        }
    }
}
```

#### LAYER 7: PROTOCOL (Encode Response)
```rust
// src/protocol/kv_codec.rs
pub fn encode_response(response: &KvResponse) -> Vec<u8> {
    let mut buf = Vec::new();
    
    match response {
        KvResponse::BeginOk { tx_id } => {
            buf.put_u8(0);          // status: success
            buf.put_u64(*tx_id);    // 8 bytes transaction ID
            // Total: 1 + 8 = 9 bytes payload
        }
        // ... other responses
    }
    
    buf
}

// Result: vec![0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00]
//  ↑ status=0  ↑ tx_id=1 (8 bytes big-endian)
```

#### LAYER 2: SESSION (Wrap in TLV)
```rust
// src/session/session.rs
fn send_response(&self, response_bytes: &[u8]) -> Result<(), Error> {
    // 1. Wrap response in TLV frame
    let msg_type = ???;  // How to get msg_type? 
    // Actually: msg_type comes from the original request (100 for BEGIN)
    // Response prefix the same msg_type
    
    let frame = [
        // TLV header
        (100_u16).to_be_bytes().to_vec(),           // [0x00, 0x64]
        (response_bytes.len() as u16).to_be_bytes().to_vec(),  // [0x00, 0x09]
        response_bytes.to_vec(),                     // [0x00, 0x00, 0x00... (tx_id)]
    ].concat();
    
    // 2. Queue to outbound sender
    self.outbound_sender.send(frame)?;
    
    Ok(())
}
```

#### LAYER 1: API (Send on Transport)
```rust
// src/api/ws.rs
async fn websocket_write_loop(outbound_rx: Receiver<Vec<u8>>, mut ws_tx: SplitSink) {
    while let Some(frame_bytes) = outbound_rx.recv().await {
        // frame_bytes = [0x00, 0x64, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
        ws_tx.send(Message::Binary(frame_bytes)).await?;
    }
}
```

#### CLIENT SIDE (Receive & Decode)
```python
# User code: Waiting for response
# Behind the scenes:
# 1. Read WebSocket binary frame: [0x00 0x64][0x00 0x09][0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]
# 2. Parse TLV:
#    - msg_type = 0x0064 = 100 (KV BEGIN response)
#    - length = 0x0009 = 9 bytes
#    - payload = [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
# 3. Decode KV response:
#    - status = 1st byte = 0x00 (success)
#    - tx_id = next 8 bytes = 0x0000000000000001 = 1
# 4. Create Transaction object: Transaction(client, route, tx_id=1)
# 5. Return to user

tx = Transaction(route="kv://prod/app/users", tx_id=1)
```

---

## Summary: Connection Flow

### 1. **TRANSPORT LAYER** (WebSocket/TCP)
   - Client: Establish socket, upgrade to WebSocket
   - Server: API layer accepts connection, spawns read task

### 2. **SESSION LAYER** (Authentication)
   - Client: Send CONNECT with JWT
   - Server: Authenticate JWT, extract RouteFamily, save session
   - Server: Silent acceptance (no explicit ACK frame)

### 3. **MULTIPLEXING LAYER** (Channel Routing)
   - Client: Send domain request (msg_type=100 for KV BEGIN)
   - Server: Route by msg_type range to logical channel (KV, RPC, Notice, etc.)

### 4. **CODEC LAYER** (Format Conversion)
   - Server: TLV decode message → typed message (KvMessage::Begin)
   - Server: Apply permissions check (realm/area/scope)

### 5. **RUNTIME LAYER** (Dispatch & Scheduling)
   - Server: Parse route → actor key (realm, area, resource)
   - Server: Queue message to actor mailbox
   - Server: Scheduler pulls actor, calls actor.receive()

### 6. **DOMAIN LAYER** (Business Logic)
   - Server: Actor processes request (allocate tx_id, lock resource)
   - Server: Return typed response (KvResponse::BeginOk)

### 7. **CODEC LAYER** (Encode Response)
   - Server: Encode response → bytes
   - Server: Wrap in TLV frame

### 8. **SESSION LAYER** (Wrap Response)
   - Server: Queue TLV frame to outbound sender

### 9. **TRANSPORT LAYER** (Send Response)
   - Server: Write frame to WebSocket/TCP socket
   - Client: Read frame, decode TLV, decode domain response
   - Client: Return Transaction object to user

---

## Key Architectural Insights

### ✅ Channel-Based Multiplexing
- Different domains run on independent logical channels
- **Within same channel**: Sequential (request/response blocking)
- **Across channels**: True parallelism (KV + Notice simultaneously)

### ✅ Synchronous Domain Processing
- **Layer 6 (Domains)** is 100% synchronous
- No async/await, no tokio in domain code
- Deterministic, predictable latency

### ✅ Self-Contained Requests
- Every request includes full routing context (route, tx_id, session_id)
- No server-side implicit state (except session auth)
- Enables stateless horizontal scaling

### ✅ Error Handling per Layer
- **Transport errors** (Layer 1): Connection failures → exponential backoff
- **Auth errors** (Layer 2): Invalid JWT → close connection
- **Permission errors** (Layer 2): Insufficient scope → domain error response
- **Domain errors** (Layer 6): Business logic failure → error response via TLV

### ✅ Backpressure Management
- Layer 3 (Mux): Per-channel counters prevent queue overflow
- Layer 5 (Runtime): Actor mailbox limits (if full: backpressure)
- Client should implement retry with exponential backoff

