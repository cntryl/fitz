# Fitz Server Implementation Guide
**Authoritative guide for implementing a Fitz server broker.**
## Table of Contents
1. [Overview](#overview)
2. [System Architecture](#system-architecture)
3. [Layer Responsibilities](#layer-responsibilities)
4. [Wire Protocol Implementation](#wire-protocol-implementation)
5. [Domain Implementation](#domain-implementation)
6. [Authentication & TLS](#authentication--tls)
7. [Boot & Initialization](#boot--initialization)
8. [Testing & Validation](#testing--validation)
9. [Performance & Tuning](#performance--tuning)
## Overview
Fitz is a **layered, synchronous-core broker** designed for:
- **Low-latency domain operations** (KV, queues, pub/sub, streams, RPC, leases, scheduling)
- **Isolation via realms** (multi-tenant resource partitioning)
- **Deterministic message routing** (no async jitter in hot paths)
- **High fanout** (pub/sub with wildcard patterns)
The server is implemented in **async I/O boundaries** (transport) with a **100% synchronous core** (routing, domains). This separation ensures:
- Clean transport abstraction (WebSocket, TCP, HTTP)
- Predictable domain latency (no async scheduling variability)
- Efficient multi-tenant isolation
## System Architecture
### Layered Design
```
Layer 1: TRANSPORT (Async)
├─ WebSocket (tokio-tungstenite)
├─ TCP (length-prefixed frames)
└─ HTTP (upgrade path to WebSocket)
Layer 2: SESSION (Sync but Transport-Driven)
├─ JWT authentication (CONNECT handshake)
├─ Frame parsing
├─ Permission enforcement
└─ Route disambiguation
Layer 3: RUNTIME (100% Sync, Deterministic)
├─ Router (message delivery, subscription matching)
├─ Scheduler (actor execution, priority lanes)
├─ Ingress (session management)
└─ Mux (frame multiplexing across channels)
Layer 4: DOMAINS (100% Sync Business Logic)
├─ KV (transactions, isolation, durability)
├─ Queue (FIFO, leasing, visibility)
├─ Notice (pub/sub, wildcard matching)
├─ Stream (append-only, watermarks)
├─ RPC (request/response, correlation)
├─ Lease (distributed locking)
└─ Schedule (delayed/recurring tasks)
Layer 5: STORAGE (Persistent Backend)
└─ Midge LSM (key-value durability)
```
### Critical Invariant: Async ↔ Sync Boundary
**Transport (Layer 1) is async.** For each connection:
1. Async task reads frames
2. Parses frame bytes
3. **Synchronously calls domain handler**
4. Waits for synchronous response
5. Encodes response
6. Writes frame back
**The domain handler NEVER blocks on async.** It returns immediately with a typed response.
```
Async WebSocket Reader
    ↓ (frame bytes)
Sync TLV Parser
    ↓ (route, message type)
Sync Router (DashMap lookup)
    ↓ (domain sink)
Sync Domain Handler (business logic)
    ↓ (domain response)
Sync TLV Encoder
    ↓ (response bytes)
Async WebSocket Writer
```
This design eliminates async scheduling jitter from the hot path.
## Layer Responsibilities
### Layer 1: Transport
**Files:** `src/api/tcp.rs`, `src/api/ws.rs`
**Responsibility:** Socket I/O and framing.
**Behavior:**
- Accept connections (TCP: port 4091, WebSocket: port 4090)
- Read frames with configurable max size
- Write frames back to client
- Handle connection lifecycle (close, timeout, error)
- Per-connection async task (long-lived)
**Constraints:**
- Do NOT parse domain payloads
- Do NOT call domain logic
- Do NOT hold locks across frame boundaries
- Pass raw frame bytes + metadata (connection ID, frame size) downstream
**Frame Format:**
- **TCP:** `[u32 BE length][payload bytes]`
- **WebSocket:** Each binary message is a complete frame
### Layer 2: Session
**Files:** `src/session/session.rs`, `src/session/permissions.rs`, `src/session/manager.rs`
**Responsibility:** Connection authentication, permission enforcement, frame routing.
**Behavior:**
- Receive raw frame from transport
- **First frame MUST be CONNECT** with JWT payload
- Validate JWT signature and claims (must validate signature; do NOT trust client parsing)
- Extract JWT claims: `realm`, `areas` (array), `scopes` (array)
- Establish session with extracted claims
- Assign unique session ID (internal; NOT sent to client)
- For each subsequent frame:
  - Parse TLV header (MessageType, length)
  - Extract route scheme (kv, notice, rpc, etc.)
  - Check permissions: realm match, area match, verb scope match
  - If permission check fails: return domain error with code ERR_UNAUTHORIZED
  - Route to appropriate domain via Router
- Return response bytes to transport
**Session Lifecycle:**
On successful CONNECT:
- Create session with extracted JWT claims
- Track subscriptions, transactions, worker registrations per session
- Ready to accept domain requests
On disconnect (graceful or abrupt):
- Immediately clean up:
  - All active subscriptions → drop
  - All active KV transactions → rollback
  - All active Stream sessions → abort
  - All held Leases → release
  - All RPC worker registrations → unregister
  - Queued notifications → discard
- Session ID becomes invalid (no recovery on reconnect)
On reconnect with new CONNECT:
- Create new session (new session ID)
- Old session ID is discarded
- Client MUST explicitly re-subscribe, re-begin, re-register if needed
**Constraints:**
- Do NOT implement domain business logic
- Do NOT block on external async operations
- Minimal parsing: enough to route, nothing more
- MUST validate JWT signature (use external JWT library; do NOT implement JWT validation manually)
- MUST reject expired JWTs (check `exp` claim)
### Layer 3: Runtime
**Files:** `src/runtime/router.rs`, `src/runtime/actor.rs`, `src/runtime/scheduler.rs`, `src/runtime/routing.rs`, `src/runtime/subscriptions.rs`
**Responsibility:** Message routing, subscription indexing, actor scheduling.
**Components:**
#### Router
Lock-free pub/sub with DashMap:
- **Subscriptions:** `{realm} → {area} → {resource} → [subscribers]`
- **Pattern matching:** Wildcard `*` (one segment) and `**` (any depth)
- **Fanout:** Single publish reaches all matched subscriptions
- **Ordering:** Delivered in subscription order per path
#### Actor Mailbox
Per-domain inbox (MPSC channel):
- Receives incoming messages
- Queued by scheduler
- Domain handler processes one message at a time
#### Scheduler
Thread pool with priority lanes:
- Multiple worker threads
- Executes domain handlers in sequence
- Respects priorities (control > data > background)
- No jitter from tokio scheduling
#### Ingress
Session management:
- Maintains per-connection session state
- Tracks active transactions (KV), stream sessions, subscriptions
- Cleans up resources on disconnect
**Constraints:**
- ALL functions are synchronous (no `.await`)
- No async primitives (no tokio locks, channels, timers)
- Use parking_lot or DashMap for concurrency
- Return responses immediately; never queue for later
### Layer 4: Domains
**Files:** `src/domains/kv/`, `src/domains/queue/`, `src/domains/notice/`, etc.
**Responsibility:** Domain-specific business logic.
**Pattern (Synchronous Actor Model):**
```rust
pub struct DomainActor { /* state */ }
pub enum DomainMessage {
    Operation1 { fields... },
    Operation2 { fields... },
}
pub enum DomainResponse {
    Ok { result_fields... },
    Error(String),
}
impl DomainActor {
    pub fn handle(&mut self, msg: DomainMessage) -> DomainResponse {
        match msg {
            DomainMessage::Operation1 { ... } => { /* sync logic */ },
            DomainMessage::Operation2 { ... } => { /* sync logic */ },
        }
    }
}
```
**Per-domain files:**
- `src/domains/{domain}/mod.rs` - Actor, message/response enums
- `src/protocol/{domain}_codec.rs` - TLV encode/decode
- `src/domains/{domain}/handlers.rs` - Business logic (transactions, leases, etc.)
**Constraints:**
- NEVER call `.await`
- NEVER use tokio types (tokio::spawn, tokio::sync, etc.)
- NEVER perform blocking I/O (use storage API synchronously)
- Return typed `DomainResponse` immediately
### Layer 5: Storage
**File:** Midge LSM (external crate)
**API:**
```rust
pub struct Engine { /* LSM state */ }
impl Engine {
    pub fn get(&self, key: &[u8]) -> Result<Vec<u8>>;
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&mut self, key: &[u8]) -> Result<()>;
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    pub fn transaction<F>(&mut self, f: F) -> Result<()> where F: FnOnce(&mut Txn);
}
```
**Key-value schema for each domain:**
- **KV:** `{realm}/{area}/{resource}/{key}` → `{value}`
- **Stream:** `{realm}/{area}/{resource}/offset:{offset}` → `{record}`
- **Queue:** `{realm}/{area}/{resource}/msg:{message_id}` → `{body}`
- **Lease:** `{realm}/{area}/{resource}` → `{owner, ttl, token}`
## Wire Protocol Implementation
### Payload encoding (domain message bodies)
**Location:** `src/protocol/payload_codec.rs`
Sequential typed fields (not frame TLV): fixed-order scalars and length-prefixed strings/bytes. Used by domain codecs to encode request/response bodies. Frame-level TLV is in `src/protocol/tlv.rs`.
**Implementation pattern:**
```rust
pub struct PayloadDecoder<'a> {
    payload: &'a [u8],
    offset: usize,
}
impl PayloadDecoder {
    pub fn new(payload: &[u8]) -> Self { /* ... */ }
    
    pub fn get_u8(&mut self) -> Result<u8> { /* ... */ }
    pub fn get_u16(&mut self) -> Result<u16> { /* ... */ }
    pub fn get_u32(&mut self) -> Result<u32> { /* ... */ }
    pub fn get_u64(&mut self) -> Result<u64> { /* ... */ }
    pub fn get_string(&mut self) -> Result<String> { /* ... */ }
    pub fn get_bytes(&mut self) -> Result<Vec<u8>> { /* ... */ }
    pub fn is_complete(&self) -> bool { /* ... */ }
}
pub struct PayloadEncoder {
    buf: Vec<u8>,
}
impl PayloadEncoder {
    pub fn new() -> Self { /* ... */ }
    
    pub fn put_u8(&mut self, val: u8) { /* ... */ }
    pub fn put_u16(&mut self, val: u16) { /* ... */ }
    pub fn put_u32(&mut self, val: u32) { /* ... */ }
    pub fn put_u64(&mut self, val: u64) { /* ... */ }
    pub fn put_string(&mut self, s: &str) { /* ... */ }
    pub fn put_bytes(&mut self, b: &[u8]) { /* ... */ }
    pub fn finish(&mut self) -> Vec<u8> { /* ... */ }
}
```
**Rules:**
- All integers are big-endian
- Strings and bytes are `[u32 len][data]`
- Consume all bytes in request; error if trailing data
- Encode response deterministically
### Frame Format Handling
**TCP (Length-Prefixed):**
```rust
pub async fn create_session(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
) {
    let mut buf_reader = BufReader::new(&stream);
    
    loop {
        // Read u32 BE length
        let mut len_bytes = [0u8; 4];
        buf_reader.read_exact(&mut len_bytes)?;
        let len = u32::from_be_bytes(len_bytes) as usize;
        
        // Validate length
        if len > ingress_config.max_frame_size {
            stream.close()?;
            return;
        }
        
        // Read payload
        let mut payload = vec![0u8; len];
        buf_reader.read_exact(&mut payload)?;
        
        // Process frame
        ingress.on_frame(session_id, &payload)?;
    }
}
```
**WebSocket (Binary Messages):**
```rust
pub async fn handle_websocket(
    socket: WebSocketStream,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
) {
    while let Some(msg) = socket.next().await {
        match msg {
            Message::Binary(payload) => {
                // Validate size
                if payload.len() > ingress_config.max_frame_size {
                    socket.close()?;
                    return;
                }
                
                // Process frame
                ingress.on_frame(session_id, &payload)?;
            }
            _ => {}
        }
    }
}
```
## Domain Implementation
### Codec Pattern (All Domains)
Every domain has two core functions:
```rust
/// Parse TLV bytes → typed message
pub fn parse_request(
    ctx: &FrameContext,
    payload: &[u8],
) -> Result<DomainMessage, String> {
    let mut dec = PayloadDecoder::new(payload);
    
    match ctx.msg_type {
        MSG_TYPE_OP1 => parse_operation1(&mut dec),
        MSG_TYPE_OP2 => parse_operation2(&mut dec),
        _ => Err(format!("Unknown message type: {}", ctx.msg_type)),
    }
}
/// Encode response → TLV bytes
pub fn encode_response(response: &DomainResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    
    match response {
        DomainResponse::Ok { fields... } => {
            // Encode success
        }
        DomainResponse::Error(e) => {
            // Encode error
        }
    }
    
    enc.finish()
}
```
### KV Domain Example
**File:** `src/domains/kv/mod.rs`
```rust
pub enum KvMessage {
    Begin { resource: String, mode: TxMode },
    Get { tx_id: u64, key: Vec<u8> },
    Put { tx_id: u64, key: Vec<u8>, value: Vec<u8> },
    Commit { tx_id: u64 },
    Rollback { tx_id: u64 },
}
pub enum KvResponse {
    BeginOk { tx_id: u64 },
    GetOk { found: bool, value: Vec<u8> },
    Ok,
    Error(String),
}
pub struct KvActor {
    transactions: HashMap<u64, KvTransaction>,
    store: Arc<Storage>,
}
impl KvActor {
    pub fn handle(&mut self, msg: KvMessage) -> KvResponse {
        match msg {
            KvMessage::Begin { resource, mode } => {
                let tx_id = self.next_tx_id();
                let tx = KvTransaction::new(resource, mode);
                self.transactions.insert(tx_id, tx);
                KvResponse::BeginOk { tx_id }
            }
            KvMessage::Get { tx_id, key } => {
                let tx = self.transactions.get(&tx_id)?;
                let value = self.store.get(tx, &key)?;
                KvResponse::GetOk {
                    found: value.is_some(),
                    value: value.unwrap_or_default(),
                }
            }
            // ... other operations
        }
    }
}
```
**Codec** (`src/protocol/kv_codec.rs`):
```rust
pub fn parse_request(ctx: &FrameContext, payload: &[u8]) -> Result<KvMessage, String> {
    let mut dec = PayloadDecoder::new(payload);
    
    match ctx.msg_type {
        100 => {
            // BEGIN
            let resource = dec.get_string()?;
            let mode_u8 = dec.get_u8()?;
            let mode = TxMode::from_u8(mode_u8)?;
            Ok(KvMessage::Begin { resource, mode })
        }
        103 => {
            // GET
            let tx_id = dec.get_u64()?;
            let key = dec.get_bytes()?;
            Ok(KvMessage::Get { tx_id, key })
        }
        // ... other operations
        _ => Err(format!("Unknown KV message type: {}", ctx.msg_type)),
    }
}
pub fn encode_response(response: &KvResponse) -> Vec<u8> {
    let mut enc = PayloadEncoder::new();
    
    match response {
        KvResponse::BeginOk { tx_id } => {
            enc.put_u64(*tx_id);
        }
        KvResponse::GetOk { found, value } => {
            enc.put_u8(*found as u8);
            enc.put_bytes(value);
        }
        KvResponse::Ok => {
            // Empty payload
        }
        KvResponse::Error(e) => {
            enc.put_string(e);
        }
    }
    
    enc.finish()
}
```
### Notice Domain (Pub/Sub) Example
**File:** `src/domains/notice/mod.rs`
```rust
pub struct NoticeActor {
    subscriptions: Vec<Subscription>, // Pattern → Subscribers
}
pub enum NoticeMessage {
    Publish { route: String, payload: Vec<u8> },
    Subscribe { pattern: String, subscriber_route: String },
    Unsubscribe { pattern: String, subscriber_route: String },
}
pub enum NoticeResponse {
    Ok { subscription_id: u64 },
    Error(String),
}
impl NoticeActor {
    pub fn handle(&mut self, msg: NoticeMessage) -> NoticeResponse {
        match msg {
            NoticeMessage::Publish { route, payload } => {
                // Match route against all subscription patterns
                for sub in &self.subscriptions {
                    if self.matches_pattern(&route, &sub.pattern) {
                        // Queue notification to subscriber
                        self.queue_notify(&sub.subscriber_route, &route, &payload);
                    }
                }
                NoticeResponse::Ok { subscription_id: 0 }
            }
            NoticeMessage::Subscribe { pattern, subscriber_route } => {
                // Validate pattern (no invalid wildcards)
                let sub_id = self.next_sub_id();
                self.subscriptions.push(Subscription {
                    pattern,
                    subscriber_route,
                    id: sub_id,
                });
                NoticeResponse::Ok { subscription_id: sub_id }
            }
            // ...
        }
    }
}
```
### Pattern Matching
For Notice (pub/sub) and Stream subscriptions, implement wildcard matching:
```rust
/// Match a route against a pattern.
/// `*` = one segment, `**` = zero or more segments
pub fn matches_pattern(route: &str, pattern: &str) -> bool {
    let route_parts: Vec<&str> = route.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();
    
    match_parts(&route_parts, &pattern_parts)
}
fn match_parts(route: &[&str], pattern: &[&str]) -> bool {
    if pattern.is_empty() {
        return route.is_empty();
    }
    
    if pattern[0] == "**" {
        // Matches zero or more segments
        if match_parts(route, &pattern[1..]) {
            return true;
        }
        if !route.is_empty() {
            return match_parts(&route[1..], pattern);
        }
        false
    } else if pattern[0] == "*" {
        // Matches exactly one segment
        if route.is_empty() {
            return false;
        }
        match_parts(&route[1..], &pattern[1..])
    } else {
        // Exact match
        if route.is_empty() || route[0] != pattern[0] {
            return false;
        }
        match_parts(&route[1..], &pattern[1..])
    }
}
```
## Authentication & TLS
### JWT Validation (Layer 2: Session)
Brokers MUST validate JWT in CONNECT handshake:
1. **Signature Validation:** Use external JWT library (e.g., `jsonwebtoken` in Rust)
   - Extract public key from configured issuer
   - Validate signature using `HS256`, `RS256`, or configured algorithm
   - Reject if signature invalid
2. **Expiration Check:** Check `exp` claim against current time
   - Reject if expired
3. **Claim Extraction:** Extract required claims:
   - `realm` (string): Route realm must match exactly
   - `areas` (array of strings): Route area must be in array
   - `scopes` (array of strings): Verb must be in scopes (e.g., `kv:read`, `notice:subscribe`)
4. **Permission Enforcement:** For each request, verify:
   - Route realm ∈ JWT realm (exact match)
   - Route area ∈ JWT areas
   - Request verb ∈ JWT scopes
If any check fails:
- Reject with domain error code `*001` (ERR_UNAUTHORIZED)
- Log rejection for audit
- Continue accepting subsequent requests from same session
### TLS Configuration
Brokers SHOULD support TLS for both WebSocket and TCP:
1. **WebSocket TLS:**
   - Listen on port 4090 (default) with TLS
   - Use `wss://` scheme
   - Configure certificate and private key
2. **TCP TLS:**
   - Listen on port 4091 (default) with TLS
   - Upgrade connection after handshake
   - Configure certificate and private key
3. **Certificate Management:**
   - Load from file on startup
   - Support certificate rotation without restart (future enhancement)
   - Log certificate expiry warnings
4. **Cipher Suites:**
   - Use strong modern cipher suites (TLS 1.2+)
   - Disable weak ciphers (RC4, DES, NULL)
### Session Identification
Brokers SHOULD NOT expose session IDs to clients in standard responses. Session IDs are internal implementation detail. However, for debugging and logs:
- Generate unique session ID per connection
- Track in logs for audit trail
- Use for internal state management (transactions, subscriptions)
- Discard on disconnect
## Boot & Initialization
### Boot Flow
**File:** `src/boot/mod.rs`
```rust
pub async fn boot(config: BootConfig) -> Result<()> {
    // 1. Initialize logging
    tracing_subscriber::fmt::init();
    
    // 2. Initialize storage
    let store = storage::init(&config).await?;
    
    // 3. Initialize runtime
    let (router, ingress, ingress_config, scheduler) = runtime::init(&store)?;
    
    // 4. Register domains
    domains::setup(&router)?;
    
    // 5. Spawn transport listeners
    tokio::spawn(handlers::spawn_tcp_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
    ));
    
    tokio::spawn(handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
    ));
    
    // 6. Wait for shutdown
    tokio::signal::ctrl_c().await?;
    tracing::info!("Fitz broker shutting down...");
    
    Ok(())
}
```
### Configuration
**Type:** `BootConfig`
```rust
pub struct BootConfig {
    pub http_port: u16,              // WebSocket listener (default: 4090)
    pub tcp_port: u16,               // TCP listener (default: 4091)
    pub bind_addr: String,           // Listen address (default: "0.0.0.0")
    pub storage_path: String,        // Midge LSM path (default: "./.fitz")
    pub max_connections: usize,      // Connection limit (default: 10000)
    pub max_frame_size: usize,       // Max frame bytes (default: 1MB)
    pub channel_capacity: usize,     // Queue depth (default: 1000)
}
impl BootConfig {
    pub fn new() -> Self { /* ... */ }
    
    pub fn with_http_port(mut self, port: u16) -> Self { self.http_port = port; self }
    pub fn with_tcp_port(mut self, port: u16) -> Self { self.tcp_port = port; self }
    pub fn with_storage_path(mut self, path: String) -> Self { self.storage_path = path; self }
}
```
## Testing & Validation
### Unit Tests (Per Domain)
Each domain should have comprehensive unit tests for:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn should_parse_operation_from_tlv() {
        // Arrange
        let mut enc = PayloadEncoder::new();
        enc.put_u64(123);
        enc.put_string("test");
        let bytes = enc.finish();
        // Act
        let ctx = FrameContext { msg_type: 100, /* ... */ };
        let result = parse_request(&ctx, &bytes);
        // Assert
        assert!(result.is_ok());
    }
    #[test]
    fn should_encode_response() {
        // Arrange
        let response = DomainResponse::Ok { /* ... */ };
        // Act
        let bytes = encode_response(&response);
        // Assert
        assert!(!bytes.is_empty());
    }
}
```
### Integration Tests
**File:** `tests/*.rs`
```rust
#[tokio::test]
async fn test_kv_begin_get_commit() {
    // Arrange
    let broker = start_broker(Default::default()).await;
    let client = connect_client(&broker).await;
    // Act
    client.send_connect_frame().await;
    let tx_id = client.send_begin_frame("resource", TxMode::ReadWrite).await;
    client.send_put_frame(tx_id, "key", "value").await;
    client.send_commit_frame(tx_id).await;
    // Assert
    // Verify transaction committed and value persisted
}
```
### Test Checklist
- [ ] All domain operations parse correctly
- [ ] All domain operations encode correctly
- [ ] Error responses encode correctly
- [ ] Multi-segment operations work (transactions, streams, etc.)
- [ ] Isolation works (realm/area partitioning)
- [ ] Permissions enforced (JWT claims)
- [ ] Backpressure handled (queue limits)
- [ ] Reconnection restores state (subscriptions, etc.)
## Performance & Tuning
### Sync-Core Benefits
1. **Predictable latency** - No async scheduling jitter
2. **CPU efficiency** - No context switches in hot path
3. **Deterministic behavior** - Reproducible test results
4. **Easier debugging** - Stack traces are meaningful
### Optimization Strategies
#### 1. Lock-Free Data Structures
Use DashMap for subscriptions and routing:
```rust
pub type SubscriptionMap = DashMap<String, Vec<Subscription>>;
```
#### 2. Pre-allocation
Allocate response buffers once:
```rust
pub fn encode_response_into(response: &Response, buf: &mut Vec<u8>) {
    buf.clear();
    // ... encode into pre-allocated buffer
}
```
#### 3. Zero-Copy Patterns
Pass `&[u8]` slices instead of cloning:
```rust
pub fn handle(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
    // Store references, not copies
}
```
#### 4. Batch Operations
For fanout (Notice publish), batch notifications:
```rust
let mut batch = Vec::with_capacity(100);
for subscriber in matched_subscribers {
    batch.push(notification);
}
router.deliver_batch(batch);
```
### Monitoring
Add tracing for performance insights:
```rust
use tracing::{instrument, span, Level};
#[instrument(skip(msg))]
pub fn handle(&mut self, msg: DomainMessage) -> DomainResponse {
    let span = span!(Level::DEBUG, "domain_handler");
    let _guard = span.enter();
    
    tracing::debug!("handling message");
    // ... logic
    tracing::debug!("response ready");
}
```
### Tuning Parameters
| Parameter | Default | Use Case |
|---|---:|---|
| `max_frame_size` | 1 MB | Limit large uploads |
| `channel_capacity` | 1000 | Backpressure threshold |
| `max_connections` | 10000 | Resource limits |
| `scheduler_threads` | num_cpus | CPU-bound work |
## Error Handling
### Transport-Level Errors
Connection is **closed** on:
- Frame size exceeded
- Invalid TLV encoding (unrecoverable)
- CONNECT missing or invalid
- Protocol violation
```rust
if payload.len() > max_frame_size {
    // Close connection
    return Err(Error::FrameTooLarge);
}
```
### Domain-Level Errors
Errors are **returned** in response payload (per-domain encoding):
```rust
pub enum DomainResponse {
    Ok { /* result */ },
    Error(String), // Encoded per domain
}
```
**KV example** (error as string):
```
Response (error):
  [u32 BE error_len]
  [bytes error_msg]
```
**Notice example** (error with status byte):
```
Response (error):
  [u8]     1 (error status)
  [u32 BE] error_len
  [bytes]  error_msg
```
### Idempotency
- **Idempotent ops** (GET, READ, SCAN): safe to retry
- **Non-idempotent ops** (PUT, PUBLISH, APPEND): clients must not retry (or must deduplicate)
Some operations use **correlation IDs** (RPC):
- Client-generated UUID (16 bytes)
- Broker tracks to prevent duplicates
- Allows safe replay
## References
- Protocol specification: [CLIENT.md](CLIENT.md)
- Transport implementation: `src/api/`
- Domain implementations: `src/domains/`
- Routing: `src/runtime/router.rs`
- Codecs: `src/protocol/*_codec.rs`
- Boot: `src/boot/mod.rs`
- Tests: `tests/`, `benches/`
