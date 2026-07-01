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
- **Two-axis isolation:** hard broker isolation by `RouteFamily`, plus app-visible namespace by `realm`
- **Deterministic message routing** (no async jitter in hot paths)
- **High fanout** (pub/sub with wildcard patterns)

For the strict internal contract that defines what each domain is allowed to do, what it must not do, and how domains compose safely, see [domain-boundaries-spec.md](domain-boundaries-spec.md).

`RouteFamily` and `realm` are orthogonal identifiers. `RouteFamily` is the broker-internal routing and isolation key for session assignment, delivery partitioning, and storage partitioning. `realm` is the opaque application-visible namespace label used in Fitz routes, permissions, and admin/API payloads. `realm` and `RouteFamily` are separate axes and must never be inferred, aliased, substituted, or used as fallback values for each other.

The server is implemented in **async I/O boundaries** (transport) with a **100% synchronous core** (routing, domains). This separation ensures:
- Clean transport abstraction (WebSocket, TCP, HTTP)
- Predictable domain latency (no async scheduling variability)
- Efficient enforcement of RouteFamily and realm boundaries
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
├─ Stream (durable append-only logs, commit-time sequencing, ephemeral append sessions)
├─ RPC (request/response, correlation)
├─ Lease (in-memory coordination)
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
### Critical Invariant: Ephemeral Sessions
> **Fitz sessions are ephemeral. The broker never restores session state after disconnect. Clients are responsible for rebuilding all state including subscriptions, transactions, workers, leases, and stream resume position.**

This is a hard Fitz rule:
- Session state exists only for the lifetime of the active connection.
- Disconnect immediately destroys session-owned state.
- Reconnect always creates a new session identity.
- Recovery is client-driven, explicit, and deterministic.

## Layer Responsibilities
### Layer 1: Transport
**Files:** `src/api/tcp.rs`, `src/api/ws.rs`
**Responsibility:** Socket I/O and framing.
**Behavior:**
- Accept connections (WebSocket/HTTP: port 4090; TCP: port 4091 when enabled)
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
**Files:** `src/session/session.rs`, `src/session/permissions.rs`, `src/api/runtime_ingress.rs`, `src/api/session.rs`
**Responsibility:** Connection authentication, permission enforcement, frame routing.
**Behavior:**
- Receive raw frame from transport
- **First frame MUST be CONNECT** with JWT payload
- Validate JWT signature and claims (must validate signature; do NOT trust client parsing)
- Extract JWT identity context (`tid` by default, or configured claim such as Auth0 `org_id`) and resolve it through the broker's route-family map
- Extract route-shaped permissions from the configured custom claim, top-level `permissions`, configured role claim array, `scp`, or `scope`
- Treat `realm` as an opaque route component authorized by permission patterns. Treat RouteFamily as a separate broker-internal routing key, never as a realm fallback or synonym.
- Establish session with extracted claims
- Assign unique session ID (internal; NOT sent to client)
- For each subsequent frame:
  - Parse TLV header (MessageType, length)
  - Extract route scheme (kv, notice, rpc, etc.)
  - Check route-shaped permissions against the frame's authorization route and access level
  - If permission check fails: return domain error with code ERR_UNAUTHORIZED
  - Route to appropriate domain via Router
- Return response bytes to transport
**Session Lifecycle:**
On successful CONNECT:
- Create session with extracted JWT claims
- Track subscriptions, transactions, worker registrations per session
- Treat all tracked state as ephemeral process-local state
- Ready to accept domain requests
On disconnect (graceful or abrupt):
- Immediately clean up:
    - All active subscriptions → drop
    - All live Stream subscriptions → drop
  - All active KV transactions → rollback
  - All active Stream sessions → abort
  - All held Leases → release
  - All RPC worker registrations → unregister
  - Queued notifications → discard
- Session ID becomes invalid (no recovery on reconnect)
On reconnect with new CONNECT:
- Create new session (new session ID)
- Old session ID is discarded
- Client MUST explicitly rebuild any needed state
### Session Recovery Model
**Rules:**
- Sessions are destroyed on disconnect.
- No server-side session recovery exists.
- No subscription persistence exists.
- No transaction persistence exists.
- No worker registration persistence exists.
- No lease ownership persistence exists beyond normal lease expiry or explicit reacquire.
- Disconnect cleanup may create cleanup retry tickets; cleanup retry tickets complete cleanup dispatch only and never restore sessions, ownership, subscriptions, transactions, workers, leases, or inflight state.

**Client requirements:**
- Clients **MUST** re-authenticate after reconnect.
- Clients **MUST** re-subscribe after reconnect.
- Clients **MUST** reopen transactions if needed.
- Clients **MUST** re-register RPC workers after reconnect.
- Clients **MUST** reacquire leases after reconnect when the workflow still requires ownership.
- Clients **SHOULD** track stream offsets locally and resume from the last known offset.
- Clients **MUST NOT** expect Stream subscribe state to resume historical delivery; replay requires an explicit read from a client-managed offset.
- Clients **SHOULD** implement reconnect backoff.
- Clients **MAY** cache subscription and worker registration configuration for fast rebuild.

**Reconnect sequence guidance:**
```text
CONNECT
AUTH
SUBSCRIBE (all)
REGISTER (workers)
RESUME (streams)
READY
```

**Performance expectation:**
Rebuilding state after reconnect must be fast and deterministic. Fitz must keep subscription registration, worker registration, and stream resume paths cheap enough that client-driven rebuild remains practical.

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
Current routing uses exact-address dispatch plus domain-owned subscription indexes. The bullets below are conceptual only; for Notice, the authoritative live state is the broker-local in-memory index owned by `NoticeDomainActor` and `NoticeDomainCore`, which is cleared on disconnect or broker restart.
- **Subscriptions:** `{realm} → {area} → {resource} → [subscribers]`
- **Pattern matching:** `NoticeDomainActor` matches wildcard subscriptions from its in-memory index
- **Fanout:** Single publish reaches all currently connected matching subscriptions
- **Ordering:** Publish order is preserved per subscriber within the running broker process
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

Use [domain-boundaries-spec.md](domain-boundaries-spec.md) as the authoritative boundary contract when deciding whether behavior belongs in a domain at all. This architecture guide explains where domains live in the system; the boundary specification explains what each domain is allowed to own.

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
- **Stream:** `{realm}/{area}/{resource}/offset:{offset}` → `{record}` plus durable resource metadata / area indexes / realm indexes for committed sequencing state
- **Queue:** `{realm}/{area}/{resource}/msg:{message_id}` → `{body}`
- **Schedule:** persisted definitions, next-fire state, and pending fire claims for durable timing intent

Lease has no Layer 5 persistent storage schema. Lease ownership, waiters, TTL state, and fencing tokens are live broker coordination state only.
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
### Notice Domain (Pub/Sub)
**Current files:** `src/domains/notice/sink.rs`, `src/domains/notice/mod.rs`

Current Notice behavior is intentionally ephemeral:
- `NoticeDomainSink` keeps subscription indexes entirely in memory for the current broker process
- Subscriptions are session-scoped and are removed on disconnect cleanup
- Broker restart clears all Notice subscriptions; clients must re-subscribe
- Admin views describe only current in-memory subscriptions and route counters
- Wildcard subscriptions are capped per session to keep matcher cost bounded
- `NoticeRouteActor` remains a focused sync actor and test model for matching and fanout invariants

### Domain Actor, Data, And Admin Contracts
#### KV
- Actor owner: `KvDomainSink` is a thin mailbox adapter; `KvDomainActor` is the managed production actor that processes KV delivery, cleanup, session `KvActor` state, watch fanout, and admin projection updates.
- Persistence: committed values are durable according to the selected write policy; open transactions and watcher state are ephemeral.
- Cleanup: disconnect cleanup is enqueued to the KV actor mailbox, which rolls back live transactions, releases session-owned locks, and drops subscriptions without implying transaction recovery.
- `RouteFamily`/`realm`: committed rows stay partitioned by exact `RouteFamily`; `realm` remains an opaque route label and is never inferred from the family.
- Admin path: live transaction views flow through the actor-maintained `AdminReadModel`; committed value and inventory reads go through `Runtime::kv_*` query facades.

#### Queue
- Actor owner: `QueueDomainSink` is a thin mailbox adapter; `QueueDomainActor` is the managed production actor for delivery, cleanup, runtime sweeps, live admin refresh, dead-letter replay/purge commands, broker-local watch state, and projections. `QueueActor` owns live reservation state, retry bookkeeping, and durable dead-letter mutations for one queue resource.
- Persistence: durable backlog and dead-letter records live in storage; inflight reservations, watch subscriptions, and fast-flush state are ephemeral.
- Cleanup: disconnect cleanup is enqueued to the Queue actor mailbox, which clears worker reservations and watch state without implying durable ownership continuity or hidden worker recovery.
- `RouteFamily`/`realm`: queue data is isolated by exact `RouteFamily`, while `realm` remains an application-defined namespace inside the queue route.
- Admin path: live queue snapshots flow through `Runtime::queue_list_*` and the actor-maintained `AdminReadModel`; dead-letter replay and purge use explicit `Runtime::queue_*_dead_letter` command/reply messages through the actor mailbox.

#### Notice
- Actor owner: `NoticeDomainActor` is the managed production actor for delivery, cleanup, live subscriptions, live count queries, route counters, fanout, and admin snapshot refresh; `NoticeDomainSink` is the mailbox adapter, and `NoticeRouteActor` remains a focused matching/fanout state-machine model.
- Persistence: Notice delivery, subscriptions, and counters are ephemeral only; there is no durable replay or broker-side subscriber recovery.
- Cleanup: disconnect removes session subscriptions immediately, and broker restart starts from an empty Notice state.
- `RouteFamily`/`realm`: fanout matches only within the exact `RouteFamily`; `realm` stays an opaque route segment used for filtering and admin presentation.
- Admin path: admin reads use `Runtime::notice_list_subscriptions()` and `Runtime::notice_list_routes()` backed by the passive `AdminReadModel`.

#### Stream
- Actor owner: `StreamDomainActor` is the managed production actor for delivery, cleanup, live append sessions, subscription overlays, live count queries, admin snapshot refresh, and committed watermark projection; `StreamActor`, `AreaActor`, and `RealmActor` remain focused state-machine models for resource, area, and realm sequencing.
- Current production boundary: `StreamDomainSink` is the mailbox adapter, and `StreamDomainRuntime` executes against `StreamDomainCore` inside the managed actor mailbox.
- Persistence: committed records, metadata, and watermarks are durable; live append sessions and subscriptions are ephemeral.
- Cleanup: disconnect aborts append sessions and drops live subscriptions without restoring them on reconnect.
- `RouteFamily`/`realm`: committed history is partitioned by exact `RouteFamily`, while realm and area indexes stay explicit storage keys rather than family aliases.
- Admin path: read-model projections and watermark views flow through `Runtime::stream_list_*`; committed record inspection uses `Runtime::stream_read_resource_records()`.

#### RPC
- Actor owner: `RpcDomainActor` is the managed production actor for delivery, cleanup, timeout sweeps, live count queries, and admin snapshot sync; `RpcRouteActor` and `ReplyInboxActor` remain focused state-machine models for worker assignment and reply sequencing.
- Current production boundary: `RpcDomainSink` is the mailbox adapter, and `RpcDomainRuntime` executes against `RpcDomainCore` inside the managed actor mailbox.
- Persistence: worker registrations, pending calls, and reply assembly are ephemeral; RPC does not provide restart-safe backlog durability.
- Cleanup: disconnect unregisters workers, expires pending session state, and never restores inflight calls or subscriptions.
- `RouteFamily`/`realm`: dispatch and replies stay within the exact `RouteFamily`; `realm` remains an application-defined route component for operation naming and filters.
- Admin path: worker and pending-call views flow through `Runtime::rpc_list_workers()` and `Runtime::rpc_list_pending()` backed by the read model.

#### Lease
- Actor owner: `LeaseDomainActor` is the production managed actor for delivery, cleanup, and expiry sweeps; `LeaseActor` remains the domain state-machine entrypoint for lease ownership, waiters, expiry, and fencing token progression inside one running broker.
- Current production boundary: `LeaseDomainSink` is the mailbox adapter, and `LeaseDomainRuntime` executes against `LeaseDomainCore` inside the managed actor mailbox.
- Persistence: leases, waiters, and fencing tokens are ephemeral broker-local coordination state only; there is no durable lease history or restart recovery.
- Cleanup: disconnect releases session-owned leases, clears waiters, and never implies cross-restart ownership continuity.
- `RouteFamily`/`realm`: lease coordination is isolated by exact `RouteFamily`; `realm` stays an opaque application namespace carried by the route, not a family synonym.
- Admin path: lease snapshots flow through `AdminReadModel`; waiter inspection uses the explicit `Runtime::lease_list_waiters()` facade, which sends a command/reply read to `LeaseDomainActor`.

#### Schedule
- Actor owner: `ScheduleDomainActor` is the production managed actor for delivery, cleanup, and due-scan commands; `ScheduleActor` owns durable definition state, next-fire tracking, pending claims, and due-scan normalization for one route family.
- Current production boundary: `ScheduleDomainSink` is the mailbox adapter, and `ScheduleDomainRuntime` executes against `ScheduleDomainCore` inside the managed actor mailbox.
- Persistence: schedule definitions, next-fire state, and pending claims are durable timing intent; subscriber watches and transient handoff coordination are ephemeral.
- Cleanup: disconnect removes live watches but does not erase persisted schedule intent or imply replay of every missed interval after downtime.
- `RouteFamily`/`realm`: schedules stay partitioned by exact `RouteFamily`, while `realm` remains an application-defined route label that is never derived from the family.
- Admin path: schedule projections flow through `Runtime::schedule_list_schedules()`; pending claim inspection uses `Runtime::schedule_list_pending_claims()`, which sends a command/reply read to `ScheduleDomainActor`.

**Historical sketch (outdated shape, not the current implementation):**
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
   - `sub`: Subject identity
   - configured route-family identity claim (`tid` by default, `org_id` for Auth0 Organizations)
   - one supported permission source: configured custom permissions claim, top-level `permissions`, configured role claim array, `scp`, or `scope`
   - server-side route family from `FITZ_ROUTE_FAMILY_MAP`; the resolved family must be provisioned by `FITZ_ROUTE_FAMILIES`
4. **Permission Enforcement:** For each request, verify:
   - Request route matches a compiled permission pattern
   - Requested access is granted by the matching permission access fragment
If any check fails:
- Reject with domain error code `*001` (ERR_UNAUTHORIZED)
- Log rejection for audit
- Continue accepting subsequent requests from same session
### TLS And Browser Perimeter
Brokers deployed with runtime auth or protected admin on non-loopback binds must be protected by TLS at the network edge:
1. **WebSocket browser traffic:**
   - Clients use the public `wss://` scheme through the TLS-terminating load balancer
   - Set `FITZ_ASSUME_EXTERNAL_TLS=true` in TLS-terminated deployments so Fitz emits TLS-dependent browser headers such as HSTS
   - Configure exact public `FITZ_WS_ALLOWED_ORIGINS` for browser WebSocket clients; Fitz defaults only to loopback local-development origins
2. **TCP traffic:**
   - Use a TLS-capable load balancer, sidecar, or private trusted network for raw TCP
   - Disable raw TCP with `FITZ_TCP_ENABLED=false` when only browser traffic is needed
3. **Certificate Management:**
   - Managed by the external TLS terminator in current deployments
   - Native listener TLS and in-process certificate rotation are future enhancements
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
    // 1. Initialize observability and validate configuration
    observability::init_observability()?;
    config.validate()?;

    // 2. Initialize runtime before storage so HTTP target health can answer
    let (router, ingress, ingress_config, runtime) = runtime::init(&config)?;

    // 3. Start HTTP early for /targetz; WebSocket upgrades stay gated
    let http = handlers::spawn_http_listener(
        &config,
        ingress.clone(),
        ingress_config.clone(),
        runtime.clone(),
    ).await?;
    
    // 4. Open storage and acquire the active Midge writer lease
    let store = storage::init(&config).await?;
    runtime.mark_storage_ready();
    
    // 5. Register domains
    let domains = domains::setup(&router, &store)?;
    runtime.attach_domains(domains);
    runtime.mark_domains_ready();
    
    // 6. Start TCP after domains exist; both TCP and WebSocket require
    //    runtime.is_ready_for_traffic() before accepting work.
    if config.tcp_enabled {
        tokio::spawn(handlers::spawn_tcp_listener(
            &config,
            ingress.clone(),
            ingress_config.clone(),
        ));
    }
    
    // 7. Mark startup complete so /readyz and /healthz can pass
    runtime.mark_startup_complete();
    
    // 8. Wait for shutdown
    tokio::signal::ctrl_c().await?;
    tracing::info!("Fitz broker shutting down...");
    
    Ok(())
}
```

`/targetz` is intentionally reachable after the HTTP listener starts and before
Midge storage is ready. It is only an orchestrator handoff signal. `/healthz`,
`/readyz`, WebSocket upgrades, and TCP sessions still require strict data-plane
readiness, including active Midge writer-lease ownership.
### Configuration
**Type:** `BootConfig`
```rust
pub struct BootConfig {
    pub http_port: u16,              // WebSocket listener (default: 4090)
    pub tcp_port: u16,               // TCP listener (default: 4091)
    pub tcp_enabled: bool,           // TCP listener enabled (default: true)
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
    pub fn with_tcp_enabled(mut self, enabled: bool) -> Self { self.tcp_enabled = enabled; self }
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
- [ ] Disconnect destroys session state immediately
- [ ] Clients can rebuild required state deterministically after reconnect
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
- Broker uses them to match live in-flight requests to responses
- They do not create a durable replay, recovery, or broker-side deduplication log
## References
- Protocol specification: [client-spec.md](../clients/client-spec.md)
- Transport implementation: `src/api/`
- Domain implementations: `src/domains/`
- Routing: `src/runtime/router.rs`
- Codecs: `src/protocol/*_codec.rs`
- Boot: `src/boot/mod.rs`
- Tests: `tests/`, `benches/`
