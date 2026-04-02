# Fitz Client Specification

**Authoritative protocol specification for Fitz client implementations.**
This document defines what every Fitz client implementation MUST do to interoperate with any conformant Fitz broker.

## Table of Contents

1. [Scope & Non-Goals](#scope--non-goals)
2. [Client Model](#client-model)
3. [Recommended Client API Design](#recommended-client-api-design)
4. [Terminology & Definitions](#terminology--definitions)
5. [Supported Transports](#supported-transports)
6. [Wire Protocol](#wire-protocol)
7. [Connection Lifecycle](#connection-lifecycle)
8. [Authentication & Security](#authentication--security)
9. [Routing](#routing)
10. [HTTP-Like Design Principle](#http-like-design-principle)
11. [Verbs](#verbs)
12. [Client Server Boundary](#client-server-boundary)
13. [Permissions](#permissions)
14. [Transactions](#transactions)
15. [Subscriptions](#subscriptions)
16. [Request/Response Correlation](#requestresponse-correlation)
17. [Error Handling](#error-handling)
18. [Idempotency & Retry Strategy](#idempotency--retry-strategy)
19. [Domains](#domains)
20. [Constants & TLV Registry](#constants--tlv-registry)
21. [Acceptance Criteria](#acceptance-criteria)

## Scope & Non-Goals

### What This Spec Covers

- Wire protocol (framing, TLV encoding, message format)
- Transport requirements (WebSocket, TCP)
- Authentication (JWT)
- Message lifecycle (send, receive, acknowledge)
- Verb definitions and wire codes
- Error codes and recovery
- Conformance test suite

### What This Spec DOES NOT Cover

This spec **explicitly does not** address:

- **Business logic modeling** - Clients MUST NOT enforce domain semantics (isolation levels, isolation theory, transaction isolation, etc.). These are server-enforced.
- **Route builders or helpers** - Clients MUST NOT provide opinionated route construction. Route strings are opaque to clients.
- **Resource modeling** - Clients MUST NOT model realms, areas, or resources as typed classes (unless purely for API ergonomics, outside the core protocol).
- **Higher-level frameworks** - Clients are transport adapters, not abstractions. Do not layer object-relational mapping, schema validation, or other framework concerns into the core client.
- **Performance optimization** - Clients MUST implement the spec correctly. Optimization strategies (connection pooling, caching, batching) are optional and local.
- **Failover or replication** - Clients connect to a single broker endpoint. Multi-broker topology is out of scope.

## Client Model

A Fitz client is a **synchronous or asynchronous transport adapter** that:

1. Manages a single connection to a Fitz broker
2. Encodes client requests as TLV frames
3. Sends frames over WebSocket or TCP
4. Receives TLV response frames
5. Decodes responses and returns them to caller
6. Handles transport-level errors (disconnect, timeout)
7. Exposes a simple, language-native API
   **Clients are NOT responsible for:**

- Broker topology or failover
- Route validation or normalization
- Domain logic enforcement
- Request deduplication or idempotency
- Caching or memoization
- Session migration across brokers

## Recommended Client API Design

While the wire protocol requires routes in every message for self-contained operations, client implementations SHOULD provide ergonomic abstractions that hide this verbosity from end users.

### Pattern: Return Domain-Specific Objects

**Good abstraction:**

```python
# begin() returns a Transaction object
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)

# Transaction methods hide route repetition
tx.put(b"key", b"value")      # Simple API
tx.get(b"key")                 # Focus on data
tx.commit()                    # No route visible
```

**Under the hood (wire protocol):**

```python
class KvTransaction:
    def __init__(self, client, route, tx_id):
        self._client = client
        self._route = route      # Stored internally
        self._tx_id = tx_id

    def put(self, key, value):
        # Wire protocol: sends tx_id + route + key + value
        return self._client._send_kv_put(
            self._tx_id,
            self._route,  # ← Sent on wire every time
            key,
            value
        )

    def get(self, key):
        return self._client._send_kv_get(self._tx_id, self._route, key)

    def commit(self):
        return self._client._send_kv_commit(self._tx_id, self._route)
```

### Why This Pattern Works

1. **Wire Protocol Compliance**: Every message includes full context (tx_id + route)
2. **User Ergonomics**: Users don't repeat `route` in every call
3. **Explicit Routing**: Every message includes full route context, so the wire format never relies on hidden per-connection addressing state
4. **Disconnect Safety**: If connection drops mid-transaction, the broker cleans up session-scoped state and the client can reconnect and start a new transaction instead of resuming the old `tx_id`
5. **Language Idiomatic**: Feels natural in each language (Python context managers, Rust Drop trait, Go defer)

### Anti-Pattern: Implicit State

**❌ WRONG - Do not do this:**

```python
# BAD: Client stores route globally or per-connection
client = FitzClient(realm="prod")  # ❌ Implicit realm
tx = client.kv_begin("users")      # ❌ Incomplete route

# BAD: Server tracks route per tx_id
tx.put(b"key", b"value")  # ❌ Server must remember route from BEGIN
```

**Why it's wrong:**

- Couples client to single realm
- Makes wire protocol stateful (server must track route per tx_id)
- Breaks on reconnection (server state lost)
- Violates self-contained operation principle

### Recommended Patterns by Language

**Python (Parallel Transactions & Queues):**

```python
# Sequential within a transaction (one tx at a time)
with client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync) as tx:
    tx.put(b"key", b"value")
    tx.commit()

# BUT: Multiple transactions to different resources run in PARALLEL
tx_users = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)
tx_posts = client.kv_begin("kv://prod/app/posts", TxMode.ReadWrite, Durability.Sync)
# Both active simultaneously, on same KV channel to different actor instances
tx_users.put(b"u1", b"alice")
tx_posts.put(b"p1", b"hello")
tx_users.commit()
tx_posts.commit()

# Parallel queue enqueues to different queues
msg_id_1 = client.queue_enqueue("queue://prod/app/tasks", b"task1")
msg_id_2 = client.queue_enqueue("queue://prod/app/events", b"event1")
# Both complete in parallel

# Cross-domain multiplexing (different channels run in parallel)
notice_sub = client.notice_subscribe("notice://prod/app/*")
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)
# Now client can receive notifications while KV transaction is in flight
# (they're on different channels)
```

**Rust (Drop trait, Parallel Transactions):**

```rust
// Multiple transactions to different resources in parallel
let mut tx_users = client.kv_begin("kv://prod/app/users", TxMode::ReadWrite, Durability::Sync)?;
let mut tx_posts = client.kv_begin("kv://prod/app/posts", TxMode::ReadWrite, Durability::Sync)?;

// Both can be active simultaneously
tx_users.put(b"key", b"value")?;
tx_posts.put(b"key", b"value")?;

tx_users.commit()?;  // Or auto-rollback in Drop
tx_posts.commit()?;
```

**Go (defer, Parallel Transactions):**

```go
// Multiple concurrent transactions to different resources
tx1, _ := client.KvBegin("kv://prod/app/users", TxModeReadWrite, DurabilitySync)
tx2, _ := client.KvBegin("kv://prod/app/posts", TxModeReadWrite, DurabilitySync)
defer tx1.Rollback()  // Safe cleanup
defer tx2.Rollback()  // Safe cleanup

// Both active simultaneously
tx1.Put([]byte("key"), []byte("value"))
tx2.Put([]byte("key"), []byte("value"))
tx1.Commit()  // Clears rollback flag
tx2.Commit()  // Clears rollback flag
```

**JavaScript (Promises, Parallel Transactions & Queues, Parallel Across Domains):**

```javascript
// Parallel transactions to different resources (same channel / KV domain)
const tx_users = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync);
const tx_posts = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite, Durability.Sync);

await Promise.all([
  tx_users.put(Buffer.from("key"), Buffer.from("alice")),
  tx_posts.put(Buffer.from("key"), Buffer.from("hello"))
]);

await Promise.all([
  tx_users.commit(),
  tx_posts.commit()
]);

// Parallel queue enqueues to different queues (same channel / Queue domain)
const msg1 = client.queueEnqueue("queue://prod/app/tasks", Buffer.from("task1"));
const msg2 = client.queueEnqueue("queue://prod/app/events", Buffer.from("event1"));
await Promise.all([msg1, msg2]);

// Cross-domain parallelism (different channels: Notice + RPC)
const notice_sub = client.noticeSubscribe("notice://prod/app/*");
const rpc_call = client.rpcRequest(
  "rpc://prod/app/worker/run",
  "inbox://session/replies",
  correlation_id_uuid,
  Buffer.from("payload")
);
// All three run in parallel (different channels)
```

### Key Principles

**"Ergonomic API, Self-Contained Wire Protocol with Channel-Based Multiplexing"**

- User-facing API: hide route repetition and correlation ID management (where applicable) via object methods
- Wire protocol: every message includes full context (route + tx_id/session_id/etc.)
- **Channel-based multiplexing**: Different domains (KV, RPC, Notice) run on independent logical channels, allowing true concurrent operations across domains
- **Multiple transactions can parallelize**: You can have 2+ transactions to different resources active simultaneously. **BUT:** one transaction (single tx_id) MUST be strictly sequential
- **Multiple queues can parallelize**: Different queue resources can have parallel requests
- **RPC exception**: RPC domain uses 16-byte UUID `correlation_id` for per-request correlation, enabling multiple in-flight RPC requests to be matched to responses
- No server-side implicit state beyond what's in the message
- Reconnection safe: breaking connection doesn't leave orphaned server state

## Concurrency & Multiplexing Patterns

Fitz supports **true concurrent multiplexing** of operations. Understanding the concurrency model is essential for building efficient clients.

### Three Levels of Concurrency

#### Level 1: Cross-Domain Parallelism ✅ FULLY PARALLEL

Different domains run on independent logical channels. A single client connection can have operations from KV, Queue, Notice, RPC, Stream, Lease, and Schedule all in flight simultaneously.

```javascript
// All four domains active at the same time
const kv_tx = client.kvBegin("kv://prod/app/data", TxMode.ReadWrite, Durability.Sync);
const queue_msg = client.queueEnqueue("queue://prod/app/tasks", payload);
const notice_sub = client.noticeSubscribe("notice://prod/app/*");
const rpc_call = client.rpcRequest("rpc://prod/app/config/get", reply_route, correlation_id, payload);

// All complete independently and concurrently
await Promise.all([kv_tx.commit(), queue_msg, notice_sub, rpc_call]);
```

**Channel Assignment:**
- KV operations → KV Channel
- Queue operations → Queue Channel
- Notice operations → Notice Channel
- RPC operations → RPC Channel
- Stream operations → Stream Channel
- Lease operations → Lease Channel
- Schedule operations → Schedule Channel

#### Level 2: Same-Domain, Different-Resource Parallelism ✅ PARALLEL

Within a single domain, you can have multiple concurrent operations to **different resources**. Each resource has its own actor instance on the server, so they execute independently.

**KV Example:**
```javascript
// Two transactions to different resources (users vs posts)
const tx_users = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync);
const tx_posts = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite, Durability.Sync);

// Both can execute concurrently (different actor instances)
await Promise.all([
  tx_users.put(b"key", b"value"),
  tx_posts.put(b"key", b"value")
]);

// Both can commit concurrently
await Promise.all([
  tx_users.commit(),
  tx_posts.commit()
]);
```

**Queue Example:**
```javascript
// Enqueue to different queues in parallel
const task_msg = client.queueEnqueue("queue://prod/app/tasks", b"task");
const event_msg = client.queueEnqueue("queue://prod/app/events", b"event");

// Both complete in parallel (different queue actor instances)
await Promise.all([task_msg, event_msg]);
```

**Why This Works:**
- Router partitions by (realm, area, resource) → different actors
- Each actor has independent state
- Server executor can schedule multiple actors from the same domain concurrently
- No blocking between resource pairs

#### Level 3: Same Transaction Cannot Parallelize ⚠️ STRICT SEQUENTIAL

**ONE transaction (single tx_id) CANNOT have parallel calls.** All operations within a single transaction MUST be sequential.

However, **MULTIPLE transactions can be parallelized** (see Level 2).

**KV: One Transaction (Sequential):**
```javascript
// ✅ CORRECT - operations on SAME transaction are sequential
const tx = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync);
await tx.put(b"k1", b"v1");      // Request 1 → Response 1
await tx.put(b"k2", b"v2");      // Request 2 → Response 2 (after Request 1 completes)
await tx.commit();                // Request 3 → Response 3 (after Request 2 completes)
```

**❌ WRONG - Parallel calls on SAME transaction:**
```javascript
// DO NOT DO THIS - multiple parallel operations on same tx_id
const tx = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync);
await Promise.all([
  tx.put(b"k1", b"v1"),  // ❌ These would interleave incorrectly
  tx.put(b"k2", b"v2"),  // ❌ Same tx_id cannot have concurrent calls
]);
```

**✅ CORRECT - Multiple transactions in parallel:**
```javascript
// DO THIS INSTEAD - different transactions to different resources
const tx1 = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync);
const tx2 = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite, Durability.Sync);
await Promise.all([
  tx1.put(b"k1", b"v1"),   // ✅ Different tx_id, can be parallel
  tx2.put(b"k1", b"v1"),   // ✅ Different tx_id, can be parallel
]);
await Promise.all([tx1.commit(), tx2.commit()]);
```

**Why Not Within One Transaction:**
- Single tx_id maintains transaction state on server
- Concurrent operations on same tx_id violate transaction semantics (ACID isolation)
- Server processes operations for a given tx_id sequentially
- Out-of-order request delivery would corrupt transaction state

**Queue: Same Queue Cannot Be Parallelized:**
```javascript
// ❌ WRONG - Parallel operations on same queue
const lease1_promise = client.queueReserve("queue://prod/app/tasks", lease_secs=30);
const lease2_promise = client.queueReserve("queue://prod/app/tasks", lease_secs=30);
await Promise.all([lease1_promise, lease2_promise]);  // ❌ FIFO ordering violated

// ✅ CORRECT - Single batch request
const leases = client.queueReserve("queue://prod/app/tasks", lease_secs=30, batch_size=10);
// Single request returns multiple messages, FIFO order preserved

// ✅ CORRECT - Multiple queues can be parallelized
const task_leases = client.queueReserve("queue://prod/app/tasks", lease_secs=30);
const event_leases = client.queueReserve("queue://prod/app/events", lease_secs=30);
await Promise.all([task_leases, event_leases]);  // ✅ Different queues, parallel OK
```

### RPC Exception: True Per-Request Multiplexing

RPC is the **only domain with true per-request multiplexing via correlation IDs**. Multiple RPC requests can be in flight simultaneously on the same channel, and responses are matched by UUID.

RPC is also explicitly ephemeral. Worker registrations and pending requests are live in-memory state for the current broker process only. If a worker disconnects or the broker restarts, registrations disappear, in-flight requests are lost, and clients must re-register workers and retry any required work at the application layer.

```javascript
// Multiple RPC calls in flight, responses matched by correlation_id
const rpc1 = client.rpcRequest("rpc://prod/app/config/get", reply_route, uuid1, payload1);
const rpc2 = client.rpcRequest("rpc://prod/app/config/get", reply_route, uuid2, payload2);

// Responses arrive in any order and are matched by correlation_id
const [resp1, resp2] = await Promise.all([rpc1, rpc2]);
```

### Backpressure & Flow Control

Clients SHOULD implement backpressure:

1. **Per-channel backpressure**: If a channel queue fills (typical limit: 1000 messages), retry with exponential backoff before sending next request
2. **Per-connection monitoring**: Track in-flight request count; implement concurrency limit on client side (e.g., max 50 concurrent requests)
3. **Graceful degradation**: If server rejects with backpressure error (429-like), pause and retry

```python
# Recommended: Limit concurrent operations per connection
MAX_INFLIGHT = 50

async def safe_enqueue(client, route, payload):
    while client.inflight_count >= MAX_INFLIGHT:
        await asyncio.sleep(0.01)  # Backoff
    return await client.queue_enqueue(route, payload)
```

### Summary: Concurrency Matrix

| Scenario | Status | Rule | Example |
|----------|--------|------|---------|
| **Different domains** | ✅ Parallel | Domains on independent channels | KV + Queue + Notice simultaneously |
| **Same domain, different resources** | ✅ Parallel | Each resource has independent actor instance | 2 KV txs to different tables; 2 queue enqueues |
| **ONE transaction, multiple calls** | ❌ NOT parallel | Single tx_id MUST be sequential | `await tx.put(); await tx.commit();` |
| **ONE queue, multiple operations** | ❌ NOT parallel | FIFO ordering must be preserved | Use `batch_size` parameter instead |
| **RPC requests** | ✅ Parallel (correlation_id) | Per-request UUID correlation matching | Multiple RPC calls matched by UUID |

### Best Practice: When to Parallelize

✅ **DO parallelize:**
- Different transactions (`tx1`, `tx2`)
- Different queues or resources
- Different domains (KV + Notice + RPC)
- RPC requests (via correlation_id)

❌ **DO NOT parallelize:**
- Operations within one transaction
- Multiple operations on same queue resource
- Anything that shares a tx_id

**Simple Rule: If they share an ID (tx_id, queue instance), make them sequential.**

## Terminology & Definitions

Use these exact terms. Other terms are forbidden.
| Term | Definition | Forbidden Alternatives |
| ---------------: | ------------------------------------------------------------------ | --------------------------------- |
| **realm** | Isolation boundary for resources within a broker | `tenant`, `organization` |
| **area** | Namespace within a realm | `namespace`, `collection` |
| **resource** | Named entity within an area (e.g., table, queue, stream) | — |
| **route** | URI-like string addressing a resource or operation | `endpoint`, `path`, `key` |
| **verb** | Operation name (e.g., `GET`, `PUT`, `PUBLISH`) | `operation`, `method` (ambiguous) |
| **domain** | Service category (kv, queue, notice, stream, rpc, lease, schedule) | — |

**Internal routing rule:** Clients send `CONNECT(jwt)` and opaque route strings only. Broker-internal partitioning, shard placement, and other session routing state are not part of the client contract.

**Forbidden terminology in client code:**

- NEVER use `tenant` — use `realm`
- NEVER use `namespace` — use `area`
- NEVER use `endpoint` — use `route`
- NEVER use `topic` (except within domain-specific docs) — use `route`

## Supported Transports

Fitz supports exactly two transports. Clients MUST implement both identically; behavior MUST be transport-agnostic.

### WebSocket (Binary Frames)

- **URI scheme:** `wss://` (recommended) or `ws://`
- **Handshake:** Standard WebSocket upgrade
- **Message format:** Each binary frame = one complete TLV frame payload
- **Use case:** Browsers, long-lived connections
  **Constraints:**
- Text frames MUST be rejected
- Connection close frames MUST be handled gracefully
- Ping/pong frames MAY be used for keepalive

### TCP (Length-Prefixed Frames)

- **Port:** Application-configurable (default: 4091)
- **Frame format:** `[u32 BE length][payload bytes]`
  - `length` = byte count of payload (excludes the 4-byte prefix)
  - `0 < length <= broker.max_frame_size`
- **Framing:** Must implement length-prefixed parsing with buffering
- **Use case:** Low-latency, long-lived, high-throughput
  **Constraints:**
- Clients MUST implement buffered reading to handle partial frames
- Clients MUST validate `length` before allocating (prevent OOM)
- Clients MUST close connection if length exceeds `max_frame_size`

### Protocol Equivalence

Both transports carry identical **TLV payloads**. A client receiving the same payload over both transports MUST produce identical behavior.

```
WebSocket transport          TCP transport
    ↓                              ↓
[binary frame payload]      [u32 len][payload]
    ↓                              ↓
     └──→ TLV parser ←─────────────┘
             ↓
        [identical decode]
```

## Wire Protocol

### TLV Record Encoding

Frame payloads consist of one or more **TLV records** concatenated back-to-back.
Each record is:

- **Type** (variable, 1 or 3 bytes):
  - If `type <= 0xFE`: encoded as a single byte
  - If `type > 0xFE`: escape byte `0xFF` followed by 2-byte big-endian u16

**IMPORTANT:** The wire examples in this document show MessageTypes as 2-byte big-endian for readability. Conformant implementations MUST use the variable-length encoding: types 0–254 are 1 byte, types 255+ use the `0xFF` escape followed by `u16 BE`. See **Type Encoding Rules** at the end of the Constants section for decoder/encoder pseudocode.
- **Length** (u16, big-endian): byte count of value (0..=65535)
- **Value**: exactly `length` bytes

### Message Framing (How Domain Operations Map to TLV)

**CRITICAL: Every Fitz message is a single TLV record where:**
- **Type** = MessageType (verb wire code: 100-108 for KV, 500-504 for Notice, etc.)
- **Length** = Total byte count of domain payload (all fields concatenated)
- **Value** = Domain-specific fields (as documented per domain)

**TLV is NOT nested** - the entire domain payload is the TLV Value, pre-encoded.

#### Message Structure

```
[MessageType (u16 BE)][Length (u16 BE)][Payload (Length bytes)]
│                     │                 │
│                     │                 └─ Domain fields (concatenated)
│                     └─ Total payload size
└─ Verb wire code
```

#### Complete Message Examples

**Example 1: KV PUT (MessageType=104)**

Wire format specification:
```
[u64 BE]   tx_id
[u32 BE]   route_len
[bytes]    route
[u32 BE]   key_len
[bytes]    key
[u32 BE]   value_len
[bytes]    value
```

Actual bytes on wire:
```
[0x00 0x68]                              (MessageType=104, KV PUT)
[0x00 0x39]                              (Length=57 bytes)
  [0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x01]  (tx_id=1, u64 BE)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]  (route="kv://prod/app/users", 21 bytes)
  [0x00 0x00 0x00 0x03]                  (key_len=3)
  [62 6f 62]                             (key="bob", 3 bytes)
  [0x00 0x00 0x00 0x05]                  (value_len=5)
  [61 6c 69 63 65]                       (value="alice", 5 bytes)

Total frame size: 2 (type) + 2 (length) + 57 (payload) = 61 bytes
```

**Example 2: Notice SUBSCRIBE (MessageType=501)**

Wire format specification:
```
[u32 BE]   route_pattern_len
[bytes]    route_pattern
```

Actual bytes on wire:
```
[0x01 0xF5]                              (MessageType=501, Notice SUBSCRIBE)
[0x00 0x18]                              (Length=24 bytes)
  [0x00 0x00 0x00 0x14]                  (route_pattern_len=20)
  [6e 6f 74 69 63 65 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 2a]  (pattern="notice://prod/app/*", 20 bytes)

Total frame size: 2 (type) + 2 (length) + 24 (payload) = 28 bytes
```

**Example 3: KV BEGIN (MessageType=100)**

Wire format specification:
```
[u32 BE]  route_len
[bytes]   route
[u8]      mode (0=ReadOnly, 1=ReadWrite)
[u8]      durability (0=Buffered, 1=Sync)
```

Actual bytes on wire:
```
[0x00 0x64]                              (MessageType=100, KV BEGIN)
[0x00 0x1F]                              (Length=31 bytes)
  [0x00 0x00 0x00 0x15]                  (route_len=21)
  [6b 76 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 75 73 65 72 73]  (route="kv://prod/app/users", 21 bytes)
  [0x01]                                 (mode=1, ReadWrite)
  [0x01]                                 (durability=1, Sync)

Total frame size: 2 (type) + 2 (length) + 31 (payload) = 35 bytes
```

**Example 4: RPC REQUEST (MessageType=302)**

Wire format specification:
```
[16 bytes] correlation_id (UUID)
[u32 BE]   route_len
[bytes]    route
[u32 BE]   reply_route_len
[bytes]    reply_route
[u32 BE]   body_len
[bytes]    body
```

Actual bytes on wire:
```
[0x01 0x2E]                              (MessageType=302, RPC REQUEST)
[0x00 0x3A]                              (Length=58 bytes)
  [12 34 56 78 9a bc de f0 12 34 56 78 9a bc de f0]  (correlation_id, 16 bytes UUID)
  [0x00 0x00 0x00 0x10]                  (route_len=16)
  [72 70 63 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 77 6f 72 6b 65 72]  (route="rpc://prod/app/worker", 16 bytes)
  [0x00 0x00 0x00 0x13]                  (reply_route_len=19)
  [72 70 63 3a 2f 2f 70 72 6f 64 2f 61 70 70 2f 63 61 6c 6c 65 72]  (reply_route="rpc://prod/app/caller", 19 bytes)
  [0x00 0x00 0x00 0x03]                  (body_len=3)
  [66 6f 6f]                             (body="foo", 3 bytes)

Total frame size: 2 (type) + 2 (length) + 58 (payload) = 62 bytes
```

#### Transport Layer Framing

**WebSocket:**
```
Binary WebSocket frame contains raw TLV message:
[MessageType][Length][Payload]

Example: Send KV PUT
WebSocket binary frame body = 51 bytes (from Example 1 above)
```

**TCP (with length-prefixed framing):**
```
[Frame Length (u32 BE)][MessageType][Length][Payload]
│                       │
│                       └─ TLV message
└─ Total message size (including MessageType + Length + Payload)

Example: Send KV PUT (51 bytes TLV)
[0x00 0x00 0x00 0x33]  (frame_length=51)
[0x00 0x68]            (MessageType=104)
[0x00 0x2F]            (Length=47)
[...47 bytes payload...]
```

#### Reference Decoder Pseudocode

```python
def decode_frame(frame_bytes):
    """Decode a single TLV frame into a domain message."""
    # Parse TLV header
    message_type = read_u16_be(frame_bytes[0:2])
    length = read_u16_be(frame_bytes[2:4])
    payload = frame_bytes[4:4+length]
    
    # Verify payload matches declared length
    if len(payload) != length:
        raise ProtocolError("Payload length mismatch")
    
    # Route to domain decoder based on MessageType
    if 100 <= message_type <= 199:
        return decode_kv_message(message_type, payload)
    elif 200 <= message_type <= 299:
        return decode_queue_message(message_type, payload)
    elif 300 <= message_type <= 399:
        return decode_rpc_message(message_type, payload)
    elif 400 <= message_type <= 499:
        return decode_lease_message(message_type, payload)
    elif 500 <= message_type <= 599:
        return decode_notice_message(message_type, payload)
    elif 600 <= message_type <= 699:
        return decode_stream_message(message_type, payload)
    elif 700 <= message_type <= 799:
        return decode_schedule_message(message_type, payload)
    else:
        raise UnknownMessageType(message_type)

def decode_kv_message(message_type, payload):
    """Decode KV domain message based on MessageType."""
    offset = 0
    
    if message_type == 100:  # BEGIN
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        mode = payload[offset]
        offset += 1
        durability = payload[offset]
        offset += 1
        
        return KvBegin(route=route, mode=mode, durability=durability)
    
    elif message_type == 104:  # PUT
        tx_id = read_u64_be(payload[offset:offset+8])
        offset += 8
        
        route_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        route = payload[offset:offset+route_len].decode('utf-8')
        offset += route_len
        
        key_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        key = payload[offset:offset+key_len]
        offset += key_len
        
        value_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        value = payload[offset:offset+value_len]
        offset += value_len
        
        # Verify all payload consumed
        if offset != len(payload):
            raise ProtocolError("Trailing data in PUT payload")
        
        return KvPut(tx_id=tx_id, route=route, key=key, value=value)
    
    # ... other KV verbs

def decode_notice_message(message_type, payload):
    """Decode Notice domain message based on MessageType."""
    offset = 0
    
    if message_type == 501:  # SUBSCRIBE
        pattern_len = read_u32_be(payload[offset:offset+4])
        offset += 4
        pattern = payload[offset:offset+pattern_len].decode('utf-8')
        offset += pattern_len
        
        if offset != len(payload):
            raise ProtocolError("Trailing data in SUBSCRIBE payload")
        
        return NoticeSubscribe(pattern=pattern)
    
    # ... other Notice verbs
```

**Key Insights for Implementers:**

1. **Single TLV Level:** Each message is ONE TLV record, not nested TLVs
2. **MessageType = Verb:** The TLV Type field IS the verb wire code (100-799)
3. **Payload = Concatenated Fields:** Just concatenate domain fields in order (no internal TLV structure)
4. **Length Validation:** Always verify `offset == len(payload)` after decoding (detects trailing data)
5. **Transport Agnostic:** Same TLV message format for WebSocket and TCP (TCP adds outer length prefix)

### Primitive Encodings

All fields use **big-endian** byte order.
| Type | Encoding |
| ------------- | ------------------------------- |
| `u8` | single byte |
| `u16` | 2 bytes, big-endian |
| `u32` | 4 bytes, big-endian |
| `u64` | 8 bytes, big-endian |
| `String` | `[u32 BE len][UTF-8 bytes]` |
| `Bytes` | `[u32 BE len][raw bytes]` |
| `Optional<T>` | `[u8 present]` + T if present=1 |
| `UUID` | 16 raw bytes (no hyphens) |

### Encoding Invariants

Clients MUST:

1. Encode all integers in big-endian byte order
2. Consume all bytes in request payloads; error if trailing data remains
3. Encode responses with exact length prefixes
4. Handle both single-byte and escape-byte MessageTypes identically
5. **Duplicate TLV tags are NOT permitted within a single frame.** If a TLV tag appears more than once in a frame the frame **MUST** be treated as malformed and the receiver **MUST** close the connection with a TLV parse error. **Rationale:** Fitz TLV disallows duplicate tags to keep decoding deterministic and to simplify client implementations and conformance testing.
6. **A single TLV value MUST NOT exceed 65535 bytes (≈64 KiB).** Large payloads MUST be split across multiple frames or multiple operations — never by repeating the same TLV tag within a single frame (which would violate rule 5).

### Response Format Convention

**All domain responses follow this standard structure:**

1. **Status byte** (u8): 0=success, 1=error
2. **If success (status=0):** Domain-specific success payload
3. **If error (status=1):** Error message
   ```
   [u32 BE] error_len
   [bytes]  error_msg (UTF-8, human-readable)
   ```

**Clients MUST check status byte first before parsing payload.**

**Exception: RPC Domain**
RPC responses include a `correlation_id` field (16-byte UUID) to match responses to requests across multiple in-flight operations. See [RPC Domain](#rpc-domain-requestresponse--streaming) for details on how RPC enables per-request correlation.

**Example (KV GET success):**
```
[0x00]                    (status=0, success)
[0x00 0x00 0x00 0x05]     (value_len=5)
[0x61 0x6c 0x69 0x63 0x65] ("alice")
```

**Example (KV GET error):**
```
[0x01]                    (status=1, error)
[0x00 0x00 0x00 0x0d]     (error_len=13)
[0x4b 0x65 0x79...]       ("Key not found")
```

**Rationale:** Standardized error format across all domains simplifies client error handling and ensures consistent debugging experience. Multiplexing is channel-based for different domains; RPC is the only domain with explicit per-request correlation IDs for true request/response matching.

### Frame Size Limits

**Default maximum frame size: 1 MB (1,048,576 bytes)**

**Rules:**
- Client MUST NOT send frames exceeding broker's limit
- Broker MUST close connection if frame exceeds limit (transport error)
- No negotiation protocol (clients assume 1 MB default)
- Deployments with custom limits MUST document them
- Clients SHOULD make `max_frame_size` configurable

**Handling large payloads:**
- Split across multiple operations (e.g., batch ENQUEUE)
- Use streaming (e.g., Stream APPEND multiple records)
- Application-level chunking (not protocol-level)
- For Queue/Notice/RPC: Keep payload under limit or use external blob storage with reference

**Discovery:**
- No runtime discovery mechanism
- Clients assume 1 MB by default
- Server documentation MUST specify if non-default

## Connection Lifecycle

### 1. Open Transport

- **WebSocket:** `wss://broker:port/` (TLS recommended)
- **TCP:** `tcp://broker:port` (TLS recommended)
- Broker address and credentials must be configured before opening

### Client State Machine

Clients SHOULD implement a simple connection state machine to keep behavior predictable and testable.
States:

- DISCONNECTED → CONNECTING → AUTHENTICATED → CLOSED
  Transitions:
- DISCONNECTED: initial state
- CONNECTING: transport open; send CONNECT
- AUTHENTICATED: CONNECT accepted (no close); ready for domain requests
- CLOSED: transport closed or unrecoverable error
  ASCII diagram:

```
DISCONNECTED --(open transport)--> CONNECTING --(CONNECT & accepted)--> AUTHENTICATED
     ^                                            |
     |                                            v
     +---------(close / unrecoverable)----------- CLOSED
```

Notes:

- Clients MUST handle transport failures and implement exponential backoff on reconnect.
- **Multiplexing Support**: Clients MAY send multiple in-flight requests **on different channels** (domains). For example, a client can send a KV PUT while also sending a Notice PUBLISH—these go to different logical channels and are processed independently. However, within a single domain, clients SHOULD follow request/response sequencing unless the domain supports explicit correlation IDs (currently only RPC).

### 2. Send CONNECT Record (FIRST MESSAGE)

Clients MUST send a **CONNECT** TLV record as the first message:

```
MessageType: 1 (CONNECT)
Value: compact JWT string bytes (UTF-8), NO length prefix
Length: JWT byte length
```

**Example (Authenticated Mode):**

```
[0x01]                    (MessageType=1)
[0x00 0x63]               (Length=99, u16)
[99 bytes of JWT...]
```

**Example (Anonymous Mode - Empty JWT):**

```
[0x01]                    (MessageType=1)
[0x00 0x00]               (Length=0, u16)
(no JWT bytes)
```

**Constraints:**

- CONNECT MUST be first frame sent
- **Authenticated mode (`FITZ_AUTH_REQUIRED=true`):** JWT required, invalid JWT causes connection close
- **Anonymous mode (`FITZ_AUTH_REQUIRED=false`):** JWT optional, empty or placeholder accepted
- JWT payload MUST be valid UTF-8 (if present)
- Clients SHOULD implement CONNECT timeout (5–10 seconds)

### 3. Await Broker Confirmation

**Session Confirmation Protocol:**
Broker behavior:

- **Valid CONNECT:** No explicit ACK message. Broker remains silent and is ready for requests.
- **Invalid CONNECT:** Broker closes connection within 1 second (no response frame sent)
- **No CONNECT within 10 seconds:** Broker closes connection with graceful shutdown

Clients MUST:

- Send CONNECT as the first frame after transport is established
- **Immediately proceed to send domain requests** after sending CONNECT (do not wait for an ACK)
- If the broker rejects the JWT, it closes the connection — the client discovers this when the transport drops or when the first domain response fails
- If the connection closes within 1 second of CONNECT, treat as authentication failure
- Do NOT retry with the same JWT after auth failure
- Implement a CONNECT timeout of 5–10 seconds — if no domain response AND no connection close within this window, close and retry with backoff

**Recommended client pattern:**

```
1. Open transport (WebSocket/TCP)
2. Send CONNECT frame with JWT
3. Immediately send first domain request (e.g., KV BEGIN, Notice SUBSCRIBE)
4. If domain response arrives → connection is authenticated and working
5. If connection closes → auth failure, do NOT retry same JWT
6. If neither within timeout → close, retry with backoff
```
  **Session State After Successful CONNECT:**
  On successful CONNECT, broker creates session and MUST:
- Assign unique session ID (internal use only)
- Extract JWT claims (realm, areas, scopes)
- Establish permissions for all subsequent requests
- Track active subscriptions, transactions, and resources
  **Session Cleanup On Disconnect:**
  When client disconnects:
- All active subscriptions are dropped
- All active transactions (KV) are rolled back
- All active stream sessions are aborted
- All held leases are released
- All RPC worker registrations are cleared
- All pending RPC requests are discarded
- Queued notifications are discarded
  **State NOT Restored On Reconnect:**
  On reconnect with new CONNECT:
- New session ID issued (previous session ID is invalid)
- Previous subscriptions, transactions, and worker registrations are NOT recovered
- Previous pending RPC requests are NOT durably recovered or replayed
- Client MUST explicitly re-subscribe, re-begin, or re-register if needed

### 4. Send Domain Requests

After successful CONNECT, client may send domain-specific requests.

**Channel-Based Multiplexing:**

- **Clients MAY send multiple in-flight requests on different channels (domains).** Each domain (KV, RPC, Notice, etc.) is routed to its own logical channel by the broker. This allows concurrent operations across different domains on the same connection.
- **Within a single domain**: Follow request/response sequencing unless the domain explicitly supports per-request correlation IDs (currently only RPC). Sending multiple requests of the same type without waiting for responses is undefined behavior.
- **RPC domain is special**: RPC REQUEST includes an explicit 16-byte UUID `correlation_id` that clients generate. This allows true request/response matching for multiple in-flight RPC requests.
- **RPC registrations are session-scoped**: A worker reconnecting after disconnect or broker restart MUST send `Subscribe` again before it will receive new requests.
- **Out-of-band messages**: Asynchronous deliveries (e.g., Notice NOTIFY, RPC RESPONSE streaming) arrive without correlation IDs to requests; clients MUST handle them separately.
- **Order guarantees**: Responses are delivered in the order requests were sent (per domain/channel).

### 5. Receive Responses

Each request receives one response frame. Response format is domain-specific (see domain specs).

### 6. Close Connection

Clients SHOULD:

- Send WebSocket close frame or TCP FIN gracefully
- Clean up resources
- Discard pending requests on abrupt close
  Clients MUST:
- Assume connection is closed if transport layer signals close
- Reconnect and explicitly rebuild any required session-scoped state

## Authentication & Security

### Authentication Modes

Fitz brokers support two authentication modes controlled by server configuration:
**1. Authenticated Mode** (`FITZ_AUTH_REQUIRED=true`):

- JWT authentication is **required** for all connections
- CONNECT frame MUST include valid JWT
- Broker validates JWT signature and claims
- Missing or invalid JWT causes immediate connection close
  **2. Anonymous Mode** (`FITZ_AUTH_REQUIRED=false`):
- JWT authentication is **optional**
- CONNECT frame MAY include empty JWT or placeholder value
- Broker assigns default permissions (typically full access to all realms/areas)
- Useful for development, testing, or trusted internal networks

### JWT (Authentication Mechanism)

**When authentication is required,** clients MUST:

1. Obtain a JWT from an external authentication service
2. Pass the compact JWT string in the CONNECT record
3. Treat JWT as opaque (do not parse or validate server-side)
4. Resend JWT on reconnect
   **When authentication is optional (anonymous mode),** clients MAY:

- Send empty JWT (zero-length payload)
- Send placeholder JWT (e.g., "anonymous")
- Omit JWT field (broker accepts connection without authentication)
  Clients MUST NOT:
- Generate or sign JWTs
- Validate JWT signatures
- Cache or reuse JWTs across sessions
- Attempt to decode JWT claims

### Authorization

Authorization is **always server-side**:

- **Authenticated mode:** Broker validates JWT claims against route permissions
- **Anonymous mode:** Broker uses default permission set (no JWT validation)
- If client sends unauthorized request, broker returns error
- Clients MUST NOT attempt local permission checking

### TLS (Mandatory in Production)

**Production Deployments (REQUIRED):**
Clients MUST:

- Use `wss://` for WebSocket (never plain `ws://`)
- Use TLS for TCP (never plain TCP on untrusted networks)
- Validate server certificate chain against system CA roots
- Perform hostname verification (certificate CN or SAN must match broker hostname)
- Reject expired certificates
- Reject revoked certificates (if OCSP stapling available)
- Reject self-signed certificates (unless explicitly in trust store via deployment config)
  **Development/Testing (MAY Skip with Explicit Flag):**
  Clients MAY accept self-signed or invalid certificates ONLY if:
- Explicitly enabled via configuration flag (e.g., `insecure_skip_verify=true`)
- User acknowledges security risk in documentation
- Never default to insecure; require explicit opt-in
  Clients MUST NOT:
- Skip certificate validation to "work around" deployment issues
- Accept expired or revoked certificates without explicit flag
- Disable hostname verification
- Accept any certificate in production (must validate chain)

## Flow Control & Backpressure

Clients SHOULD implement queueing and backoff:

- Implement configurable write queue with maximum size
- On queue full, return error to caller (do NOT silently drop)
- Implement exponential backoff for retries

**Server backpressure signaling:**

Brokers signal backpressure through **domain error responses**. There is no separate backpressure frame. When a domain's internal queue is full, the broker returns a domain error in the standard response format:

- RPC: `6003 = ERR_RPC_BACKPRESSURE`
- Queue: `4005 = ERR_QUEUE_FULL`
- Other domains: connection close if internal buffers overflow

**Client behavior on backpressure errors:**

1. Pause sending to the affected domain
2. Apply exponential backoff (starting at 100ms, max 30s)
3. Retry the failed operation after backoff
4. If backpressure persists, surface error to caller

**Notice domain exception:** Since PUBLISH is fire-and-forget with no response, the broker silently drops notifications under backpressure. Clients have no visibility into dropped notices — this is by design (best-effort semantics).

## Routing

Routes are **opaque URI-like strings** that address resources and operations.

### Route Format

```
{scheme}://{realm}/{area}/{resource}/{operation}
```

**Components:**
| Component | Type | Example | Rules |
| ----------- | ------ | ------------------------- | ----------------------------------------------------- |
| `scheme` | string | `kv`, `queue`, `notice` | Identifies domain; MUST match known domain list |
| `realm` | string | `prod`, `tenant-123` | Opaque to client; case-sensitive |
| `area` | string | `app`, `system` | Opaque to client; case-sensitive |
| `resource` | string | `users`, `orders` | Opaque to client; may be omitted for admin operations |
| `operation` | string | `get`, `put`, `subscribe` | Verb; MUST match defined verb set |
**Route Examples:**

```
kv://prod/app/users/get          # KV read operation
queue://prod/app/orders/send     # Queue enqueue
notice://prod/app/events/publish # Pub/sub publish
```

## HTTP-Like Design Principle

Fitz follows an **HTTP-like model** where every operation is explicitly
addressed on the wire, while domains may still maintain live session-scoped
state when their contract requires it:

### Core Analogy

**HTTP:**

```
POST /api/users HTTP/1.1
Host: example.com
Content-Type: application/json

{"name": "alice"}
```

**Fitz:**

```
Verb: PUT (MessageType=104)
Route: kv://prod/app/users
Payload: [tx_id][key][value]
```

### Key Principles

1. **Every operation includes a route** (like HTTP URL)
   - KV PUT: `[tx_id][route][key][value]`
   - Queue ENQUEUE: `[route][body][delay]`
   - Stream APPEND: `[session_id][route][body]`

2. **Operations are explicitly addressed** (like HTTP stateless routing)
   - Server doesn't track implicit routing context beyond session and domain state
   - Each message has full addressing information
   - Connection loss doesn't require reconstructing hidden route context, though session-scoped domain state may still be cleaned up

3. **Verbs determine action** (like HTTP GET/POST/PUT/DELETE)
   - MessageType selects operation
   - Wire codes are stable ABI
   - Domain+Verb fully specifies behavior

4. **TLV is the wire format** (like HTTP has headers+body)
   - Type-Length-Value encoding
   - Binary efficient, not text-based
   - Extensible without version negotiation

### Why This Matters

**For implementers:**

- Simple mental model: "It's like HTTP but binary and over WebSocket/TCP"
- Familiar patterns: routes, verbs, self-contained requests
- Easy to reason about: no hidden state machines

**For operations:**

- Debuggable: every message carries explicit addressing and can be inspected without hidden route context
- Explicitly addressed: the wire format does not depend on per-connection route defaults
- Session-scoped domains still require re-establishing live state after reconnect when the domain contract says that state is ephemeral

### Comparison

| Aspect         | HTTP                            | Fitz                                   |
| -------------- | ------------------------------- | -------------------------------------- |
| **Addressing** | URL path                        | Route (kv://realm/area/resource)       |
| **Verb**       | GET, POST, PUT, DELETE          | MessageType (100=BEGIN, 104=PUT, etc.) |
| **Transport**  | TCP + TLS                       | WebSocket or TCP + TLS                 |
| **Format**     | Text (headers + body)           | Binary (TLV)                           |
| **State**      | Stateless (cookies for session) | Explicit routing; some domains keep live session state |
| **Operations** | Self-contained requests         | Self-contained requests                |

## Route Acceptance Criteria (Authoritative)

A request is valid **only if**:

1. The route shape is valid for the domain
2. Wildcards appear only in allowed positions (per domain)
3. The method permits those wildcards
4. The route depth matches the method's plane
   **Violations are protocol errors.** Broker MUST reject; clients **MAY** perform local route shape validation for ergonomics, but the broker is authoritative. Clients **MUST** accept broker rejection as the source of truth and MUST NOT rely on local validation as a substitute for server-side checks.

## Global Route Rules (Normative)

- Routes are opaque strings with a fixed, domain-defined shape
- `{realm}` is **always concrete** (never `*`)
- `*` MAY appear only in positions explicitly allowed by the domain
- Extra path segments are **forbidden**
- Route shape validation occurs **before** permission or dispatch checks

### Wildcard Support by Domain

**Domains supporting wildcards (`*` and `**` patterns):**
- **Notice:** Full wildcard support in SUBSCRIBE patterns (`notice://realm/area/*`, `notice://realm/**`)
- **Stream:** Wildcards in READ patterns (check Stream domain spec for details)
- **Queue:** Wildcards in RESERVE patterns (check Queue domain spec for details)

**Domains requiring concrete routes only (no wildcards):**
- **KV:** All operations use concrete routes only (`kv://realm/area/resource`)
- **Lease:** All operations use concrete routes only (`lease://realm/area/resource`)
- **RPC:** Worker registrations and requests use exact routes only. The common operation-style form is `rpc://realm/area/resource/operation`
- **Schedule:** `CREATE`, `CANCEL`, `SUBSCRIBE`, and `UNSUBSCRIBE` use concrete routes only (`schedule://realm/area/resource/operation`)

**Pattern matching semantics:**
- `*` matches exactly one path segment
- `**` matches zero or more path segments (greedy)
- Concrete routes (no wildcards) match exactly

## Route Shapes by Domain

### KV Domain

**Valid Route Shapes:**

- `kv://{realm}/{area}`
- `kv://{realm}/{area}/{resource}`
- `kv://{realm}/{area}/*`
- `kv://{realm}/*/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `GET` | `{realm}/{area}/{resource}` |
  | `PUT` | `{realm}/{area}/{resource}` |
  | `INSERT` | `{realm}/{area}/{resource}` |
  | `SCAN` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
  | `COMMIT` | `{realm}/{area}/{resource}` |
  | `ROLLBACK` | `{realm}/{area}/{resource}` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on data operations: BEGIN, GET, PUT, INSERT, SCAN, COMMIT, ROLLBACK.

### Stream Domain

**Valid Route Shapes:**

- `stream://{realm}/{area}/{resource}`
- `stream://{realm}/{area}/*`
- `stream://{realm}/*/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------------- | -------------------------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `CREATE` | `{realm}/{area}` |
  | `DELETE` (admin) | `{realm}/{area}` |
  | `BEGIN` | `{realm}/{area}/{resource}` |
  | `APPEND` | `{realm}/{area}/{resource}` |
  | `READ` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `COMMIT` | `{realm}/{area}/{resource}` |
  **Note:** `LIST`, `CREATE`, and `DELETE` (admin) operations are broker-internal management operations not currently exposed in the client wire protocol. Clients should focus on stream operations: BEGIN, APPEND, READ, SUBSCRIBE, UNSUBSCRIBE, COMMIT, ROLLBACK.
  | `ROLLBACK` | `{realm}/{area}/{resource}` |

### Queue Domain

**Valid Route Shapes:**

- `queue://{realm}/{area}`
- `queue://{realm}/{area}/{resource}`
- `queue://{realm}/{area}/*`
- `queue://{realm}/*/*`

**Route format:** For per-resource isolation, use the 3-segment form `queue://{realm}/{area}/{resource}`. Each distinct resource has its own queue and lease state.

**Lease expiry:** Servers process lease expiry lazily (e.g. when the next RESERVE or other operation runs). A reserved message whose lease has expired is returned to the ready queue on the next operation that touches that queue. Clients that rely on lease expiry (e.g. to re-reserve) should allow for this delay (e.g. wait a few seconds after lease TTL before re-reserving).

  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `ENQUEUE` | `{realm}/{area}/{resource}` |
  | `RESERVE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
  | `COMPLETE` | `{realm}/{area}/{resource}` |
  | `EXTEND` | `{realm}/{area}/{resource}` |

  **Note:** `LIST` is a broker-internal management operation not currently exposed in the client wire protocol. Clients should use: ENQUEUE, RESERVE, COMPLETE, EXTEND as documented in the wire format section.

### Schedule Domain

**Valid Route Shapes:**

- concrete route: `schedule://{realm}/{area}/{resource}/{operation}`
- list selector: `schedule://{realm}/{area}/{resource}/{operation}`
- list selector: `schedule://{realm}/{area}/{resource}/*`
- list selector: `schedule://{realm}/{area}/*`
- list selector: `schedule://{realm}/**`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | -------- | ----------------------------------------------- |
  | `CREATE` | `{realm}/{area}/{resource}/{operation}` |
  | `CANCEL` | `{realm}/{area}/{resource}/{operation}` |
  | `LIST` | exact 4-part route, `{realm}/{area}/{resource}/*`, `{realm}/{area}/*`, `{realm}/**` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}/{operation}` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

  **Note:** `DELETE` (admin) and `TRIGGER` operations are broker-internal. Clients should use: CREATE, CANCEL, LIST, SUBSCRIBE, UNSUBSCRIBE as documented in the wire format section. LIST is fully documented with streaming protocol.

### Lease Domain

**Valid Route Shapes:**

- `lease://{realm}/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | --------- | --------------------------- |
  | `ACQUIRE` | `{realm}/{area}/{resource}` |
  | `RENEW` | `{realm}/{area}/{resource}` |
  | `RELEASE` | `{realm}/{area}/{resource}` |
  | `QUERY` | `{realm}/{area}/{resource}` |

### Notice Domain

**Valid Route Shapes:**

- `notice://{realm}/{area}/{resource}`
- `notice://{realm}/{area}/*`
- `notice://{realm}/*/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | -------------------------------------------------------------- |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |
  | `PUBLISH` | `{realm}/{area}/{resource}` |

### RPC Domain

**Route Shape Guidance:**

- Worker registrations and request routes use exact route strings only.
- Wildcard worker registration is not part of the contract.
- The common operation-style form is `rpc://{realm}/{area}/{resource}/{operation}`.
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | ----------------------------------------------- |
  | `CALL` | exact route (commonly `{realm}/{area}/{resource}/{operation}`) |
  | `SUBSCRIBE` | exact route only |
  | `UNSUBSCRIBE` | same as `SUBSCRIBE` |

## Lock-In Rule

**If a route shape is not explicitly listed for a method, it is invalid.**
This specification is the **single source of truth** for:

- Broker validation
- SDK conformance testing
- Permission enforcement
- Long-term protocol stability

## Verbs

Verbs are the **primary behavior selector**. They determine what action a request performs.

### Verb Requirements

Clients MUST:

1. **Expose verbs as constants or enums** in the client's native language
   - Python: `class KvVerb: GET = "GET"; PUT = "PUT"`
   - Rust: `enum KvVerb { Get, Put, ... }`
   - JavaScript: `const KvVerb = { Get: "get", Put: "put" }`
2. **Never expose wire codes** in public API
3. **Map verbs to i16 wire codes internally**
4. **Treat wire codes as ABI-stable** (never reused, append-only)

### Verb Set (All Domains)

| Domain   | Verb               | Wire Code | Plane         | Notes                   |
| -------- | ------------------ | --------: | ------------- | ----------------------- |
| KV       | BEGIN              |       100 | Data          | Start transaction       |
| KV       | COMMIT             |       101 | Data          | Finalize transaction    |
| KV       | ROLLBACK           |       102 | Data          | Abort transaction       |
| KV       | GET                |       103 | Data          | Read key                |
| KV       | PUT                |       104 | Data          | Write key               |
| KV       | INSERT             |       105 | Data          | Insert (fail if exists) |
| KV       | DELETE             |       106 | Data          | Delete key              |
| KV       | DELETE_RANGE       |       107 | Data          | Delete key range        |
| KV       | SCAN               |       108 | Data          | Scan keys in range      |
| Queue    | ENQUEUE            |       200 | Data          | Add message             |
| Queue    | ENQUEUE_BATCH      |       201 | Reserved      | Batch add (future)      |
| Queue    | RESERVE            |       202 | Data          | Lease message(s)        |
| Queue    | EXTEND             |       203 | Data          | Extend lease            |
| Queue    | COMPLETE           |       204 | Data          | Mark complete           |
| Queue    | SUBSCRIBE          |       207 | Data          | Subscribe to pattern    |
| Queue    | UNSUBSCRIBE        |       208 | Data          | Unsubscribe pattern     |
| Queue    | NOTIFY             |       209 | Notification  | Availability event      |
| RPC      | SUBSCRIBE_WORKER   |       300 | Data          | Register worker         |
| RPC      | UNSUBSCRIBE_WORKER |       301 | Data          | Unregister worker       |
| RPC      | REQUEST            |       302 | Data          | Send request            |
| RPC      | RESPONSE           |       303 | Data          | Send response           |
| RPC      | ACK                |       304 | Data          | Acknowledge             |
| Lease    | ACQUIRE            |       400 | Data          | Acquire lease           |
| Lease    | RENEW              |       401 | Data          | Extend lease            |
| Lease    | RELEASE            |       402 | Data          | Release lease           |
| Lease    | QUERY              |       403 | Data          | Query lease status      |
| Notice   | PUBLISH            |       500 | Data          | Publish message         |
| Notice   | SUBSCRIBE          |       501 | Data          | Subscribe to pattern    |
| Notice   | UNSUBSCRIBE        |       502 | Data          | Unsubscribe             |
| Notice   | UNSUBSCRIBE_ALL    |       503 | Data          | Clear all subscriptions |
| Notice   | NOTIFY             |       504 | Server→Client | Delivery                |
| Stream   | BEGIN              |       600 | Data          | Start session           |
| Stream   | APPEND             |       601 | Data          | Append record           |
| Stream   | COMMIT             |       602 | Data          | Finalize session        |
| Stream   | ROLLBACK           |       603 | Data          | Abort session           |
| Stream   | READ               |       604 | Data          | Read range              |
| Stream   | LAST               |       605 | Data          | Get last record         |
| Stream   | GET_METADATA       |       606 | Data          | Get metadata            |
| Stream   | SUBSCRIBE          |       607 | Data          | Subscribe to changes    |
| Stream   | UNSUBSCRIBE        |       608 | Data          | Unsubscribe             |
| Stream   | NOTIFY             |       609 | Server→Client | Change notification     |
| Schedule | CREATE             |       700 | Data          | Create schedule         |
| Schedule | CANCEL             |       701 | Data          | Cancel schedule         |
| Schedule | LIST               |       702 | Data          | List schedules          |
| Schedule | SUBSCRIBE          |       703 | Data          | Subscribe to fires      |
| Schedule | UNSUBSCRIBE        |       704 | Data          | Unsubscribe             |
| Schedule | NOTIFY             |       705 | Server→Client | Fire notification       |

### MessageType Ranges Are Non-Overlapping

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Clients MUST use the wire codes from the Constants & TLV Registry section.**

## Domain Operations Reference (Canonical Standard)

This section documents the **canonical operations** for each of the seven Fitz domains. These operations define the complete API surface for each domain and MUST be implemented by all conformant clients and brokers.

**Design Principle:** Each domain has a focused, minimal set of operations.
Operations are explicitly addressed and avoid implicit routing state, though
domains may still keep live server-side state where their contract requires it.

### KV Domain (Key-Value Store)

**Purpose:** Transactional key-value storage with ACID isolation.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Begin` | 100 | C→S | Start transaction |
| `Commit` | 101 | C→S | Finalize transaction |
| `Rollback` | 102 | C→S | Abort transaction |
| `Get` | 103 | C→S | Retrieve value by key |
| `Put` | 104 | C→S | Upsert key-value pair |
| `Insert` | 105 | C→S | Insert new key (fail if exists) |
| `Delete` | 106 | C→S | Delete single key |
| `DeleteRange` | 107 | C→S | Delete key range |
| `Scan` | 108 | C→S | Scan key range |

**Constraints:**
- All data operations MUST be within a transaction (explicit BEGIN required)
- Operations within a single transaction MUST be sequential (no parallel calls with same tx_id)
- Multiple transactions to different resources MAY be parallel

---

### Queue Domain (Message Queue)

**Purpose:** Durable FIFO message queue with leasing.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Send` | 200 | C→S | Add message to queue |
| `Receive` | 202 | C→S | Lease messages for processing |
| `Extend` | 203 | C→S | Extend message lease TTL |
| `Ack` | 204 | C→S | Acknowledge and delete message |
| `Subscribe` | (future) | C→S | Watch queue for availability |
| `Unsubscribe` | (future) | C→S | Stop watching |

**Constraints:**
- Messages are leased (not immediately deleted)
- Lease token MUST match to complete or extend (fencing)
- FIFO ordering preserved within single reserve call
- Duplicate reserves may violate FIFO (wait or use single-call pattern)

---

### Lease Domain (Ephemeral In-Memory Coordination)

**Purpose:** In-memory mutual exclusion with TTL-based leases inside one broker process.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Acquire` | 400 | C→S | Acquire lease with TTL |
| `Extend` | 401 | C→S | Extend existing lease |
| `Release` | 402 | C→S | Release lease |
| `Query` | 403 | C→S | Query lease status |
| `Subscribe` | (future) | C→S | Watch lease changes |
| `Unsubscribe` | (future) | C→S | Stop watching |

**Constraints:**
- Token MUST match to extend or release (prevents cross-holder mutations)
- Expiry is lazy (expires when next operation touches resource)
- Disconnect cleanup and broker restart both clear lease ownership; clients MUST reacquire if they still need exclusivity
- Atomic compare-and-swap via token (no blindupdate)

---

### Notice Domain (Pub/Sub)

**Purpose:** Best-effort topic-based pub/sub notifications.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Publish` | 500 | C→S | Publish notification (fire-and-forget) |
| `Subscribe` | 501 | C→S | Subscribe to pattern |
| `Unsubscribe` | 502 | C→S | Unsubscribe from pattern |
| `UnsubscribeAll` | 503 | C→S | Clear all subscriptions |
| `Deliver` | 504 | S→C | Server delivery (asynchronous) |

**Constraints:**
- PUBLISH is fire-and-forget (no response)
- Subscriptions use wildcard patterns (`*`, `**`)
- Delivery is best-effort (may drop under backpressure)
- Session-scoped (lost on disconnect)
- Client-side multiplexing: one server subscription per pattern, multiple handlers per subscription_id

---

### RPC Domain (Request/Response)

**Purpose:** Synchronous request/response with reply inbox pattern and optional streaming.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Subscribe` | 300 | C→S | Register as worker |
| `Unsubscribe` | 301 | C→S | Deregister worker |
| `Request` | 302 | C→S | Send RPC request |
| `Response` | 303 | S→C | Send RPC response |
| `Ack` | 304 | C↔S | Acknowledge receipt |
| `Deliver` | (via 303) | S→C | Server→Worker delivery |

**Constraints:**
- Each request uses 16-byte UUID `correlation_id` for matching the live in-flight response
- Multiple RPC requests MAY be in flight simultaneously (true multiplexing via correlation_id)
- Workers register exact listening routes and receive DELIVER (async push)
- Callers include reply inbox routing metadata with each request
- Worker registrations and pending requests are process-local and are not recovered or replayed after broker restart
- A worker reconnecting after disconnect or restart MUST send `Subscribe` again before it can receive new requests

---

### Stream Domain (Log/Event Stream)

**Purpose:** Durable append-only log with strict ordering and watermarking.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Append` | 601 | C→S | Append records |
| `Read` | 604 | C→S | Read from offset |
| `Peek` | (future; use Read) | C→S | Non-consuming read |
| `Subscribe` | 607 | C→S | Watch for new records |
| `Unsubscribe` | 608 | C→S | Stop watching |

**Additional ops:** Begin, Commit, Rollback (session control; see stream spec)

**Constraints:**
- Records strictly ordered by offset within resource
- Read cannot advance beyond watermark (uncommitted protection)
- BEGIN uses optimistic concurrency (`expected_offset`)
- Offset-based reads (no consumer groups)
- Watermark tracks committed data

---

### Schedule Domain (Cron Scheduler)

**Purpose:** Distributed one-time or recurring scheduled tasks.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Create` | 700 | C→S | Create/update schedule |
| `Cancel` | 701 | C→S | Delete schedule |
| `List` | 702 | C→S | List schedules |
| `Subscribe` | 703 | C→S | Watch for schedule fires |
| `Unsubscribe` | 704 | C→S | Stop watching |
| `Notify` | 705 | S→C | Fire notification |

**Constraints:**
- Cron-style scheduling (precise timing, best-effort delivery)
- Subscriptions are session-scoped
- NOTIFY is best-effort (may be dropped under backpressure)

---

### Operation Name Stability (ABI Contract)

**These operation names and wire codes are STABLE and AUTHORITATIVE:**

1. **No Synonyms:** Each operation has exactly one canonical name. Aliases (e.g., `Renew` for `Extend`) are forbidden.
2. **No Rewording:** `Receive` is not `Reserve`, `Ack` is not `Complete`. Use exact names.
3. **Wire Code Stability:** Wire codes are never reused. Deprecated operations remain in the wire code registry.
4. **Cross-Language Consistency:** All client implementations (Rust, Go, Python, JavaScript, etc.) MUST expose these exact operation names (adapted for language conventions, e.g., snake_case in Python, PascalCase in Go).

**Rationale:** Operation names form the semantic contract between clients and brokers. Stabilizing names ensures:
- Long-term interoperability across versions and languages
- Consistent documentation and debugging
- Clear migration paths for protocol evolution
- Reduced confusion in multi-language deployments

---

## Client Server Boundary

The broker may validate JWT claims, attach internal session metadata, select storage or compute shards, and route requests to domain workers. Those mechanisms are server concerns, not client protocol features.

Clients MUST:

- send `CONNECT` with JWT only
- send only the documented domain payload fields
- treat routes as opaque strings

Clients MUST NOT:

- send undocumented shard or routing metadata
- derive dispatch behavior from JWT claims or route segments
- expose broker-internal topology or partition state in public APIs

## Permissions

### Permission Model (Server-Enforced)

Authorization behavior depends on server authentication mode:
**Authenticated Mode (`FITZ_AUTH_REQUIRED=true`):**

- Broker MUST extract claims from JWT: `realm`, `areas` (array), `scopes` (array)
- For each request, broker MUST check:
  1. **Realm match**: Route realm ∈ JWT realm (MUST be exact match)
  2. **Area match**: Route area ∈ JWT areas
  3. **Scope match**: Request verb ∈ JWT scopes (e.g., `kv:read`, `notice:subscribe`, `queue:send`)
- If any check fails, broker returns permission error (domain-specific error code)
  **Anonymous Mode (`FITZ_AUTH_REQUIRED=false`):**
- Broker assigns default permissions (typically unrestricted access)
- No JWT validation or permission checks
- All routes and verbs allowed
- Used for development/testing or trusted internal networks
  **Permission Check Order (Authenticated Mode):**
  Broker MUST enforce permissions in this order:

1. **Route validation:** Scheme known, depth valid, shape matches method (if fails: protocol error)
2. **JWT validation:** Signature valid, not expired (if fails: transport error)
3. **Permission enforcement:** Realm/area/scope match (if fails: domain error with code ERR_UNAUTHORIZED)
4. **Domain dispatch:** Route to domain handler

### Permission Error Codes (Authenticated Mode Only)

If permission check fails, broker returns error in domain error encoding with these standard codes:
| Error Code | Meaning | HTTP Equivalent |
| ---------- | ------------------ | --------------- |
| `*001` | ERR_UNAUTHORIZED | 403 Forbidden |
| `*002` | ERR_INVALID_SCOPE | 403 Forbidden |
| `*003` | ERR_REALM_MISMATCH | 403 Forbidden |
Where `*` is domain prefix (1xxx for KV, 3xxx for Notice, etc.).
**Example (KV domain):**

- 1001 = ERR_UNAUTHORIZED
- 1002 = ERR_INVALID_SCOPE
- 1003 = ERR_REALM_MISMATCH

### JWT Claims Schema

**Required Claims:**

```json
{
  "realm": "prod",
  "areas": ["app", "system"],
  "scopes": ["kv:read", "kv:write", "notice:subscribe"],
  "exp": 1234567890
}
```

**Scope Format:** `{domain}:{verb}` or `{domain}:*` (all verbs in domain)

### Client-Side Guidance

**For Authenticated Mode:**
Clients MUST:

- Obtain JWT from external auth service
- Treat JWT as opaque string
- Pass JWT in CONNECT record
- Handle ERR_UNAUTHORIZED gracefully (return to user, suggest re-authentication)
  Clients MUST NOT:
- Validate JWT signatures
- Parse or check JWT claims
- Attempt local permission checking
- Cache JWT results
- Infer permissions from routes
  **For Anonymous Mode:**
  Clients MAY:
- Pass empty JWT (zero-length)
- Pass placeholder value (e.g., "anonymous")
- Use configuration flag to indicate anonymous mode (e.g., `anonymous=true`)
  **Client Configuration Example:**

```python
# Authenticated mode
client = FitzClient(
    broker="wss://prod.example.com:4090",
    jwt=get_jwt_from_auth_service(),
    anonymous=False
)
# Anonymous mode (development/testing)
client = FitzClient(
    broker="ws://localhost:4090",
    jwt="",  # Empty or omitted
    anonymous=True
)
```

- Validate route against JWT claims (server does this)
- Cache permission decisions
- Attempt token generation or validation
- Model permission scopes in client code

### Permission Metadata (Optional)

Clients MAY expose permission metadata from JWT claims for **diagnostics only**:

```python
# Optional, for debugging
client.permitted_realms()  # Returns list from JWT claims (if exposed)
```

This is **NOT** used for request validation.

## Transactions

Transactions are **explicit and domain-specific**. Clients MUST NOT provide implicit transaction handling.

### Transaction APIs (Where Supported)

Clients MUST expose explicit methods for supported domains:
| Domain | API | Required |
| -------- | ----------------------------------------- | -------- |
| KV | `begin()`, `commit()`, `rollback()` | YES |
| Stream | `begin()`, `commit()`, `rollback()` | YES |
| Queue | N/A (message-oriented, not transactional) | — |
| Notice | N/A (fire-and-forget) | — |
| RPC | N/A (request-scoped) | — |
| Lease | N/A (stateless operations) | — |
| Schedule | N/A (fire-and-forget) | — |

### Transaction Constraints

Clients MUST:

1. **Require explicit `BEGIN` before data operations** (no auto-open)
2. **Require explicit `COMMIT` or `ROLLBACK`** (no auto-commit)
3. **Surface transaction errors** (e.g., isolation conflicts)
4. **NOT retry transactions automatically** (client chooses)
5. **Support multiple concurrent transactions to different resources** (same domain, different actor instances)
6. **NOT parallelize operations within ONE transaction** (single tx_id must be sequential)

**Example (Rust-like pseudocode):**

```rust
// ✅ CORRECT - explicit transaction lifecycle
let tx_id = client.begin(KvBeginRequest { route, mode, durability })?;
client.put(KvPutRequest { tx_id, key, value })?;
client.get(KvGetRequest { tx_id, key })?;
client.commit(KvCommitRequest { tx_id })?;

// ✅ CORRECT - multiple concurrent transactions to different resources
let tx1 = client.begin(KvBeginRequest { route: "kv://prod/app/users", mode, durability })?;
let tx2 = client.begin(KvBeginRequest { route: "kv://prod/app/posts", mode, durability })?;
// Both tx1 and tx2 active simultaneously
client.put(KvPutRequest { tx_id: tx1, key, value })?;
client.put(KvPutRequest { tx_id: tx2, key, value })?;
client.commit(KvCommitRequest { tx_id: tx1 })?;
client.commit(KvCommitRequest { tx_id: tx2 })?;

// ❌ WRONG - parallel operations on SAME transaction
let tx = client.begin(KvBeginRequest { route, mode, durability })?;
// DO NOT DO THIS:
// futures::join_all(vec![
//   client.put(KvPutRequest { tx_id: tx, key: "k1", value: "v1" }),
//   client.put(KvPutRequest { tx_id: tx, key: "k2", value: "v2" }),
// ])?;  // ❌ Same tx_id cannot have parallel calls

// ❌ WRONG - auto-open transactions
let value = client.get(key)?;  // Do NOT auto-begin
// ❌ WRONG - auto-commit
client.put(key, value)?;  // Do NOT auto-commit
```

## Subscriptions

Subscriptions are **explicit and connection-scoped**.

### Subscription APIs

Clients MUST expose:

1. **`SUBSCRIBE`** - Subscribe to route pattern
2. **`UNSUBSCRIBE`** - Unsubscribe from pattern
3. **`on_notification` / callback** - Receive notifications
   **Example (JavaScript-like pseudocode):**

```javascript
// ✅ CORRECT - explicit subscribe
client.subscribe({
  pattern: "notice://prod/app/*",
  handler: (route, payload) => {
    /* ... */
  },
});
// ✅ CORRECT - explicit unsubscribe
client.unsubscribe({
  pattern: "notice://prod/app/*",
});
// ❌ WRONG - implicit subscriptions
client.on("app.events", handler); // Magic pattern; unclear when subscribed
```

### Subscription Constraints

Clients MUST:

1. **Track subscriptions per connection** (session-scoped)
2. **NOT assume subscriptions persist across reconnect**; subscriptions are session-scoped and lost on disconnect. Clients **MUST** be able to re-subscribe after reconnect if desired.
3. **Surface subscription errors** (invalid pattern, limit exceeded)
4. **Handle duplicate notifications** (at-least-once delivery)
5. **Provide backoff for subscription errors**

### Reconnection Behavior

On disconnect:

- Subscriptions are **lost server-side**
- Clients **MUST** re-subscribe explicitly after reconnect if they need subscriptions restored
- Clients **MAY** implement transparent auto-resubscribe helpers; such helpers SHOULD use exponential backoff and be opt-in
- **Servers MUST** treat duplicate subscribe requests as idempotent to make client-side resubscribe helpers robust (duplicate subscriptions SHOULD NOT create duplicate deliveries)

## Error Handling

Errors fall into two categories: **transport** and **domain**.

### Transport Errors

Transport errors signal connection failure:
| Error | Cause | Client Action |
| ------------------ | -------------------------------- | ----------------------------------- |
| Connection refused | Broker unreachable | Retry with backoff; raise to caller |
| Connection reset | Broker crashed or closed | Reconnect; re-establish session |
| Frame too large | Payload exceeds `max_frame_size` | Close connection; raise error |
| Invalid UTF-8 | Malformed frame | Close connection; raise error |
| TLV decode error | Unrecoverable frame format | Close connection; raise error |
**Clients MUST:**

- Distinguish transport errors from domain errors
- Implement exponential backoff for retries (1s → 2s → 4s → ...)
- NOT attempt to recover from unrecoverable errors (close connection)

### Domain Errors

Domain errors are returned in response payloads. Format is **domain-specific** (see domain specs).
**Clients MUST:**

- Parse domain error responses according to domain spec
- Surface error code and message to caller
- NOT reinterpret or hide server error messages
  **Example (KV domain):**

```
Error response:
  [u32 BE error_len]
  [bytes error_msg]
// Client parses and raises:
raise DomainError(error_msg)
```

## Request/Response Correlation

### Synchronous Model (Per Domain)

**Fitz uses channel-based multiplexing:**

- Different domains (KV, RPC, Notice, etc.) run on independent logical channels
- Within a single domain/channel: Client sends request and blocks waiting for response
- **Across domains**: Multiple in-flight requests on different channels are allowed (e.g., KV PUT while Notice PUBLISH)
- **Within same domain**: Sending multiple requests without waiting for responses is undefined behavior; the broker MAY close the connection

**Exception: RPC Domain**
RPC REQUEST uses explicit 16-byte UUID `correlation_id` to match responses across multiple in-flight requests. Example:

```python
# RPC allows true multiplexing (multiple in-flight requests)
future1 = client.rpc_request(..., correlation_id=uuid1)
future2 = client.rpc_request(..., correlation_id=uuid2)
# Both in-flight simultaneously; responses matched by correlation_id
response1 = future1.wait()  # Matched by uuid1
response2 = future2.wait()  # Matched by uuid2
```

**Channel-Based Multiplexing (Typical):**

```python
# KV, Notice, Queue, etc. are sequential within domain
# But concurrent across domains
kv_tx = client.kv_begin(route, mode, durability)  # Blocks on KV channel
notice_sub = client.notice_subscribe(pattern)  # Queued on Notice channel (concurrent)
# KV transaction continues on KV channel while Notice processes on Notice channel
```

### Multi-Response Operations

When a single operation generates multiple responses:
**Notice SUBSCRIBE:**

- Request → Response 1 (subscription ID)
- Subsequent PUBLISHes → NOTIFY frames (asynchronous)
- Client reads NOTIFYs from same connection
  **RPC REQUEST:**
- Request → Response 1 (accepted via correlation_id)
- Worker responses → Response 2+ (streaming, matched by correlation_id)
- Multiple RPC responses matched by correlation_id on same connection
  **Stream READ:**
- Request → Response 1 (record stream)
- Multiple records may arrive in single response or multiple frames
- Broker MAY split large responses across multiple frames

### Reconnection & In-Flight Requests

**When connection drops during an operation:**

**In-Flight Request Semantics (Per Channel):**
- Any request sent but not yet responded to is **LOST** (for that channel)
- Server may have processed the request before disconnect
- Client CANNOT know if request succeeded or failed
- **No automatic replay or recovery**

**Client Retry Strategy:**
- **Idempotent operations** (GET, SCAN, READ): SAFE to retry
- **Non-idempotent operations** (PUT, ENQUEUE, PUBLISH): DO NOT retry
  - Retrying may cause duplicate execution
- Use application-level idempotency tokens if needed; do not rely on RPC `correlation_id` alone for durable deduplication

**Transaction-Specific Behavior:**
- If disconnect during KV transaction: server ROLLS BACK automatically
- If disconnect during Stream session: server ROLLS BACK automatically
- Client MUST re-BEGIN transaction/session and retry all operations from scratch

**Subscription-Specific Behavior:**
- All active subscriptions (Notice, RPC worker) are **dropped** on disconnect
- Clients MUST re-subscribe explicitly after reconnect
- Clients MAY implement transparent auto-resubscribe (opt-in, with exponential backoff)
- Servers MUST treat duplicate SUBSCRIBE as idempotent

**Reconnection Flow:**
1. Detect transport failure (connection lost, read error, timeout)
2. Wait (exponential backoff: 1s → 2s → 4s → 8s → cap at 30s)
3. Re-open transport connection
4. Send new CONNECT frame (authentication may have changed)
5. Re-establish any subscriptions if needed
6. Resume normal operations

**Clients SHOULD:**
- Log all in-flight requests at disconnect for debugging
- Surface connection state changes to application
- Provide hooks for reconnection events

### Connection Handling

For all operations:

- Connection remains open after response received
- Client MAY send next request immediately (re-enters sync wait)
- Asynchronous frames (notifications, RPC responses) arrive while waiting for next response
- Client MUST buffer asynchronous frames and dispatch to handlers

## Idempotency & Retry Strategy

Clients MUST NOT automatically retry operations unless:

1. Operation is idempotent (read-only, safe to retry)
2. OR client has deduplication mechanism (correlation ID tracking)
   **Idempotent Operations (Safe to Retry, No Deduplication Needed):**
   Read-only operations are safe to retry without deduplication:

- KV: `GET`, `SCAN`
- Stream: `READ`, `GET_METADATA`, `LAST`
- Lease: `QUERY`
- Queue: `RESERVE` (with caveats; see context-dependent below)
  Retry behavior: If transport fails after sending request but before receiving response, safe to resend identical request.
  Broker behavior: MAY return stale data if resource has changed between retries.
  **NOT Idempotent (MUST NOT Retry Automatically):**
  Write operations, control operations, and pub/sub are NOT idempotent:
- KV: `PUT`, `INSERT`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`
- Stream: `APPEND`, `BEGIN`, `COMMIT`, `ROLLBACK`
- Notice: `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`
- Queue: `ENQUEUE`, `COMPLETE`, `EXTEND`, `DELETE`
- RPC: `REQUEST`, `RESPONSE`, `ACK`
- Lease: `ACQUIRE`, `RENEW`, `RELEASE`
- Schedule: `CREATE`, `CANCEL`
  Retry behavior: Retrying these operations MAY cause duplicate execution, lost updates, or unexpected state changes.
  **Context-Dependent (Safe to Retry WITH Deduplication):**
  Operations that are safe to retry only if client tracks correlation ID:
- Queue: `COMPLETE` (safe to retry if message_id+token already deleted)
- RPC: `REQUEST` (safe to retry only if the application owns idempotency; broker correlation matching does not cache, replay, or deduplicate requests)
  Retry behavior: Clients MUST maintain application-level deduplication state if they want to retry safely; broker `correlation_id` tracking is only for live in-flight response matching.

### Recommended Retry Strategy

```
IF operation in IDEMPOTENT_LIST:
  retry_count ← 0
  retry_max ← 3
  backoff ← 1 second
  WHILE retry_count < retry_max:
    TRY send request
    IF response received THEN return
    IF transport error AND retry_count < retry_max THEN
      wait(backoff)
      backoff ← backoff * 2 (exponential backoff)
      retry_count ← retry_count + 1
    ELSE
      raise error
ELSE IF operation in CONTEXT_DEPENDENT_LIST:
  IF correlation_id in dedup_cache THEN
    return cached_result
  ELSE
    result ← send request
    dedup_cache[correlation_id] ← result
    return result
ELSE  (NOT idempotent)
  send request exactly once
  IF transport error THEN raise error (do NOT retry)
```

Each domain has a specific wire format, verb set, and semantics. Implement each domain codec according to its specification below.

### Notice Domain (Fire-and-Forget Pub/Sub)

**Purpose:** Low-latency session-scoped notifications with wildcard pattern matching.

#### Message Types

| Type | Name            | Direction                  |
| ---: | --------------- | -------------------------- |
|  500 | PUBLISH         | Client → Server            |
|  501 | SUBSCRIBE       | Client → Server            |
|  502 | UNSUBSCRIBE     | Client → Server            |
|  503 | UNSUBSCRIBE_ALL | Client → Server            |
|  504 | NOTIFY          | Server → Client (delivery) |

#### PUBLISH Request

```
[u32 BE]  route_len
[bytes]   route (UTF-8, e.g., "notice://realm/area/events")
[u32 BE]  payload_len
[bytes]   payload
```

**No response frame.** PUBLISH is fire-and-forget. The broker accepts the frame and fans out to matching subscribers. The client MUST NOT wait for a response after sending PUBLISH.

**Design Notes:**

- No delivery confirmation (best-effort)
- No error returned for invalid routes or missing subscribers
- Client-side errors (e.g., connection closed, frame too large) are transport-level only
- This matches the Notice domain's non-durable, best-effort semantics

#### SUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0                    // status: success
  [u8]     1                    // has_subscription_id flag (always 1 for success)
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1                    // status: error
  [u32 BE] error_len
  [bytes]  error_msg
```

**Wire Format Note:**
The success response uses the "optional u64" encoding pattern: a 1-byte flag followed by the value if present. For SUBSCRIBE success responses, the flag is always `1` (has value), followed by the `subscription_id` as a big-endian u64.

**Idempotency:**
- If client re-subscribes to same pattern, server returns the SAME `subscription_id`
- No duplicate server-side subscription created
- Client is responsible for local multiplexing (tracking multiple handlers per subscription_id)

**Design Notes:**

- Server tracks subscriptions by `(session_id, route_pattern)` tuple
- `session_id` is implicit from connection (not sent by client)
- Duplicate SUBSCRIBE to same pattern is idempotent (returns same `subscription_id`)
- Client handles local multiplexing (multiple handlers per `subscription_id`)

#### UNSUBSCRIBE Request

```
[u64 BE]  subscription_id
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**

- Client sends `subscription_id` returned from SUBSCRIBE
- Server removes subscription for calling session
- Idempotent: unsubscribing non-existent subscription_id returns success

#### UNSUBSCRIBE_ALL Request

```
(no payload - session_id implicit from connection)
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**

- Removes all subscriptions for calling session
- Idempotent: safe to call even if no subscriptions exist

#### NOTIFY (Server Delivery)

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route (exact published route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload
```

**Design Notes:**

- `subscription_id` tells client which subscription(s) matched
- Client demultiplexes to local handlers registered for that `subscription_id`
- Single NOTIFY delivered per `(session_id, pattern)` tuple (server deduplicates)
- If client has multiple handlers for same pattern, client calls all handlers locally

**Example Flow (Client-Side Routing):**

1. Client subscribes: `"notice://prod/app/*"` → server returns `subscription_id=42`
2. Server receives PUBLISH to: `"notice://prod/app/orders/created"`
3. Server sends NOTIFY: `[subscription_id=42][route="notice://prod/app/orders/created"][payload]`
4. Client looks up `subscription_id=42` → finds `[handler1, handler2]`
5. Client calls:
   - `handler1("notice://prod/app/orders/created", payload)`
   - `handler2("notice://prod/app/orders/created", payload)`

#### Pattern Matching

- `*` matches one segment (e.g., `notice://realm/*/events` matches `notice://realm/orders/events`)
- `**` matches zero or more segments (e.g., `notice://realm/**` matches all routes in realm)
- Exact routes (no wildcards) also supported

#### Client-Side Multiplexing

**Key Design Principle:** One subscription per `(session, pattern)` on server, multiple handlers per subscription on client.

**What Happens When Multiple Handlers Subscribe to Same Pattern:**

```python
# User creates two handlers for same pattern
sub1 = client.notice_subscribe("notice://prod/app/*", handler1)
sub2 = client.notice_subscribe("notice://prod/app/*", handler2)

# Behind the scenes:
# - First subscribe: Client sends SUBSCRIBE to server, gets subscription_id=42
# - Second subscribe: Client reuses subscription_id=42, adds handler2 to local list
# - Server has ONE subscription (session_id, "notice://prod/app/*") → subscription_id=42
# - Client tracks: subscription_id=42 → [handler1, handler2]

# When message published to "notice://prod/app/orders":
# - Server sends ONE NOTIFY with subscription_id=42
# - Client demuxes locally: calls handler1(route, payload) then handler2(route, payload)
```

**Reference Counting for Unsubscribe:**

```python
sub1.unsubscribe()  # Client removes handler1 from local list
# subscription_id=42 still has handler2 registered
# NO UNSUBSCRIBE sent to server

sub2.unsubscribe()  # Client removes handler2 from local list
# subscription_id=42 now has zero handlers
# NOW client sends UNSUBSCRIBE(subscription_id=42) to server
```

**Benefits:**

- **Wire efficiency**: One NOTIFY per pattern match (not N duplicates)
- **Server simplicity**: No duplicate subscription tracking
- **Familiar pattern**: Same as EventEmitter, RxJS, Redis pub/sub listeners

**Client Implementation Responsibilities:**

- Track map: `subscription_id → [handler1, handler2, ...]`
- Track map: `route_pattern → subscription_id` (for dedup)
- Reference count handlers per subscription
- Only send UNSUBSCRIBE when last handler removed

#### Usage Example

**Recommended User-Facing API (Subscription Object with Multiplexing):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# Multiple handlers can subscribe to same pattern
sub1 = client.notice_subscribe(
    pattern="notice://prod/app/orders/*",
    handler=lambda route, payload: print(f"Handler1: {route}")
)

sub2 = client.notice_subscribe(
    pattern="notice://prod/app/orders/*",
    handler=lambda route, payload: print(f"Handler2: {route}")
)

# Behind scenes: Both share subscription_id=42 from server
# Client tracks: sub_id=42 → [handler1, handler2]

# When notification arrives, client calls both handlers
# (Server sent ONE NOTIFY, client demuxes locally)

# Unsubscribe individual handler
sub1.unsubscribe()  # Removes handler1, keeps handler2
# No server UNSUBSCRIBE sent yet (handler2 still active)

sub2.unsubscribe()  # Removes handler2 (last handler)
# NOW sends UNSUBSCRIBE(subscription_id=42) to server

# Publisher (stateless)
client.notice_publish(
    route="notice://prod/app/orders/created",
    payload=b'{"order_id": 123}'
)
```

**NoticeSubscription Object Implementation:**

```python
class NoticeClient:
    def __init__(self):
        self._subscriptions = {}  # subscription_id → [handlers]
        self._patterns = {}       # route_pattern → subscription_id
        self._handler_refs = {}   # handler → (subscription_id, pattern)

    def subscribe(self, pattern, handler):
        """Subscribe handler to pattern (may reuse existing subscription)"""
        # Check if already subscribed to this pattern
        if pattern in self._patterns:
            sub_id = self._patterns[pattern]
            self._subscriptions[sub_id].append(handler)
            self._handler_refs[id(handler)] = (sub_id, pattern)
            return NoticeSubscription(self, pattern, sub_id, handler)

        # New pattern - send SUBSCRIBE to server
        sub_id = self._send_subscribe_wire(pattern)  # Wire: [pattern_len][pattern]
        self._patterns[pattern] = sub_id
        self._subscriptions[sub_id] = [handler]
        self._handler_refs[id(handler)] = (sub_id, pattern)
        return NoticeSubscription(self, pattern, sub_id, handler)

    def _handle_notify(self, subscription_id, route, payload):
        """Called when NOTIFY frame arrives from server"""
        # Fan out to all local handlers for this subscription_id
        handlers = self._subscriptions.get(subscription_id, [])
        for handler in handlers:
            handler(route, payload)

    def _unsubscribe_handler(self, subscription_id, pattern, handler):
        """Remove specific handler (may unsubscribe from server)"""
        handlers = self._subscriptions.get(subscription_id, [])
        if handler in handlers:
            handlers.remove(handler)
            del self._handler_refs[id(handler)]

        # If no handlers left, unsubscribe from server
        if not handlers:
            self._send_unsubscribe_wire(subscription_id)  # Wire: [subscription_id]
            del self._subscriptions[subscription_id]
            del self._patterns[pattern]

class NoticeSubscription:
    def __init__(self, client, pattern, subscription_id, handler):
        self._client = client
        self._pattern = pattern              # Stored internally
        self._subscription_id = subscription_id  # From server
        self._handler = handler              # Specific handler

    def unsubscribe(self):
        """Remove this handler (may send UNSUBSCRIBE if last handler)"""
        self._client._unsubscribe_handler(
            self._subscription_id,
            self._pattern,
            self._handler
        )
```

**Wire Protocol (what actually goes on the wire):**

```
CLIENT → SERVER (first subscribe to pattern):
  SUBSCRIBE: [pattern_len]["notice://prod/app/*"]

SERVER → CLIENT:
  Response: [status=0][subscription_id=42]

CLIENT → SERVER (second subscribe to SAME pattern):
  (nothing sent - client reuses subscription_id=42 locally)

SERVER → CLIENT (when message published):
  NOTIFY: [subscription_id=42][route_len]["notice://prod/app/orders/created"][payload_len][payload]

CLIENT PROCESSING:
  - Looks up subscription_id=42 → finds [handler1, handler2]
  - Calls handler1(route, payload)
  - Calls handler2(route, payload)

CLIENT → SERVER (first unsubscribe):
  (nothing sent - handler2 still active locally)

CLIENT → SERVER (second unsubscribe, last handler removed):
  UNSUBSCRIBE: [subscription_id=42]
```

**Key Points:**

- User calls `client.subscribe(pattern, handler)` - simple, handler-focused API
- First subscribe to pattern sends SUBSCRIBE to server, gets `subscription_id`
- Subsequent subscribes to same pattern reuse server subscription, track handler locally
- NOTIFY includes `subscription_id` for client demux (NOT route pattern)
- Unsubscribe only sends to server when last handler removed (reference counting)
- Pattern: Familiar to EventEmitter, RxJS, Redux listeners, etc.

#### Semantics

- **Fire-and-Forget PUBLISH**: PUBLISH sends a frame with no response. No delivery confirmation, no error response. Transport errors (connection closed) are the only failure mode.
- **Client-Side Multiplexing**: Server tracks one subscription per `(session, pattern)`. Client tracks multiple handlers per `subscription_id`. Server sends one NOTIFY per pattern match; client demuxes to all local handlers.
- **Idempotent SUBSCRIBE**: Duplicate SUBSCRIBE to same pattern returns same `subscription_id` (no duplicate server subscription created)
- **Delivery**: Best-effort; under backpressure, notifications may be dropped
- **Ordering**: Delivered in publish order per subscription
- **Fanout**: Single publish reaches all matching subscriptions
- **Session-Scoped**: Subscriptions tied to connection; lost on disconnect
- **Acknowledgements & Retries**: `NOTIFY` frames are never acknowledged by clients and are never retried by the broker. Clients MUST NOT send acknowledgements for `NOTIFY` frames and MUST NOT expect guaranteed replay.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage Guidance:** `NOTICE` is a **best-effort, non-durable** mechanism. **Clients MUST NOT use Notices for workflows that require acknowledgement, durability, or guaranteed delivery. Use Queue for durable delivery. Use RPC only for low-latency request/response when callers and workers can tolerate disconnect or broker-restart loss and retry explicitly at the application layer.**

##### Pattern Matching & Precedence

**Multiple pattern matches:**

- Single PUBLISH may match multiple subscriptions
- Each matching subscription receives NOTIFY
- No precedence or filtering (all matches deliver)

**Example:**

- Client subscribes to: `notice://prod/app/*`
- Client subscribes to: `notice://prod/**`
- Publish to: `notice://prod/app/orders`
- Result: Client receives 2 NOTIFY frames (one per subscription)

**Deduplication:**

- Server does NOT deduplicate across different subscriptions
- If client subscribes twice to same pattern: both reference same `subscription_id` (client-side multiplexing)
- Client SHOULD use reference counting to avoid premature unsubscribe

#### Error Codes (3xxx range)

- 3001 = ERR_INVALID_ROUTE
- 3002 = ERR_INVALID_PATTERN
- 3003 = ERR_SUBSCRIPTION_LIMIT
- 3004 = ERR_TRANSPORT_CLOSED

#### Acceptance Tests

- subscribe to pattern, receive matching publications
- **client-side multiplexing**: two handlers subscribe to same pattern, both receive NOTIFY (client tracks, server sends one)
- **reference counting**: unsubscribe first handler doesn't send UNSUBSCRIBE to server; unsubscribe second handler (last) sends UNSUBSCRIBE
- **idempotent subscribe**: second SUBSCRIBE to same pattern returns same `subscription_id`
- publish with no subscribers returns ok
- unsubscribe stops delivery
- wildcard patterns match correctly (`*` single segment, `**` multi-segment)
- exact routes work without wildcards

### Stream Domain (Durable Append-Only Logs)

**Purpose:** Strictly ordered append/read with watermark protection and optimistic concurrency.

#### Message Types

| Type | Name         | Direction                  |
| ---: | ------------ | -------------------------- |
|  600 | BEGIN        | Client → Server            |
|  601 | APPEND       | Client → Server            |
|  602 | COMMIT       | Client → Server            |
|  603 | ROLLBACK     | Client → Server            |
|  604 | READ         | Client → Server            |
|  605 | LAST         | Client → Server            |
|  606 | GET_METADATA | Client → Server            |
|  607 | SUBSCRIBE    | Client → Server            |
|  608 | UNSUBSCRIBE  | Client → Server            |
|  609 | NOTIFY       | Server → Client (delivery) |

#### BEGIN Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  expected_offset
[u8]      has_ingest_metadata (0 or 1)
[u32 BE]  ingest_metadata_len (if has_ingest_metadata=1)
[bytes]   ingest_metadata
Response (status=0):
  [u8]     0
  [u64 BE] session_id
  [u32 BE] data_len
  [bytes]  data (broker-defined opaque bytes; clients MUST treat as opaque and MAY ignore)
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**expected_offset (OCC):** Clients MUST send `expected_offset` on every BEGIN. It is the client's view of the stream's next write offset for that route (0 for a new stream). Servers MUST enforce it: if `expected_offset` does not match the server's next offset for that route, the server MUST reject the request with status=1 and an error message (e.g. containing "conflict"). This provides optimistic concurrency control; clients that receive a conflict should re-read the stream and retry with the correct offset.

**Design Note:** The `data` field in Stream responses carries broker-defined metadata (e.g., current watermark, stream info). Clients MUST parse past it (read `data_len` bytes) but SHOULD NOT interpret its contents unless broker documentation specifies a schema.

#### APPEND Request

```
[u64 BE]  session_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  body_len
[bytes]   body
[u8]      has_metadata
[u32 BE]  metadata_len (if has_metadata=1)
[bytes]   metadata
Response (status=0):
  [u8]     0
  [u32 BE] data_len
  [bytes]  data
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Note:** `session_id` is `u64` (not string), returned from BEGIN response.

#### COMMIT Request

```
[u64 BE]  session_id
[u32 BE]  route_len
[bytes]   route
[u8]      mode (0=Buffered, 1=Sync)
Response (status=0):
  [u8]     0
  [u32 BE] data_len
  [bytes]  data
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Commit Modes:**

- **Sync (mode=1)**: Appends are flushed to durable storage (fsync) before COMMIT returns. Survives broker crash/restart. Higher latency, guaranteed durability.
- **Buffered (mode=0)**: Appends written to memory buffer, background flush to storage. Lower latency, best-effort durability. May lose recent appends on broker crash (up to flush interval). Use for non-critical events or when throughput > durability.

#### ROLLBACK Request

```
[u64 BE]  session_id
[u32 BE]  route_len
[bytes]   route
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### READ Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  from_offset
[u64 BE]  limit
[u8]      has_max_bytes
[u64 BE]  max_bytes (if present)
Response: status byte + data
```

#### LAST Request

```
[u32 BE]  route_len
[bytes]   route
Response: status byte + data
```

#### GET_METADATA Request

```
[u32 BE]  route_len
[bytes]   route
Response: status byte + data
```

#### Usage Example

**Recommended User-Facing API (StreamSession Object):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# BEGIN returns a StreamSession object
session = client.stream_begin(
    route="stream://prod/app/events",
    expected_offset=100
)

# Session methods are slim - no route or session_id needed in API
session.append(b"event_data_1")
session.append(b"event_data_2")
session.commit(mode=CommitMode.Sync)

# Or rollback
session.rollback()

# Read from stream (stateless - no session needed)
records = client.stream_read(
    route="stream://prod/app/events",
    from_offset=100,
    limit=10
)
```

**StreamSession Object Implementation:**

```python
class StreamSession:
    def __init__(self, client, route, session_id):
        self._client = client
        self._route = route         # Stored internally
        self._session_id = session_id  # Stored internally

    def append(self, body, metadata=None):
        """Slim API - route/session_id hidden from user"""
        # Wire protocol: packs session_id + route + body
        return self._client._send_stream_append(
            self._session_id,
            self._route,  # ← Sent on wire every time
            body,
            metadata
        )

    def commit(self, mode=CommitMode.Sync):
        """Commit session"""
        # Wire: [session_id][route_len][route][mode]
        return self._client._send_stream_commit(
            self._session_id,
            self._route,
            mode
        )

    def rollback(self):
        """Rollback session"""
        # Wire: [session_id][route_len][route]
        return self._client._send_stream_rollback(
            self._session_id,
            self._route
        )
```

**Wire Protocol (what actually happens):**

Every session operation includes **both session_id AND route** on the wire:

- `BEGIN`: `[route_len][route][expected_offset][...] → returns session_id`
- `APPEND`: `[session_id][route_len][route][body_len][body][metadata]`
- `COMMIT`: `[session_id][route_len][route][mode]`
- `ROLLBACK`: `[session_id][route_len][route]`
- `READ`: `[route_len][route][from_offset][limit][...]` (stateless)

**Key Points:**

- User calls `session.append(data)` - simple, focused on data
- Internally, client packs `[session_id][route][data]` on wire
- Server processes self-contained messages (no implicit state)
- Connection loss doesn't leave orphaned server state

#### Semantics

- **Self-Contained Operations**: Every request includes route for stateless server processing
- **Atomicity**: Appends are atomic; partial writes never visible
- **Ordering**: Records strictly ordered by offset within resource
- **Watermarks**: Reads cannot advance beyond watermark (protects uncommitted data)
- **Optimistic Concurrency**: `expected_offset` prevents lost updates
- **Durability**: All committed data survives broker restart
- **Isolation**: Stream sessions isolated per resource

#### Error Codes (2xxx)

- 2001 = ERR_CONCURRENCY_CONFLICT (expected_offset mismatch)
- 2002 = ERR_OFFSET_TOO_FAR_AHEAD
- 2003 = ERR_INVALID_READ_BOUND
- 2004 = ERR_READ_BEYOND_WATERMARK
- 2005 = ERR_RESOURCE_NOT_FOUND
- 2010 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 2011 = ERR_SUBSCRIPTION_LIMIT

#### Acceptance Tests

- begin/append/commit cycle
- read returns records in offset order
- read beyond watermark fails
- append with mismatched expected_offset fails
- rollback discards uncommitted appends
- multiple sessions can read concurrently

#### Stream SUBSCRIBE (607)

Subscribe to stream change notifications for a route pattern.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0                    // status: success
  [u8]     1                    // has_subscription_id flag (always 1 for success)
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1                    // status: error
  [u32 BE] error_len
  [bytes]  error_msg
```

**Wire Format Note:**
The success response uses the "optional u64" encoding pattern: a 1-byte flag followed by the value if present. For SUBSCRIBE success responses, the flag is always `1` (has value), followed by the `subscription_id` as a big-endian u64.

**Pattern Examples:**
- `stream://realm/area/resource` — specific resource changes
- `stream://realm/area/*` — area-level (all resources in area)
- `stream://realm/**` — realm-level (all areas and resources in realm)

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same pattern returns the same `subscription_id`
- Server tracks subscriptions by `(session_id, route_pattern)` tuple
- Wildcard patterns follow the same matching rules as Notice domain

#### Stream UNSUBSCRIBE (608)

Unsubscribe from stream change notifications.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**
- Client sends the original pattern string used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent pattern returns success

#### Stream NOTIFY (609) — Server to Client

Server pushes a stream change notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route (exact resource route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload (JSON)
```

**Payload Schemas:**

Commit notification (one or more records committed):
```json
{
  "event": "committed",
  "first_resource_offset": <u64>,
  "last_resource_offset": <u64>,
  "first_area_offset": <u64>,
  "last_area_offset": <u64>,
  "first_realm_offset": <u64>,
  "last_realm_offset": <u64>,
  "batch_size": <u32>
}
```

Watermark advance notification:
```json
{
  "event": "watermark_advanced",
  "previous": <u64>,
  "watermark": <u64>
}
```

**Delivery Semantics:**
- **Best-effort**: notifications may be dropped under backpressure
- **Debounced**: commit notifications are batched per 25ms window — multiple rapid commits to the same resource produce a single notification covering the full offset range
- **Session-scoped**: all subscriptions lost on disconnect; clients must re-subscribe after reconnecting
- `subscription_id` tells the client which subscription matched; client demultiplexes to local handlers

### Queue Domain (Durable At-Least-Once Delivery)

**Purpose:** FIFO-ish message queues with leasing and visibility timeouts.

#### Message Types

| Type | Name        |
| ---: | ----------- |
|  200 | ENQUEUE     |
|  202 | RESERVE     |
|  203 | EXTEND      |
|  204 | COMPLETE    |
|  207 | SUBSCRIBE   |
|  208 | UNSUBSCRIBE |
|  209 | NOTIFY      |

#### ENQUEUE Request

```
[u32 BE]  route_len
[bytes]   route (e.g., "queue://realm/area/resource")
[u32 BE]  body_len
[bytes]   body
[u8]      has_delay
[u64 BE]  delay_seconds (if present)
Response (status=0):
  [u8]     0
  [u64 BE] message_id
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### RESERVE Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  lease_seconds
[u8]      has_batch_size
[u32 BE]  batch_size (if present)
[u8]      has_wait_seconds
[u64 BE]  wait_seconds (if present)
Response (status=0):
  [u8]     0
  [u32 BE] lease_count
  [repeat for each lease]
    [u64 BE] message_id
    [u64 BE] lease_token
    [u32 BE] body_len
    [bytes]  body
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### EXTEND Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  message_id
[u64 BE]  lease_token
[u64 BE]  lease_seconds
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### COMPLETE Request

```
[u32 BE]  route_len
[bytes]   route
[u64 BE]  message_id
[u64 BE]  lease_token
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### Error Codes (4xxx range)

- 4001 = ERR_INVALID_TOKEN
- 4002 = ERR_LEASE_EXPIRED
- 4003 = ERR_MESSAGE_NOT_FOUND
- 4004 = ERR_QUEUE_NOT_FOUND
- 4005 = ERR_QUEUE_FULL

#### Queue SUBSCRIBE (207)

Subscribe to queue availability notifications for a route pattern.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0                    // status: success
  [u8]     1                    // has_subscription_id flag (always 1 for success)
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1                    // status: error
  [u32 BE] error_len
  [bytes]  error_msg
```

**Wire Format Note:**
The success response uses the "optional u64" encoding pattern: a 1-byte flag followed by the value if present. For SUBSCRIBE success responses, the flag is always `1` (has value), followed by the `subscription_id` as a big-endian u64.

**Pattern Examples:**
- `queue://realm/area/resource` — specific resource availability
- `queue://realm/area/*` — area-level (all resources in area)
- `queue://realm/**` — realm-level (all areas and resources in realm)

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same pattern returns the same `subscription_id`
- Server tracks subscriptions by `(session_id, route_pattern)` tuple
- Wildcard patterns follow the same matching rules as Notice domain
- Notifications are sent when messages become available in matching queues

#### Queue UNSUBSCRIBE (208)

Unsubscribe from queue availability notifications.

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**
- Client sends the original pattern string used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent pattern returns success

#### Queue NOTIFY (209) — Server to Client

Server pushes a queue availability notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route (exact resource route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload (notification details)
```

**Client Handling:**
- Client looks up `subscription_id` in local subscription map
- Invokes registered handler(s) with the notification data
- Handler receives exact route and notification payload
- Notifications indicate message availability (consumer should RESERVE)

#### Usage Example

**Recommended User-Facing API:**

```python
# Producer: Sequential enqueues to SAME queue
msg_id_1 = client.queue_enqueue(
    route="queue://prod/app/tasks",
    body=b"task_1",
    delay_seconds=0
)
msg_id_2 = client.queue_enqueue(
    route="queue://prod/app/tasks",
    body=b"task_2",
    delay_seconds=0
)

# Producer: Parallel enqueues to DIFFERENT queues
# Both complete independently and concurrently
msg_tasks = client.queue_enqueue(
    route="queue://prod/app/tasks",
    body=b"task_payload"
)
msg_events = client.queue_enqueue(
    route="queue://prod/app/events",
    body=b"event_payload"
)
# Both msg_tasks and msg_events complete in parallel

# Consumer: Reserve, process, complete
leases = client.queue_reserve(
    route="queue://prod/app/tasks",
    lease_seconds=30,
    batch_size=5
)

for lease in leases:
    try:
        process_task(lease.body)
        # Complete includes route for self-contained operation
        client.queue_complete(
            route="queue://prod/app/tasks",
            message_id=lease.message_id,
            lease_token=lease.lease_token
        )
    except ProcessingError:
        # Let lease expire, message returns to queue
        pass
```

**Wire Protocol:**

Every operation includes route:

- `ENQUEUE`: `[route_len][route][body_len][body][...]`
- `RESERVE`: `[route_len][route][lease_seconds][...]`
- `COMPLETE`: `[route_len][route][message_id][lease_token]`

#### Semantics

- **Self-Contained Operations**: Every request includes route; no server-side implicit state
- **At-Least-Once**: Messages delivered until completed; expired leases requeue them
- **FIFO-ish**: Generally delivered in enqueue order; leasing can cause out-of-order
- **Visibility Timeout**: Leased messages invisible to other consumers until expiry
- **Token Binding**: Complete/Extend require both message_id and lease_token
- **Durability**: All enqueued messages survive broker restart

##### Opaque Server-Generated IDs

**`message_id` and `lease_token` are server-generated opaque `u64` values:**

- Clients MUST NOT generate, predict, or cache these values
- Clients MUST treat them as opaque cookies
- `message_id`: Unique identifier assigned at ENQUEUE time
- `lease_token`: Fencing token generated at RESERVE time, prevents stale operations
- Sending wrong `message_id` or `lease_token`: ERR_INVALID_TOKEN

##### RESERVE Long Polling

**`wait_seconds` behavior:**

- If messages available: Return immediately (up to `batch_size`)
- If no messages available:
  - `wait_seconds=0` or omitted: Return empty immediately
  - `wait_seconds>0`: Block up to `wait_seconds` waiting for message
    - Returns as soon as message available
    - Returns empty if timeout expires
- Long polling pattern: Set `wait_seconds=30`, client loops

#### Acceptance Tests

- enqueue/reserve/complete cycle
- lease expiry returns message to ready queue
- extend lease delays expiry
- complete with wrong token fails
- reserve with batch_size returns up to that many
- multiple consumers can reserve from same queue

### RPC Domain (Request/Response & Streaming)

**Purpose:** Low-latency request/response with reply inbox and optional streaming.

#### Message Types

| Type | Name               | Direction       |
| ---: | ------------------ | --------------- |
|  300 | SUBSCRIBE_WORKER   | Client → Server |
|  301 | UNSUBSCRIBE_WORKER | Client → Server |
|  302 | REQUEST            | Client → Server (sync: client sends request, broker delivers to worker) |
|  303 | RESPONSE           | Server ↔ Client (sync or async: worker sends response(s) back to caller) |
|  304 | ACK                | Client ↔ Server |

#### SUBSCRIBE_WORKER Request

```
[u32 BE]  worker_route_len
[bytes]   worker_route
Response (status=0):
  [u8]     0
  [u32 BE] data_len
  [bytes]  data (empty)
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### UNSUBSCRIBE_WORKER Request

```
[u32 BE]  worker_route_len
[bytes]   worker_route
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### REQUEST Request (Client sends to server)

```
[16 bytes] correlation_id (UUID, big-endian)
[u32 BE]   route_len
[bytes]    route
[u32 BE]   reply_route_len
[bytes]    reply_route
[u32 BE]   body_len
[bytes]    body
Response from broker (status=0):
  [u8]     0
Response from broker (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Note:** `correlation_id` is always exactly 16 bytes (UUID). No length prefix needed.

#### REQUEST Delivery (Server forwards to worker)

When the broker selects a worker for an incoming REQUEST, it delivers the same REQUEST frame (MessageType=302) to the worker's connection. The worker receives:

```
[16 bytes] correlation_id (UUID, from caller)
[u32 BE]   route_len
[bytes]    route (the target route)
[u32 BE]   reply_route_len
[bytes]    reply_route (caller's reply route)
[u32 BE]   body_len
[bytes]    body
```

The worker MUST use `correlation_id` when sending RESPONSE frames back. The broker forwards RESPONSE frames to the caller currently associated with that in-memory pending request.

#### RESPONSE (From worker to caller via broker)

```
[16 bytes] correlation_id (UUID, big-endian)
[u64 BE]   sequence
[u32 BE]   body_len
[bytes]    body
[u8]       stream_end (0=more, 1=end)
Response from broker:
  [u8]     status
  [u32 BE] data_len
  [bytes]  data
```

**Design Note:** `stream_end` is a flag within the RESPONSE (303) frame payload, not a separate user frame type. It indicates whether this RESPONSE frame is the last one for that correlation_id (1=end, 0=more frames may follow).

#### ACK (Acknowledge receipt)

```
[16 bytes] correlation_id (UUID, big-endian)
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Note:** `correlation_id` is always exactly 16 bytes (UUID). No length prefix needed (consistent with REQUEST/RESPONSE).

#### Usage Example

**Recommended User-Facing API (WorkerSubscription Object):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# SUBSCRIBE_WORKER returns a WorkerSubscription object
worker = client.rpc_subscribe_worker(
    worker_route="rpc://prod/app/compute/run"
)

# Worker: Handle incoming requests through subscription
for request in worker.receive_requests():
    result = process_request(request.body)
    # Slim response method - worker_route stored internally
    worker.respond(
        correlation_id=request.correlation_id,
        body=result,
        stream_end=True
    )

# Slim unsubscribe method
worker.unsubscribe()

# Caller: Send request and wait for response (stateless)
correlation_id = generate_uuid()
responses = client.rpc_request(
    route="rpc://prod/app/compute/run",
    reply_route="inbox://session/replies",
    correlation_id=correlation_id,
    body=b"request_payload"
)
for response in responses:
    if response.stream_end:
        break
```

**WorkerSubscription Object Implementation:**

```python
class WorkerSubscription:
    def __init__(self, client, worker_route):
        self._client = client
        self._worker_route = worker_route  # Stored internally

    def unsubscribe(self):
        """Slim API - worker_route hidden from user"""
        # Wire protocol: packs worker_route
        return self._client._send_rpc_unsubscribe_worker(
            self._worker_route  # ← Sent on wire
        )

    def receive_requests(self):
        """Receive RPC requests for this worker"""
        # Filter incoming REQUEST frames by worker_route
        return self._client._receive_rpc_requests(self._worker_route)

    def respond(self, correlation_id, body, sequence=0, stream_end=True):
        """Send response - simplified API"""
        return self._client._send_rpc_response(
            correlation_id,
            sequence,
            body,
            stream_end
        )
```

**Wire Protocol (what actually happens):**

Every operation includes full context:

- `SUBSCRIBE_WORKER`: `[worker_route_len][worker_route]`
- `UNSUBSCRIBE_WORKER`: `[worker_route_len][worker_route]`
- `REQUEST`: `[16 bytes correlation_id][route_len][route][reply_route_len][reply_route][body_len][body]`
- `RESPONSE`: `[16 bytes correlation_id][sequence][body_len][body][stream_end]`

**Key Points:**

- User calls `worker.unsubscribe()` - simple, no route repetition
- Internally, client packs `[worker_route]` on wire
- `correlation_id` is fixed 16 bytes (UUID) - no length prefix
- Pattern: Same as KV Transaction, Stream Session, and Notice Subscription objects

#### Semantics

- **Self-Contained Operations**: Every SUBSCRIBE/REQUEST includes full route information
- **Correlation**: UUID links a live in-flight request to its responses (client-generated)
- **Streaming**: Multi-frame responses have incrementing `sequence` and `stream_end` flag
- **Backpressure**: ERR_RPC_BACKPRESSURE if outbound queue full
- **Ordering**: Responses delivered in sequence order
- **Single-Worker Assignment**: Each accepted request is assigned to at most one live worker while tracked in memory

##### Worker Selection & Load Balancing

**Multiple workers on same route:**

- Server selects worker using round-robin
- No least-connections or load-aware routing
- Clients should register multiple workers for horizontal scaling

**If all workers busy:**

- Server queues request (up to backpressure limit)
- Returns ERR_RPC_BACKPRESSURE if queue full
- Client should implement retry with backoff

**Worker failure:**

- If worker disconnects during request: ERR_WORKER_NOT_FOUND
- Caller receives timeout after configured interval (default 30s)
- Broker restart or reconnect does not recover worker registrations or replay pending requests

#### Error Codes (6xxx range)

- 6001 = ERR_RPC_TIMEOUT
- 6002 = ERR_WORKER_NOT_FOUND
- 6003 = ERR_RPC_BACKPRESSURE
- 6004 = ERR_ROUTE_NOT_REGISTERED
- 6005 = ERR_CORRELATION_NOT_FOUND

#### Acceptance Tests

- single request/response cycle
- streaming response reassembled in order
- request timeout returns error
- multiple workers on same route handle requests
- response with wrong correlation_id rejected with `6005 ERR_CORRELATION_NOT_FOUND`, while the original caller request remains pending until a valid response or timeout
- backpressure error when buffer full

### KV Domain (Durable Key-Value)

**Purpose:** Transaction-based CRUD and range operations with isolation.
**IMPORTANT:** All KV operations occur within transactions (Begin/Commit/Rollback).

#### Message Types

| Type | Name         |
| ---: | ------------ |
|  100 | BEGIN        |
|  101 | COMMIT       |
|  102 | ROLLBACK     |
|  103 | GET          |
|  104 | PUT          |
|  105 | INSERT       |
|  106 | DELETE       |
|  107 | DELETE_RANGE |
|  108 | SCAN         |

#### BEGIN Request

```
[u32 BE]  route_len
[bytes]   route (UTF-8, e.g., "kv://realm/area/resource")
[u8]      mode (0=ReadOnly, 1=ReadWrite)
[u8]      durability (0=Buffered, 1=Sync)
Response (success):
  [u8]     0 (status: success)
  [u64 BE] tx_id
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### PUT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### GET Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response (success):
  [u8]     0 (status: success)
  [u8]     found (0=not_found, 1=found)
  [u32 BE] value_len (present only if found=1)
  [bytes]  value (present only if found=1)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### INSERT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### DELETE Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### DELETE_RANGE Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  start_key_len
[bytes]   start_key
[u32 BE]  end_key_len
[bytes]   end_key
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### SCAN Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u8]      has_start (0 or 1)
[u32 BE]  start_key_len (if present)
[bytes]   start_key
[u8]      has_end
[u32 BE]  end_key_len (if present)
[bytes]   end_key
[u8]      has_limit
[u32 BE]  limit (if present)
[u8]      reverse (0 or 1)
Response (success):
  [u8]     0 (status: success)
  [u32 BE] item_count
  [repeat]
    [u32 BE] key_len
    [bytes]  key
    [u32 BE] value_len
    [bytes]  value
  [u8]     has_more (0 or 1)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### COMMIT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### ROLLBACK Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### Semantics

- **Self-Contained Operations**: Every request includes the full route; no implicit state beyond tx_id
- **Persistence**: All committed data survives broker restart
- **Client Convenience**: Clients MAY track route internally per tx_id for ergonomics, but MUST send route with every operation on the wire

##### Isolation Levels

**ReadOnly (mode=0):**

- Multiple ReadOnly transactions MAY run concurrently on same resource
- Reads see committed state at BEGIN time (snapshot isolation)
- Cannot see uncommitted writes from active ReadWrite transactions
- Cannot perform writes (PUT/INSERT/DELETE fail with ERR_WRITE_IN_READONLY)

**ReadWrite (mode=1):**

- Exclusive lock on resource (realm+area+resource tuple)
- Only one ReadWrite transaction active per resource at a time
- Other transactions (ReadOnly or ReadWrite) block until COMMIT/ROLLBACK
- Serializable isolation (all-or-nothing commit)

**Conflict Resolution:**

- If ReadWrite transaction begins while another active: ERR_ISOLATION_CONFLICT
- If ReadOnly transaction begins during ReadWrite: blocks until commit/rollback

##### Durability Modes

**Sync (durability=1):**

- Commits are flushed to durable storage (WAL fsync) before returning
- Survives broker crash/restart
- Higher latency, guaranteed durability

**Buffered (durability=0):**

- Commits written to memory buffer, background flush to storage
- Lower latency, best-effort durability
- May lose recent commits on broker crash (up to flush interval)
- Use for caching or when throughput > durability

##### SCAN Semantics

**`reverse` flag:**

- `reverse=0` (forward): Scan keys in ascending lexicographic order
  - Start at `start_key` (or first key if omitted)
  - End at `end_key` (or last key if omitted)
- `reverse=1` (backward): Scan keys in descending lexicographic order
  - Start at `end_key` (or last key if omitted)
  - End at `start_key` (or first key if omitted)
- `limit` applies regardless of direction

#### Usage Example

**Recommended User-Facing API (see [Recommended Client API Design](#recommended-client-api-design)):**

```python
# Connect with JWT
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# Begin transaction - returns Transaction object
# Route is full URI: kv://realm/area/resource
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)

# Transaction methods focus on data, hide route repetition
tx.put(b"user:123", b"alice")
value = tx.get(b"user:123")
tx.commit()

# Context manager pattern (Python)
with client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync) as tx:
    tx.put(b"key", b"value")
    tx.commit()  # Or auto-commit on __exit__
```

**Wire Protocol (what actually happens under the hood):**

Every transaction operation sends **both tx_id AND route** on the wire:

- `PUT`: `[tx_id][route_len][route][key_len][key][value_len][value]`
- `GET`: `[tx_id][route_len][route][key_len][key]`
- `COMMIT`: `[tx_id][route_len][route]`

The Transaction object stores the route internally and includes it in every wire
message, making each request fully addressable on the wire while the broker
still maintains live session-scoped transaction state keyed by `tx_id`.

#### Error Codes (1xxx)

- 1001 = ERR_TRANSACTION_NOT_FOUND
- 1002 = ERR_INVALID_MODE
- 1003 = ERR_KEY_NOT_FOUND
- 1004 = ERR_ISOLATION_CONFLICT
- 1005 = ERR_WRITE_IN_READONLY
- 1006 = ERR_KEY_EXISTS (INSERT on existing key)
- 1007 = ERR_INVALID_ROUTE
- 1008 = ERR_REALM_MISMATCH
- 1009 = ERR_BACKEND_ERROR
- 1010 = ERR_TRANSACTION_ABORTED

#### Acceptance Tests

- begin/put/commit cycle
- begin/get on non-existent key
- ReadOnly mode rejects put
- two transactions on same resource conflict
- rollback discards all changes
- scan returns lexicographically ordered pairs

### Lease Domain (Ephemeral Exclusive Coordination with Queueing)

**Purpose:** In-memory exclusive leases for single-broker coordination with optional FIFO queue-based acquisition, process-local fencing tokens, and configurable wait timeouts.

**Key Concepts:**
- **Lease**: A scarce resource identified by route (lease://realm/area/resource)
- **Fencing Token (`u64`)**: Server-generated opaque value that is only meaningful within the current broker process
- **Owner ID**: String identifier for the holder
- **TTL (Time-to-Live)**: Server-enforced expiration in seconds
- **Queueing**: Optional FIFO wait for availability with timeout
- **Route Partitioned**: Each route has independent lease state

#### Message Types

| Type | Name    | Semantics |
| ---: | ------- | --- |
|  400 | ACQUIRE | Request exclusive ownership with optional wait |
|  401 | RENEW   | Extend lease expiration by issuing new token |
|  402 | RELEASE | Relinquish lease, grant to next waiter |
|  403 | QUERY   | Inspect current holder and waiter count |

#### ACQUIRE Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  ttl_secs
[u32 BE]  wait_seconds (optional, defaults to 0)
```

**Parameters:**
- `route`: Lease identity (e.g., `lease://realm/area/leader`)
- `owner_id`: String identifier for owner (e.g., `"node-1"`)
- `ttl_secs`: Server-enforced expiration duration (e.g., `60`)
- `wait_seconds`: Max time to wait if lease held by other owner
  - `0` (or omitted): Immediate fail if unavailable → response `HeldByOther`
  - `1-30`: Queue and wait up to N seconds → response `Queued` or `Timeout`
  - Server enforces max 30 seconds per request

**Response (status=0, success):**
```
[u8]     0 (status)
[u8]     response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)
[u64 BE] fencing_token
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### RENEW Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token
[u64 BE]  ttl_secs
```

**Design Notes:**
- `fencing_token` MUST match current holder; mismatch returns `Fenced` error
- Issues new token on successful renewal
- Fails if lease expired or held by different owner

**Response (status=0, success):**
```
[u8]     0 (status)
[u64 BE] new_fencing_token
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### RELEASE Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token
```

**Design Notes:**
- `fencing_token` MUST match current holder
- Failing token prevents zombie holders from releasing
- Server FIFO-grants next waiter (if any) immediately

**Response (status=0, success):**
```
[u8]     0 (status)
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### QUERY Request

```
[u32 BE]  route_len
[bytes]   route
```

**Response (status=0, lease free):**
```
[u8]     0 (status)
[u8]     0 (has_holder=false)
[u32 BE] pending_waiters (always 0 when free)
```

**Response (status=0, lease held):**
```
[u8]     0 (status)
[u8]     1 (has_holder=true)
[u32 BE] owner_id_len
[bytes]  owner_id
[u64 BE] ttl_remaining_secs
[u32 BE] pending_waiters (count of clients waiting in queue)
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### Response Types (Detailed)

**Successful Responses:**

| Response | Meaning | Next Action |
|----------|---------|-----------|
| `Acquired { token }` | Lease granted immediately | Proceed with work; renew before TTL expiry |
| `AlreadyHeld { token }` | Already own this lease (idempotent) | No-op; renew/release as needed |
| `Queued { token }` | Waiting for lease in FIFO queue | Watch for async `Acquired` message from server |
| `AlreadyQueued { token }` | Already waiting for same lease | No-op; continue waiting |
| `Renewed { token }` | Lease TTL extended with new token | Use new token for future renew/release |
| `Released` | Lease released successfully | Lease available; next waiter (if any) receives async `Acquired` |
| `Status { owner, token, ttl_secs, pending }` | Lease holder & queue info | Read-only; useful for debugging |

**Error Responses:**

| Error | Meaning | Cause | Recovery |
|-------|---------|-------|----------|
| `HeldByOther` | Immediate rejection | `wait_seconds=0` and lease held by other | Retry with `wait_seconds=N` or back off |
| `Timeout` | Wait expired | Waited full `wait_seconds`, still unavailable | Retry with longer timeout or try different resource |
| `QueueFull` | Too many waiters | >100 pending acquirers on lease | Back off; DoS prevention limit reached |
| `NotHeld` | Not lease owner | Renew/Release without ownership | Check current holder via QUERY |
| `Fenced` | Token mismatch | `fencing_token` doesn't match server | Acquire fresh lease; old lease was released |
| `Expired` | Lease expired | Query on released lease (no holder) | Lease available; acquire fresh |
| `NotFound` | Lease doesn't exist | Route never acquired before | New lease; acquire automatically creates |

#### Queueing Semantics

**FIFO Ordering:**
- When lease unavailable and `wait_seconds > 0`, client enters queue
- Waiters served in strict FIFO order (first-come, first-served)
- No fairness guarantees across multiple attempts or clients

**Wait Timeout:**
- Client-specified `wait_seconds` (1-30) limits how long to wait
- Server counts down; if lease becomes available within timeout → `Acquired` (async)
- If lease still held after timeout → `Timeout` response (async)
- Client should NOT assume timeout fires at exactly `wait_seconds`; treat as "approximately" N seconds

**Server Constraints:**
- Max queue depth: 100 pending acquirers per lease (return `QueueFull` to reject)
- Max wait time: 30 seconds per request (server enforces)
- Auto-expiry: Pending waiters discarded after their timeout expires

**Deferred Response Behavior:**
- ACQUIRE with `wait_seconds > 0` returns `Queued` immediately
- Later, server sends async `Acquired` or `Timeout` when state changes
- Client must listen for these out-of-band responses on connection

**Grant on Release/Expiry:**
- When current holder releases or lease expires, next FIFO waiter automatically gets `Acquired` response
- All other waiters remain queued or time out

**Lease Transitions:**
```
                ACQUIRE
                   |
                   v
         ┌─────────────────┐
         |   FREE LEASE    |
         └─────────────────┘
              ^         |
              |         | wait_seconds > 0
              |         v
          RELEASE  ┌──────────┐
           (async) |  QUEUED  |
             ^     └──────────┘
             |          |
           Next  (on expiry or release)
           Waiter|
             returns Acquired (async)
```

#### Usage Scenarios (with Pseudocode)

**Scenario 1: Immediate Acquire (Fast Path)**

```python
# Try to acquire with no wait
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-1",
    ttl_secs=60,
    wait_seconds=0  # Explicit: fail immediately if unavailable
)

if response.type == "Acquired":
    token = response.token
    # Proceed with exclusive work
    perform_critical_section()
    
    # Release when done
    client.lease_release(
        route="lease://prod/app/counter",
        owner_id="client-1",
        fencing_token=token
    )
elif response.type == "HeldByOther":
    print(f"Lease held by {response.current_owner}, cannot proceed")
    # Backoff or try alternate path
```

**Scenario 2: Queue-Wait Acquire (Contended Resource)**

```python
# Try to acquire with optional wait
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-1",
    ttl_secs=60,
    wait_seconds=10  # Wait up to 10 seconds
)

if response.type == "Queued":
    print("Lease unavailable; waiting in queue...")
    # Server will send async "Acquired" when lease available
    # OR async "Timeout" if not available within 10 seconds
    
    # Listen for async messages from server
    while True:
        msg = client.receive_async()
        if msg.type == "Acquired":
            token = msg.token
            print("Lease granted!")
            perform_critical_section()
            client.lease_release(...)
            break
        elif msg.type == "Timeout":
            print("Wait timeout; lease still unavailable")
            # Retry with longer timeout or abandon
            break
```

**Scenario 3: Timeout During Wait**

```python
# Acquire with short timeout
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-2",
    ttl_secs=60,
    wait_seconds=2  # Wait max 2 seconds
)

if response.type == "Queued":
    # Wait for async response (timeout or acquire)
    msg = client.receive_async(timeout=3)  # Client-side safety timeout
    
    if msg.type == "Timeout":
        print("Could not acquire within 2 seconds")
        # Abandon attempt or retry with backoff
    elif msg.type == "Acquired":
        # Late grant; proceed
        pass
```

**Scenario 4: Queue Full (Rejection)**

```python
# Many clients contending; queue at capacity
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-99",
    ttl_secs=60,
    wait_seconds=10
)

if response.type == "QueueFull":
    print(f"Too many waiters ({response.pending_count}); rejected")
    # Server gate to prevent cascading waits
    # Backoff exponentially or use alternate strategy
    time.sleep(random_backoff())
    retry_acquire_or_failover()
```

**Scenario 5: Release & Grant Next Waiter**

```python
# Client-1 holds lease; Client-2 is oldest waiter
# Client-1 releases
client.lease_release(
    route="lease://prod/app/counter",
    owner_id="client-1",
    fencing_token=token_from_client_1
)
# Response: Released (immediate)

# Server automatically grants Client-2 (oldest waiter)
# Client-2's async listener receives:
msg = client.receive_async()  # From server
# msg.type == "Acquired"
# msg.token = new_token
# Client-2 can now proceed with work
```

**Scenario 6: Renew While Holding**

```python
# Client has lease; renew before expiry
token = initial_token_from_acquire()

response = client.lease_renew(
    route="lease://prod/app/counter",
    owner_id="client-1",
    fencing_token=token,
    ttl_secs=60
)

if response.type == "Renewed":
    new_token = response.token
    # Continue work; use new_token for next renew/release
    perform_more_work()
    
    # Later, renew again or release
    client.lease_release(
        route="lease://prod/app/counter",
        owner_id="client-1",
        fencing_token=new_token
    )
elif response.type == "Fenced":
    print(f"Token mismatch; lease no longer ours (held by {response.current_holder})")
    # Abort and acquire fresh lease if needed
```

#### Common Patterns & Best Practices

**Idempotent Acquire:**
- Calling ACQUIRE when already holding the lease returns `AlreadyHeld` (safe re-issue)
- Useful for "set desired state" patterns

**Fencing Against Zombies:**
- RENEW and RELEASE require matching `fencing_token`
- Prevents stale holder (expired but still executing) from affecting current holder
- Token changes on each RENEW, ensuring linear ordering

**Linearizability:**
- Each token change represents a serialization point
- Holders with newer tokens always supersede older tokens
- Safe for strong consistency coordination

**Timeout Tuning:**
- **Short waits (1-2s):** Fast-fail scenarios; high throughput, less fairness
- **Medium waits (5-10s):** Balanced; allows real contention, bounded latency
- **Long waits (20-30s):** High fairness; higher latency for unavailable resources
- **No wait (wait_seconds=0):** Pessimistic locking; immediate fail-fast

**Queue Monitoring:**
- Use QUERY to inspect `pending_waiters` count
- High count → high contention; consider resource partitioning or load shedding
- Helps detect cascading waits or application logic issues

**Client-Side Timeouts:**
- Pair server-side `wait_seconds` with client-side receive timeout
- Prevents client hang if async message lost/delayed
- Example: `receive_async(timeout_ms=max(wait_seconds+2, 5000))`

#### Semantics

- **Self-Contained Operations**: Every ACQUIRE/RENEW/RELEASE includes full route
- **Mutual Exclusion**: Only one owner holds a lease at a time
- **TTL-based Expiry**: Expired leases automatically released; next waiter (if any) granted
- **Route Partitioned**: Different routes have independent lease state and queues
- **In-Memory**: Lost on broker restart (use for coordination, not durability)
- **FIFO Fairness**: Waiters served in order; no starvation within single lease

##### Fencing Token

**`fencing_token` is server-generated opaque `u64` value:**
- Prevents stale commands from affecting current holder or leaked state
- Clients MUST treat as cookie (no prediction, caching, or reuse)
- Generated fresh at ACQUIRE time
- Changes on each successful RENEW
- Restart resets the token lineage; tokens are not durable or cluster-wide
- Validated by server on RENEW/RELEASE; mismatch → `Fenced` error

**Use Case Example:**
- Client-A acquires lease (token T1), gets delayed
- Lease expires; Client-B acquires lease (token T2)
- Client-A wakes up, tries RELEASE with T1
- Server rejects: token T1 ≠ current T2 → Error `Fenced`
- Client-A knows it lost the lease; Client-B's lease protected

#### Error Codes (5xxx)

- 5001 = ERR_LEASE_HELD (immediate rejection with `wait_seconds=0`)
- 5002 = ERR_LEASE_HELD_BY_OTHER (detailed: `current_owner` provided)
- 5003 = ERR_INVALID_FENCE (token mismatch on RENEW/RELEASE)
- 5004 = ERR_LEASE_EXPIRED (lease no longer valid)
- 5005 = ERR_LEASE_NOT_FOUND (route never acquired)
- 5006 = ERR_QUEUE_FULL (too many pending waiters)
- 5007 = ERR_INVALID_OWNER (owner_id mismatch on RENEW/RELEASE)
- 5008 = ERR_WAIT_OUT_OF_RANGE (wait_seconds > 30)

#### Acceptance Tests

- acquire succeeds on free lease, returns `Acquired`
- acquire fails on held lease with `wait_seconds=0`, returns `HeldByOther`
- acquire with `wait_seconds>0` returns `Queued`, later receives `Acquired` when available
- acquire with short `wait_seconds` receives `Timeout` if unavailable
- multiple waiters receive `Acquired` in FIFO order as lease released
- renew with valid token extends TTL and issues new token
- renew with invalid token fails with `Fenced`
- release with valid token releases, grants next waiter
- release with invalid token fails with `Fenced`
- expired lease acquirable by new owner (FIFO waiters bypass expiry)
- query shows holder, TTL, and pending_waiters count
- idempotent acquire (already holding) returns `AlreadyHeld`
- queue-full rejection at 101st concurrent waiter

### Schedule Domain (Delayed/Recurring Tasks)

**Purpose:** Durable scheduling of delayed tasks and recurring jobs.

#### Message Types

| Type | Name        | Direction                  |
| ---: | ----------- | -------------------------- |
|  700 | CREATE      | Client → Server            |
|  701 | CANCEL      | Client → Server            |
|  702 | LIST        | Client → Server            |
|  703 | SUBSCRIBE   | Client → Server            |
|  704 | UNSUBSCRIBE | Client → Server            |
|  705 | NOTIFY      | Server → Client (delivery) |

#### CREATE Request

**Wire Format:**
```
[u32 BE]  route_len
[bytes]   route (e.g., "schedule://realm/area/resource")
[u32 BE]  cron_len
[bytes]   cron (UTF-8 cron expression, 5-field format)
[u32 BE]  payload_len
[bytes]   payload (arbitrary bytes to deliver on notification)

Response (success=0):
  [u8]     0

Response (error=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Route serves as the unique schedule identifier (upsert behavior)
- Creating a schedule with an existing route updates that schedule
- Payload is arbitrary binary data delivered to subscribers on notification
- Invalid cron expression returns error code 7002

#### CANCEL Request

**Wire Format:**
```
[u32 BE]  route_len
[bytes]   route

Response (success=0):
  [u8]     0

Response (error=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Canceling a nonexistent schedule succeeds (idempotent)
- Cancel prevents all future executions
- Already-running notifications may still be delivered

#### LIST Request

**Wire Format:**
```
(no parameters - lists all schedules in current realm)

Response 1..N (streaming, one schedule per response):
  [u8]     0 (status=success)
  [u8]     1 (has_entry=true)
  [u32 BE] route_len
  [bytes]   route
  [u32 BE] cron_len
  [bytes]   cron
  [u32 BE] payload_len
  [bytes]  payload

Response N+1 (final, end-of-stream):
  [u8]     0 (status=success)
  [u8]     0 (has_entry=false, no more schedules)

Response (error):
  [u8]     1 (status=error)
  [u32 BE] error_len
  [bytes]  error_msg
```

**Streaming Protocol:**
- Client continues reading until response with `has_entry=0`
- Empty result set: Single response with `status=0, has_entry=0`
- Non-empty result: N responses with schedules, then final response with `has_entry=0`
- Client MUST NOT assume stream ends after fixed count

#### Cron Syntax (Broker-Enforced)

Brokers MUST support standard 5-field cron format:

```
* * * * *
| | | | |
| | | | +---- Day of week (0-6, Sunday is 0)
| | | +------ Month (1-12)
| | +-------- Day of month (1-31)
| +---------- Hour (0-23)
+------------ Minute (0-59)
```

**Supported patterns:**

- `*` = every unit (e.g., `* * * * *` = every minute)
- Numeric values = exact match (e.g., `0 9 * * 1` = 9:00 AM every Monday)
- Ranges = `start-end` (e.g., `0 9-17 * * *` = every hour from 9 AM to 5 PM)
- Lists = `value,value,value` (e.g., `0 9,12,15 * * *` = 9 AM, 12 PM, 3 PM)
- Steps = `*/step` or `range/step` (e.g., `*/15 * * * *` = every 15 minutes)
- Combined = (e.g., `0 9-17/2 * * 1-5` = every 2 hours from 9 AM-5 PM on weekdays)
  **Examples:**
- `0 9 * * 1` = 9:00 AM every Monday
- `*/5 * * * *` = Every 5 minutes
- `0 */2 * * *` = Every 2 hours
- `0 9-17 * * 1-5` = Every hour from 9 AM-5 PM on weekdays
- `30 2 1 * *` = 2:30 AM on the 1st of every month

#### Persistence & Recovery

Schedules are durable (persisted to storage):

- Survive broker restart
- Execution resumes at next scheduled time
- Missed schedules (broker down at scheduled time) are skipped
- No catch-up or backfill for missed executions

#### LIST Streaming

LIST returns multiple responses (one schedule per response):

```
Response 1:
  [u8]     0 (status)
  [u8]     1 (has_entry)
  [u32 BE] route_len
  [bytes]  route
  [u32 BE] cron_len
  [bytes]  cron
  [u32 BE] payload_len
  [bytes]  payload
Response 2:
  [u8]     0 (status)
  [u8]     1 (has_entry)
  [u32 BE] route_len
  [bytes]  route
  ...
Response N (final):
  [u8]     0 (status)
  [u8]     0 (has_entry=0, no more schedules)
```

Client MUST continue reading until `has_entry=0`.

#### Usage Example

```python (notification-only model)
client.schedule_create(
    route="schedule://prod/app/reminders/send",
    cron="0 9 * * 1",  # Every Monday at 9 AM
    payload=b"weekly-reminder-config"
)

# Subscribe to schedule notifications
client.schedule_subscribe(
    route="schedule://prod/app/reminders/send"
)

# Receive notification when schedule fires (Message Type 705)
# Server sends: SCHEDULE_NOTIFY(subscription_id, payload)

# List schedules
schedules = client.schedule_list()

# Cancel schedule
client.schedule_cancel(
    route="schedule://prod/app/reminders/send"
)
```uses route as identity:

- `CREATE`: `[route_len][route][cron_len][cron][payload_len][payload]`
- `LIST`: (no parameters, lists all schedules)
- `CANCEL`: `[route_len][route]`

#### Semantics

- **Route-Based Identity**: Routes uniquely identify schedules (CREATE is upsert)
- **Durability**: Schedules persist across broker restarts
- **Notification-Only**: When schedules fire, SCHEDULE_NOTIFY (705) is sent to subscribers
- **Recurring**: Interval-based recurring tasks (cron-like)
- **Cancellation**: Cancels future runs; already-delivered notifications cannot be revoked
- **Realm Scoped**: Schedules isolated per realm

##### Execution Model (Notification-Only)

When a schedule fires, the broker performs **one action**:

**SCHEDULE_NOTIFY (705):** The broker emits a `SCHEDULE_NOTIFY` message to all clients subscribed to the schedule's exact route via `SCHEDULE_SUBSCRIBE (703)`. The notification contains the schedule's configured payload bytes.

**Client observability:** Clients observe schedule execution by subscribing to schedule routes via `SCHEDULE_SUBSCRIBE` to receive `SCHEDULE_NOTIFY` when schedules fire.

**Payload semantics:** The payload is opaque to Fitz — clients can encode configuration, task identifiers, or any data needed to handle the notification. Common patterns:
- JSON-encoded task config
- Protobuf-serialized parameters  
- Simple string identifiers
- Arbitrary binary data

**Client observability:** Clients have two options for observing schedule execution:
- **Direct:** Subscribe to the schedule route via `SCHEDULE_SUBSCRIBE` to receive `SCHEDULE_NOTIFY` when the schedule fires
- **Indirect:** Subscribe to the target resource via the appropriate domain (e.g., Notice SUBSCRIBE for Notice targets, Queue RESERVE for Queue targets)

#### Error Codes (7xxx)

- 7001 = ERR_SCHEDULE_NOT_FOUND
- 7002 = ERR_INVALID_CRON (informational - cancel is idempotent)
- 7002 = ERR_INVALID_CRON
- 7003 = ERR_SCHEDULE_LIMIT
- 7004 = ERR_PARSE_ERROR
- 7005 = ERR_INVALID_ROUTE
- 7006 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 7007 = ERR_SUBSCRIPTION_LIMIT

#### Acceptance Tests

- create schedules task with cron expression
- create on existing route updates (upsert)
- cancel prevents future notifications
- cancel on nonexistent route succeeds (idempotent)
- list returns all created schedules
- schedule persists across broker restart
- subscribers receive SCHEDULE_NOTIFY when schedule fires
- invalid cron expression rejected with 7002
#### Schedule SUBSCRIBE (703)

Subscribe to schedule fire notifications for an exact schedule route.

```
[u32 BE]  route_len
[bytes]   route
Response (status=0):
  [u8]     0
  [u8]     1
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Route Example:**
- `schedule://realm/area/resource/operation` — specific schedule fires

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same route returns the same `subscription_id`
- Client is responsible for local multiplexing when multiple handlers share the same route
- Wildcard schedule subscribe is invalid; use `LIST` selectors for discovery instead
- When the schedule fires, the server sends SCHEDULE_NOTIFY (705) with subscription_id and payload; the client matches notifications to the route they subscribed with

#### Schedule UNSUBSCRIBE (704)

Unsubscribe from schedule fire notifications.

```
[u32 BE]  route_len
[bytes]   route
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**
- Client sends the original exact route string used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent route returns success

#### Schedule NOTIFY (705) — Server to Client

Server pushes a schedule fire notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  payload_len
[bytes]   payload (the schedule's configured payload bytes)
```

**Design Notes:**
- `subscription_id` tells the client which subscription matched; the subscription already identifies the exact route
- Payload is the raw payload bytes configured when the schedule was created
- Client demultiplexes to local handlers registered for that `subscription_id`
- Delivery is best-effort; notifications may be dropped under backpressure

## Constants & TLV Registry

### MessageType Ranges

**Control (0–99):**
| Value | Name |
|---:|---|
| 1 | CONNECT |
**KV Domain (100–108):**
| Value | Name |
|---:|---|
| 100 | BEGIN |
| 101 | COMMIT |
| 102 | ROLLBACK |
| 103 | GET |
| 104 | PUT |
| 105 | INSERT |
| 106 | DELETE |
| 107 | DELETE_RANGE |
| 108 | SCAN |
**Queue Domain (200–209):**
| Value | Name |
|---:|---|
| 200 | ENQUEUE |
| 201 | ENQUEUE_BATCH (reserved; servers may reject with unknown message type until defined) |
| 202 | RESERVE |
| 203 | EXTEND |
| 204 | COMPLETE |
| 207 | SUBSCRIBE |
| 208 | UNSUBSCRIBE |
| 209 | NOTIFY |
**RPC Domain (300–304):**
| Value | Name |
|---:|---|
| 300 | SUBSCRIBE_WORKER |
| 301 | UNSUBSCRIBE_WORKER |
| 302 | REQUEST |
| 303 | RESPONSE |
| 304 | ACK |
**Lease Domain (400–403):**
| Value | Name |
|---:|---|
| 400 | ACQUIRE |
| 401 | RENEW |
| 402 | RELEASE |
| 403 | QUERY |
**Notice Domain (500–504):**
| Value | Name |
|---:|---|
| 500 | PUBLISH |
| 501 | SUBSCRIBE |
| 502 | UNSUBSCRIBE |
| 503 | UNSUBSCRIBE_ALL |
| 504 | NOTIFY |
**Stream Domain (600–609):**
| Value | Name |
|---:|---|
| 600 | BEGIN |
| 601 | APPEND |
| 602 | COMMIT |
| 603 | ROLLBACK |
| 604 | READ |
| 605 | LAST |
| 606 | GET_METADATA |
| 607 | SUBSCRIBE |
| 608 | UNSUBSCRIBE |
| 609 | NOTIFY |
**Schedule Domain (700–705):**
| Value | Name |
|---:|---|
| 700 | CREATE |
| 701 | CANCEL |
| 702 | LIST |
| 703 | SUBSCRIBE |
| 704 | UNSUBSCRIBE |
| 705 | NOTIFY |

### MessageType Routing

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Future compatibility:** If a domain exhausts its range, extend to a new 100-block (e.g., 1100–1199 for KV expansion)

### Error Code Allocation (Authoritative)

Error codes are allocated by domain in 100-block ranges:
| Range | Domain | Capacity | Notes |
| --------- | -------- | --------- | ----------------------------------- |
| 1000–1099 | KV | 100 codes | Transactions, isolation, durability |
| 2000–2099 | Stream | 100 codes | Concurrency, watermarks, ordering |
| 3000–3099 | Notice | 100 codes | Routing, patterns, delivery |
| 4000–4099 | Queue | 100 codes | Leasing, visibility, delivery |
| 5000–5099 | Lease | 100 codes | Mutual exclusion, fencing, TTL |
| 6000–6099 | RPC | 100 codes | Routing, backpressure, correlation |
| 7000–7099 | Schedule | 100 codes | Scheduling, persistence, execution |
**Expansion Strategy:**
If domain exhausts range (>99 error codes allocated):

- First expansion block: {base}100–{base}199 (e.g., 1100–1199 for KV)
- Second expansion: {base}200–{base}299 (e.g., 1200–1299 for KV)
- Continue as needed
  **Cross-Domain Error Codes:**
  These error codes are standardized across ALL domains:
- `*001` = ERR_UNAUTHORIZED (permission denied, see Permissions section)
- `*002` = ERR_INVALID_SCOPE (scope mismatch)
- `*003` = ERR_REALM_MISMATCH (realm not in JWT)
  All other error codes are domain-specific and MUST NOT be reused across domains.

### Channel IDs (Broker-Internal Reference)

Clients do NOT encode these; listed for reference:
| ChannelId | Value | Purpose |
| --------- | ----: | ---------------------- |
| Control | 0 | Control/handshake |
| Pub | 1 | Publishing/notice |
| Sub | 2 | Subscriptions/delivery |
| Rpc | 3 | RPC request/response |
| Lease | 4 | Lease domain |

### Type Encoding Rules

- `type 0x00..0xFE`: single byte on wire
- `type 0xFF`: escape marker — followed by `u16 BE` for actual type value (for types > 0xFE)

**Decoder pseudocode:**

```python
def read_message_type(stream):
    """Read MessageType from wire. Returns u16."""
    first_byte = stream.read_u8()
    if first_byte == 0xFF:
        # Escape: next 2 bytes are the actual type
        return stream.read_u16_be()
    else:
        # Single byte type (0x00–0xFE)
        return first_byte
```

**Encoder pseudocode:**

```python
def write_message_type(stream, msg_type):
    """Write MessageType to wire."""
    if msg_type <= 0xFE:
        stream.write_u8(msg_type)
    else:
        stream.write_u8(0xFF)
        stream.write_u16_be(msg_type)
```

**Current implications:**

- CONNECT (type=1): encodes as 1 byte `[0x01]`
- KV BEGIN (type=100): encodes as 1 byte `[0x64]`
- Notice PUBLISH (type=500): encodes as 3 bytes `[0xFF][0x01][0xF4]`

**IMPORTANT:** The wire examples elsewhere in this document show all MessageTypes as 2-byte `[u16 BE]` for readability. Conformant implementations MUST use the variable-length encoding described above.

## Acceptance Criteria

Client implementations MUST pass the following test suite against a reference broker:

### Transport-Level Tests

1. **WebSocket connect** - Establish WebSocket, send CONNECT, verify session opens
2. **TCP connect** - Establish TCP, send length-prefixed CONNECT, verify session opens
3. **Frame size enforcement** - Send frame > `max_frame_size`, broker closes connection
4. **Reconnect** - Drop connection, reconnect, re-send CONNECT, verify session re-established

### Domain-Level Tests (per domain)

**Notice:**

- Subscribe to pattern, receive matching publications
- Multiple subscriptions on same pattern both receive
- Publish with no subscribers returns ok
- Unsubscribe stops delivery
- Wildcard patterns match correctly
  **Stream:**
- Begin/append/commit cycle succeeds
- Read returns records in offset order
- Read beyond watermark fails appropriately
- Append with mismatched expected_offset fails
- Rollback discards uncommitted appends
  **Queue:**
- Enqueue/reserve/complete cycle succeeds
- Lease expiry returns message to ready queue
- Extend lease delays expiry
- Complete with wrong token fails
- Batch reserve returns up to specified count
  **RPC:**
- Single request/response cycle succeeds
- Streaming response reassembled in order
- Request timeout returns error
- Multiple workers on same route handle requests
  **KV:**
- Begin/put/commit cycle succeeds
- Begin/get on non-existent key handled correctly
- ReadOnly mode rejects write operations
- Two transactions on same resource conflict
- Scan returns lexicographically ordered pairs
  **Lease:**
- Acquire succeeds when free, fails when held
- Renew with valid token extends TTL
- Release with valid token releases lease
- Expired lease acquirable by new owner
  **Schedule:**
- Create schedule and verify execution
- Cancel prevents future runs
- List returns created schedules

### Interoperability Tests

Client implementations MUST pass these cross-cutting tests:
**Multi-Realm Isolation:**

- Create two clients with different JWT realms
- One client publishes to realm A, other subscribes in realm B
- Verify no cross-realm delivery (subscriber receives nothing)
  **Permission Enforcement:**
- Client with `kv:read` scope sends PUT request
- Broker returns ERR_UNAUTHORIZED (1001 domain error)
- Verify client surfaces error correctly to caller
  **Multiplexing Across Domains (Channel-Based):**
- Client sends KV PUT (KV channel)
- While KV is in-flight, client sends Notice PUBLISH (Notice channel)
- Both proceed concurrently (independent channels)
- Verify both responses received correctly
  **Reconnect State:**
- Client subscribes to pattern, closes connection
- Reconnects with same JWT, old subscription is lost
- Verify client must re-subscribe explicitly (no auto-recovery)
  **Fanout Scale:**
- Single PUBLISH to 1000 SUBSCRIBE clients
- All clients receive NOTIFY within 100ms (broker-dependent)
- Verify no message loss
  **Within-Domain Pipelining (NOT Supported):**
- Client sends KV REQUEST 1 without waiting for response
- Client sends KV REQUEST 2 on same channel while REQUEST 1 pending
- Broker MAY close connection or serialize; behavior undefined
- Clients MUST NOT pipeline multiple requests within a single domain/channel
  **RPC Multiplexing (Exception: Correlation ID Based):**
- Client sends RPC REQUEST with correlation_id_1
- Client sends another RPC REQUEST with correlation_id_2 (both in-flight)
- Broker matches responses by correlation_id
- Clients MAY truly multiplex RPC requests via correlation IDs

## Known Broker-Specific Behaviors

### Implementation Notes

These items are **not standardized** and may require broker-specific implementation notes.

#### Session IDs and State Tracking

**When broker tracks session state:**

- Notice subscriptions: Broker maintains per-session subscription list
- Stream sessions: Broker maintains per-session stream offset and metadata
- RPC workers: Broker maintains per-session worker registration
  **Session ID lifetime:**
- Issued on CONNECT, unique per connection
- Lost on disconnect (previous session ID becomes invalid)
- NOT returned to client in standard response (internal only, except where specified per domain)

#### Wire Protocol Philosophy (Explicit Routing)

**All Fitz operations include full routing context in every message:**
- KV: Every operation includes `[tx_id][route_len][route]` (not just BEGIN)
- Stream: Every operation includes `[session_id][route_len][route]` (not just BEGIN)
- Queue: Every operation includes `[route_len][route]`
- Notice: PUBLISH/SUBSCRIBE include full route/pattern
- RPC: REQUEST includes full route
- Lease: ACQUIRE/RENEW/RELEASE include full route
- Schedule: CREATE/CANCEL/LIST include full route

**Why this design:**
- Explicit routing: Each message is self-contained and fully addressable
- No hidden route defaults: Clients and brokers do not rely on per-connection realm/area/resource state
- Domain state still exists where required: KV and Stream keep live transaction/session state, and reconnect requires re-establishing that state
- Debuggable: Every message can be inspected without hidden addressing context

**Client convenience wrappers:**
- Client implementations MAY provide ergonomic wrapper objects (Transaction, Session, Subscription)
- These wrappers store route/session_id internally to hide repetition from users
- Example: `tx.put(key, value)` internally sends `[tx_id][route][key][value]` on wire
- But the wire protocol always remains fully explicit

**Session-scoped behavior:**
- Transactions (KV, Stream): Breaking connection triggers auto-rollback
- Subscriptions (Notice, RPC): Breaking connection drops all subscriptions
- Leases: In-memory only; lost on disconnect or broker restart

#### Serialization Formats (Domain-Specific)

- **Stream data:** Binary-safe; format broker-defined (client treats as opaque payload)
- **RPC response:** Binary-safe; serialization app-dependent
- **Lease tokens:** Opaque binary; do not parse or modify

#### Version Negotiation (Future)

No version negotiation in current protocol. If new verbs are added:

1. New verb codes use next available in range (e.g., 109 for KV)
2. Old clients reject unknown verbs with ERR_UNKNOWN_VERB (domain error)
3. Clients MUST gracefully handle unknown verbs (close connection or error)
   Recommended: Brokers should document supported verbs and wire codes in deployment docs.

### Broker-Specific Behaviors Summary

1. **Session ID exposure**: Notice/Stream payloads include session IDs, but no standard server-to-client notification mechanism yet
2. **KV routing**: KV payloads include route on every operation alongside `tx_id`; the broker still treats `tx_id` as a live session-scoped handle that becomes invalid after disconnect or restart
3. **Stream response data**: Response data is opaque; serialization format is broker-defined
4. **Verb code extensions**: New verbs added after current broker release use new wire codes in existing ranges
   Clients SHOULD consult broker documentation for domain-specific behavior.

## References

- Fitz repository: https://github.com/cntryl/fitz
- Domain specifications: See [Domains](#domains) section
- Codec implementations: See Fitz `src/protocol/` directory
- Integration tests: See Fitz `tests/` directory
