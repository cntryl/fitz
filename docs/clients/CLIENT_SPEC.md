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
12. [Server-Side Architecture](#server-side-architecture-client-non-concerns)
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
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite)

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
3. **Stateless Operations**: Server doesn't track implicit state; each message is self-contained
4. **Reconnection Safety**: If connection drops mid-transaction, no server-side cleanup needed
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
with client.kv_begin("kv://prod/app/users", TxMode.ReadWrite) as tx:
    tx.put(b"key", b"value")
    tx.commit()

# BUT: Multiple transactions to different resources run in PARALLEL
tx_users = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite)
tx_posts = client.kv_begin("kv://prod/app/posts", TxMode.ReadWrite)
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
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite)
# Now client can receive notifications while KV transaction is in flight
# (they're on different channels)
```

**Rust (Drop trait, Parallel Transactions):**

```rust
// Multiple transactions to different resources in parallel
let mut tx_users = client.kv_begin("kv://prod/app/users", TxMode::ReadWrite)?;
let mut tx_posts = client.kv_begin("kv://prod/app/posts", TxMode::ReadWrite)?;

// Both can be active simultaneously
tx_users.put(b"key", b"value")?;
tx_posts.put(b"key", b"value")?;

tx_users.commit()?;  // Or auto-rollback in Drop
tx_posts.commit()?;
```

**Go (defer, Parallel Transactions):**

```go
// Multiple concurrent transactions to different resources
tx1, _ := client.KvBegin("kv://prod/app/users", TxModeReadWrite)
tx2, _ := client.KvBegin("kv://prod/app/posts", TxModeReadWrite)
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
const tx_users = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite);
const tx_posts = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite);

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
  "rpc://prod/app/worker",
  "rpc://prod/app/caller",
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
const kv_tx = client.kvBegin("kv://prod/app/data", TxMode.ReadWrite);
const queue_msg = client.queueEnqueue("queue://prod/app/tasks", payload);
const notice_sub = client.noticeSubscribe("notice://prod/app/*");
const rpc_call = client.rpcRequest("rpc://prod/app/svc", reply_route, correlation_id, payload);

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
const tx_users = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite);
const tx_posts = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite);

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
const tx = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite);
await tx.put(b"k1", b"v1");      // Request 1 → Response 1
await tx.put(b"k2", b"v2");      // Request 2 → Response 2 (after Request 1 completes)
await tx.commit();                // Request 3 → Response 3 (after Request 2 completes)
```

**❌ WRONG - Parallel calls on SAME transaction:**
```javascript
// DO NOT DO THIS - multiple parallel operations on same tx_id
const tx = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite);
await Promise.all([
  tx.put(b"k1", b"v1"),  // ❌ These would interleave incorrectly
  tx.put(b"k2", b"v2"),  // ❌ Same tx_id cannot have concurrent calls
]);
```

**✅ CORRECT - Multiple transactions in parallel:**
```javascript
// DO THIS INSTEAD - different transactions to different resources
const tx1 = client.kvBegin("kv://prod/app/users", TxMode.ReadWrite);
const tx2 = client.kvBegin("kv://prod/app/posts", TxMode.ReadWrite);
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

```javascript
// Multiple RPC calls in flight, responses matched by correlation_id
const rpc1 = client.rpcRequest("rpc://prod/app/svc", reply_route, uuid1, payload1);
const rpc2 = client.rpcRequest("rpc://prod/app/svc", reply_route, uuid2, payload2);

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

**Note on RouteFamily (Server-Side Only):**

`RouteFamily` is a **server-internal sharding concept** determined from the JWT during session authentication. Clients MUST NOT send, store, or manage RouteFamily values. The server extracts RouteFamily from JWT claims to partition resources across shards. This is transparent to clients — routes are opaque strings.
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

- **Type** (u16, big-endian):
  - If `type <= 0xFE`: single byte
  - If `type > 0xFE`: escape byte `0xFF` followed by 2-byte big-endian u16
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
- Wait 5–10 seconds after sending CONNECT before considering it failed
- If no close frame within 5 seconds, assume connection is ready
- If connection closes immediately, treat as authentication failure
- Handle immediate connection close (treat as auth failure, do NOT retry same JWT)
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
- Queued notifications are discarded
  **State NOT Restored On Reconnect:**
  On reconnect with new CONNECT:
- New session ID issued (previous session ID is invalid)
- Previous subscriptions, transactions, and worker registrations are NOT recovered
- Client MUST explicitly re-subscribe, re-begin, or re-register if needed

### 4. Send Domain Requests

After successful CONNECT, client may send domain-specific requests.

**Channel-Based Multiplexing:**

- **Clients MAY send multiple in-flight requests on different channels (domains).** Each domain (KV, RPC, Notice, etc.) is routed to its own logical channel by the broker. This allows concurrent operations across different domains on the same connection.
- **Within a single domain**: Follow request/response sequencing unless the domain explicitly supports per-request correlation IDs (currently only RPC). Sending multiple requests of the same type without waiting for responses is undefined behavior.
- **RPC domain is special**: RPC REQUEST includes an explicit 16-byte UUID `correlation_id` that clients generate. This allows true request/response matching for multiple in-flight RPC requests.
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
- Reconnect if resubscription or state restoration is needed

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
- **Server backpressure:** Brokers MAY signal backpressure via rate-limit or backpressure error codes (or an explicit backpressure frame). Clients MUST respect such signals and apply backoff and queue-management strategies.

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

Fitz follows an **HTTP-like model** where every operation is self-contained and stateless on the server side:

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

2. **Operations are self-contained** (like HTTP statelessness)
   - Server doesn't track implicit context beyond session auth
   - Each message has full addressing information
   - Connection loss doesn't leave orphaned server state

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

- Debuggable: every message is complete, can be logged/replayed
- Reconnect-safe: operations don't depend on connection history
- Scalable: stateless server processing enables horizontal scaling

### Comparison

| Aspect         | HTTP                            | Fitz                                   |
| -------------- | ------------------------------- | -------------------------------------- |
| **Addressing** | URL path                        | Route (kv://realm/area/resource)       |
| **Verb**       | GET, POST, PUT, DELETE          | MessageType (100=BEGIN, 104=PUT, etc.) |
| **Transport**  | TCP + TLS                       | WebSocket or TCP + TLS                 |
| **Format**     | Text (headers + body)           | Binary (TLV)                           |
| **State**      | Stateless (cookies for session) | Stateless (JWT for session auth)       |
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
- **RPC:** Wildcards in SUBSCRIBE_WORKER patterns (`rpc://realm/area/*`)
- **Stream:** Wildcards in READ patterns (check Stream domain spec for details)
- **Queue:** Wildcards in RESERVE patterns (check Queue domain spec for details)

**Domains requiring concrete routes only (no wildcards):**
- **KV:** All operations use concrete routes only (`kv://realm/area/resource`)
- **Lease:** All operations use concrete routes only (`lease://realm/area/resource`)
- **Schedule:** All operations use concrete routes only (`schedule://realm/area/resource`)

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
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ---------- | ----------------------------------------------- |
  | `LIST` | `{realm}/{area}`, `{realm}/*/*` |
  | `ENQUEUE` | `{realm}/{area}/{resource}` |
  | `RESERVE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
  | `COMPLETE` | `{realm}/{area}/{resource}` |
  | `EXTEND` | `{realm}/{area}/{resource}` |
  **Note:** `LIST`, `CREATE`, `DELETE` (admin), and `SEND`/`RECEIVE`/`RELEASE` operations are either broker-internal or legacy verbs. Clients should use: ENQUEUE, RESERVE, COMPLETE, EXTEND as documented in the wire format section.source`|
|`RELEASE`|`{realm}/{area}/{resource}`|
|`EXTEND`|`{realm}/{area}/{resource}` |

### Schedule Domain

**Valid Route Shapes:**

- `schedule://{realm}/{area}`
- `schedule://{realm}/{area}/{resource}`
- `schedule://{realm}/{area}/*`
  **Method Acceptance:**
  | Method | Accepted Route Sh/{resource}`|
|`CANCEL`|`{realm}/{area}/{resource}`|
**Note:**`DELETE`(admin) and`TRIGGER`operations are broker-internal. Clients should use: CREATE, CANCEL, LIST as documented in the wire format section. LIST is fully documented with streaming protocol.
|`PUT`|`{realm}/{area}/{resource}`|
|`DELETE`(data) |`{realm}/{area}/{resource}`|
|`TRIGGER`|`{realm}/{area}/{resource}` |

### Lease Domain

**Valid Route Shapes:**

- `lease://{realm}/{area}/{resource}`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | --------- | --------------------------- |
  | `ACQUIRE` | `{realm}/{area}/{resource}` |
  | `RENEW` | `{realm}/{area}/{resource}` |
  | `RELEASE` | `{realm}/{area}/{resource}` |

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

**Valid Route Shapes:**

- `rpc://{realm}/{area}/{resource}`
- `rpc://{realm}/{area}/*`
  **Method Acceptance:**
  | Method | Accepted Route Shapes |
  | ------------- | ----------------------------------------------- |
  | `CALL` | `{realm}/{area}/{resource}` |
  | `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
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
| Queue    | ENQUEUE_BATCH      |       201 | Data          | Batch add messages      |
| Queue    | RESERVE            |       202 | Data          | Lease message(s)        |
| Queue    | EXTEND             |       203 | Data          | Extend lease            |
| Queue    | COMPLETE           |       204 | Data          | Mark complete           |
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
| Schedule | CREATE             |       700 | Data          | Create schedule         |
| Schedule | CANCEL             |       701 | Data          | Cancel schedule         |
| Schedule | LIST               |       702 | Data          | List schedules          |

### MessageType Ranges Are Non-Overlapping

Each domain occupies an exclusive 100-code block. The broker's mux layer routes by numeric range — **no overlap, no disambiguation needed**.
**Clients MUST use the wire codes from the Constants & TLV Registry section.**

## Server-Side Architecture (Client Non-Concerns)

This section documents broker-internal mechanisms that clients **MUST NOT** implement or interact with. These details are provided for completeness and to explain why certain client design decisions were made.

### RouteFamily (Sharding Key)

**What It Is:**

`RouteFamily` is a **server-internal numeric identifier (u64)** used for resource partitioning and sharding across broker instances or threads. It determines which shard handles operations for a given route.

**How Server Determines RouteFamily:**

The broker extracts `RouteFamily` from the **JWT during session authentication**:

1. Client sends CONNECT frame with JWT
2. Broker parses JWT claims: `tenant_id`, `org_id`, `env`, etc.
3. Broker calls control plane lookup: `RouteFamily::from_jwt(jwt)` (see `src/session/tenant.rs`)
4. Resulting `RouteFamily` value is **stored in session state** and used for all subsequent operations
5. Current implementation returns `RouteFamily::new(0)` as a stub (all realms map to family 0 until multi-tenant control plane is integrated)

**Why Clients Don't Send RouteFamily:**

- **Separation of Concerns**: RouteFamily is a server implementation detail for sharding and load distribution
- **Security**: Allowing clients to specify RouteFamily would enable cross-tenant access and sharding bypass
- **Simplicity**: Clients treat routes as opaque strings; server handles all partitioning logic
- **Future-Proof**: Server can change sharding strategy without breaking client protocol

**Client Requirements:**

- Clients MUST NOT send `family_id` in any wire protocol message
- Clients MUST NOT store or track RouteFamily values
- Clients MUST treat all routes as opaque strings (no parsing realm/area to derive family)
- Server determines RouteFamily from session JWT; clients have **zero visibility** into this value

**Protocol Evolution:**

In earlier protocol iterations, some domains required `[u64 BE] family_id` in wire formats. This was removed to enforce proper separation: clients send routes, servers extract RouteFamily from session. The current specification reflects this corrected design.

**Implementation Reference:**

See Fitz server code:

- `src/session/tenant.rs:45-132` - RouteFamily extraction from JWT
- `src/session/session.rs:97` - SessionInfo stores RouteFamily
- `src/session/manager.rs:651` - Server passes RouteFamily to codec parsers (clients never send it)

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
let tx_id = client.begin(KvBeginRequest { route, mode })?;
client.put(KvPutRequest { tx_id, key, value })?;
client.get(KvGetRequest { tx_id, key })?;
client.commit(KvCommitRequest { tx_id })?;

// ✅ CORRECT - multiple concurrent transactions to different resources
let tx1 = client.begin(KvBeginRequest { route: "kv://prod/app/users", mode })?;
let tx2 = client.begin(KvBeginRequest { route: "kv://prod/app/posts", mode })?;
// Both tx1 and tx2 active simultaneously
client.put(KvPutRequest { tx_id: tx1, key, value })?;
client.put(KvPutRequest { tx_id: tx2, key, value })?;
client.commit(KvCommitRequest { tx_id: tx1 })?;
client.commit(KvCommitRequest { tx_id: tx2 })?;

// ❌ WRONG - parallel operations on SAME transaction
let tx = client.begin(KvBeginRequest { route, mode })?;
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
kv_tx = client.kv_begin(route, mode)  # Blocks on KV channel
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
  - Use application-level idempotency tokens if needed (e.g., RPC correlation_id)

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
- RPC: `REQUEST` (safe to retry if broker caches correlation_id)
  Retry behavior: Clients MUST maintain deduplication state (correlation ID → result cache) to safely retry.

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
Response (status=0 success):
  [u8]     0
  [u8]     has_subscription_id (0 or 1)
  [u64 BE] subscription_id (if has_subscription_id=1)
Response (status=1 error):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### SUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
Response (status=0):
  [u8]     0
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

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

- **Client-Side Multiplexing**: Server tracks one subscription per `(session, pattern)`. Client tracks multiple handlers per `subscription_id`. Server sends one NOTIFY per pattern match; client demuxes to all local handlers.
- **Idempotent SUBSCRIBE**: Duplicate SUBSCRIBE to same pattern returns same `subscription_id` (no duplicate server subscription created)
- **Delivery**: Best-effort; under backpressure, notifications may be dropped
- **Ordering**: Delivered in publish order per subscription
- **Fanout**: Single publish reaches all matching subscriptions
- **Session-Scoped**: Subscriptions tied to connection; lost on disconnect
- **Acknowledgements & Retries**: `NOTIFY` frames are never acknowledged by clients and are never retried by the broker. Clients MUST NOT send acknowledgements for `NOTIFY` frames and MUST NOT expect guaranteed replay.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage Guidance:** `NOTICE` is a **best-effort, non-durable** mechanism. **Clients MUST NOT use Notices for workflows that require acknowledgement, durability, or guaranteed delivery. Use RPC or Queue for guaranteed delivery or acknowledgement-based workflows.**

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

| Type | Name         |
| ---: | ------------ |
|  600 | BEGIN        |
|  601 | APPEND       |
|  602 | COMMIT       |
|  603 | ROLLBACK     |
|  604 | READ         |
|  605 | LAST         |
|  606 | GET_METADATA |

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
  [bytes]  data
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

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

#### Acceptance Tests

- begin/append/commit cycle
- read returns records in offset order
- read beyond watermark fails
- append with mismatched expected_offset fails
- rollback discards uncommitted appends
- multiple sessions can read concurrently

### Queue Domain (Durable At-Least-Once Delivery)

**Purpose:** FIFO-ish message queues with leasing and visibility timeouts.

#### Message Types

| Type | Name     |
| ---: | -------- |
|  200 | ENQUEUE  |
|  202 | RESERVE  |
|  203 | EXTEND   |
|  204 | COMPLETE |

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
|  302 | REQUEST            | Client → Server |
|  303 | RESPONSE           | Server ↔ Client |
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
    worker_route="rpc://prod/app/compute"
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
    route="rpc://prod/app/compute",
    reply_route="rpc://prod/app/caller",
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
- **Correlation**: UUID links request to responses (client-generated)
- **Streaming**: Multi-frame responses have incrementing `sequence` and `stream_end` flag
- **Backpressure**: ERR_RPC_BACKPRESSURE if outbound queue full
- **Ordering**: Responses delivered in sequence order
- **Exactly-Once**: Each request reaches worker once

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
- response with wrong correlation_id rejected
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
[u8]      durability (0=Sync, 1=Buffered)
Response (success):
  [u64 BE] tx_id
Response (error):
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
Response: (empty on success, error on failure)
```

#### GET Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response (success):
  [u8]     found (0=not_found, 1=found)
  [u32 BE] value_len
  [bytes]  value (empty if not found)
Response (error):
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
Response: (empty on success)
```

#### DELETE Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  key_len
[bytes]   key
Response: (empty on success)
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
Response: (empty on success)
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
Response:
  [u32 BE] item_count
  [repeat]
    [u32 BE] key_len
    [bytes]  key
    [u32 BE] value_len
    [bytes]  value
  [u8]     has_more (0 or 1)
```

#### COMMIT Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response: (empty on success)
```

#### ROLLBACK Request

```
[u64 BE]  tx_id
[u32 BE]  route_len
[bytes]   route
Response: (empty on success)
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
# Connect with JWT (server extracts RouteFamily from JWT)
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# Begin transaction - returns Transaction object
# Route is full URI: kv://realm/area/resource
tx = client.kv_begin("kv://prod/app/users", TxMode.ReadWrite, Durability.Sync)

# Transaction methods focus on data, hide route repetition
tx.put(b"user:123", b"alice")
value = tx.get(b"user:123")
tx.commit()

# Context manager pattern (Python)
with client.kv_begin("kv://prod/app/users", TxMode.ReadWrite) as tx:
    tx.put(b"key", b"value")
    tx.commit()  # Or auto-commit on __exit__
```

**Wire Protocol (what actually happens under the hood):**

Every transaction operation sends **both tx_id AND route** on the wire:

- `PUT`: `[tx_id][route_len][route][key_len][key][value_len][value]`
- `GET`: `[tx_id][route_len][route][key_len][key]`
- `COMMIT`: `[tx_id][route_len][route]`

The Transaction object stores the route internally and includes it in every wire message, making operations self-contained and stateless on the server side.

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

### Lease Domain (Ephemeral Coordination)

**Purpose:** In-memory exclusive leases for distributed locking and coordination.

#### Message Types

| Type | Name    |
| ---: | ------- |
|  400 | ACQUIRE |
|  401 | RENEW   |
|  402 | RELEASE |
|  403 | QUERY   |

#### ACQUIRE Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  ttl_secs
Response (status=0):
  [u8]     0
  [u64 BE] fencing_token
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**

- `fencing_token` is server-generated opaque `u64` value
- Clients MUST treat as cookie (no prediction or caching)
- Used for preventing stale releases

#### RENEW Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token
[u64 BE]  ttl_secs
Response (status=0):
  [u8]     0
  [u64 BE] new_fencing_token
Response (status=1):
  [u8]     1
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
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### QUERY Request

```
[u32 BE]  route_len
[bytes]   route
Response (status=0, lease free):
  [u8]     0
  [u8]     0 (has_holder=false)
Response (status=0, lease held):
  [u8]     0
  [u8]     1 (has_holder=true)
  [u32 BE] owner_id_len
  [bytes]  owner_id
  [u64 BE] ttl_remaining_secs
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### Usage Example

```python
# Acquire lease
token = client.lease_acquire(
    route="lease://prod/app/leader",
    owner_id="node-1",
    ttl_secs=30
)
if token:
    try:
        # Do work as leader
        perform_leader_duties()
        # Renew before expiry
        client.lease_renew(
            route="lease://prod/app/leader",
            owner_id="node-1",
            fencing_token=token,
            ttl_secs=30
        )
    finally:
        # Release when done
        client.lease_release(
            route="lease://prod/app/leader",
            owner_id="node-1",
            fencing_token=token
        )
else:
    print("Lease held by another owner")
```

#### Semantics

- **Self-Contained Operations**: Every ACQUIRE/RENEW/RELEASE includes full route
- **Mutual Exclusion**: Only one owner holds a lease at a time
- **TTL-based Expiry**: Expired leases automatically released; query returns free
- **Route Partitioned**: Different routes have independent leases
- **In-Memory**: Lost on broker restart (use for coordination, not durability)

##### Fencing Token

**`fencing_token` is server-generated opaque `u64` value:**
- Prevents stale commands from releasing new holder's lease
- Clients MUST treat as cookie (no prediction or caching)
- Generated at ACQUIRE time, changes on each RENEW
- Sending wrong `fencing_token`: ERR_INVALID_FENCE
- Use case: Prevents "zombie" lease holder from releasing lease after it expired and was re-acquired by another owner

#### Error Codes (5xxx)

- 4001 = ERR_LEASE_HELD
- 4002 = ERR_INVALID_FENCE
- 4003 = ERR_LEASE_EXPIRED
- 4004 = ERR_LEASE_NOT_FOUND

#### Acceptance Tests

- acquire succeeds when free, fails when held
- renew with valid token extends TTL
- renew with invalid token fails
- release with valid token releases
- release with invalid token fails
- expired lease acquirable by new owner

### Schedule Domain (Delayed/Recurring Tasks)

**Purpose:** Durable scheduling of delayed tasks and recurring jobs.

#### Message Types

| Type | Name   |
| ---: | ------ |
|  700 | CREATE |
|  701 | CANCEL |
|  702 | LIST   |

#### CREATE Request

```
[u32 BE]  route_len
[bytes]   route (e.g., "schedule://realm/area/resource")
[u32 BE]  cron_len
[bytes]   cron (UTF-8 cron expression)
[u32 BE]  target_resource_len
[bytes]   target_resource
[u32 BE]  target_operation_len
[bytes]   target_operation
Response (status=0):
  [u8]     0
  [u8]     has_schedule_id
  [u32 BE] schedule_id_len (if present)
  [bytes]  schedule_id
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### CANCEL Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  schedule_id_len
[bytes]   schedule_id
Response: status + optional schedule_id
```

#### LIST Request

```
[u32 BE]  route_len
[bytes]   route
Response 1..N (streaming):
  [u8]     0 (status)
  [u8]     1 (has_schedule_id=true)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id
  [u32 BE] cron_len
  [bytes]  cron
  [u32 BE] target_resource_len
  [bytes]  target_resource
  [u32 BE] target_operation_len
  [bytes]  target_operation
Response N+1 (final, end-of-stream):
  [u8]     0 (status)
  [u8]     0 (has_schedule_id=false, no more schedules)
Response (error):
  [u8]     1 (status=error)
  [u32 BE] error_len
  [bytes]  error_msg
```

**Streaming Protocol:**
- Client continues reading until response with `has_schedule_id=0`
- Empty result set: Single response with `status=0, has_schedule_id=0`
- Non-empty result: N responses with schedules, then final response with `has_schedule_id=0`
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
  [u8]     1 (has_schedule_id)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id
  [schedule data...]
Response 2:
  [u8]     0 (status)
  [u8]     1 (has_schedule_id)
  [u32 BE] schedule_id_len
  [bytes]  schedule_id
  [schedule data...]
Response N (final):
  [u8]     0 (status)
  [u8]     0 (has_schedule_id = empty, no more)
```

Client MUST continue reading until `has_schedule_id=0`.

#### Usage Example

```python
# Create schedule
schedule_id = client.schedule_create(
    route="schedule://prod/app/reminders",
    cron="0 9 * * 1",  # Every Monday at 9 AM
    target_resource="notice://prod/app/notifications",
    target_operation="PUBLISH"
)

# List schedules
schedules = client.schedule_list(
    route="schedule://prod/app/reminders"
)

# Cancel schedule
client.schedule_cancel(
    route="schedule://prod/app/reminders",
    schedule_id=schedule_id
)
```

**Wire Protocol:**

Every operation includes route:

- `CREATE`: `[route_len][route][cron_len][cron][target_resource_len][target_resource][target_operation_len][target_operation]`
- `LIST`: `[route_len][route]`
- `CANCEL`: `[route_len][route][schedule_id_len][schedule_id]`

#### Semantics

- **Self-Contained Operations**: Every request includes route for stateless processing
- **Durability**: Schedules persist across broker restarts
- **Strict Timing**: Tasks execute at designated times (best-effort)
- **Recurring**: Interval-based recurring tasks (cron-like)
- **Cancellation**: Cancels future runs; already-running tasks may not abort
- **Route Scoped**: Independent schedules per route

#### Error Codes (7xxx)

- 7001 = ERR_SCHEDULE_NOT_FOUND
- 7002 = ERR_INVALID_CRON
- 7003 = ERR_SCHEDULE_LIMIT
- 7004 = ERR_PARSE_ERROR
- 7005 = ERR_INVALID_TARGET

#### Acceptance Tests

- create_once schedules task and executes at delay
- create_recurring executes at intervals
- cancel prevents future runs
- list returns created schedules
- schedule persists across restart

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
**Queue Domain (200–204):**
| Value | Name |
|---:|---|
| 200 | ENQUEUE |
| 201 | ENQUEUE_BATCH |
| 202 | RESERVE |
| 203 | EXTEND |
| 204 | COMPLETE |
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
**Stream Domain (600–606):**
| Value | Name |
|---:|---|
| 600 | BEGIN |
| 601 | APPEND |
| 602 | COMMIT |
| 603 | ROLLBACK |
| 604 | READ |
| 605 | LAST |
| 606 | GET_METADATA |
**Schedule Domain (700–702):**
| Value | Name |
|---:|---|
| 700 | CREATE |
| 701 | CANCEL |
| 702 | LIST |

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

- `type 0x00..0xFE`: single byte
- `type 0xFF`: escape marker for types > 0xFE
  - Followed by `u16 BE` for actual type

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

#### Wire Protocol Philosophy (Stateless Operations)

**All Fitz operations include full routing context in every message:**
- KV: Every operation includes `[tx_id][route_len][route]` (not just BEGIN)
- Stream: Every operation includes `[session_id][route_len][route]` (not just BEGIN)
- Queue: Every operation includes `[route_len][route]`
- Notice: PUBLISH/SUBSCRIBE include full route/pattern
- RPC: REQUEST includes full route
- Lease: ACQUIRE/RENEW/RELEASE include full route
- Schedule: CREATE/CANCEL/LIST include full route

**Why this design:**
- HTTP-like statelessness: Each message is self-contained and fully addressable
- Reconnect-safe: No server-side implicit state (beyond session auth)
- Debuggable: Every message can be logged/replayed without context
- Scalable: Stateless processing enables horizontal scaling

**Client convenience wrappers:**
- Client implementations MAY provide ergonomic wrapper objects (Transaction, Session, Subscription)
- These wrappers store route/session_id internally to hide repetition from users
- Example: `tx.put(key, value)` internally sends `[tx_id][route][key][value]` on wire
- But the wire protocol always remains fully explicit

**Session-scoped behavior:**
- Transactions (KV, Stream): Breaking connection triggers auto-rollback
- Subscriptions (Notice, RPC): Breaking connection drops all subscriptions
- Leases: In-memory only, survive until TTL expiry (not connection-bound)

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
2. **KV/Queue routing**: KV/Queue payloads do not include route; broker derives from envelope/connection context
3. **Stream response data**: Response data is opaque; serialization format is broker-defined
4. **Verb code extensions**: New verbs added after current broker release use new wire codes in existing ranges
   Clients SHOULD consult broker documentation for domain-specific behavior.

## References

- Fitz repository: https://github.com/cntryl/fitz
- Domain specifications: See [Domains](#domains) section
- Codec implementations: See Fitz `src/protocol/` directory
- Integration tests: See Fitz `tests/` directory
