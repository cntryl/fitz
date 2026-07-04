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
- Messages stay self-contained on the wire, but several domains still maintain live broker-local state (for example KV transactions, Queue inflight entries, subscriptions, and pending RPC work)
- Reconnect creates a new live session; clients MUST re-establish any required session-owned state and MUST NOT assume it survives disconnect

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
const rpc_call = client.rpcRequest("rpc://prod/app/config/get", correlation_id, payload);

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
const rpc1 = client.rpcRequest("rpc://prod/app/config/get", uuid1, payload1);
const rpc2 = client.rpcRequest("rpc://prod/app/config/get", uuid2, payload2);

// Responses arrive in any order and are matched by correlation_id
const [resp1, resp2] = await Promise.all([rpc1, rpc2]);
```

### Backpressure, Concurrency Budget & Flow Control

Clients MUST implement backpressure and a bounded concurrency budget:

1. **Per-connection concurrency ceiling**: Clients MUST expose a configurable maximum in-flight work limit per connection. The limit applies to all admitted outbound work on that connection.
2. **Bounded admission**: When the ceiling is reached, the client MUST queue, block, or surface a retryable backpressure error before admitting more work. The client MUST NOT spawn unbounded goroutines, tasks, or promises to absorb load.
3. **Per-channel backpressure**: If a channel queue fills (typical limit: 1000 messages), retry with exponential backoff before sending the next request.
4. **Graceful degradation**: If the server rejects with backpressure error (429-like), pause and retry after backoff instead of flooding the broker.

```python
# Bound concurrent operations per connection with a semaphore or equivalent
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
| **realm** | Opaque, application-defined isolation boundary for resources within a broker. A realm may represent a tenant, department, cost center, user, environment, or another developer-chosen partition. | `tenant`, `organization`, `department`, `cost_center`, `user` |
| **area** | Namespace within a realm | `namespace`, `collection` |
| **resource** | Named entity within an area (e.g., table, queue, stream) | — |
| **route** | URI-like string addressing a resource or operation | `endpoint`, `path`, `key` |
| **verb** | Operation name (e.g., `GET`, `PUT`, `PUBLISH`) | `operation`, `method` (ambiguous) |
| **domain** | Service category (kv, queue, notice, stream, rpc, lease, schedule) | — |

**Internal routing rule:** Clients send `CONNECT(jwt)` and opaque route strings only. Broker-internal partitioning, shard placement, and other session routing state are not part of the client contract.

**Realm vs RouteFamily:** `realm` is the application-visible namespace label carried in routes and permissions. RouteFamily is a separate broker-internal routing key resolved server-side from verified identity context. They are orthogonal and must never be inferred or defaulted from one another.

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

