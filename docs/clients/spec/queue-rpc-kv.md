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

- **Route-Carrying Operations**: Every request includes the full queue route, but the broker still maintains live lease ownership in memory for the running process
- **At-Least-Once**: Messages delivered until completed; expired inflight reservations requeue them
- **FIFO-ish**: Generally delivered in enqueue order; leasing can cause out-of-order
- **Visibility Timeout**: Reserved messages are invisible to other consumers until expiry
- **Token Binding**: Complete/Extend require both message_id and inflight_token
- **Durability Split**: Queue data survives restart according to `FITZ_QUEUE_WRITE_POLICY`; the default fast policy can lose accepted recent mutations before the flush window closes. Live inflight reservations and inflight tokens do not survive restart.

##### Opaque Server-Generated IDs

**`message_id` and `inflight_token` are server-generated opaque `u64` values:**

- Clients MUST NOT generate, predict, or cache these values
- Clients MUST treat them as opaque cookies
- `message_id`: Unique identifier assigned at ENQUEUE time
- `lease_token`: Ephemeral fencing token generated at RESERVE time for one live lease instance only
- `lease_token` becomes invalid after COMPLETE, lease expiry, disconnect cleanup, or broker restart
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

**Purpose:** Low-latency request/response with session-derived caller routing and optional streaming.

#### Message Types

| Type | Name               | Direction       |
| ---: | ------------------ | --------------- |
|  300 | SUBSCRIBE_WORKER   | Client → Server |
|  301 | UNSUBSCRIBE_WORKER | Client → Server |
|  302 | REQUEST            | Client → Server (sync: client sends request, broker delivers to worker) |
|  303 | RESPONSE           | Server ↔ Client (sync or async: worker sends response(s) back to caller) |

Message type 304 is unsupported. Fitz has no RPC ACK frame.

#### SUBSCRIBE_WORKER Request

```
[u32 BE]  worker_route_len
[bytes]   worker_route
[u32 BE]  max_concurrent (must be 1..=1024)
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
[u32 BE]   body_len
[bytes]    body
Immediate error response from broker (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Note:** `correlation_id` is always exactly 16 bytes (UUID). No length prefix needed. Successful REQUEST submission produces no immediate broker success frame.

#### REQUEST Delivery (Server forwards to worker)

When the broker selects a worker for an incoming REQUEST, it delivers the same REQUEST frame (MessageType=302) to the worker's connection. The worker receives:

```
[16 bytes] correlation_id (UUID, from caller)
[u32 BE]   route_len
[bytes]    route (the target route)
[u32 BE]   body_len
[bytes]    body
```

The worker MUST use `correlation_id` when sending RESPONSE frames back. The broker forwards RESPONSE frames to the caller currently associated with that in-memory pending request.

#### RESPONSE (From worker to caller via broker)

```
[16 bytes] correlation_id (UUID, big-endian)
[u64 BE]   sequence
[u8]       flags (bit 0x01 means stream_end)
[u32 BE]   body_len
[bytes]    body
```

**Design Note:** `stream_end` is bit `0x01` in the RESPONSE (303) flags byte, not a separate user frame type. It indicates whether this RESPONSE frame is the last one for that correlation_id. The broker releases pending state and worker credit only after a valid terminal response.

#### Usage Example

**Recommended User-Facing API (WorkerSubscription Object):**

```python
# Client connects
client = FitzClient.connect_tcp("127.0.0.1:4091", jwt_token)

# SUBSCRIBE_WORKER returns a WorkerSubscription object
worker = client.rpc_subscribe_worker(
    worker_route="rpc://prod/app/compute/run",
    max_concurrent=32
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

- `SUBSCRIBE_WORKER`: `[worker_route_len][worker_route][max_concurrent]`
- `UNSUBSCRIBE_WORKER`: `[worker_route_len][worker_route]`
- `REQUEST`: `[16 bytes correlation_id][route_len][route][body_len][body]`
- `RESPONSE`: `[16 bytes correlation_id][sequence][flags][body_len][body]`

**Key Points:**

- User calls `worker.unsubscribe()` - simple, no route repetition
- Internally, client packs `[worker_route]` on wire
- `correlation_id` is fixed 16 bytes (UUID) - no length prefix
- Worker credit is explicit on subscribe through `max_concurrent`
- Pattern: Same as KV Transaction, Stream Session, and Notice Subscription objects

#### Semantics

- **Self-Contained Operations**: Every SUBSCRIBE/REQUEST includes full route information
- **Correlation**: UUID links a live in-flight request to its responses (client-generated)
- **Streaming**: Multi-frame responses have incrementing `sequence` and `stream_end` flag
- **Acceptance**: Successful REQUEST submission is silent; immediate failures return errors
- **Credit**: Worker capacity is bounded by `max_concurrent` and is released by a terminal response
- **Backpressure**: ERR_RPC_BACKPRESSURE if outbound queue full
- **Ordering**: Responses delivered in sequence order
- **Single-Worker Assignment**: Each accepted request is assigned to at most one live worker while tracked in memory

##### Worker Selection & Load Balancing

**Multiple workers on same route:**

- Server selects the first available worker credit before fanning out to another worker
- This is not least-connections or load-aware routing
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
|  109 | SUBSCRIBE    |
|  110 | UNSUBSCRIBE  |
|  111 | NOTIFY       |

#### BEGIN Request

```
[u32 BE]  route_len
[bytes]   route (UTF-8, e.g., "kv://realm/area/resource")
[u8]      mode (0=ReadOnly, 1=ReadWrite)
[u8]      durability (0=Buffered, 1=Sync; other values invalid)
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

#### SUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (success):
  [u8]     0 (status: success)
  [u64 BE] subscription_id
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

`route_pattern` MAY be an exact resource route like `kv://realm/area/resource` or a wildcard pattern like `kv://realm/area/*` or `kv://realm/*/*`.

#### UNSUBSCRIBE Request

```
[u32 BE]  route_pattern_len
[bytes]   route_pattern
Response (success):
  [u8]     0 (status: success)
Response (error):
  [u8]     1 (status: error)
  [u32 BE] error_len
  [bytes]  error_msg
```

#### NOTIFY Delivery

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route
[u64 BE]  mutation_count
```

`NOTIFY` is server-to-client only. The broker emits it after a successful `COMMIT` when the committed transaction changed at least one key in a watched resource.

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

Only `0` and `1` are valid durability values. Other values are rejected.

**Sync (durability=1):**

- Commits are flushed to durable storage (WAL fsync) before returning
- Survives broker crash/restart
- Higher latency, stronger crash durability at the configured storage layer

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

**Recommended User-Facing API (see [Recommended Client API Design](overview.md#recommended-client-api-design)):**

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
