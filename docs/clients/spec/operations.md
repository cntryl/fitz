## Domain Operations Reference (Canonical Standard)

This section documents the **canonical operations** for each of the seven Fitz domains. These operations define the complete API surface for each domain and MUST be implemented by all conformant clients and brokers.

**Design Principle:** Each domain has a focused, minimal set of operations.
Operations are explicitly addressed and avoid implicit routing state, though
domains may still keep live server-side state where their contract requires it.

### Registration Contract

KV, Queue, Notice, Stream, RPC, and Schedule registrations accept exact routes
and strict whole-segment `*` or `**` patterns, including wildcard realms. `*`
matches one segment and `**` matches zero or more segments. The scheme must
match the domain, segments must be non-empty, partial wildcard tokens are
invalid, and structured-domain patterns must be capable of matching their
concrete route depth (three segments for KV, Queue, and Stream; four for
Schedule). Notice and RPC have flexible depth.

Each wildcard-capable domain permits 128 wildcard registrations per session.
Exact registrations do not count. Duplicate `(session, original registration
string)` requests are idempotent and checked before the limit. Matching never
crosses `RouteFamily`; overlaps remain independent and exact registrations have
no precedence. Notifications include the matching subscription identifier and
the exact concrete route.

Lease is intentionally different: every Lease operation, SUBSCRIBE, and
UNSUBSCRIBE requires an exact `lease://realm/area/resource` route. Lease rejects
all wildcards and has no wildcard quota.

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
| `Subscribe` | 109 | C→S | Watch committed changes |
| `Unsubscribe` | 110 | C→S | Stop watching changes |
| `Notify` | 111 | S→C | Deliver committed change |

**Constraints:**
- All data operations MUST be within a transaction (explicit BEGIN required)
- Operations within a single transaction MUST be sequential (no parallel calls with same tx_id)
- Multiple transactions to different resources MAY be parallel
- Watches are session-scoped and MUST be re-established after reconnect
- Watches accept exact routes or patterns capable of matching a three-segment KV route
- `Notify` is emitted only after a successful `Commit` that applied one or more mutations

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
| `Subscribe` | 207 | C→S | Watch queue for availability |
| `Unsubscribe` | 208 | C→S | Stop watching |
| `Notify` | 209 | S→C | Deliver queue availability |

**Constraints:**
- Messages are reserved with an inflight visibility window (not immediately deleted)
- Queue inflight token MUST match to complete or extend
- FIFO ordering preserved within single reserve call
- Duplicate reserves may violate FIFO (wait or use single-call pattern)
- ENQUEUE, EXTEND, and COMPLETE use exact three-segment Queue routes
- RESERVE accepts either an exact route or a whole-segment pattern capable of matching that shape
- Exact RESERVE items retain the route-less legacy encoding; wildcard RESERVE items carry their matched concrete route

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
| `Subscribe` | 407 | C→S | Watch lease changes |
| `Unsubscribe` | 408 | C→S | Stop watching |
| `Notify` | 409 | S→C | Deliver lease change event |

**Constraints:**
- Token MUST match to extend or release (prevents cross-holder mutations)
- Expiry is lazy (expires when next operation touches resource)
- Watches are session-scoped and are removed automatically on disconnect
- Watches require exact three-segment `lease://` routes and reject every wildcard
- `Notify` is a best-effort hint that lease state changed; clients still use `Query` or `Acquire` for authoritative state transitions
- Disconnect cleanup and broker restart both clear lease ownership; clients MUST reacquire if they still need exclusivity
- Atomic compare-and-swap via token (no blindupdate)

---

### Notice Domain (Live Fanout)

**Purpose:** Best-effort live notifications.

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
- Subscriptions accept exact routes and flexible-depth whole-segment `*`/`**` patterns
- Delivery is best-effort (may drop under backpressure)
- Session-scoped (lost on disconnect)
- Client-side multiplexing: one server subscription per pattern, multiple handlers per subscription_id

---

### RPC Domain (Request/Response)

**Purpose:** Synchronous request/response with session-derived caller routing and optional streaming.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Subscribe` | 300 | C→S | Register as worker |
| `Unsubscribe` | 301 | C→S | Deregister worker |
| `Request` | 302 | C→S and S→Worker | Send RPC request and deliver it to a worker |
| `Response` | 303 | Worker→S and S→Caller | Send RPC response |

**Constraints:**
- Each request uses 16-byte UUID `correlation_id` for matching the live in-flight response
- Multiple RPC requests MAY be in flight simultaneously (true multiplexing via correlation_id)
- Workers register exact routes or whole-segment `*`/`**` patterns with explicit
  `max_concurrent` credit shared across every matching concrete route, and receive
  REQUEST frames as async pushes
- The broker derives caller response routing from the source session; callers do not send a reply route
- Successful REQUEST submission is silent; callers wait for RESPONSE or an error frame
- Message type 304 is unsupported and MUST NOT be sent
- Worker registrations and pending requests are process-local and are not recovered or replayed after broker restart
- A worker reconnecting after disconnect or restart MUST send `Subscribe` again before it can receive new requests

---

### Stream Domain (Log/Event Stream)

**Purpose:** Durable append-only log with commit-time resource, area, and realm ordering.

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
- Records are strictly ordered by offset within each resource
- APPEND uses optimistic concurrency (`expected_offset`)
- COMMIT order defines the durable area and realm order across resources
- Offset-based reads only; consumer cursors remain client-managed
- Watermark tracks committed visible data; reads past it return an empty success
- READ accepts its documented exact/area/realm patterns; live subscriptions
  accept strict patterns capable of matching a three-segment Stream route

---

### Schedule Domain (Cron Scheduler)

**Purpose:** Durable one-time or recurring schedule definitions with best-effort live fire notifications.

**Canonical Operations:**

| Operation | Wire Code | Direction | Purpose |
| --------- | --------: | --------- | ------- |
| `Create` | 700 | C→S | Create/update schedule |
| `Cancel` | 701 | C→S | Delete schedule |
| `List` | 702 | C→S | List schedules |
| `Subscribe` | 703 | C→S | Watch for schedule fires |
| `Unsubscribe` | 704 | C→S | Stop watching |
| `Notify` | 705 | S→C | Fire notification |

The broker manifest also exposes additive `CreateBatch` (706) and `ListV2`
(707) extensions. They are not substitutes for the canonical cross-client
operations above; in particular, portable clients use offset/limit `List` 702.

**Constraints:**
- Cron-style scheduling (precise timing, best-effort delivery)
- Exact and whole-segment `*`/`**` registration patterns are session-scoped and
  limited to 128 wildcard registrations per session
- NOTIFY carries `[subscription_id][exact_route][payload]` and is best-effort
  (may be dropped under backpressure)
- No matching registration or an entirely rejected handoff still advances the
  occurrence

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

- Broker MUST extract identity context and normalized permissions from JWT:
  configured route-family identity claim plus one explicit permission source in this order:
  configured custom permissions claim, top-level `permissions`, configured role claim array, `scp`, then `scope`
- For each request, broker MUST check the request route and access level against compiled route-shaped permission patterns
- If any check fails, broker returns permission error (domain-specific error code)
  **Anonymous Mode (`FITZ_AUTH_REQUIRED=false`):**
- Broker assigns default permissions (typically unrestricted access)
- No JWT validation or permission checks
- Broker always uses internal route family `1`
- All routes and verbs allowed
- Used for development/testing or trusted internal networks
  **Permission Check Order (Authenticated Mode):**
  Broker MUST enforce permissions in this order:

1. **Route validation:** Scheme known, depth valid, shape matches method (if fails: protocol error)
2. **JWT validation:** Signature valid, not expired, and identity context resolves to a provisioned route family (if fails: transport error)
3. **Permission enforcement:** Route-shaped permission match grants requested access (if fails: domain error with code ERR_UNAUTHORIZED)
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

### JWT Claim Contract

Authenticated deployments require standard JWT envelope claims plus:

- one configured identity claim used for route-family resolution
- one supported permission source

Example token:

```json
{
  "iss": "https://issuer.example.com/",
  "aud": ["https://fitz.example.com/api"],
  "sub": "user-1",
  "exp": 1234567890,
  "org_id": "org_acme",
  "permissions": ["notice://prod/orders/**#read"]
}
```

The broker resolves route family server-side from the configured identity claim
and `FITZ_ROUTE_FAMILY_MAP`. JWTs do not carry Fitz `route_family`, `realm`,
`areas`, or legacy `scopes` claims.

First-class claim profiles:

- organization identity: `org_id` plus top-level `permissions`
- tenant identity: `tid` plus `scp`
- app role identity: `tid` plus `roles`, where each role string is already a Fitz permission or recognized coarse scope
- subject identity: `sub` plus `scope`; resource-server prefixes like `api/notice.read` are accepted
- exact custom or namespaced identity claim plus `scope`, a configured custom permissions claim, or a configured role claim array

Permission strings must be either route-shaped Fitz permissions such as
`notice://prod/orders/**#read` or recognized coarse scopes such as
`notice.read`. Clients should treat the compact JWT as opaque input and let the
broker enforce the contract.

For an organization-identity setup, use an API access token for the Fitz API
identifier, emit the organization id claim, include route-shaped permissions,
map organization ids with `FITZ_ROUTE_FAMILY_MAP`, and validate the issuer
through `FITZ_JWT_JWKS_MAP`. Configured JWKS URLs must be absolute HTTPS URLs
without credentials or fragments.

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

Clients MAY surface provider-owned token metadata that they already possess from
their auth layer for diagnostics only, but Fitz authorization decisions still
belong exclusively to the broker.

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
- Request → no success ACK on accepted submission
- Worker responses → RESPONSE frame(s), matched by correlation_id
- Multiple RPC responses matched by correlation_id on same connection
  **Stream READ:**
- Request → one response page containing zero or more route-prefixed items and
  one cursor
- Each event, filtered offset, and filtered range includes its exact concrete
  three-segment route

### Reconnection & In-Flight Requests

**When connection drops during an operation:**

**In-Flight Request Semantics (Per Channel):**
- Any request sent but not yet responded to is **LOST** (for that channel)
- Server may have processed the request before disconnect
- Client CANNOT know if request succeeded or failed
- **No automatic replay or recovery**

**Client Retry Strategy:**
- **Idempotent operations** (GET, SCAN, READ): SAFE to retry
- **Non-idempotent operations** (PUT, ENQUEUE, RESERVE, PUBLISH): DO NOT retry
  - Retrying may cause duplicate execution
- Use application-level idempotency tokens if needed; do not rely on RPC `correlation_id` alone for durable deduplication

**Transaction-Specific Behavior:**
- If disconnect during KV transaction: server ROLLS BACK automatically
- If disconnect during Stream session: server ROLLS BACK automatically
- Client MUST re-BEGIN transaction/session and retry all operations from scratch

**Subscription-Specific Behavior:**
- All active subscriptions are **dropped** on disconnect: Notice fanout, Queue availability, Lease changes, Stream commit notifications, and Schedule fire notifications.
- RPC worker registrations are also session-scoped and are **dropped** on disconnect.
- Clients MUST re-subscribe or re-register explicitly after reconnect before reporting those handles as active.
- Clients MAY implement transparent auto-resubscribe or worker re-registration from client-owned configuration, with exponential backoff.
- Servers MUST treat duplicate SUBSCRIBE requests as idempotent for the same session and pattern.

**Session-Bound Handle Behavior:**
- Open KV transactions and Stream append sessions are invalidated; clients must begin fresh handles after reconnect.
- Queue item handles reserved before disconnect are invalid; clients must reserve again. Durable queue messages may redeliver according to queue policy.
- Lease handles acquired before disconnect are invalid; clients must reacquire if ownership is still required.
- Pending RPC calls fail with a connection/interruption error instead of stalling or silently replaying.
- Stream subscriptions are live wake signals only. Replay resumes through explicit `READ` calls from client-owned offsets.

**Reconnection Flow:**
1. Detect transport failure (connection lost, read error, timeout)
2. Wait (exponential backoff: 1s → 2s → 4s → 8s → cap at 30s)
3. Re-open transport connection
4. Send new CONNECT frame (authentication may have changed)
5. Re-establish client-owned subscriptions and RPC worker registrations if needed
6. Invalidate stale transaction/session/queue/lease handles and fail pending calls
7. Resume Stream reads from client-owned offsets where applicable
8. Resume normal operations

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
  **NOT Idempotent (MUST NOT Retry Automatically):**
  Write operations, control operations, and live fanout operations are NOT idempotent:
- KV: `PUT`, `INSERT`, `DELETE`, `BEGIN`, `COMMIT`, `ROLLBACK`
- Stream: `APPEND`, `BEGIN`, `COMMIT`, `ROLLBACK`
- Notice: `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`
- Queue: `ENQUEUE`, `RESERVE`, `EXTEND`, `DELETE`
- RPC: `REQUEST`, `RESPONSE`
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
