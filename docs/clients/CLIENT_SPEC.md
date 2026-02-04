# Fitz Client Specification

**Authoritative protocol specification for Fitz client implementations.**

This document defines what every Fitz client implementation MUST do to interoperate with any conformant Fitz broker.

---

## Table of Contents

1. [Scope & Non-Goals](#scope--non-goals)
2. [Client Model](#client-model)
3. [Terminology & Definitions](#terminology--definitions)
4. [Supported Transports](#supported-transports)
5. [Wire Protocol](#wire-protocol)
6. [Connection Lifecycle](#connection-lifecycle)
7. [Authentication & Security](#authentication--security)
8. [Routing](#routing)
9. [Verbs](#verbs)
10. [Permissions](#permissions)
11. [Transactions](#transactions)
12. [Subscriptions](#subscriptions)
13. [Request/Response Correlation](#requestresponse-correlation)
14. [Error Handling](#error-handling)
15. [Idempotency & Retry Strategy](#idempotency--retry-strategy)
16. [Domains](#domains)
17. [Constants & TLV Registry](#constants--tlv-registry)
18. [Acceptance Criteria](#acceptance-criteria)

---

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

---

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

---

## Terminology & Definitions

Use these exact terms. Other terms are forbidden.

| Term | Definition | Forbidden Alternatives |
|---:|---|---|
| **realm** | Isolation boundary for resources within a broker | `tenant`, `organization` |
| **area** | Namespace within a realm | `namespace`, `collection` |
| **resource** | Named entity within an area (e.g., table, queue, stream) | — |
| **route** | URI-like string addressing a resource or operation | `endpoint`, `path`, `key` |
| **verb** | Operation name (e.g., `GET`, `PUT`, `PUBLISH`) | `operation`, `method` (ambiguous) |
| **route_family** | Numeric partition key for sharding (u64) | `shard_id`, `partition_id` |
| **domain** | Service category (kv, queue, notice, stream, rpc, lease, schedule) | — |

**Forbidden terminology in client code:**
- NEVER use `tenant` — use `realm`
- NEVER use `namespace` — use `area`
- NEVER use `endpoint` — use `route`
- NEVER use `topic` (except within domain-specific docs) — use `route`

---

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

---

## Wire Protocol

### TLV Record Encoding

Frame payloads consist of one or more **TLV records** concatenated back-to-back.

Each record is:
- **Type** (u16, big-endian):
  - If `type <= 0xFE`: single byte
  - If `type > 0xFE`: escape byte `0xFF` followed by 2-byte big-endian u16
- **Length** (u16, big-endian): byte count of value (0..=65535)
- **Value**: exactly `length` bytes

### Primitive Encodings

All fields use **big-endian** byte order.

| Type | Encoding |
|---|---|
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
5. **Duplicate TLV tags are NOT permitted.** If a TLV tag appears more than once in a frame the frame **MUST** be treated as malformed and the receiver **MUST** close the connection with a TLV parse error. **Rationale:** Fitz TLV disallows duplicate tags to keep decoding deterministic and to simplify client implementations and conformance testing.
6. **A single TLV value MUST NOT exceed 65535 bytes (≈64 KiB).** Large payloads MUST be chunked across multiple TLV records or multiple frames/operations.

---

---

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
- Clients MUST follow the single in-flight request rule in AUTHENTICATED state.

---

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

Important client model constraint:
- **Clients MUST NOT have more than one in-flight request per connection.** The Fitz protocol is synchronous (request → response). Sending a second request before receiving the response to the first request is undefined behavior and the broker MAY close the connection. Asynchronous deliveries (e.g., `NOTIFY`, RPC worker invocations) are out-of-band and do not count as request responses; clients MUST handle them separately.

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

---

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

---

## Routing

Routes are **opaque URI-like strings** that address resources and operations.

### Route Format

```
{scheme}://{realm}/{area}/{resource}/{operation}
```

**Components:**

| Component | Type | Example | Rules |
|---|---|---|---|
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

## Route Acceptance Criteria (Authoritative)

A request is valid **only if**:

1. The route shape is valid for the domain
2. Wildcards appear only in allowed positions (per domain)
3. The method permits those wildcards
4. The route depth matches the method's plane

**Violations are protocol errors.** Broker MUST reject; clients **MAY** perform local route shape validation for ergonomics, but the broker is authoritative. Clients **MUST** accept broker rejection as the source of truth and MUST NOT rely on local validation as a substitute for server-side checks.

---

## Global Route Rules (Normative)

- Routes are opaque strings with a fixed, domain-defined shape
- `{realm}` is **always concrete** (never `*`)
- `*` MAY appear only in positions explicitly allowed by the domain
- Extra path segments are **forbidden**
- Route shape validation occurs **before** permission or dispatch checks

---

## Route Shapes by Domain

### KV Domain

**Valid Route Shapes:**
- `kv://{realm}/{area}`
- `kv://{realm}/{area}/{resource}`
- `kv://{realm}/{area}/*`
- `kv://{realm}/*/*`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
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

---

### Stream Domain

**Valid Route Shapes:**
- `stream://{realm}/{area}/{resource}`
- `stream://{realm}/{area}/*`
- `stream://{realm}/*/*`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
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

---

### Queue Domain

**Valid Route Shapes:**
- `queue://{realm}/{area}`
- `queue://{realm}/{area}/{resource}`
- `queue://{realm}/{area}/*`
- `queue://{realm}/*/*`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
| `LIST` | `{realm}/{area}`, `{realm}/*/*` |
| `ENQUEUE` | `{realm}/{area}/{resource}` |
| `RESERVE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
| `COMPLETE` | `{realm}/{area}/{resource}` |
| `EXTEND` | `{realm}/{area}/{resource}` |

**Note:** `LIST`, `CREATE`, `DELETE` (admin), and `SEND`/`RECEIVE`/`RELEASE` operations are either broker-internal or legacy verbs. Clients should use: ENQUEUE, RESERVE, COMPLETE, EXTEND as documented in the wire format section.source}` |
| `RELEASE` | `{realm}/{area}/{resource}` |
| `EXTEND` | `{realm}/{area}/{resource}` |

---

### Schedule Domain

**Valid Route Shapes:**
- `schedule://{realm}/{area}`
- `schedule://{realm}/{area}/{resource}`
- `schedule://{realm}/{area}/*`

**Method Acceptance:**

| Method | Accepted Route Sh/{resource}` |
| `CANCEL` | `{realm}/{area}/{resource}` |

**Note:** `DELETE` (admin) and `TRIGGER` operations are broker-internal. Clients should use: CREATE, CANCEL, LIST as documented in the wire format section. LIST is fully documented with streaming protocol.
| `PUT` | `{realm}/{area}/{resource}` |
| `DELETE` (data) | `{realm}/{area}/{resource}` |
| `TRIGGER` | `{realm}/{area}/{resource}` |

---

### Lease Domain

**Valid Route Shapes:**
- `lease://{realm}/{area}/{resource}`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
| `ACQUIRE` | `{realm}/{area}/{resource}` |
| `RENEW` | `{realm}/{area}/{resource}` |
| `RELEASE` | `{realm}/{area}/{resource}` |

---

### Notice Domain

**Valid Route Shapes:**
- `notice://{realm}/{area}/{resource}`
- `notice://{realm}/{area}/*`
- `notice://{realm}/*/*`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
| `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*`, `{realm}/*/*` |
| `UNSUBSCRIBE` | same as `SUBSCRIBE` |
| `PUBLISH` | `{realm}/{area}/{resource}` |

---

### RPC Domain

**Valid Route Shapes:**
- `rpc://{realm}/{area}/{resource}`
- `rpc://{realm}/{area}/*`

**Method Acceptance:**

| Method | Accepted Route Shapes |
|--------|--------|
| `CALL` | `{realm}/{area}/{resource}` |
| `SUBSCRIBE` | `{realm}/{area}/{resource}`, `{realm}/{area}/*` |
| `UNSUBSCRIBE` | same as `SUBSCRIBE` |

---

## Lock-In Rule

**If a route shape is not explicitly listed for a method, it is invalid.**

This specification is the **single source of truth** for:
- Broker validation
- SDK conformance testing
- Permission enforcement
- Long-term protocol stability

---

## Route Family

**Route family** is a numeric `u64` partition key, **separate from the route string**.

- Provided by client with each request
- Used by broker for sharding/isolation
- MUST be consistent for same realm/area/resource tuple
- Opaque to client (no semantic meaning)

---

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

| Domain | Verb | Wire Code | Plane | Notes |
|---|---|---:|---|---|
| KV | BEGIN | 100 | Data | Start transaction |
| KV | COMMIT | 101 | Data | Finalize transaction |
| KV | ROLLBACK | 102 | Data | Abort transaction |
| KV | GET | 103 | Data | Read key |
| KV | PUT | 104 | Data | Write key |
| KV | INSERT | 105 | Data | Insert (fail if exists) |
| KV | DELETE | 106 | Data | Delete key |
| KV | DELETE_RANGE | 107 | Data | Delete key range |
| KV | SCAN | 108 | Data | Scan keys in range |
| Stream | BEGIN | 200 | Data | Start session |
| Stream | APPEND | 201 | Data | Append record |
| Stream | COMMIT | 202 | Data | Finalize session |
| Stream | ROLLBACK | 203 | Data | Abort session |
| Stream | READ | 204 | Data | Read range |
| Stream | LAST | 205 | Data | Get last record |
| Stream | GET_METADATA | 206 | Data | Get metadata |
| Notice | PUBLISH | 100 | Data | Publish message |
| Notice | SUBSCRIBE | 101 | Data | Subscribe to pattern |
| Notice | UNSUBSCRIBE | 102 | Data | Unsubscribe |
| Notice | UNSUBSCRIBE_ALL | 103 | Data | Clear all subscriptions |
| Notice | NOTIFY | 104 | Server→Client | Delivery |
| Queue | ENQUEUE | 200 | Data | Add message |
| Queue | RESERVE | 202 | Data | Lease message(s) |
| Queue | EXTEND | 203 | Data | Extend lease |
| Queue | COMPLETE | 204 | Data | Mark complete |
| RPC | SUBSCRIBE_WORKER | 300 | Data | Register worker |
| RPC | UNSUBSCRIBE_WORKER | 301 | Data | Unregister worker |
| RPC | REQUEST | 302 | Data | Send request |
| RPC | RESPONSE | 303 | Data | Send response |
| RPC | ACK | 304 | Data | Acknowledge |
| Lease | ACQUIRE | 400 | Data | Acquire lease |
| Lease | RENEW | 401 | Data | Extend lease |
| Lease | RELEASE | 402 | Data | Release lease |
| Lease | QUERY | 403 | Data | Query lease status |
| Schedule | CREATE | 500 | Data | Create schedule |
| Schedule | CANCEL | 501 | Data | Cancel schedule |
| Schedule | LIST | 502 | Data | List schedules |

### MessageType Overlap

Wire codes **overlap across domains** (e.g., KV and Notice both use 100–104). The domain is disambiguated by **route scheme** (broker-side routing).

**Clients MUST NOT assume MessageType alone identifies a domain.**

---

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
|---|---|---|
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

---

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

---

## Transactions

Transactions are **explicit and domain-specific**. Clients MUST NOT provide implicit transaction handling.

### Transaction APIs (Where Supported)

Clients MUST expose explicit methods for supported domains:

| Domain | API | Required |
|---|---|---|
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

**Example (Rust-like pseudocode):**

```rust
// ✅ CORRECT - explicit transaction lifecycle
let tx_id = client.begin(KvBeginRequest { resource, mode })?;
client.put(KvPutRequest { tx_id, key, value })?;
client.get(KvGetRequest { tx_id, key })?;
client.commit(KvCommitRequest { tx_id })?;

// ❌ WRONG - auto-open transactions
let value = client.get(key)?;  // Do NOT auto-begin

// ❌ WRONG - auto-commit
client.put(key, value)?;  // Do NOT auto-commit
```

---

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
  family_id: 1,
  pattern: "notice://prod/app/*",
  handler: (route, payload) => { /* ... */ }
});

// ✅ CORRECT - explicit unsubscribe
client.unsubscribe({
  family_id: 1,
  pattern: "notice://prod/app/*"
});

// ❌ WRONG - implicit subscriptions
client.on("app.events", handler);  // Magic pattern; unclear when subscribed
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

---

## Error Handling

Errors fall into two categories: **transport** and **domain**.

### Transport Errors

Transport errors signal connection failure:

| Error | Cause | Client Action |
|---|---|---|
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

### Synchronous Model

**Fitz uses synchronous request/response:**
- Client sends one request, blocks waiting for response
- Broker processes request, sends exactly one response
- Client receives response, unblocks
- **Pipelining:** NOT supported (no request IDs, no response tagging). **Clients MUST NOT have more than one in-flight request per connection.** Sending another request before the response to the previous request is received is undefined behavior and the broker MAY close the connection.

**Exception: Streaming and Fanout**

For operations with multiple responses (Notice, RPC, Stream):
- First response is immediate (operation accepted, subscription ID, etc.)
- Subsequent responses (notifications, RPC calls) arrive asynchronously
- Client MUST handle asynchronous delivery (subscribe to in-band notifications)
- Connection remains open; no explicit end marker for streaming

### Multi-Response Operations

When a single operation generates multiple responses:

**Notice SUBSCRIBE:**
- Request → Response 1 (subscription ID)
- Subsequent PUBLISHes → NOTIFY frames (asynchronous)
- Client reads NOTIFYs from same connection

**RPC REQUEST:**
- Request → Response 1 (accepted, empty body)
- Worker responses → Response 2+ (streaming, with `sequence` and `stream_end` flag)
- Correlation ID links all responses

**Stream READ:**
- Request → Response 1 (record stream)
- Multiple records may arrive in single response or multiple frames
- Broker MAY split large responses across multiple frames

### Connection Handling

For all operations:
- Connection remains open after response received
- Client MAY send next request immediately (re-enters sync wait)
- Asynchronous frames (notifications, RPC responses) arrive while waiting for next response
- Client MUST buffer asynchronous frames and dispatch to handlers

---

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

---

Each domain has a specific wire format, verb set, and semantics. Implement each domain codec according to its specification below.

### Notice Domain (Fire-and-Forget Pub/Sub)

**Purpose:** Low-latency session-scoped notifications with wildcard pattern matching.

#### Message Types
| Type | Name | Direction |
|---:|---|---|
| 100 | PUBLISH | Client → Server |
| 101 | SUBSCRIBE | Client → Server |
| 102 | UNSUBSCRIBE | Client → Server |
| 103 | UNSUBSCRIBE_ALL | Client → Server |
| 104 | NOTIFY | Server → Client (delivery) |

#### PUBLISH Request
```
[u64 BE]  family_id
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
[u64 BE]  family_id
[u32 BE]  route_pattern_len
[bytes]   route_pattern (supports * and ** wildcards)
[u64 BE]  session_id
[u32 BE]  subscriber_route_len
[bytes]   subscriber_route

Response (status=0):
  [u8]     0
  [u8]     has_subscription_id
  [u64 BE] subscription_id (if present)

Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### UNSUBSCRIBE Request
```
[u64 BE]  family_id
[u32 BE]  route_pattern_len
[bytes]   route_pattern
[u64 BE]  session_id
[u32 BE]  subscriber_route_len
[bytes]   subscriber_route

Response: status byte + error (if status=1)
```

#### UNSUBSCRIBE_ALL Request
```
[u64 BE]  session_id
[u64 BE]  family_id
[u32 BE]  subscriber_route_len
[bytes]   subscriber_route

Response: status byte + error (if status=1)
```

#### NOTIFY (Server Delivery)
```
[u32 BE]  route_len
[bytes]   route (published route, not subscription pattern)
[u32 BE]  payload_len
[bytes]   payload
```

#### Pattern Matching
- `*` matches one segment (e.g., `notice://realm/*/events` matches `notice://realm/orders/events`)
- `**` matches zero or more segments (e.g., `notice://realm/**` matches all routes in realm)
- Exact routes (no wildcards) also supported

#### Usage Example

```python
# Subscriber
sub_id = client.notice_subscribe(
    pattern="notice://prod/app/orders/*",
    subscriber_route="notice://prod/app/listener"
)

# Receive notifications
while notification = client.receive_notification():
    print(f"Received: {notification.route} => {notification.payload}")

# Publisher
client.notice_publish(
    route="notice://prod/app/orders/created",
    payload=b'{"order_id": 123}'
)

# Cleanup
client.notice_unsubscribe(pattern="notice://prod/app/orders/*")
```

#### Semantics
- **Delivery**: Best-effort; under backpressure, notifications may be dropped
- **Ordering**: Delivered in publish order per subscription
- **Fanout**: Single publish reaches all matching subscriptions
- **Session-Scoped**: Subscriptions tied to connection; lost on disconnect
- **Acknowledgements & Retries**: `NOTIFY` frames are never acknowledged by clients and are never retried by the broker. Clients MUST NOT send acknowledgements for `NOTIFY` frames and MUST NOT expect guaranteed replay.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage Guidance:** `NOTICE` is a **best-effort, non-durable** mechanism. **Clients MUST NOT use Notices for workflows that require acknowledgement, durability, or guaranteed delivery. Use RPC or Queue for guaranteed delivery or acknowledgement-based workflows.**

#### Error Codes (3xxx range)
- 3001 = ERR_INVALID_ROUTE
- 3002 = ERR_INVALID_PATTERN
- 3003 = ERR_SUBSCRIPTION_LIMIT
- 3004 = ERR_TRANSPORT_CLOSED

#### Acceptance Tests
- subscribe to pattern, receive matching publications
- multiple subscriptions on same pattern both receive
- publish with no subscribers returns ok
- unsubscribe stops delivery
- wildcard patterns match correctly
- exact routes take precedence

---

### Stream Domain (Durable Append-Only Logs)

**Purpose:** Strictly ordered append/read with watermark protection and optimistic concurrency.

#### Message Types
| Type | Name |
|---:|---|
| 200 | BEGIN |
| 201 | APPEND |
| 202 | COMMIT |
| 203 | ROLLBACK |
| 204 | READ |
| 205 | LAST |
| 206 | GET_METADATA |

#### BEGIN Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route
[u64 BE]  expected_offset
[u8]      has_ingest_metadata (0 or 1)
[u32 BE]  ingest_metadata_len (if has_ingest_metadata=1)
[bytes]   ingest_metadata

Response (status=0):
  [u8]     0
  [u8]     has_session_id
  [u64 BE] session_id (if present)
  [u32 BE] data_len
  [bytes]  data

Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### APPEND Request
```
[u32 BE]  session_id_len
[bytes]   session_id (UTF-8, from BEGIN response)
[u32 BE]  body_len
[bytes]   body
[u8]      has_metadata
[u32 BE]  metadata_len (if has_metadata=1)
[bytes]   metadata

Response: status byte + optional session_id + data
```

#### COMMIT Request
```
[u32 BE]  session_id_len
[bytes]   session_id
[u32 BE]  mode_len
[bytes]   mode ("Buffered" or "Sync")

Response: status byte + data
```

#### ROLLBACK Request
```
[u32 BE]  session_id_len
[bytes]   session_id

Response: status byte + data
```

#### READ Request
```
[u64 BE]  family_id
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
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route

Response: status byte + data
```

#### GET_METADATA Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route

Response: status byte + data
```

#### Usage Example

```python
# Append to stream
session_id = client.stream_begin(
    route="stream://prod/app/events",
    expected_offset=100  # Optimistic concurrency
)

client.stream_append(session_id, b"event_data_1")
client.stream_append(session_id, b"event_data_2")
client.stream_commit(session_id, mode="Sync")  # Durably committed

# Read from stream
records = client.stream_read(
    route="stream://prod/app/events",
    from_offset=100,
    limit=10
)
```

#### Semantics
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

---

### Queue Domain (Durable At-Least-Once Delivery)

**Purpose:** FIFO-ish message queues with leasing and visibility timeouts.

#### Message Types
| Type | Name |
|---:|---|
| 200 | ENQUEUE |
| 202 | RESERVE |
| 203 | EXTEND |
| 204 | COMPLETE |

#### ENQUEUE Request
```
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

```python
# Producer: Enqueue messages
msg_id = client.queue_enqueue(
    route="queue://prod/app/tasks",
    body=b"task_payload",
    delay_seconds=0
)

# Consumer: Reserve, process, complete
leases = client.queue_reserve(
    route="queue://prod/app/tasks",
    lease_seconds=30,
    batch_size=5
)

for lease in leases:
    try:
        process_task(lease.body)
        client.queue_complete(lease.message_id, lease.lease_token)
    except ProcessingError:
        # Let lease expire, message returns to queue
        pass
```

#### Semantics
- **At-Least-Once**: Messages delivered until completed; expired leases requeue them
- **FIFO-ish**: Generally delivered in enqueue order; leasing can cause out-of-order
- **Visibility Timeout**: Leased messages invisible to other consumers until expiry
- **Token Binding**: Complete/Extend require both message_id and lease_token
- **Durability**: All enqueued messages survive broker restart

#### Acceptance Tests
- enqueue/reserve/complete cycle
- lease expiry returns message to ready queue
- extend lease delays expiry
- complete with wrong token fails
- reserve with batch_size returns up to that many
- multiple consumers can reserve from same queue

---

### RPC Domain (Request/Response & Streaming)

**Purpose:** Low-latency request/response with reply inbox and optional streaming.

#### Message Types
| Type | Name | Direction |
|---:|---|---|
| 300 | SUBSCRIBE_WORKER | Client → Server |
| 301 | UNSUBSCRIBE_WORKER | Client → Server |
| 302 | REQUEST | Client → Server |
| 303 | RESPONSE | Server ↔ Client |
| 304 | ACK | Client ↔ Server |

#### SUBSCRIBE_WORKER Request
```
[u64 BE]  family_id
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
[u64 BE]  family_id
[u32 BE]  worker_route_len
[bytes]   worker_route

Response: status byte + data
```

#### REQUEST Request (Client sends to server)
```
[u64 BE]  family_id
[u32 BE]  correlation_id_len
[bytes]   correlation_id (16 bytes, UUID)
[u32 BE]  route_len
[bytes]   route
[u32 BE]  reply_route_len
[bytes]   reply_route
[u32 BE]  body_len
[bytes]   body

Response from broker:
  [u8]     status (0=ok, 1=error)
  [u32 BE] data_len
  [bytes]  data (empty on success)
```

#### RESPONSE (From worker to caller via broker)
```
[u32 BE]  correlation_id_len
[bytes]   correlation_id (16 bytes)
[u64 BE]  sequence
[u32 BE]  body_len
[bytes]   body
[u8]      stream_end (0=more, 1=end)

Response from broker:
  [u8]     status
  [u32 BE] data_len
  [bytes]  data
```

#### ACK (Acknowledge receipt)
```
[u32 BE]  correlation_id_len
[bytes]   correlation_id (16 bytes)

Response: status + data
```

**Important:** `correlation_id_len` MUST be exactly 16. Any other value is a protocol error.

#### Usage Example

```python
# Worker: Register to handle requests
client.rpc_subscribe_worker(
    worker_route="rpc://prod/app/compute"
)

# Worker: Handle incoming requests
while request = client.receive_rpc_request():
    result = process_request(request.body)
    client.rpc_response(
        correlation_id=request.correlation_id,
        sequence=0,
        body=result,
        stream_end=True
    )

# Caller: Send request and wait for response
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

#### Semantics
- **Correlation**: UUID links request to responses (client-generated)
- **Streaming**: Multi-frame responses have incrementing `sequence` and `stream_end` flag
- **Backpressure**: ERR_RPC_BACKPRESSURE if outbound queue full
- **Ordering**: Responses delivered in sequence order
- **Exactly-Once**: Each request reaches worker once

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

---

### KV Domain (Durable Key-Value)

**Purpose:** Transaction-based CRUD and range operations with isolation.

**IMPORTANT:** All KV operations occur within transactions (Begin/Commit/Rollback).

#### Message Types
| Type | Name |
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

#### BEGIN Request
```
[u32 BE]  resource_len
[bytes]   resource_name
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
[u32 BE]  resource_len
[bytes]   resource_name
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value

Response: (empty on success, error on failure)
```

#### GET Request
```
[u64 BE]  tx_id
[u32 BE]  resource_len
[bytes]   resource_name
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
[u32 BE]  resource_len
[bytes]   resource_name
[u32 BE]  key_len
[bytes]   key
[u32 BE]  value_len
[bytes]   value

Response: (empty on success)
```

#### DELETE Request
```
[u64 BE]  tx_id
[u32 BE]  resource_len
[bytes]   resource_name
[u32 BE]  key_len
[bytes]   key

Response: (empty on success)
```

#### DELETE_RANGE Request
```
[u64 BE]  tx_id
[u32 BE]  resource_len
[bytes]   resource_name
[u32 BE]  start_key_len
[bytes]   start_key
[u32 BE]  end_key_len
[bytes]   end_key

Response: (empty on success)
```

#### SCAN Request
```
[u64 BE]  tx_id
[u32 BE]  resource_len
[bytes]   resource_name
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

Response: (empty on success)
```

#### ROLLBACK Request
```
[u64 BE]  tx_id

Response: (empty on success)
```

#### Semantics
- **Isolation**: Transactions isolated by realm + area
- **Durability**: Sync mode guarantees WAL; buffered is best-effort
- **Read Modes**: ReadOnly allows multi-reader; ReadWrite is exclusive per resource
- **Persistence**: All committed data survives broker restart

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

---

### Lease Domain (Ephemeral Coordination)

**Purpose:** In-memory exclusive leases for distributed locking and coordination.

#### Message Types
| Type | Name |
|---:|---|
| 400 | ACQUIRE |
| 401 | RENEW |
| 402 | RELEASE |
| 403 | QUERY |

#### ACQUIRE Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  ttl_secs

Response (status=0):
  [u8]     0
  [u8]     has_token
  [u32 BE] token_len (if has_token=1)
  [bytes]  token

Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

#### RENEW Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token
[u64 BE]  ttl_secs

Response: status + optional token
```

#### RELEASE Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token

Response: status + optional token
```

#### QUERY Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route

Response: status + optional token
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
- **Mutual Exclusion**: Only one owner holds a lease at a time
- **Fencing Token**: Prevents stale commands from releasing new holder's lease
- **TTL-based Expiry**: Expired leases automatically released; query returns free
- **Route Partitioned**: Different routes have independent leases
- **In-Memory**: Lost on broker restart (use for coordination, not durability)

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

---

### Schedule Domain (Delayed/Recurring Tasks)

**Purpose:** Durable scheduling of delayed tasks and recurring jobs.

#### Message Types
| Type | Name |
|---:|---|
| 500 | CREATE |
| 501 | CANCEL |
| 502 | LIST |

#### CREATE Request
```
[u32 BE]  schedule_payload_len
[bytes]   schedule_payload (nested TLV, see below)

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
[u32 BE]  schedule_id_len
[bytes]   schedule_id

Response: status + optional schedule_id
```

#### LIST Request
```
(empty payload)

Response: status + optional schedule_id
```

**Note:** LIST returns one schedule per response. If no schedules, respond with status=0 and `has_schedule_id=0`.

#### Schedule Payload (Nested TLV)
```
Type 1: cron (UTF-8 string)
Type 2: target_resource (UTF-8 string)
Type 3: target_operation (UTF-8 string)
```

Total payload = concatenated TLV records, no outer length prefix.

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
# Create one-time delayed task
schedule_id = client.schedule_create(
    route="schedule://prod/app/reminders",
    cron="0 9 * * 1",  # Every Monday at 9 AM
    target_resource="notice://prod/app/notifications",
    target_operation="PUBLISH"
)

# Create recurring task
recurring_id = client.schedule_create(
    route="schedule://prod/app/cleanup",
    cron="0 2 * * *",  # Daily at 2 AM
    target_resource="kv://prod/app/cache",
    target_operation="DELETE_RANGE"
)

# List schedules
schedules = client.schedule_list(
    route="schedule://prod/app/reminders"
)

# Cancel schedule
client.schedule_cancel(schedule_id)
```

#### Semantics
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

---

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

**Stream/Queue Domain (200–206):**
Stream uses 200–206, Queue uses 200–204 (overlapping by design, disambiguated by route scheme).

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

**Schedule Domain (500–502):**
| Value | Name |
|---:|---|
| 500 | CREATE |
| 501 | CANCEL |
| 502 | LIST |

**Notice Domain (100–104, overlaps with KV):**
| Value | Name |
|---:|---|
| 100 | PUBLISH |
| 101 | SUBSCRIBE |
| 102 | UNSUBSCRIBE |
| 103 | UNSUBSCRIBE_ALL |
| 104 | NOTIFY |

### MessageType Disambiguation

When MessageType overlaps across domains (e.g., KV 100 = BEGIN, Notice 100 = PUBLISH):
- **Disambiguation:** By route scheme (first segment of route string in request)
- Broker MUST parse route from first TLV field to determine domain
- Same MessageType value in different domains is independent (no collision)

Example:
- `kv://realm/area/resource` with MessageType 100 = KV BEGIN
- `notice://realm/area/resource` with MessageType 100 = Notice PUBLISH

**Future compatibility:** If domain ranges exhaust, extend to new range blocks (e.g., 1100–1199 for KV expansion)

### Error Code Allocation (Authoritative)

Error codes are allocated by domain in 100-block ranges:

| Range | Domain | Capacity | Notes |
|---|---|---|---|
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
|---|---:|---|
| Control | 0 | Control/handshake |
| Pub | 1 | Publishing/notice |
| Sub | 2 | Subscriptions/delivery |
| Rpc | 3 | RPC request/response |
| Lease | 4 | Lease domain |

### Type Encoding Rules
- `type 0x00..0xFE`: single byte
- `type 0xFF`: escape marker for types > 0xFE
  - Followed by `u16 BE` for actual type

---

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

**Reconnect State:**
- Client subscribes to pattern, closes connection
- Reconnects with same JWT, old subscription is lost
- Verify client must re-subscribe explicitly (no auto-recovery)

**Fanout Scale:**
- Single PUBLISH to 1000 SUBSCRIBE clients
- All clients receive NOTIFY within 100ms (broker-dependent)
- Verify no message loss

**Concurrent Request Handling (Pipelining Rejection):**
- Client sends REQUEST 1 without waiting for response
- Client sends REQUEST 2 while REQUEST 1 pending
- Broker behavior is implementation-defined (MAY close connection, MAY serialize)
- Clients MUST NOT rely on pipelining support

---

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

#### Resource Disambiguation (KV/Queue)

Some domains derive resource from context rather than explicit payload:
- **KV transactions:** The resource is established at `BEGIN` and subsequent KV operations omit a route; resource identity is implicit in the transaction context (this keeps transaction operations compact and bound to the transaction's resource).
- **Queue operations:** Queue requests usually include route or are derived from the producer/consumer context; the queue name is typically explicit in the operation payload.

Why this matters:
- Domains that derive resource from context (KV transactions) are **session-scoped**: breaking the connection mid-transaction loses context and triggers rollback.
- Domains that require explicit routes (Notice, RPC) do so to enable fanout and correct addressing across many subscribers or workers.

Clients MUST be aware that breaking connection mid-transaction loses context (transaction auto-rollback).

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

---

### Broker-Specific Behaviors Summary

1. **Session ID exposure**: Notice/Stream payloads include session IDs, but no standard server-to-client notification mechanism yet
2. **KV/Queue routing**: KV/Queue payloads do not include route; broker derives from envelope/connection context
3. **Stream response data**: Response data is opaque; serialization format is broker-defined
4. **Verb code extensions**: New verbs added after current broker release use new wire codes in existing ranges

Clients SHOULD consult broker documentation for domain-specific behavior.

---

## References

- Fitz repository: https://github.com/cntryl/fitz
- Domain specifications: See [Domains](#domains) section
- Codec implementations: See Fitz `src/protocol/` directory
- Integration tests: See Fitz `tests/` directory

