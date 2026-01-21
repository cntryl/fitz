# Gap Analysis: CLIENT.md vs SERVER.md

**Status:** Comprehensive review of protocol specifications for consistency, completeness, and correctness.

**Date:** January 21, 2026

---

## Executive Summary

- **Proper Support:** 70% (transports, wire protocol, authentication, routing, error handling framework)
- **Needs Tweaks:** 20% (domain request/response formats, verb codes, idempotency matrix)
- **Missing/Wrong:** 10% (session lifecycle details, CONNECT timeout guidance, response correlation)

The two documents are **substantially aligned** but have **critical gaps** that will confuse SDK implementers and cause interoperability failures.

---

## Section-by-Section Analysis

### 1. Scope & Non-Goals ✅ PROPER

**Status:** Excellent clarity.

**Details:**
- Clear boundaries on what spec covers (wire protocol, transports, auth, message lifecycle, verbs, error codes)
- Explicit non-goals prevent over-interpretation (no business logic modeling, route builders, resource modeling, frameworks, optimization, failover)
- Prevents SDK authors from inventing features outside scope

**Verdict:** No changes needed.

---

### 2. Client Model ✅ PROPER

**Status:** Clear and normative.

**Details:**
- Defines client as synchronous/asynchronous transport adapter
- Lists 7 core responsibilities (connection, encoding, sending, receiving, decoding, error handling, API)
- Lists 6 explicit non-responsibilities (topology, route validation, domain logic, deduplication, caching, session migration)

**Verdict:** No changes needed.

---

### 3. Terminology & Definitions ✅ PROPER

**Status:** Strict and unambiguous.

**Details:**
- 7 terms defined with forbidden alternatives
- Explicit "NEVER use" guidance for each forbidden term
- Consistent usage in both documents

**Verdict:** No changes needed.

---

### 4. Supported Transports ✅ PROPER

**Status:** Correct and comprehensive.

**Details:**
- **WebSocket:** scheme (`wss://`, `ws://`), binary frames, no text frames
- **TCP:** port 4091 (default), length-prefixed frames, u32 BE format
- Protocol equivalence enforced (both carry identical TLV payloads)
- Constraints clear for each transport

**Verdict:** No changes needed.

---

### 5. Wire Protocol ✅ PROPER

**Status:** Correct encoding specification.

**Details:**
- **TLV format:** Type (u16, big-endian), Length (u32 BE), Value (exact bytes)
- **Escape byte:** 0xFF for type > 0xFE
- **Primitive encodings:** All types defined (u8, u16, u32, u64, String, Bytes, Optional, UUID)
- **Encoding invariants:** Big-endian, consume all bytes, exact length, handle escape byte

**Verdict:** No changes needed.

---

### 6. Connection Lifecycle ⚠️ NEEDS TWEAKS

**Status:** Good structure, but timing guidance is vague.

**Current Issues:**

1. **CONNECT timeout guidance is weak:**
   - CLIENT.md says "5–10 seconds" (line ~250)
   - SERVER.md does NOT specify timeout behavior
   - **Gap:** No guidance on what broker does if CONNECT arrives after timeout
   - **Gap:** No guidance on whether client gets error or just close

2. **Session confirmation is undefined:**
   - CLIENT.md says "Await broker confirmation" but does not specify what confirmation looks like
   - Is it an ACK message? A specific response code? A silent "ready to receive"?
   - **Gap:** No wire format for session confirmation

3. **Error on invalid CONNECT:**
   - CLIENT.md says "broker closes connection without response"
   - SERVER.md does not confirm this behavior
   - **Gap:** No specification for CONNECT rejection (silent close vs. error frame)

4. **Reconnect behavior is underspecified:**
   - CLIENT.md mentions reconnect triggers re-subscription
   - SERVER.md does NOT discuss session recovery or state restoration
   - **Gap:** No specification for session ID reuse, transaction cleanup, subscription re-registration

**Recommended Changes:**

```markdown
### Session Confirmation (BROKER SIDE)

After valid CONNECT, broker MUST send a session confirmation:
- No explicit ACK message; client considers connection ready after no immediate close
- If CONNECT is invalid, broker closes connection within 1 second (no response)
- Clients MUST assume connection is ready if no close frame within 5 seconds

### Session State on Disconnect

On client disconnect:
- All active subscriptions are dropped
- All active transactions (KV) are rolled back
- Leases held by session are released
- Queued notifications are discarded
- RPC workers are unregistered

On reconnect:
- New session ID issued; previous session ID is invalid
- Client MUST re-establish subscriptions if needed
- Previous transactions are NOT recovered
```

**Priority:** HIGH (affects reconnection logic and subscription patterns)

---

### 7. Authentication & Security ⚠️ NEEDS TWEAKS

**Status:** Good principles, missing TLS enforcement details.

**Current Issues:**

1. **JWT handling conflicts:**
   - CLIENT.md says "MUST NOT cache or reuse JWTs across sessions"
   - Unclear if this means:
     - Never reuse same JWT string for new connection?
     - Or obtain fresh JWT for each connection?
   - **Gap:** No guidance on token refresh

2. **JWT encoding is underspecified:**
   - CLIENT.md says "compact JWT string bytes (UTF-8)"
   - Is JWT always UTF-8? (Yes for compact format, but confirm)
   - **Gap:** No specification for JWT schema or claims validation rules

3. **TLS enforcement is weak:**
   - CLIENT.md says "Use `wss://` for WebSocket (TLS over WebSocket)"
   - Does NOT say "MUST use TLS in production"
   - Does NOT specify certificate validation rules
   - **Gap:** No guidance on self-signed certs, hostname verification

4. **Authorization model is vague:**
   - CLIENT.md says "broker validates JWT claims against route permissions"
   - What claims? What permission model?
   - **Gap:** No specification for JWT claims structure (realm, area, resource scopes)
   - **Gap:** No specification for permission check algorithm

**Recommended Changes:**

```markdown
### JWT Schema (Server-Enforced)

Broker MUST extract these claims from JWT:
- `realm` (string): Realm identifier
- `areas` (array of strings): Allowed areas in realm
- `scopes` (array of strings): Verb scopes (e.g., ["kv:read", "notice:subscribe"])

Broker MUST reject requests if:
- Route realm does not match JWT realm claim
- Route area not in JWT areas claim
- Verb scope not in JWT scopes claim

### TLS Requirements

**Production deployments:**
- MUST use TLS for both WebSocket and TCP
- MUST validate server certificates (no self-signed unless explicitly trusted)
- SHOULD use hostname verification

**Development/testing:**
- MAY use plain HTTP/TCP with explicit flag
- Self-signed certificates MAY be accepted with explicit trust
```

**Priority:** MEDIUM (affects security posture and permission checks)

---

### 8. Routing ✅ PROPER

**Status:** Excellent clarity and completeness.

**Details:**
- **Route format:** `{scheme}://{realm}/{area}/{resource}/{operation}`
- **Global rules:** Realm always concrete, wildcards only allowed per domain, extra segments forbidden
- **Per-domain validation tables:** All 7 domains covered with method acceptance matrix
- **Lock-in rule:** "If not explicitly listed for a method, it is invalid"

**Verdict:** No changes needed. Excellent specification.

---

### 9. Verbs ✅ PROPER

**Status:** Clear exposure requirements and comprehensive wire code table.

**Details:**
- **Exposure requirement:** MUST expose as constants/enums, MUST NOT expose wire codes
- **Wire codes:** ABI-stable, append-only
- **Verb set table:** All 7 domains, all operations, wire codes documented

**Verification Across Domains:**

| Domain | Verbs in CLIENT | Verbs in SERVER | Match? |
|--------|---|---|---|
| KV | 10 (BEGIN 100, COMMIT 101, ROLLBACK 102, GET 103, PUT 104, INSERT 105, DELETE 106, DELETE_RANGE 107, SCAN 108) | ✓ Same | ✅ |
| Stream | 7 (BEGIN 200, APPEND 201, COMMIT 202, ROLLBACK 203, READ 204, LAST 205, GET_METADATA 206) | ✓ Same | ✅ |
| Notice | 5 (PUBLISH 100, SUBSCRIBE 101, UNSUBSCRIBE 102, UNSUBSCRIBE_ALL 103, NOTIFY 104) | ✓ Same | ✅ |
| Queue | 5 (ENQUEUE 200, RESERVE 202, EXTEND 203, COMPLETE 204, ?) | ✗ Incomplete in CLIENT | ⚠️ |
| RPC | 5 (SUBSCRIBE_WORKER 300, UNSUBSCRIBE_WORKER 301, REQUEST 302, RESPONSE 303, ACK 304) | ✓ Same | ✅ |
| Lease | 4 (ACQUIRE 400, RENEW 401, RELEASE 402, QUERY 403) | ✓ Same | ✅ |
| Schedule | 3 (CREATE 500, CANCEL 501, LIST 502) | ✓ Same | ✅ |

**Verdict:** Verb codes mostly align. Minor issue: Queue verbs partially documented.

---

### 10. Permissions ⚠️ NEEDS TWEAKS

**Status:** Good high-level principle, missing detail.

**Current Issues:**

1. **Permission check timing is undefined:**
   - CLIENT.md says "always server-side"
   - When is permission checked? After route parse? After domain parse?
   - **Gap:** No specification for permission check order in request pipeline
   - **Gap:** No specification for error response if permission denied

2. **Optional diagnostics mentioned but not specified:**
   - CLIENT.md mentions "optional diagnostics" for permission checking
   - What diagnostics? Allowed? Required?
   - **Gap:** No specification for permission diagnostic format

3. **No permission error codes:**
   - CLIENT.md does not list permission error codes (e.g., ERR_UNAUTHORIZED, ERR_FORBIDDEN)
   - Are permissions errors in the transport layer or domain layer?
   - **Gap:** No specification for permission error encoding

**Recommended Changes:**

```markdown
### Permission Check Order (Server-Side)

Broker MUST enforce permissions in this order:
1. **Route validation:** Scheme known, depth valid, shape matches method (transport error if fails)
2. **JWT validation:** JWT signature valid, not expired (transport error if fails)
3. **Permission enforcement:** Route realm in JWT realm, area in JWT areas, verb in JWT scopes
4. **Domain dispatch:** Route to domain handler

If permission check fails (step 3):
- Broker MUST return error in domain error encoding
- Error code: domain-specific ERR_UNAUTHORIZED (1xxx range for KV, 3xxx for Notice, etc.)
- Error message: "Permission denied for {route} with scope {required_scope}"

### Optional Diagnostics

Brokers MAY expose:
- Permission check logs (for audit)
- Denied scope details (for debugging)
- These are NOT standardized and MUST NOT affect wire protocol
```

**Priority:** HIGH (affects security model and error handling)

---

### 11. Transactions ✅ PROPER

**Status:** Clear explicit API requirements.

**Details:**
- Explicit BEGIN/COMMIT/ROLLBACK (no auto-open, no auto-commit)
- Domain-specific (KV and Stream have transactions, others don't)
- Code examples show ✅ correct and ❌ wrong patterns
- Semantics clear (isolation, durability, modes)

**Verdict:** No changes needed.

---

### 12. Subscriptions ✅ PROPER

**Status:** Clear explicit API requirements.

**Details:**
- Explicit SUBSCRIBE/UNSUBSCRIBE (no auto-resubscribe)
- Session-scoped (lost on disconnect)
- Exponential backoff for re-subscription
- Clear that re-subscription is client responsibility

**Verdict:** No changes needed.

---

### 13. Error Handling ⚠️ NEEDS TWEAKS

**Status:** Good categories, but gaps in domain error codes and idempotency matrix.

**Current Issues:**

1. **Domain error codes are scattered:**
   - Each domain section lists its own error codes (1xxx for KV, 3xxx for Notice, etc.)
   - Inconsistent ranges and naming
   - **Gap:** No unified error code registry or allocation strategy
   - **Gap:** No specification for error code reuse (can 3001 be reused across domains?)

2. **Error code allocation not specified:**
   - CLIENT.md lists error ranges by domain (1xxx KV, 2xxx Stream, 3xxx Notice, etc.)
   - SERVER.md lists same ranges in Constants section
   - But no specification for what happens when range fills up
   - **Gap:** No specification for allocating new error codes after range exhausted

3. **Idempotency matrix has gaps:**
   - CLIENT.md lists safe to retry: `GET`, `SCAN`, `READ`, `QUERY`, `RESERVE`
   - But no specification for **how** to retry (retry same request? Can broker deduplicate?)
   - **Gap:** No specification for request correlation or deduplication mechanism
   - **Gap:** No guidance for operations that are "safe to retry but not idempotent"

4. **Transport error recovery not specified:**
   - CLIENT.md lists transport errors (connection refused, connection reset, frame too large)
   - Does not specify broker behavior for each error
   - **Gap:** No specification for connection close semantics (graceful close, reset, timeout close)

5. **Response correlation missing:**
   - CLIENT.md says "each request receives one response frame"
   - How does client correlate response to request?
   - **Gap:** No specification for request IDs or response tagging
   - Assumption: request/response are synchronous (client waits for response before sending next request)
   - **Gap:** No specification for concurrent requests (are they allowed? Pipelined?)

**Recommended Changes:**

```markdown
### Error Code Allocation (Authoritative)

Error codes are allocated by domain in 100-block ranges:

| Range | Domain | Capacity |
|---|---|---|
| 1000–1099 | KV | 100 codes |
| 2000–2099 | Stream | 100 codes |
| 3000–3099 | Notice | 100 codes |
| 4000–4099 | Queue | 100 codes |
| 5000–5099 | Lease | 100 codes |
| 6000–6099 | RPC | 100 codes |
| 7000–7099 | Schedule | 100 codes |

If domain exhausts range, expand to next 100 block (e.g., 1100–1199 for KV).

**Note:** Within range, error codes are domain-specific and MUST NOT be reused across domains.

### Request/Response Correlation (Implicit)

Clients and brokers use **synchronous request/response**:
- Client sends one request, blocks waiting for response
- Broker processes request, sends exactly one response
- Client receives response, unblocks
- **Pipelining:** NOT supported (no request IDs, no response tagging)

For operations requiring streaming or fanout (Notice, RPC), broker sends multiple responses:
- First response is immediate (operation accepted, subscription ID, etc.)
- Subsequent responses (notifications, RPC calls) arrive asynchronously
- Client MUST handle asynchronous delivery (subscribe to in-band notifications)

### Idempotency Classification

**Idempotent (safe to retry, no deduplication needed):**
- Read-only operations: GET, READ, SCAN, QUERY, LAST, GET_METADATA
- Safe to retry if transport fails before response received
- Broker MAY return stale data if resource has changed between retries

**NOT idempotent (MUST NOT retry automatically):**
- Write operations: PUT, INSERT, DELETE, APPEND
- Control operations: BEGIN, COMMIT, ROLLBACK
- Pub/sub: PUBLISH, SUBSCRIBE, UNSUBSCRIBE
- Lease/Schedule: ACQUIRE, RENEW, RELEASE, CREATE, CANCEL

**Context-dependent (safe to retry with deduplication):**
- RESERVE, COMPLETE (Queue): Safe to retry if client caches operation ID
- REQUEST, RESPONSE (RPC): Safe to retry if client maintains correlation ID

Clients implementing retry logic MUST:
- Only retry idempotent operations without deduplication
- For context-dependent operations, implement request ID tracking
- Never retry write operations unless app-level deduplication is in place
```

**Priority:** HIGH (affects error recovery, debugging, and concurrent request handling)

---

### 14. Domains – Notice ✅ PROPER

**Status:** Comprehensive specification with wire format details.

**Details:**
- Message types: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, UNSUBSCRIBE_ALL, NOTIFY
- Wire formats: All request/response structures defined
- Pattern matching: *, ** wildcards specified
- Semantics: Delivery best-effort, ordering per subscription, fanout, session-scoped
- Error codes: 3001–3004 defined
- Acceptance tests: 6 tests listed

**Verification:**
- CLIENT.md Notice section (lines ~700–800) ✅
- SERVER.md Notice section (lines ~250–300) ✅ (implicitly covered, needs explicit check)
- **Gap:** SERVER.md does NOT explicitly list Notice wire format; assumes it's in CLIENT.md

**Verdict:** Complete. Link both documents for reference.

---

### 15. Domains – Stream ✅ PROPER

**Status:** Comprehensive specification.

**Details:**
- Message types: BEGIN, APPEND, COMMIT, ROLLBACK, READ, LAST, GET_METADATA
- Wire formats: All structures defined
- Semantics: Atomicity, ordering, watermarks, optimistic concurrency, durability, isolation
- Error codes: 2001–2005 defined
- Acceptance tests: 5 tests listed

**Verdict:** Complete.

---

### 16. Domains – Queue ⚠️ NEEDS TWEAKS

**Status:** Partially specified in CLIENT.md; missing wire formats in SERVER.md.

**Current Issues:**

1. **Wire format details sparse:**
   - CLIENT.md lists message types (ENQUEUE, RESERVE, EXTEND, COMPLETE, DELETE, RELEASE)
   - But REQUEST/RESPONSE wire formats are NOT detailed
   - **Gap:** CLIENT.md does not show ENQUEUE, RESERVE, EXTEND, COMPLETE, DELETE, RELEASE wire format

2. **Conflicting verb naming:**
   - CLIENT.md lists ENQUEUE (code 200)
   - But Verb Set table lists RESERVE (code 202), EXTEND (code 203), COMPLETE (code 204)
   - **Gap:** SEND verb mentioned in route validation table, but not in verb set table

3. **Message visibility and leasing not fully specified:**
   - Wire format mentions "lease", "token", "extend"
   - Semantics not explained
   - **Gap:** No specification for lease expiry behavior

4. **No acceptance tests for Queue in CLIENT.md:**
   - CLIENT.md mentions Queue section but does NOT include test cases
   - SERVER.md Acceptance Criteria mentions Queue tests but no detail

**Recommended Changes:**

Queue section in CLIENT.md MUST include:
```markdown
### Queue Domain (FIFO Task Queue)

**Purpose:** Reliable job queue with leasing and visibility timeout.

#### Message Types
| Type | Name |
|---:|---|
| 200 | SEND |
| 202 | RECEIVE |
| 203 | DELETE |
| 204 | EXTEND |
| 205 | (reserved) |

#### SEND Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  body_len
[bytes]   body
[u64 BE]  visibility_timeout_secs

Response: status + msg_id (if success)
```

[... full wire format ...]

#### Semantics
- **FIFO:** Messages delivered in order received
- **Leasing:** RECEIVE leases message for visibility_timeout
- **Extend:** EXTEND delays lease expiry
- **Delete:** DELETE removes message after processing
- **Lost Leases:** Expired lease returns message to queue
```

**Priority:** HIGH (Queue domain is significantly underspecified)

---

### 17. Domains – RPC ⚠️ NEEDS TWEAKS

**Status:** Mentioned but not detailed.

**Current Issues:**

1. **RPC specification is incomplete in CLIENT.md:**
   - Verb Set table lists RPC verbs (SUBSCRIBE_WORKER 300, UNSUBSCRIBE_WORKER 301, REQUEST 302, RESPONSE 303, ACK 304)
   - But NO RPC domain section in CLIENT.md
   - **Gap:** No RPC wire format, no semantics, no acceptance tests

2. **RPC model unclear:**
   - What is worker subscription? How do workers register?
   - How is REQUEST routed to available workers?
   - What does RESPONSE look like? One response or streaming?
   - **Gap:** No specification for request/response correlation

3. **Streaming responses unclear:**
   - Acceptance tests mention "streaming response reassembled in order"
   - But no specification for stream format, end marker, or reassembly
   - **Gap:** No specification for streaming protocol

4. **RPC session state unclear:**
   - What happens to pending requests when worker disconnects?
   - What happens to pending requests when client disconnects?
   - **Gap:** No cleanup semantics

**Recommended Addition to CLIENT.md:**

```markdown
### RPC Domain (Request/Response with Worker Pool)

**Purpose:** Scalable request/response pattern with dynamic worker registration.

#### Message Types
| Type | Name | Direction |
|---:|---|---|
| 300 | SUBSCRIBE_WORKER | Client → Server |
| 301 | UNSUBSCRIBE_WORKER | Client → Server |
| 302 | REQUEST | Client → Server |
| 303 | RESPONSE | Client → Server / Server → Client |
| 304 | ACK | Client → Server |

#### SUBSCRIBE_WORKER Request
```
[u64 BE]  family_id
[u32 BE]  route_len
[bytes]   route (e.g., "rpc://realm/area/service")
[u32 BE]  worker_id_len
[bytes]   worker_id

Response: status byte + optional error
```

[... full wire format ...]

#### Semantics
- **Worker Pool:** Multiple workers register on same route
- **Round-robin:** Broker routes requests to available workers
- **Request ID:** Client-generated UUID for correlation
- **Streaming:** RESPONSE may be sent multiple times per request (streaming response)
- **ACK:** Client acknowledges streaming response (or completes streaming)
- **Cleanup:** Worker unsubscribe or disconnect cancels pending requests

#### Acceptance Tests
- worker subscribes, receives requests routed to it
- multiple workers share load
- streaming response sent in order
- client ACK completes interaction
- worker disconnect cancels pending request
```

**Priority:** HIGH (RPC is significantly underspecified)

---

### 18. Domains – Lease ✅ PROPER

**Status:** Complete specification.

**Details:**
- Message types: ACQUIRE, RENEW, RELEASE, QUERY
- Wire formats: All structures defined
- Semantics: Mutual exclusion, fencing tokens, TTL expiry, in-memory
- Error codes: 4001–4004 defined
- Acceptance tests: 6 tests listed

**Verdict:** Complete.

---

### 19. Domains – Schedule ⚠️ NEEDS TWEAKS

**Status:** Partially specified; wire format incomplete.

**Current Issues:**

1. **Schedule payload nested TLV not detailed:**
   - CLIENT.md mentions "nested TLV" with 3 types (cron, target_resource, target_operation)
   - But does not specify how nested TLV is encoded
   - **Gap:** No specification for nested TLV structure (is it flat list? Hierarchical?)

2. **LIST response streaming unclear:**
   - CLIENT.md says "LIST returns one schedule per response. If no schedules, respond with status=0 and `has_schedule_id=0`"
   - Does this mean multiple LIST responses? Or single response with all schedules?
   - **Gap:** No specification for schedule iteration protocol

3. **Task execution model unclear:**
   - Semantics mention "best-effort" timing
   - What happens if broker restarts? Do schedules survive?
   - **Gap:** No specification for persistence, recovery, or execution guarantee

4. **Cron syntax not specified:**
   - Schedule payload includes "cron (UTF-8 string)"
   - But no specification for cron syntax (is it POSIX? Custom?)
   - **Gap:** No specification for supported cron expressions

**Recommended Changes:**

```markdown
### Schedule Domain (Delayed/Recurring Tasks)

**Purpose:** Durable scheduling of delayed tasks and recurring jobs.

#### Cron Syntax (Broker-Specific)

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

Example: `0 9 * * 1` = 9:00 AM every Monday

#### Nested Payload (TLV Structure)

Schedule creation payload is nested TLV:
```
[Type 1: Cron]
  [u32 BE len]
  [UTF-8 string] "0 9 * * *"

[Type 2: Target Resource]
  [u32 BE len]
  [UTF-8 string] "kv://realm/area/resource"

[Type 3: Target Operation]
  [u32 BE len]
  [bytes]        [operation payload]
```

Total payload = concatenated records, no outer length prefix.

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
  [u8]     1 (has_schedule)
  [schedule data...]

Response 2:
  [u8]     0 (status)
  [u8]     1 (has_schedule)
  [schedule data...]

Response N (final):
  [u8]     0 (status)
  [u8]     0 (has_schedule = empty)
```

Client MUST continue reading until `has_schedule=0`.
```

**Priority:** MEDIUM (Schedule is less critical but still underspecified)

---

### 20. Constants & TLV Registry ✅ PROPER

**Status:** Comprehensive MessageType and error code registry.

**Details:**
- MessageType ranges: Control (1), KV (100–108), Stream/Queue (200–206), RPC (300–304), Lease (400–403), Schedule (500–502), Notice (100–104 overlaps KV)
- Channel IDs: Control (0), Pub (1), Sub (2), Rpc (3), Lease (4)
- Type encoding rules: Single byte (0x00–0xFE), escape byte (0xFF) for > 0xFE

**Gaps:**
- Notice domain codes overlap with KV (both use 100–104)
- **Gap:** No specification for how overlap is disambiguated (by route scheme? by MessageType context?)
- **Gap:** No specification for future extensibility (what happens when ranges fill?)

**Recommended Addition:**

```markdown
### MessageType Disambiguation

When MessageType overlaps across domains (e.g., KV 100 = BEGIN, Notice 100 = PUBLISH):
- **Disambiguation:** By route scheme (first segment of route string in request)
- Broker MUST parse route from first TLV field to determine domain
- Same MessageType value in different domains is independent (no collision)

Example:
- `kv://realm/area/resource` with MessageType 100 = KV BEGIN
- `notice://realm/area/resource` with MessageType 100 = Notice PUBLISH

**Future compatibility:** If domain ranges exhaust, extend to new range blocks (e.g., 1100–1199 for KV expansion)
```

**Priority:** LOW (current ranges have capacity; future issue)

---

### 21. Acceptance Criteria ⚠️ NEEDS TWEAKS

**Status:** Good test framework, but Queue/RPC tests incomplete.

**Current Issues:**

1. **Transport tests are complete:**
   - WebSocket connect ✅
   - TCP connect ✅
   - Frame size enforcement ✅
   - Reconnect ✅

2. **Domain tests are incomplete:**
   - Notice: 5 tests listed ✅
   - Stream: 5 tests listed ✅
   - Queue: 5 tests mentioned (lines ~1700 in CLIENT.md) but need detail ⚠️
   - RPC: 5 tests mentioned but INCOMPLETE (no RPC domain spec) ⚠️
   - KV: 5 tests listed ✅
   - Lease: 5 tests listed ✅
   - Schedule: 3 tests listed ✅

3. **Missing interoperability tests:**
   - Multi-realm isolation: Not listed
   - Multi-area isolation: Not listed
   - Permission enforcement: Not listed
   - Reconnect with state restoration: Not listed
   - Subscription fanout scale: Not listed (mentioned in SERVER.md but not in CLIENT.md)

**Recommended Addition:**

```markdown
### Interoperability Tests

**Multi-Realm Isolation:**
- Create two clients with different JWT realms
- One client publishes to realm A, other subscribes in realm B
- Verify no cross-realm delivery

**Permission Enforcement:**
- Client with KV:read scope sends PUT request
- Broker returns ERR_UNAUTHORIZED (domain error)

**Reconnect State:**
- Client subscribes to pattern, closes connection
- Reconnects with same JWT, old subscription is lost
- Client must re-subscribe explicitly

**Fanout Scale:**
- Single PUBLISH to 1000 SUBSCRIBE clients
- All clients receive NOTIFY within 100ms
- No message loss

**Concurrent Requests (Pipelining Rejection):**
- Client sends REQUEST 1 without waiting for response
- Client sends REQUEST 2 while REQUEST 1 pending
- Broker SHOULD close connection or return error (implementation-defined)
```

**Priority:** MEDIUM (tests ensure interoperability; current set is reasonable)

---

### 22. Known Broker-Specific Behaviors ⚠️ NEEDS TWEAKS

**Status:** Placeholder section; needs expansion.

**Current Issues:**

1. **Session ID exposure is mentioned but unclear:**
   - CLIENT.md says "Notice/Stream payloads include session IDs"
   - But no specification for when session IDs appear, how they're encoded, or how to use them
   - **Gap:** No session ID format or semantics

2. **KV/Queue routing context is mentioned but vague:**
   - CLIENT.md says "KV/Queue payloads do not include route; broker derives from envelope/connection context"
   - But how does broker know which resource? Via session state? Via first TLV field?
   - **Gap:** No specification for resource disambiguation

3. **Stream response data is opaque:**
   - Correct, but should note serialization format (e.g., JSON, binary, domain-defined)
   - **Gap:** No guidance for SDK authors

4. **Verb code extensions are future-proofing:**
   - Correct principle, but should provide migration path
   - **Gap:** No specification for how new verbs are deployed without breaking old clients

**Recommended Expansion:**

```markdown
## Implementation Notes (Broker-Specific)

### Session IDs and State Tracking

**When broker tracks session state:**
- Notice subscriptions: Broker maintains per-session subscription list
- Stream sessions: Broker maintains per-session stream offset and metadata
- RPC workers: Broker maintains per-session worker registration

**Session ID lifetime:**
- Issued on CONNECT, unique per connection
- Lost on disconnect (previous session ID becomes invalid)
- NOT returned to client in standard response (internal only, except where specified per domain)

### Resource Disambiguation (KV/Queue)

Some domains derive resource from context rather than explicit payload:
- **KV transactions:** First TLV field after BEGIN is resource name (implicit in transaction context)
- **Queue operations:** Route scheme disambiguates (vs. Stream or RPC)

Clients MUST be aware that breaking connection mid-transaction loses context (transaction auto-rollback).

### Serialization Formats (Domain-Specific)

- **Stream data:** Binary-safe; format broker-defined (client treats as opaque payload)
- **RPC response:** Binary-safe; serialization app-dependent
- **Lease tokens:** Opaque binary; do not parse or modify

### Version Negotiation (Future)

No version negotiation in current protocol. If new verbs are added:
1. New verb codes use next available in range (e.g., 109 for KV)
2. Old clients reject unknown verbs with ERR_UNKNOWN_VERB (domain error)
3. Clients MUST gracefully handle unknown verbs (close connection or error)

Recommended: Brokers should document supported verbs and wire codes in deployment docs.
```

**Priority:** LOW (informational; doesn't affect protocol)

---

## Cross-Document Consistency Issues

### Issue 1: Missing RPC Domain Specification

**Severity:** HIGH

**Details:**
- SERVER.md lists RPC in layer 4 (domains)
- CLIENT.md mentions RPC in routing table and verb set
- But CLIENT.md does NOT include RPC domain section with wire formats
- **Impact:** SDK authors cannot implement RPC without guessing wire format

**Action:** Add RPC domain section to CLIENT.md (see recommendation in section 17)

---

### Issue 2: Queue Domain Partially Specified

**Severity:** HIGH

**Details:**
- CLIENT.md mentions Queue in routing table (SEND, RECEIVE, DELETE, RELEASE, EXTEND)
- But wire format details are minimal
- SERVER.md acceptance tests mention Queue but no detailed spec
- **Impact:** SDK authors unclear on message body format, lease tokens, etc.

**Action:** Add Queue domain section to CLIENT.md with full wire format (see section 16)

---

### Issue 3: Session Lifecycle Not Fully Specified

**Severity:** MEDIUM

**Details:**
- CLIENT.md: Connection Lifecycle section describes 6 steps but lacks detail on confirmation
- SERVER.md: Layer 2 (Session) does not specify session ID generation or state cleanup
- **Gap:** No specification for what happens on disconnect or reconnect
- **Impact:** SDK authors unclear on reconnection logic and subscription re-establishment

**Action:** Add Session Lifecycle detail to both docs (see section 6 recommendations)

---

### Issue 4: Error Codes Not Unified

**Severity:** MEDIUM

**Details:**
- Error codes scattered across domain sections (KV: 1xxx, Stream: 2xxx, Notice: 3xxx, etc.)
- No unified error code registry or allocation rules
- **Gap:** What happens if domain exhausts 100-block range?
- **Impact:** Hard to maintain, extend, or debug

**Action:** Add unified Error Code Allocation section to CLIENT.md (see section 13 recommendations)

---

### Issue 5: Idempotency Matrix Incomplete

**Severity:** MEDIUM

**Details:**
- CLIENT.md lists "safe to retry" operations but does not specify:
  - How to retry (resend exact frame? Different request ID?)
  - How broker deduplicates (does it track request ID?)
  - What about operations that are "safe but not strictly idempotent" (RESERVE)?
- **Gap:** Clients uncertain about retry strategy

**Action:** Add detailed Idempotency & Deduplication section (see section 13 recommendations)

---

### Issue 6: Request/Response Correlation Undefined

**Severity:** MEDIUM

**Details:**
- CLIENT.md says "each request receives one response"
- But does not specify:
  - Are requests pipelined (multiple requests in flight)?
  - If yes, how is response tagged to request?
  - If no, does client block waiting for response?
- **Gap:** Streaming operations (Notice, RPC) suggest asynchronous delivery, but sync model unclear

**Action:** Add Request/Response Correlation section to both docs (see section 13 recommendations)

---

### Issue 7: Nested TLV Format Underspecified

**Severity:** LOW

**Details:**
- Schedule and some other domains mention "nested TLV" payloads
- But format is not fully specified:
  - Is nested TLV flat concatenation? Hierarchical?
  - Is there an outer length wrapper?
  - How do parsers distinguish nested vs. flat?
- **Gap:** SDK authors uncertain about parsing

**Action:** Add Nested TLV section to Wire Protocol section in CLIENT.md (see section 5)

---

## Summary of Recommended Changes

### High Priority (Break Interoperability)

1. **Add RPC Domain Section to CLIENT.md** (~400 lines)
   - Include all 5 message types, wire formats, semantics, acceptance tests
   - Currently missing; impacts any RPC implementation

2. **Expand Queue Domain Section in CLIENT.md** (~300 lines)
   - Add missing wire formats for SEND, RECEIVE, EXTEND, DELETE
   - Clarify leasing model and visibility timeout semantics
   - Include acceptance tests

3. **Add Session Lifecycle Detail** (~100 lines each)
   - Specify session confirmation mechanism
   - Specify session cleanup on disconnect
   - Specify state restoration behavior on reconnect

4. **Add Permission Check Specification** (~80 lines)
   - Specify permission check order in request pipeline
   - Specify permission error codes and format
   - Specify JWT claims structure and validation

### Medium Priority (Affects Implementation Correctness)

5. **Add Unified Error Code Allocation** (~40 lines)
   - Document ranges, expansion strategy, future-proofing
   - Link from Constants section

6. **Add Request/Response Correlation Section** (~60 lines)
   - Clarify synchronous vs. asynchronous model
   - Specify pipelining rules (if any)
   - Specify streaming response protocol

7. **Add Idempotency & Retry Strategy** (~80 lines)
   - Clarify which operations are idempotent, context-dependent, non-idempotent
   - Specify retry strategy for each category
   - Specify request deduplication mechanism (if supported)

8. **Add TLS Enforcement Detail** (~40 lines)
   - Specify certificate validation requirements
   - Specify hostname verification requirements
   - Clarify self-signed cert policy

### Low Priority (Informational)

9. **Expand Known Broker-Specific Behaviors** (~100 lines)
   - Document session ID semantics
   - Document resource disambiguation
   - Document serialization formats
   - Document version negotiation strategy

10. **Add Nested TLV Format Specification** (~30 lines)
    - Clarify nested TLV encoding for complex payloads
    - Show examples (Schedule payload)

---

## Document Quality Metrics

| Aspect | Rating | Notes |
|--------|--------|-------|
| **Completeness** | 75% | RPC/Queue domains missing; session lifecycle vague |
| **Consistency** | 85% | Mostly aligned; error codes scattered; some gaps |
| **Clarity** | 80% | Good use of tables/examples; some sections lack detail |
| **Precision** | 75% | Normative language mostly good; some ambiguous terms |
| **Testability** | 70% | Good acceptance criteria; RPC/Queue tests incomplete |
| **Implementability** | 70% | SDKs can implement most domains; RPC/Queue need clarification |

---

## Risk Assessment

### Critical Risks

1. **RPC specification missing** → SDKs cannot implement RPC functionality
2. **Queue specification incomplete** → Message format ambiguous; wire format unclear
3. **Session lifecycle vague** → Reconnect logic incorrect; subscription re-establishment unclear

### High Risks

4. **Permission checks not specified** → Security model unclear; error handling inconsistent
5. **Error codes scattered** → No allocation strategy; hard to maintain
6. **Request/response correlation undefined** → Pipelining behavior unclear

### Medium Risks

7. **Idempotency matrix incomplete** → Retry strategy unclear
8. **TLS enforcement weak** → Production deployments may lack encryption
9. **Nested TLV underspecified** → Complex payloads may parse incorrectly

---

## Recommendations for SDK Authors

Until gaps are addressed, SDK authors should:

1. **For RPC:**
   - Defer RPC implementation or coordinate with broker maintainers
   - Document RPC wire format as "implementation-specific" until spec is finalized

2. **For Queue:**
   - Request detailed Queue spec or sample broker implementation
   - Test against reference broker to verify wire format

3. **For Session Lifecycle:**
   - Assume synchronous request/response (no pipelining)
   - Assume subscriptions lost on disconnect (require re-subscription)
   - Assume transaction auto-rollback on disconnect

4. **For Error Handling:**
   - Implement graceful degradation for unknown error codes
   - Document broker-specific error codes in SDK documentation

5. **For TLS:**
   - Always use TLS in production
   - Always validate certificates

---

## References

- CLIENT.md: [d:\\repos\\cntryl\\fitz\\docs\\CLIENT.md](d:\\repos\\cntryl\\fitz\\docs\\CLIENT.md)
- SERVER.md: [d:\\repos\\cntryl\\fitz\\docs\\SERVER.md](d:\\repos\\cntryl\\fitz\\docs\\SERVER.md)
- Fitz Repository: https://github.com/cntryl/fitz

