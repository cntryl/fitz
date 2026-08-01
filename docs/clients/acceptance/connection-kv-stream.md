## Connection & Authentication

### AC-CONN-001: WebSocket Connection Establishment

**MUST** successfully establish WebSocket connection to broker
**Given:** Broker running on default port (4090 for HTTP, 4091 for TCP)  
**When:** Client initiates WebSocket connection  
**Then:**

- Connection upgrade succeeds with 101 status
- WebSocket handshake completes
- Client receives connection confirmation

### AC-CONN-002: CONNECT Frame with JWT

**MUST** authenticate with valid JWT
**Given:** Valid JWT with required claims (`sub`, `iss`, `aud`, `exp`, configured route-family identity claim, and one supported permission source: configured custom permissions claim, top-level `permissions`, configured role claim array, `scp`, or `scope`)
**When:** Client sends CONNECT frame as first message after WebSocket upgrade  
**Then:**

- Server authenticates the JWT and establishes internal session state
- Server responds with success (silent acceptance — no explicit ACK)
- Client sends no extra shard or routing metadata
- Client can proceed with domain operations
- Subsequent frames are authorized per normalized route-shaped permissions

### AC-CONN-003: CONNECT Frame Rejection

**MUST** handle authentication rejection
**Given:** Invalid or expired JWT, or JWT with missing, unmapped, or unprovisioned route-family identity context
**When:** Client sends CONNECT frame  
**Then:**

- Server closes connection with error message
- Client receives close reason: "connect failed: <reason>"
- Client does NOT retry with same JWT

### AC-CONN-004: Anonymous Mode (When Enabled)

**MUST** handle anonymous access when `FITZ_AUTH_REQUIRED=false`
**Given:** Broker running with `FITZ_AUTH_REQUIRED=false`  
**When:** Client sends CONNECT frame with empty/invalid JWT  
**Then:**

- Server accepts connection
- Client has full access to all domains
- No permission errors occur

### AC-CONN-005: Frame Before Authentication

**MUST NOT** send non-CONNECT frames before authentication
**Given:** New connection, not yet authenticated  
**When:** Client attempts to send domain frame (KV, Notice, etc.)  
**Then:**

- Server closes connection immediately
- Connection terminates with "unauthenticated: connect required"

### AC-CONN-006: Rebuild client-owned state on reconnect

**MUST** re-establish reconnect-safe state after reconnect; clients **MUST NOT** assume broker session state persists across disconnects

**Given:** Client had active Notice, Queue, Lease, Stream, or Schedule subscriptions, RPC worker registrations, or session-bound handles before disconnect

**When:** Connection is lost and client reconnects and re-authenticates

**Then:**

- Client **MUST** re-send SUBSCRIBE frames for Notice, Queue availability, Lease change, Stream commit, and Schedule fire subscriptions that the application still wants active
- Client **MUST** re-send RPC worker registration frames for workers that the application still wants active
- Subscription and worker state is **NOT** preserved by broker across disconnects
- SUBSCRIBE is idempotent - duplicate subscription to same pattern returns same subscription_id
- Client resumes receiving notifications or RPC worker requests only after re-subscription or re-registration is confirmed
- KV transactions and Stream append sessions open during disconnect are invalidated and MUST NOT be reused
- Queue item handles and Lease handles issued before disconnect are invalidated; clients MUST reserve or acquire again instead of extending or completing old handles
- RPC calls pending during disconnect fail promptly with a connection/interruption error and MUST NOT silently stall
- Stream replay resumes only from client-owned offsets. Stream live subscriptions are wake signals, not durable replay cursors.

## KV Domain

### AC-KV-001: Transaction Lifecycle (Begin → Commit)

**MUST** complete full transaction lifecycle
**Given:** Authenticated session with `kv://**#write` permission  
**When:**

1. Client sends `Begin(read_write, buffered)`
2. Server responds with `tx_id`
3. Client sends `Put(tx_id, key, value)`
4. Client sends `Commit(tx_id)`
   **Then:**

- Begin succeeds, returns valid `tx_id`
- Put succeeds
- Commit succeeds
- Data persists after commit

### AC-KV-002: Transaction Rollback

**MUST** rollback transaction on request
**Given:** Active transaction with uncommitted writes  
**When:**

1. Client sends `Put(tx_id, key, value)`
2. Client sends `Rollback(tx_id)`
3. Client begins new transaction and reads same key
   **Then:**

- Rollback succeeds
- Previous write is NOT visible in new transaction
- Key either doesn't exist or has pre-transaction value

### AC-KV-003: Get Existing Key

**MUST** retrieve existing key-value pair
**Given:** Transaction with `Put(key="user:123", value="Alice")` committed  
**When:** Client sends `Get(tx_id, "user:123")`  
**Then:**

- Server returns `GetResult::Found(value="Alice")`
- Value matches committed data exactly

### AC-KV-004: Get Non-Existent Key

**MUST** handle missing keys correctly
**Given:** Key "user:999" does not exist  
**When:** Client sends `Get(tx_id, "user:999")`  
**Then:**

- Server returns `GetResult::NotFound`
- Client does NOT throw exception (this is valid response)

### AC-KV-005: Insert Existing Key

**MUST** reject insert when key exists
**Given:** Key "counter" already exists in storage  
**When:** Client sends `Insert(tx_id, "counter", "1")`  
**Then:**

- Server returns error code `1006` (ERR_KEY_EXISTS)
- Transaction remains active (can rollback)

### AC-KV-006: Delete Existing Key

**MUST** delete key within transaction
**Given:** Transaction with existing key "temp:data"  
**When:**

1. Client sends `Delete(tx_id, "temp:data")`
2. Client sends `Commit(tx_id)`
3. Client begins new transaction and reads "temp:data"
   **Then:**

- Delete succeeds
- After commit, key is not found

### AC-KV-007: Scan Range

**MUST** scan key range with prefix
**Given:** Keys exist: `"user:001"`, `"user:002"`, `"user:010"`  
**When:** Client sends `Scan(tx_id, start="user:001", end="user:010", limit=100)`  
**Then:**

- Server returns 2 keys: `"user:001"`, `"user:002"`
- Keys are in lexicographic order
- `"user:010"` is excluded (end is exclusive)

### AC-KV-008: Reverse Scan

**MUST** scan in reverse order
**Given:** Keys: `"a"`, `"b"`, `"c"`  
**When:** Client sends `Scan(tx_id, start="c", end="a", limit=100)` (reverse range)  
**Then:**

- Server returns keys in reverse: `["c", "b"]`
- `"a"` is excluded (end is exclusive)

### AC-KV-009: Transaction Scope Isolation

**MUST** enforce transaction-resource binding
**Given:** Transaction began with resource `"users"`  
**When:** Client attempts operation on resource `"posts"` with same `tx_id`  
**Then:**

- Server returns error (transaction scope violation)
- Transaction is invalidated or operation rejected

### AC-KV-010: Unauthorized Write

**MUST** reject write without permission
**Given:** Session has `kv://realm/area/**#read` (read-only)  
**When:** Client sends `Put(tx_id, key, value)`  
**Then:**

- Server returns error code `1011` (ERR_UNAUTHORIZED)
- Write does NOT occur

### AC-KV-011: Delete Range with Invalid Bounds

**MUST** reject invalid range bounds
**Given:** Active transaction  
**When:** Client sends `DeleteRange(tx_id, start="z", end="a")` (inverted range)  
**Then:**

- Server returns error (invalid range bounds)
- No keys deleted

**Note:** The specific error code for invalid range bounds is broker-defined within the 1xxx KV range.

### AC-KV-012: Unified change-watch registration contract

**MUST** implement KV subscriptions with the shared wildcard contract
**Given:** An authenticated session
**When:** The client subscribes or unsubscribes using exact, `*`, `**`, or
wildcard-realm KV patterns capable of matching three segments
**Then:**

- Exact and overlapping registrations remain independent and notifications carry the exact concrete KV route
- Duplicate original registration strings return the existing identifier, including at the 128-wildcard cap
- Exact registrations still succeed at that cap; the 129th distinct wildcard returns 1013
- Wrong schemes, empty segments, partial wildcards, and impossible depths return 1012 on both TCP and WebSocket
- Matching and cleanup remain isolated by `RouteFamily`, and disconnect removes the session's registrations
- KV mutations continue to reject wildcard routes

## Stream Domain

### AC-STREAM-001: Append to Stream

**MUST** append message to stream through a session
**Given:** Session with `stream://realm/area/resource#write` permission
**When:**

1. Client sends `Begin(route="stream://prod/logs/events")`
2. Server returns `session_id`
3. Client sends `Append(session_id, expected_offset=0, payload="event1")`
4. Client sends `Commit(session_id, mode=Sync)`
**Then:**

- Server returns success for BEGIN, APPEND, and COMMIT
- Offset is monotonically increasing
- Message is durable after COMMIT acknowledgment

### AC-STREAM-002: Read from Offset

**MUST** read messages starting from offset
**Given:** Stream contains messages at offsets 0, 1, 2  
**When:** Client sends `Read(realm, area, resource, start_offset=1, limit=10)`  
**Then:**

- Server returns messages at offsets 1, 2
- Messages are in order
- Offset 0 is NOT included

### AC-STREAM-003: Read Non-Existent Offset

**MUST** handle read beyond stream end
**Given:** Stream's highest offset is 5  
**When:** Client sends `Read(start_offset=100, limit=10)`  
**Then:**

- Server returns empty result set
- No error (valid state)

### AC-STREAM-004: Session-Based Writes

**MUST** use session for durable write tracking
**Given:** Client creates session with `Begin(route)`  
**When:**

1. Server returns `session_id` (u64)
2. Client sends `Append(session_id, expected_offset=0, payload="event1")`
3. Client sends `Append(session_id, expected_offset=1, payload="event2")`
4. Client sends `Commit(session_id, mode=Sync)`
   **Then:**

- All appends are batched within session
- Commit makes writes durable
- session_id is opaque u64 (client treats as cookie)

### AC-STREAM-005: Unauthorized Append (renumbered)

**MUST** reject append without write permission
**Given:** Session JWT has no `stream:write` scope for target route  
**When:** Client sends `Append(session_id, payload)`  
**Then:**

- Server returns error code `2009` (Unauthorized)
- Message NOT appended to stream

### AC-STREAM-006: Append with discriminator sidecar

**MUST** preserve optional append discriminators for filtered replay
**Given:** Session with `stream://realm/area/resource#write` permission
**When:**

1. Client sends `Begin(route="stream://prod/logs/events")`
2. Server returns `session_id`
3. Client sends `Append(session_id, expected_offset=0, payload="event1", discriminator="proj.alpha")`
4. Client sends `Commit(session_id, mode=Sync)`
5. Client sends `Read(route="stream://prod/logs/events", start_offset=0, limit=10, filter=StreamFilterSet(clauses=[Equals("proj.alpha")]))`
   **Then:**

- Committed record is readable through the matching filter
   - Server may emit synthetic filtered markers for skipped committed offsets, and the cursor still advances through them
- Discriminator does not affect offset ordering or durability
- Client APIs MAY omit discriminator when unused

### AC-STREAM-007: Filtered read by discriminator

**MUST** filter replay results by the supplied discriminator clauses
**Given:** Stream contains records with discriminators `proj.alpha` and `audit.beta`
**When:** Client sends `Read(route="stream://prod/logs/events", start_offset=0, limit=10, filter=StreamFilterSet(clauses=[StartsWith("proj.")]))`
**Then:**

- Server returns matching records in offset order and may emit synthetic filtered markers for skipped offsets
- Client cursor progression remains monotonic even when some offsets are filtered out
- Missing discriminators are treated as empty strings for matching
- Client APIs MAY omit filter when replay filtering is not needed

**Wire shape note:** The request layout is `route`, `from_offset`, `limit`, `has_max_bytes`, optional `max_bytes`, `has_filter`, optional `filter_len`, and raw `StreamFilterSet` bytes. The filter bytes use the same stream filter codec defined in `client-spec.md`.

### AC-STREAM-008: Filtered replay cursor progression

**MUST** preserve offset progress even when replay hides some committed records
**Given:** Stream contains committed records where only some match the supplied filter
**When:** Client sends `Read(route="stream://prod/logs/events", start_offset=0, limit=10, filter=StreamFilterSet(clauses=[Equals("proj.alpha")]))`
**Then:**

- Returned read page may contain `event`, `filtered`, or `filtered_range` items
- `filtered.offset` reflects the actual skipped committed offset
- Event-only convenience APIs MAY flatten filtered items away, but they MUST preserve cursor semantics

### AC-STREAM-009: Filter compatibility error contract

**MUST** return a typed stream error for unsupported or malformed filter payloads without dropping the connection
**Given:** Client sends `Read(..., filter=...)` using a filter payload the broker cannot decode
**When:** Filter marker/version is unsupported or filter bytes are malformed
**Then:**

- Server returns stream error `ERR_STREAM_FILTER_UNSUPPORTED_VERSION` (2006) for unsupported marker/version
- Server returns stream error `ERR_STREAM_FILTER_INVALID_PAYLOAD` (2007) for malformed filter bytes
- Server keeps the session/transport open so a subsequent valid request can succeed

**Filter codec contract:** `StreamFilterSet` is an opaque client-visible value type, but the wire encoding is fixed and versioned. Clients MUST keep the marker, clause tags, and length prefixes aligned with `client-spec.md`; if the broker responds with 2006 or 2007, the client MUST surface the error and continue using the same connection only for subsequent valid requests.

### AC-STREAM-010: Subscribe to resource commit notifications

**MUST** receive notifications when events are committed to a subscribed resource
**Given:** Session with `stream://realm/area/resource#read` permission  
**When:**

1. Client sends `STREAM_SUBSCRIBE` (607) with pattern `stream://realm/area/resource`
2. Server responds with `subscription_id`
3. Another client commits events to that resource
   **Then:**

- Client receives `STREAM_NOTIFY` (609) with `event="committed"` plus resource, area, realm offset ranges and `batch_size`
- Subscription is session-scoped (lost on disconnect)

### AC-STREAM-011: Subscribe with area wildcard

**MUST** receive notifications for any resource committed within a subscribed area
**Given:** Session with `stream://realm/area/**#read` permission  
**When:**

1. Client sends `STREAM_SUBSCRIBE` (607) with pattern `stream://realm/area/*`
2. Events are committed to `stream://realm/area/resource-a` and `stream://realm/area/resource-b`
   **Then:**

- Client receives `STREAM_NOTIFY` (609) for both resources

### AC-STREAM-013: Optimistic concurrency (expected_offset) at Append

**MUST** reject Append when expected_offset does not match server's next offset
**Given:** Stream with at least one committed record (server's next offset = 1)  
**When:** Client sends `Append(session_id, expected_offset=99999, payload="event2")`  
**Then:**

- Server returns error (status=1) with message indicating concurrency conflict (e.g. containing "conflict")
- No new record is appended
- Clients MUST send expected_offset on every Append; servers MUST enforce it

### AC-STREAM-012: Unsubscribe stops delivery

**MUST** stop receiving notifications after unsubscribe
**Given:** Client subscribed with pattern `stream://realm/area/resource`  
**When:**

1. Client sends `STREAM_UNSUBSCRIBE` (608) with the same pattern
2. Events are committed to that resource
   **Then:**

- Client does NOT receive `STREAM_NOTIFY` for that pattern

### AC-STREAM-014: Session-scoped cleanup

**MUST** clean up all stream subscriptions on disconnect
**Given:** Client has active `STREAM_SUBSCRIBE` subscriptions  
**When:** Client disconnects (graceful or ungraceful)  
**Then:**

- All `STREAM_SUBSCRIBE` subscriptions for that session are removed
- No `STREAM_NOTIFY` frames are sent after disconnect

### AC-STREAM-015: Unified live-registration validation and quota

**MUST** apply the shared subscription contract to Stream live watches
**Given:** An authenticated session
**When:** The client registers exact, `*`, `**`, overlapping, or wildcard-realm
patterns capable of matching a three-segment Stream route
**Then:**

- Notifications contain the matching identifier and exact concrete Stream route
- Duplicate registration is idempotent and is checked before the 128-wildcard limit
- Exact registration succeeds at the wildcard cap; the 129th distinct wildcard returns 2011
- Invalid subscribe and unsubscribe patterns return 2010 on TCP and WebSocket
- Matching and disconnect cleanup stay isolated by `RouteFamily`
- Stream writes stay concrete; READ retains its separately documented pattern support

## Queue Domain
