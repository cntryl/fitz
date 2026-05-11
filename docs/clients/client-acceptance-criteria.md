# Fitz Client Acceptance Criteria

**Version:** 1.0  
**Date:** February 3, 2026  
**Purpose:** Explicit acceptance criteria for client implementations to verify conformance with Fitz protocol
This document defines testable acceptance criteria for each Fitz domain. Client implementations MUST pass all criteria marked as **MUST**. Criteria marked as **SHOULD** are strongly recommended for production-grade clients.

For cross-language parity enforcement across fitz-go, fitz-ts, and fitz-py, run these companion artifacts:

- `cross-language-conformance-suite.yaml`
- `cross-language-conformance-runner.md`

## Table of Contents

- [Connection & Authentication](#connection--authentication)
- [KV Domain](#kv-domain)
- [Stream Domain](#stream-domain)
- [Queue Domain](#queue-domain)
- [Notice Domain](#notice-domain)
- [RPC Domain](#rpc-domain)
- [Lease Domain](#lease-domain)
- [Schedule Domain](#schedule-domain)
- [Error Handling](#error-handling)
- [Performance](#performance)

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
**Given:** Valid JWT with required claims (`sub`, `iss`, `aud`, `scopes`, `exp`, optional for multitenant scenarios `tid` or `tenant_id`)  
**When:** Client sends CONNECT frame as first message after WebSocket upgrade  
**Then:**

- Server authenticates the JWT and establishes internal session state
- Server responds with success (silent acceptance — no explicit ACK)
- Client sends no extra shard or routing metadata
- Client can proceed with domain operations
- Subsequent frames are authorized per JWT `scopes`

### AC-CONN-003: CONNECT Frame Rejection

**MUST** handle authentication rejection
**Given:** Invalid or expired JWT  
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

### AC-CONN-006: Re-subscribe on reconnect

**MUST** re-establish subscriptions after reconnect; clients **MUST NOT** assume subscriptions persist across disconnects
**Given:** Client had active Notice, RPC, Stream, or Schedule subscriptions before disconnect  
**When:** Connection is lost and client reconnects and re-authenticates  
**Then:**

- Client **MUST** re-send SUBSCRIBE frames for Notice/RPC/Stream/Schedule subscriptions
- Subscription state is **NOT** preserved by broker across disconnects
- SUBSCRIBE is idempotent - duplicate subscription to same pattern returns same subscription_id
- Client resumes receiving notifications/requests only after re-subscription confirmed
- In-flight operations (KV transactions, Stream append sessions, Queue inflight entries) are lost on disconnect

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

### AC-STREAM-008: Filtered replay cursor progression

**MUST** preserve offset progress even when replay hides some committed records
**Given:** Stream contains committed records where only some match the supplied filter
**When:** Client sends `Read(route="stream://prod/logs/events", start_offset=0, limit=10, filter=StreamFilterSet(clauses=[Equals("proj.alpha")]))`
**Then:**

- Returned read page may contain `event`, `filtered`, or `filtered_range` items
- `filtered.offset` reflects the actual skipped committed offset
- Event-only convenience APIs MAY flatten filtered items away, but they MUST preserve cursor semantics

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

## Queue Domain

### AC-QUEUE-001: Enqueue Message

**MUST** enqueue message to queue
**Given:** Session with `queue://realm/area/resource#write` permission  
**When:** Client sends `Enqueue(realm, area, resource, payload="task1")`  
**Then:**

- Server returns `EnqueueOk(message_id=<uuid>)`
- Message is durable
- Message is available for reservation

### AC-QUEUE-002: Reserve Message

**MUST** reserve message from queue
**Given:** Queue contains 1 message  
**When:** Client sends `Reserve(realm, area, resource, lease_seconds=30, count=1)`  
**Then:**

- Server returns `ReservedOk(messages=[{id, payload, token}])`
- Message includes a queue inflight token
- Message is invisible to other consumers during the inflight reservation

### AC-QUEUE-003: Complete Message

**MUST** complete (acknowledge) message
**Given:** Message reserved with token `T1`  
**When:** Client sends `Complete(realm, area, resource, message_id, token=T1)`  
**Then:**

- Server returns `CompleteOk`
- Message is permanently removed from queue
- Message does NOT reappear after lease expires

### AC-QUEUE-004: Extend Message Lease

**MUST** extend an inflight reservation before expiration
**Given:** Message reserved with 30s inflight window, 20s elapsed  
**When:** Client sends `Extend(message_id, token, extend_seconds=30)`  
**Then:**

- Server returns `ExtendOk`
- Inflight reservation is extended by 30s (now 40s remaining)
- Message remains invisible to other consumers

### AC-QUEUE-005: Message Redelivery on Lease Expiration

**MUST** redeliver message after the inflight reservation expires
**Given:** Message reserved with 5s inflight window, client does not complete  
**When:**

1. Client waits for the inflight TTL to expire (e.g. 5s) plus a short margin (servers may process expiry lazily on the next operation)
2. Same or another client sends `Reserve()` on that queue
   **Then:**

- The message is returned to the ready queue and can be reserved again
- New reserve returns the same message with a new inflight token

### AC-QUEUE-006: Invalid Token Rejection (renumbered)

**MUST** reject complete with wrong token
**Given:** Message reserved with token `T1`  
**When:** Client sends `Complete(message_id, token=<invalid_u64>)`  
**Then:**

- Server returns error code `4001` (ERR_INVALID_TOKEN)
- Message remains in queue
- Lease remains active

### AC-QUEUE-007: Delayed Message Visibility (renumbered)

**MUST** delay message visibility
**Given:** Client enqueues message with `visibility_delay=10s`  
**When:**

1. Client sends `Enqueue(payload, visibility_delay=10)`
2. Another client immediately sends `Reserve()`
3. Client waits 11s and sends `Reserve()` again
   **Then:**

- First reserve returns empty (message invisible)
- Second reserve returns message (now visible)

### AC-QUEUE-008: Unauthorized Enqueue (renumbered)

**MUST** reject enqueue without write permission
**Given:** Session has `queue://realm/area/**#read` (read-only)  
**When:** Client sends `Enqueue(payload)`  
**Then:**

- Server returns error code `4009` (Unauthorized)
- Message NOT enqueued

## Notice Domain

### AC-NOTICE-001: Subscribe to Route Pattern

**MUST** subscribe to notice route pattern
**Given:** Session JWT includes `notice:read` scope  
**When:** Client sends `Subscribe(route_pattern="notice://prod/orders/create")`  
**Then:**

- Server returns `SubscribeOk(subscription_id=<u64>)`
- Client stores subscription_id for this pattern
- Client can receive NOTIFY frames with this subscription_id
- Repeat SUBSCRIBE with same pattern returns same subscription_id (idempotent)

### AC-NOTICE-002: Publish to Route

**MUST** publish notice to subscribers
**Given:** Client A subscribed to `notice://prod/orders/create` with subscription_id=123  
**When:** Client B publishes to `notice://prod/orders/create` with payload `"order:456"`  
**Then:**

- Server delivers NOTIFY(subscription_id=123, route="notice://prod/orders/create", payload="order:456") to Client A
- Payload matches published data
- Delivery occurs within reasonable time (< 100ms)

### AC-NOTICE-003: Wildcard Subscription (Single Star)

**MUST** match single-segment wildcard
**Given:** Client subscribes to `notice://prod/orders/*`  
**When:**

1. Another client publishes to `notice://prod/orders/create`
2. Another client publishes to `notice://prod/orders/update`
   **Then:**

- Client receives BOTH notifications
- `*` matches single segment (`create`, `update`)

### AC-NOTICE-004: Wildcard Subscription (Double Star)

**MUST** match multi-segment wildcard
**Given:** Client subscribes to `notice://prod/**`  
**When:**

1. Client publishes to `notice://prod/orders/create`
2. Client publishes to `notice://prod/inventory/update`
   **Then:**

- Subscriber receives BOTH notifications
- `**` matches any depth under `prod/`

### AC-NOTICE-005: Unsubscribe

**MUST** stop receiving after unsubscribe
**Given:** Client subscribed with subscription_id=123  
**When:**

1. Client sends `Unsubscribe(subscription_id=123)`
2. Another client publishes to matching route
   **Then:**

- Client does NOT receive notification
- Server confirms unsubscribe

### AC-NOTICE-006: Client-Side Multiplexing

**MUST** support multiple local handlers for same subscription
**Given:** Client library API allows registering multiple handlers  
**When:**

1. App registers handler1 for pattern `notice://prod/orders/*`
2. App registers handler2 for same pattern `notice://prod/orders/*`
3. Both use same underlying subscription_id
4. Server sends NOTIFY for `notice://prod/orders/create`
   **Then:**

- Client receives one NOTIFY frame (subscription_id=X)
- Client routes to BOTH handler1 and handler2 locally
- UNSUBSCRIBE uses reference counting (only sent when both handlers unsubscribe)

### AC-NOTICE-007: Realm Isolation (renumbered)

**MUST** isolate notifications by realm
**Given:**

- Client A subscribes to `notice://prod/**`
- Client B subscribes to `notice://staging/**`
  **When:** Client publishes to `notice://prod/orders/create`  
  **Then:**
- Client A receives notification
- Client B does NOT receive notification (different realm)

### AC-NOTICE-008: Fanout to Multiple Subscribers (renumbered)

**MUST** deliver to all matching subscribers
**Given:**

- Client A subscribes to `notice://prod/orders/create`
- Client B subscribes to `notice://prod/orders/*`
- Client C subscribes to `notice://prod/**`
  **When:** Client publishes to `notice://prod/orders/create`  
  **Then:**

- All 3 clients (A, B, C) receive NOTIFY
- Each receives with their own subscription_id
- Delivery is concurrent (no serialization)

### AC-NOTICE-009: Unauthorized Publish (renumbered)

**MUST** reject publish without write permission
**Given:** Session JWT has no `notice:write` scope  
**When:** Client sends `Publish(route, payload)`  
**Then:**

- Server returns error code `3009` (ERR_UNAUTHORIZED)
- No notifications delivered
  **Notes:**
- Fitz notices are **best-effort, non-durable signals**. Clients **MUST NOT** assume guaranteed delivery, ordering across reconnects, or replay after disconnect.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage constraint:** Notices **MUST NOT** be used for workflow coordination, acknowledgement, or durability guarantees; use RPC or Queue for those needs.

## RPC Domain

### AC-RPC-001: Worker Registration

**MUST** register as RPC worker
**Given:** Session JWT includes `rpc:read` and `rpc:write` scopes  
**When:** Client sends `Subscribe(route_pattern="rpc://prod/users/validate")`  
**Then:**

- Server returns `SubscribeOk(subscription_id=<u64>)`
- Client is registered as worker
- Client receives REQUEST frames with this subscription_id
- Idempotent: repeat SUBSCRIBE returns same subscription_id

### AC-RPC-002: RPC Call and Response

**MUST** complete RPC request-response cycle
**Given:** Worker registered for `rpc://prod/users/validate` with subscription_id=456  
**When:**

1. Caller sends `Call(route="rpc://prod/users/validate", payload="user:123", timeout=5s)`
2. Worker receives REQUEST(subscription_id=456, correlation_id=<16 bytes>, payload)
3. Worker sends `Reply(correlation_id, result="valid")`
   **Then:**

- Caller receives response within timeout
- Response payload matches worker's reply
- correlation_id is fixed 16 bytes (UUID)

### AC-RPC-003: RPC Timeout

**MUST** timeout when no worker responds
**Given:** A worker accepted `rpc://prod/users/check` and never replied  
**When:** Client sends `Call(route, timeout=2s)` and waits  
**Then:**

- After 2 seconds, client receives error code `6001` (ERR_RPC_TIMEOUT)
- Request is abandoned
- Client can retry or handle error

### AC-RPC-004: Multiple Workers (Load Balancing)

**MUST** distribute requests across workers
**Given:**

- Worker A registered for `rpc://prod/tasks/process`
- Worker B registered for `rpc://prod/tasks/process`
  **When:** Caller sends 10 RPC calls to same route  
  **Then:**
- Requests are distributed to both workers (not all to one)
- Distribution is approximately even (5:5 or 4:6)

### AC-RPC-005: Chunked Response

**MUST** handle multi-chunk responses
**Given:** Worker sends response larger than single frame limit  
**When:**

1. Worker sends `Response(correlation_id, sequence=0, body=data1, stream_end=false)`
2. Worker sends `Response(correlation_id, sequence=1, body=data2, stream_end=false)`
3. Worker sends `Response(correlation_id, sequence=2, body=data3, stream_end=true)`
   **Then:**

- Caller receives complete response after all chunks arrive
- Chunks are reassembled in order
- Data integrity is maintained

### AC-RPC-006: Worker Unregister

**MUST** stop receiving requests after unregister
**Given:** Worker registered with subscription_id=789  
**When:**

1. Worker sends `Unsubscribe(subscription_id=789)`
2. Caller sends RPC request to same route
   **Then:**

- Worker does NOT receive request
- Request routes to other workers OR times out if none available

### AC-RPC-007: Unauthorized Worker Registration

**MUST** reject worker registration without admin permission
**Given:** Session has `rpc://prod/tasks/**#read` (no admin/`*`)  
**When:** Client sends `Subscribe(route)` to register as worker  
**Then:**

- Server returns error code `6009` (Unauthorized)
- Client is NOT registered as worker

### AC-RPC-008: Unauthorized RPC Call

**MUST** reject call without write permission
**Given:** Session has no `rpc://` permissions  
**When:** Client sends `Call(route, payload)`  
**Then:**

- Server returns error code `6009` (Unauthorized)
- Request is NOT forwarded to workers

## Lease Domain

### AC-LEASE-001: Acquire Lease

**MUST** acquire unowned lease
**Given:** Lease `"lock:resource:123"` is not held by anyone  
**When:** Client sends `Acquire(route="lease://prod/locks/resource:123", ttl=30)`  
**Then:**

- Server returns `AcquireOk(token=<u64>, fencing_token=<u64>)`
- token is opaque u64 (client treats as cookie)
- fencing_token is u64 (monotonically increasing)
- Lease expires after 30 seconds if not renewed

### AC-LEASE-002: Lease Conflict

**MUST** reject acquire when lease held by other
**Given:** Client A holds lease with token `T1`  
**When:** Client B sends `Acquire(same_route, ttl=30)`  
**Then:**

- Server returns error (Already Held)
- Client B does NOT acquire lease
- Client A retains lease

### AC-LEASE-003: Renew Lease

**MUST** extend lease before expiration
**Given:** Client holds lease with token `T1`, TTL=30s, 20s elapsed  
**When:** Client sends `Renew(token=T1, extend_seconds=30)`  
**Then:**

- Server returns `RenewOk(new_expiration)`
- Lease is extended (now 40s remaining)
- Fencing token remains unchanged

### AC-LEASE-004: Release Lease

**MUST** voluntarily release lease
**Given:** Client holds lease with token `T1`  
**When:** Client sends `Release(token=T1)`  
**Then:**

- Server returns `ReleaseOk`
- Lease is released immediately
- Other clients can now acquire same lease

### AC-LEASE-005: Automatic Expiration

**MUST** release lease after TTL expires
**Given:** Client acquires lease with TTL=5s, does not renew  
**When:**

1. Client waits 6 seconds
2. Another client sends `Acquire(same_route)`
   **Then:**

- Second client successfully acquires lease
- New fencing token is higher than previous

### AC-LEASE-006: Idempotent Acquire

**MUST** return existing token on duplicate acquire
**Given:** Client holds lease with token `T1` (u64), fencing_token `123`  
**When:** Same client sends `Acquire(same_route)` again  
**Then:**

- Server returns same token `T1`, fencing_token `123`
- Lease TTL is NOT reset
- No new lease is created

### AC-LEASE-007: Monotonic Fencing Tokens

**MUST** issue increasing fencing tokens
**Given:** Multiple acquire/release cycles on same lease  
**When:**

1. Client A acquires (gets fencing token 1)
2. Client A releases
3. Client B acquires (gets fencing token 2)
4. Client B releases
5. Client C acquires
   **Then:**

- Client C receives fencing token 3
- Tokens are strictly increasing (1 < 2 < 3)

### AC-LEASE-008: Query Lease Status

**MUST** query current lease holder
**Given:** Session with `lease://realm/area/**#read` permission  
**When:** Client sends `Query(route="lease://prod/locks/resource:123")`  
**Then:**

- Server returns lease status:
  - If held: holder ID, expiration time, fencing token
  - If free: status = "available"

### AC-LEASE-009: Invalid Token Rejection

**MUST** reject operations with wrong token
**Given:** Lease held by Client A with token `T1`  
**When:** Client B sends `Renew(token="WRONG")`  
**Then:**

- Server returns error code `5005` (Invalid Token)
- Lease state unchanged

### AC-LEASE-010: Unauthorized Acquire

**MUST** reject acquire without write permission
**Given:** Session has `lease://prod/locks/**#read` (read-only)  
**When:** Client sends `Acquire(route, ttl)`  
**Then:**

- Server returns error code `5009` (Unauthorized)
- Lease NOT granted

## Schedule Domain

### AC-SCHEDULE-001: Create Scheduled Job

**MUST** create job with cron expression
**Given:** Session with `schedule://realm/area/**#write` permission  
**When:** Client sends `Create(route="schedule://prod/jobs/backup", cron="0 2 * * *", payload="backup-db")`  
**Then:**

- Server returns `CreateOk(job_id)`
- Job is persisted
- Job will trigger at 2:00 AM daily

### AC-SCHEDULE-002: Cron Expression Validation

**MUST** reject invalid cron expressions
**Given:** Client attempts to create job  
**When:** Client sends `Create(cron="invalid syntax")`  
**Then:**

- Server returns error code `7002` (Invalid Cron)
- Job is NOT created

### AC-SCHEDULE-003: Job Execution Notification via SCHEDULE_SUBSCRIBE / SCHEDULE_NOTIFY

**MUST** receive notification when schedule fires
**Given:**

- Job created with cron `"*/1 * * * *"` (every minute) on route `schedule://prod/app/reminders`
- Client sends `SCHEDULE_SUBSCRIBE` (703) to `schedule://prod/app/reminders`
  **When:** Time advances to next minute boundary  
  **Then:**
- Client receives `SCHEDULE_NOTIFY` (705) with the job's configured payload
- The broker also executes the schedule's `target_resource` via the `DomainPublishEvent` system
- Payload matches job's configured payload
- Notification arrives within 1 second of scheduled time

### AC-SCHEDULE-004: Cancel Job (renumbered)

**MUST** cancel scheduled job
**Given:** Job exists with `job_id=J1`  
**When:** Client sends `Cancel(job_id=J1)`  
**Then:**

- Server returns `CancelOk`
- Job no longer fires
- Future scheduled times do not trigger notifications

### AC-SCHEDULE-005: List Jobs (renumbered)

**MUST** retrieve all jobs for realm/area
**Given:** Jobs exist for `schedule://prod/jobs/*`  
**When:** Client sends `List(realm="prod", area="jobs")`  
**Then:**

- Server returns list of jobs with:
  - Job ID
  - Cron expression
  - Next scheduled time
  - Payload

### AC-SCHEDULE-006: Cron Wildcards (renumbered)

**MUST** support wildcard expressions
**Given:** Job with cron `"* * * * *"` (every minute)  
**When:** Time advances through multiple minutes  
**Then:**

- Job fires every minute
- No missed executions (within 1s tolerance)

### AC-SCHEDULE-007: Cron Ranges and Lists (renumbered)

**MUST** support range and list syntax
**Given:** Job with cron `"0 9-17 * * 1-5"` (9 AM to 5 PM, Mon-Fri)  
**When:** Time is Monday 10:00 AM  
**Then:** Job fires
**When:** Time is Saturday 10:00 AM  
**Then:** Job does NOT fire
**When:** Time is Monday 8:00 AM  
**Then:** Job does NOT fire

### AC-SCHEDULE-008: Unauthorized Create (renumbered)

**MUST** reject job creation without write permission
**Given:** Session JWT has no `schedule:write` scope  
**When:** Client sends `Create(route, cron, payload)`  
**Then:**

- Server returns error code `7009` (ERR_UNAUTHORIZED)
- Job NOT created

## Error Handling

### AC-ERROR-001: TLV Parse Errors

**MUST** handle malformed TLV frames gracefully
**Given:** Client sends invalid TLV (incorrect length field)  
**When:** Server receives malformed frame  
**Then:**

- Server closes connection with parse error
- Client logs error and does NOT retry same malformed data
- **Duplicate TLV tags are NOT permitted.** If a TLV tag appears more than once the frame **MUST** be treated as malformed and the server **MUST** close the connection with a parse error. **Rationale:** Disallowing duplicate tags keeps decoding deterministic and simplifies client implementations. Clients **MUST NOT** send duplicate tags.

### AC-ERROR-002: Domain Error Codes

**MUST** correctly parse domain-specific error codes
**Given:** Client sends unauthorized operation  
**When:** Server returns error with domain-specific code (e.g., `4009` for Queue)  
**Then:**

- Client recognizes error code format: `XXYY` where `XX` = domain, `YY` = error
- Client maps to appropriate error type (Unauthorized)
- Client does NOT misinterpret as different error

### Error Code Ranges (Normative)

| Domain   | Code range |
| -------- | ---------- |
| KV       | 1000-1999  |
| Stream   | 2000-2999  |
| Notice   | 3000-3999  |
| Queue    | 4000-4999  |
| Lease    | 5000-5999  |
| RPC      | 6000-6999  |
| Schedule | 7000-7999  |

Clients **MUST** interpret error codes using this mapping.

### AC-ERROR-003: Retryable vs Fatal Errors

**MUST** distinguish retryable from fatal errors
**Given:** Client encounters error  
**When:** Error code is:

- `1001` (Transaction Not Found) → Fatal, do NOT retry
- `6001` (ERR_RPC_TIMEOUT; worker accepted but did not reply before timeout) → Retryable with backoff
- `6004` (ERR_ROUTE_NOT_REGISTERED; no workers registered for route) → Retryable with backoff
- `1011` (KV Unauthorized) → Fatal, do NOT retry
- `2009` (Stream Unauthorized) → Fatal, do NOT retry
- `4009` (Queue Unauthorized) → Fatal, do NOT retry
- `5009` (Lease Unauthorized) → Fatal, do NOT retry
- `6009` (RPC Unauthorized) → Fatal, do NOT retry
- `7009` (Schedule Unauthorized) → Fatal, do NOT retry
  **Then:**
- Client retries only retryable errors
- Client uses exponential backoff for retries
- Client fails fast on fatal errors

### AC-ERROR-004: Connection Loss Recovery

**MUST** recover from connection loss
**Given:** Active connection with in-flight operations  
**When:** Network connection drops  
**Then:**

- Client detects disconnection within 5 seconds
- Client attempts reconnection with exponential backoff
- Client re-authenticates with CONNECT frame
- Client re-establishes subscriptions (Notice/RPC)
- In-flight transactions (KV, Stream sessions) are lost

## Performance

### AC-PERF-001: Frame Size Limits

**MUST** respect maximum frame size (default 1 MB production, configurable)
**Given:** Client attempts to send large payload  
**When:** Payload exceeds configured limit (1 MB production default)  
**Then:**

- Client either:
  - Rejects operation before sending, OR
  - Server rejects with frame size error
- Client chunks large data across multiple frames/operations
- **A single TLV value MUST NOT exceed 65535 bytes (≈64 KiB).** Large payloads **MUST** be chunked across multiple frames or operations; clients and servers **MUST NOT** rely on a single TLV value larger than 65535 bytes even when the frame size permits it.
  **Chunking notes:**
- **RPC** supports explicit chunked responses (see AC-RPC-005).
- **Stream** responses MAY be split across multiple frames or partial records.
- Other domains (e.g., KV, Queue) should use multiple logical operations or application-level chunking; clients MUST NOT rely on implicit TLV chunk reassembly in those domains.
  **Configuration:**
- Server default: 1 MB (configurable via `BootConfig::max_frame_size`)
- Client SDK default: May be higher (e.g., 100 MB) but should be reduced to match server in production
- Test environments: May use larger limits (e.g., 16 MB) for convenience

### AC-PERF-002: Connection Pooling

**SHOULD** reuse connections efficiently
**Given:** Client makes multiple operations  
**When:** Operations occur within short time window  
**Then:**

- Client reuses same WebSocket connection
- Client does NOT create new connection per operation
- Client maintains connection pool (if multi-threaded)

### AC-PERF-003: Backpressure Handling

**MUST** handle backpressure signals
**Given:** Server experiencing high load  
**When:** Server responds with rate-limit or backpressure error codes (or an explicit backpressure frame)  
**Then:**

- Client pauses sending
- Client applies exponential backoff
- Client does NOT flood server with retries

### AC-PERF-004: Subscription Throughput

**SHOULD** handle high-volume subscriptions
**Given:** Client subscribed to high-traffic route (1000+ msg/sec)  
**When:** Messages arrive rapidly  
**Then:**

- Client processes messages without blocking
- Client does NOT accumulate unbounded backlog
- Client drops messages if processing can't keep up (with logging)

### AC-PERF-005: Latency Measurement

**SHOULD** track operation latency
**Given:** Client performs operations  
**When:** Client tracks time from send to response  
**Then:**

- Client exposes latency metrics (p50, p95, p99)
- Client logs slow operations (> 1s)
- Client can identify performance regressions

## Appendix: Error Code Reference

This appendix provides a complete reference of all Fitz error codes by domain, as required by AC-ERROR-002.

### Error Code Format

Error codes follow the format `XXYY` where:
- `XX` = Domain identifier (10-79)
- `YY` = Domain-specific error number (01-99)

### KV Domain (1000-1999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 1001 | ERR_TRANSACTION_NOT_FOUND | Transaction ID does not exist or expired | No |
| 1002 | ERR_INVALID_MODE | Invalid transaction mode specified | No |
| 1003 | ERR_KEY_NOT_FOUND | Key does not exist in transaction view | No |
| 1004 | ERR_ISOLATION_CONFLICT | Read-write set conflict detected | Yes (with backoff) |
| 1005 | ERR_WRITE_IN_READONLY | Write attempted in read-only transaction | No |
| 1006 | ERR_KEY_EXISTS | Key already exists (Insert failed) | No |
| 1007 | ERR_INVALID_ROUTE | Route format invalid or malformed | No |
| 1008 | ERR_REALM_MISMATCH | Operation crosses realm boundaries | No |
| 1009 | ERR_BACKEND_ERROR | Storage backend error | Yes (with backoff) |
| 1010 | ERR_TRANSACTION_ABORTED | Transaction aborted by system | No |
| 1011 | ERR_UNAUTHORIZED | Permission denied for KV operation | No |

### Stream Domain (2000-2999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 2001 | ERR_CONCURRENCY_CONFLICT | Expected offset does not match (AC-STREAM-013) | No |
| 2002 | ERR_SESSION_ALREADY_ACTIVE | Another append session is already active for that resource | No |
| 2003 | ERR_SESSION_NOT_FOUND | Session ID is missing, stale, or already cleaned up | No |
| 2004 | ERR_INVALID_READ_BOUND | Read range bounds invalid | No |
| 2005 | ERR_RESOURCE_NOT_FOUND | Stream resource does not exist | No |
| 2009 | ERR_UNAUTHORIZED | Permission denied for stream operation | No |
| 2010 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 2011 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |

### Notice Domain (3000-3999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 3001 | ERR_INVALID_ROUTE | Notice route format invalid | No |
| 3002 | ERR_INVALID_PATTERN | Subscription pattern syntax invalid | No |
| 3003 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |
| 3004 | ERR_TRANSPORT_CLOSED | Transport connection closed | No |
| 3009 | ERR_UNAUTHORIZED | Permission denied for notice operation | No |

### Queue Domain (4000-4999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 4001 | ERR_INVALID_TOKEN | Queue inflight token invalid or wrong (AC-QUEUE-006) | No |
| 4002 | ERR_INFLIGHT_EXPIRED | Message inflight reservation expired | No |
| 4003 | ERR_MESSAGE_NOT_FOUND | Message ID not found in queue | No |
| 4004 | ERR_QUEUE_NOT_FOUND | Queue resource does not exist | No |
| 4005 | ERR_QUEUE_FULL | Queue at capacity (backpressure) | Yes (with backoff) |
| 4009 | ERR_UNAUTHORIZED | Permission denied for queue operation | No |

### Lease Domain (5000-5999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 5001 | ERR_LEASE_HELD | Lease already held by another client | Yes (with backoff) |
| 5002 | ERR_INVALID_FENCE | Fencing token invalid or out of order | No |
| 5003 | ERR_LEASE_EXPIRED | Lease TTL expired | No |
| 5004 | ERR_LEASE_NOT_FOUND | Lease resource does not exist | No |
| 5005 | ERR_INVALID_TOKEN | Lease token invalid or wrong (AC-LEASE-009) | No |
| 5009 | ERR_UNAUTHORIZED | Permission denied for lease operation | No |

### RPC Domain (6000-6999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 6001 | ERR_RPC_TIMEOUT | No response within timeout period | Yes (with backoff) |
| 6002 | ERR_WORKER_NOT_FOUND | Worker disconnected or unregistered | Yes |
| 6003 | ERR_RPC_BACKPRESSURE | RPC queue at capacity (backpressure) | Yes (with backoff) |
| 6004 | ERR_ROUTE_NOT_REGISTERED | No workers registered for route (AC-RPC-003) | Yes (with backoff) |
| 6005 | ERR_CORRELATION_NOT_FOUND | Correlation ID not found (orphaned response) | No |
| 6009 | ERR_UNAUTHORIZED | Permission denied for RPC operation | No |

### Schedule Domain (7000-7999)

| Code | Name | Description | Retryable |
|------|------|-------------|-----------|
| 7001 | ERR_SCHEDULE_NOT_FOUND | Schedule job does not exist | No |
| 7002 | ERR_INVALID_CRON | Cron expression syntax invalid (AC-SCHEDULE-002) | No |
| 7003 | ERR_SCHEDULE_LIMIT | Maximum schedules reached | No |
| 7004 | ERR_PARSE_ERROR | Schedule payload parse error | No |
| 7005 | ERR_INVALID_TARGET | Target route invalid or unsupported | No |
| 7006 | ERR_INVALID_SUBSCRIPTION_PATTERN | Subscription pattern syntax invalid | No |
| 7007 | ERR_SUBSCRIPTION_LIMIT | Maximum subscriptions reached | No |
| 7009 | ERR_UNAUTHORIZED | Permission denied for schedule operation | No |

### Error Handling Guidelines

**Retryable Errors:**
- Implement exponential backoff (e.g., 100ms, 200ms, 400ms, 800ms, max 5s)
- Limit retry attempts (e.g., max 5 retries)
- Add jitter to prevent thundering herd

**Fatal Errors:**
- Do NOT retry automatically
- Return error to application layer
- Log for debugging

**Backpressure Errors (4005, 6003):**
- Special case of retryable errors
- Indicate server load, not client fault
- Use longer backoff periods (start at 500ms-1s)

## Summary Checklist

Use this checklist to verify client implementation completeness:

### Connection

- [ ] AC-CONN-001: WebSocket connection
- [ ] AC-CONN-002: JWT authentication
- [ ] AC-CONN-003: Auth rejection handling
- [ ] AC-CONN-004: Anonymous mode
- [ ] AC-CONN-005: Pre-auth frame rejection
- [ ] AC-CONN-006: Resubscribe on reconnect

### KV Domain (11 criteria)

- [ ] AC-KV-001 through AC-KV-011

### Stream Domain (12 criteria)

- [ ] AC-STREAM-001 through AC-STREAM-007
- [ ] AC-STREAM-010 through AC-STREAM-014

### Queue Domain (8 criteria)

- [ ] AC-QUEUE-001 through AC-QUEUE-008

### Notice Domain (9 criteria)

- [ ] AC-NOTICE-001 through AC-NOTICE-009

### RPC Domain (8 criteria)

- [ ] AC-RPC-001 through AC-RPC-008

### Lease Domain (10 criteria)

- [ ] AC-LEASE-001 through AC-LEASE-010

### Schedule Domain (8 criteria)

- [ ] AC-SCHEDULE-001 through AC-SCHEDULE-008

### Error Handling (4 criteria)

- [ ] AC-ERROR-001 through AC-ERROR-004

### Performance (5 criteria)

- [ ] AC-PERF-001 through AC-PERF-005

**Total:** 78 explicit acceptance criteria

## Compliance Levels

### Level 1: Core Compliance (MUST)

All criteria marked as **MUST** - Required for basic Fitz client

### Level 2: Production Ready (SHOULD)

All MUST + SHOULD criteria - Recommended for production deployments

### Level 3: Full Compliance

All criteria including performance and edge cases

## Notes

- Criteria are written in **Given-When-Then** format for clarity
- Each criterion is independently testable
- Error codes reference CLIENT.md specification
- Timing requirements use reasonable defaults (adjust per deployment)
- Permission syntax follows format: `domain://realm/area/resource#access`
  **Last Updated:** February 3, 2026
