# Fitz Client Acceptance Criteria

**Version:** 1.0  
**Date:** February 3, 2026  
**Purpose:** Explicit acceptance criteria for client implementations to verify conformance with Fitz protocol

This document defines testable acceptance criteria for each Fitz domain. Client implementations MUST pass all criteria marked as **MUST**. Criteria marked as **SHOULD** are strongly recommended for production-grade clients.

---

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

---

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

**Given:** Valid JWT with required claims (`iss`, `aud`, `sub`, `exp`, `tid`, `fitz.permissions`)  
**When:** Client sends CONNECT frame with JWT on control channel  
**Then:**
- Server responds with Accept
- Client can proceed with domain operations
- Subsequent frames are authorized per JWT permissions

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
**MUST** be able to re-subscribe to previously active subscriptions after reconnect; clients **MUST NOT** assume subscriptions persist across reconnects

**Given:** Client had active subscriptions or registrations (for example: Notice subscriptions, RPC worker registrations, Stream/subscriber registrations) and was authenticated before a disconnect
**When:** Connection is lost and the client reconnects and successfully re-authenticates
**Then:**
- Client **MUST** be able to re-send `Subscribe` (or equivalent) frames for previously active subscriptions; subscription state is **not** preserved by the broker on disconnect
- Client **MAY** implement automatic re-subscribe helpers (opt-in); such helpers **SHOULD** use exponential backoff and handle partial failures
- Server **MUST** treat duplicate subscribe requests as idempotent (duplicate subscriptions SHOULD NOT create duplicate deliveries)
- Client resumes receiving new notifications, RPC requests, or stream deliveries only after explicit re-subscription and server confirmation
- Reconnect is a new session: clients **MUST** re-authenticate and re-register subscriptions when needed.

---

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
- Server returns error code `1003` (Key Exists)
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
- Server returns error code `1009` (Unauthorized)
- Write does NOT occur

### AC-KV-011: Delete Range with Invalid Bounds
**MUST** reject invalid range bounds

**Given:** Active transaction  
**When:** Client sends `DeleteRange(tx_id, start="z", end="a")` (inverted range)  
**Then:**
- Server returns error code `1006` (Invalid Range)
- No keys deleted

---

## Stream Domain

### AC-STREAM-001: Append to Stream
**MUST** append message to stream

**Given:** Session with `stream://realm/area/resource#write` permission  
**When:** Client sends `Append(realm="prod", area="logs", resource="events", payload="event1")`  
**Then:**
- Server returns `AppendOk(offset=N)`
- Offset is monotonically increasing
- Message is durable after acknowledgment

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

### AC-STREAM-004: Subscribe to Stream
**MUST** receive new messages via subscription

**Given:** Client subscribes to `stream://prod/logs/events`  
**When:**
1. Client sends `Subscribe(pattern="stream://prod/logs/events")`
2. Another client appends message to same stream
3. Client waits for push notification

**Then:**
- Server pushes new message to subscriber
- Message includes offset and payload
- Delivery occurs within reasonable time (< 1s)

### AC-STREAM-005: Commit Consumer Offset
**MUST** track consumer position

**Given:** Consumer reads messages up to offset 10  
**When:**
1. Client sends `CommitOffset(consumer_id, offset=10)`
2. Client disconnects and reconnects
3. Client sends `GetOffset(consumer_id)`

**Then:**
- Server returns last committed offset (10)
- Client can resume from offset 11

### AC-STREAM-006: Unauthorized Append
**MUST** reject append without write permission

**Given:** Session has `stream://prod/logs/**#read` (read-only)  
**When:** Client sends `Append(realm, area, resource, payload)`  
**Then:**
- Server returns error code `2009` (Unauthorized)
- Message NOT appended to stream

---

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
- Message includes lease token
- Message is invisible to other consumers during lease

### AC-QUEUE-003: Complete Message
**MUST** complete (acknowledge) message

**Given:** Message reserved with token `T1`  
**When:** Client sends `Complete(realm, area, resource, message_id, token=T1)`  
**Then:**
- Server returns `CompleteOk`
- Message is permanently removed from queue
- Message does NOT reappear after lease expires

### AC-QUEUE-004: Extend Message Lease
**MUST** extend lease before expiration

**Given:** Message reserved with 30s lease, 20s elapsed  
**When:** Client sends `Extend(message_id, token, extend_seconds=30)`  
**Then:**
- Server returns `ExtendOk`
- Lease is extended by 30s (now 40s remaining)
- Message remains invisible to other consumers

### AC-QUEUE-005: Message Redelivery on Lease Expiration
**MUST** redeliver message after lease expires

**Given:** Message reserved with 5s lease, client does not complete  
**When:**
1. Client waits 6 seconds (lease expired)
2. Another client sends `Reserve()`

**Then:**
- Second client receives the same message
- Message has new lease token
- Delivery count increments

### AC-QUEUE-006: Dead Letter Queue After Max Attempts
**MUST** move message to DLQ after max retries

**Given:** Queue configured with `max_attempts=3`  
**When:**
1. Message fails and redelivers 3 times
2. Client sends `Reserve()` after 3rd failure

**Then:**
- Message is NOT returned in reserve
- Message is moved to DLQ (dead letter queue)
- DLQ contains message for inspection

### AC-QUEUE-007: Invalid Token Rejection
**MUST** reject complete with wrong token

**Given:** Message reserved with token `T1`  
**When:** Client sends `Complete(message_id, token="WRONG")`  
**Then:**
- Server returns error code `4003` (Invalid Token)
- Message remains in queue
- Lease remains active

### AC-QUEUE-008: Delayed Message Visibility
**MUST** delay message visibility

**Given:** Client enqueues message with `visibility_delay=10s`  
**When:**
1. Client sends `Enqueue(payload, visibility_delay=10)`
2. Another client immediately sends `Reserve()`
3. Client waits 11s and sends `Reserve()` again

**Then:**
- First reserve returns empty (message invisible)
- Second reserve returns message (now visible)

### AC-QUEUE-009: Unauthorized Enqueue
**MUST** reject enqueue without write permission

**Given:** Session has `queue://realm/area/**#read` (read-only)  
**When:** Client sends `Enqueue(payload)`  
**Then:**
- Server returns error code `4009` (Unauthorized)
- Message NOT enqueued

---

## Notice Domain

### AC-NOTICE-001: Subscribe to Route
**MUST** subscribe to notice route

**Given:** Session with `notice://realm/area/**#read` permission  
**When:** Client sends `Subscribe(route="notice://prod/orders/create", channel=1)`  
**Then:**
- Server returns `SubscribeOk`
- Client is registered as subscriber
- Client can receive notifications on channel 1

### AC-NOTICE-002: Publish to Route
**MUST** publish notice to subscribers

**Given:** Client A subscribed to `notice://prod/orders/create`  
**When:** Client B publishes to `notice://prod/orders/create` with payload `"order:123"`  
**Then:**
- Server delivers notification to Client A on subscribed channel
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

**Given:** Client subscribed to `notice://prod/orders/create`  
**When:**
1. Client sends `Unsubscribe(route)`
2. Another client publishes to same route

**Then:**
- Client does NOT receive notification
- Server confirms unsubscribe

### AC-NOTICE-006: Realm Isolation
**MUST** isolate notifications by realm

**Given:**
- Client A subscribes to `notice://prod/**`
- Client B subscribes to `notice://staging/**`

**When:** Client publishes to `notice://prod/orders/create`  
**Then:**
- Client A receives notification
- Client B does NOT receive notification (different realm)

### AC-NOTICE-007: Fanout to Multiple Subscribers
**MUST** deliver to all matching subscribers

**Given:**
- Client A subscribes to `notice://prod/orders/create`
- Client B subscribes to `notice://prod/orders/*`
- Client C subscribes to `notice://prod/**`

**When:** Client publishes to `notice://prod/orders/create`  
**Then:**
- All 3 clients (A, B, C) receive notification
- Delivery is concurrent (no serialization)

### AC-NOTICE-008: Unauthorized Publish
**MUST** reject publish without write permission

**Given:** Session has `notice://prod/orders/**#read` (read-only)  
**When:** Client sends `Publish(route, payload)`  
**Then:**
- Server returns error code `3009` (Unauthorized)
- No notifications delivered

**Notes:**
- Fitz notices are **best-effort, non-durable signals**. Clients **MUST NOT** assume guaranteed delivery, ordering across reconnects, or replay after disconnect.
- **Toleration:** Clients **MUST** tolerate missed notifications across reconnects and transient backpressure periods.
- **Usage constraint:** Notices **MUST NOT** be used for workflow coordination, acknowledgement, or durability guarantees; use RPC or Queue for those needs.

---

## RPC Domain

### AC-RPC-001: Worker Registration
**MUST** register as RPC worker

**Given:** Session with `rpc://realm/area/resource#*` (admin permission)  
**When:** Client sends `Subscribe(route="rpc://prod/users/validate", channel=2)`  
**Then:**
- Server returns `SubscribeOk`
- Client is registered as worker
- Client can receive RPC requests on channel 2

### AC-RPC-002: RPC Call and Response
**MUST** complete RPC request-response cycle

**Given:** Worker registered for `rpc://prod/users/validate`  
**When:**
1. Caller sends `Call(route, payload="user:123", timeout=5s)`
2. Worker receives request
3. Worker sends `Reply(correlation_id, result="valid")`

**Then:**
- Caller receives response within timeout
- Response payload matches worker's reply
- Correlation ID matches request

### AC-RPC-003: RPC Timeout
**MUST** timeout when no worker responds

**Given:** No workers registered for `rpc://prod/users/check`  
**When:** Client sends `Call(route, timeout=2s)` and waits  
**Then:**
- After 2 seconds, client receives error code `6004` (Timeout)
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
1. Worker sends `ReplyChunk(correlation_id, chunk=0, total=3, data1)`
2. Worker sends `ReplyChunk(correlation_id, chunk=1, total=3, data2)`
3. Worker sends `ReplyChunk(correlation_id, chunk=2, total=3, data3)`

**Then:**
- Caller receives complete response after all chunks arrive
- Chunks are reassembled in order
- Data integrity is maintained

### AC-RPC-006: Worker Unregister
**MUST** stop receiving requests after unregister

**Given:** Worker registered for `rpc://prod/tasks/process`  
**When:**
1. Worker sends `Unsubscribe(route)`
2. Caller sends RPC request

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

---

## Lease Domain

### AC-LEASE-001: Acquire Lease
**MUST** acquire unowned lease

**Given:** Lease `"lock:resource:123"` is not held by anyone  
**When:** Client sends `Acquire(route="lease://prod/locks/resource:123", ttl=30)`  
**Then:**
- Server returns `AcquireOk(token=<uuid>, fencing_token=1)`
- Lease is granted to client
- Lease expires after 30 seconds if not renewed

### AC-LEASE-002: Lease Conflict
**MUST** reject acquire when lease held by other

**Given:** Client A holds lease with token `T1`  
**When:** Client B sends `Acquire(same_route, ttl=30)`  
**Then:**
- Server returns error code `5002` (Already Held)
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

**Given:** Client holds lease with token `T1`, fencing token `123`  
**When:** Same client sends `Acquire(same_route)` again  
**Then:**
- Server returns same token `T1`, fencing token `123`
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

---

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

### AC-SCHEDULE-003: Job Execution Notification
**MUST** receive notification when job fires

**Given:**
- Job created with cron `"*/1 * * * *"` (every minute)
- Client subscribed to job's route

**When:** Time advances to next minute boundary  
**Then:**
- Client receives notification on subscribed channel
- Payload matches job's configured payload
- Notification arrives within 1 second of scheduled time

### AC-SCHEDULE-004: Update Job Schedule
**MUST** update existing job's cron expression

**Given:** Job exists with cron `"0 2 * * *"`  
**When:** Client sends `Update(job_id, new_cron="0 3 * * *")`  
**Then:**
- Server returns `UpdateOk`
- Job's schedule is updated
- Next execution occurs at 3:00 AM (not 2:00 AM)

### AC-SCHEDULE-005: Delete Job
**MUST** delete scheduled job

**Given:** Job exists with `job_id=J1`  
**When:** Client sends `Delete(job_id=J1)`  
**Then:**
- Server returns `DeleteOk`
- Job no longer fires
- Future scheduled times do not trigger notifications

### AC-SCHEDULE-006: List Jobs
**MUST** retrieve all jobs for realm/area

**Given:** Jobs exist for `schedule://prod/jobs/*`  
**When:** Client sends `List(realm="prod", area="jobs")`  
**Then:**
- Server returns list of jobs with:
  - Job ID
  - Cron expression
  - Next scheduled time
  - Payload

### AC-SCHEDULE-007: Pause/Resume Job
**MUST** pause and resume job execution

**Given:** Active job with `job_id=J1`  
**When:**
1. Client sends `Pause(job_id=J1)`
2. Scheduled time arrives
3. Client sends `Resume(job_id=J1)`

**Then:**
- After pause, job does NOT fire
- After resume, job fires at next scheduled time

### AC-SCHEDULE-008: Cron Wildcards
**MUST** support wildcard expressions

**Given:** Job with cron `"* * * * *"` (every minute)  
**When:** Time advances through multiple minutes  
**Then:**
- Job fires every minute
- No missed executions (within 1s tolerance)

### AC-SCHEDULE-009: Cron Ranges and Lists
**MUST** support range and list syntax

**Given:** Job with cron `"0 9-17 * * 1-5"` (9 AM to 5 PM, Mon-Fri)  
**When:** Time is Monday 10:00 AM  
**Then:** Job fires

**When:** Time is Saturday 10:00 AM  
**Then:** Job does NOT fire

**When:** Time is Monday 8:00 AM  
**Then:** Job does NOT fire

### AC-SCHEDULE-010: Unauthorized Create
**MUST** reject job creation without write permission

**Given:** Session has `schedule://prod/jobs/**#read` (read-only)  
**When:** Client sends `Create(route, cron, payload)`  
**Then:**
- Server returns error code `7009` (Unauthorized)
- Job NOT created

---

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
| Domain | Code range |
|--------|------------|
| KV     | 1000-1999  |
| Stream | 2000-2999  |
| Notice | 3000-3999  |
| Queue  | 4000-4999  |
| Lease  | 5000-5999  |
| RPC    | 6000-6999  |
| Schedule | 7000-7999 |

Clients **MUST** interpret error codes using this mapping.

### AC-ERROR-003: Retryable vs Fatal Errors
**MUST** distinguish retryable from fatal errors

**Given:** Client encounters error  
**When:** Error code is:
- `1004` (Transaction Not Found) → Fatal, do NOT retry
- `6004` (RPC Timeout) → Retryable with backoff
- `1009` (Unauthorized) → Fatal, do NOT retry

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
- Client re-establishes subscriptions

### AC-ERROR-005: Idempotency Tokens
**SHOULD** use idempotency for critical operations

**Given:** Client sends operation with idempotency token  
**When:** Client retries due to timeout (uncertain if first attempt succeeded)  
**Then:**
- Server recognizes duplicate via idempotency token
- Server returns same result as first attempt
- Operation executes exactly once

---

## Performance

### AC-PERF-001: Frame Size Limits
**MUST** respect maximum frame size (default 1 MB)

**Given:** Client attempts to send large payload  
**When:** Payload exceeds 1 MB  
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

---

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

### Stream Domain (6 criteria)
- [ ] AC-STREAM-001 through AC-STREAM-006

### Queue Domain (9 criteria)
- [ ] AC-QUEUE-001 through AC-QUEUE-009

### Notice Domain (8 criteria)
- [ ] AC-NOTICE-001 through AC-NOTICE-008

### RPC Domain (8 criteria)
- [ ] AC-RPC-001 through AC-RPC-008

### Lease Domain (10 criteria)
- [ ] AC-LEASE-001 through AC-LEASE-010

### Schedule Domain (10 criteria)
- [ ] AC-SCHEDULE-001 through AC-SCHEDULE-010

### Error Handling (5 criteria)
- [ ] AC-ERROR-001 through AC-ERROR-005

### Performance (5 criteria)
- [ ] AC-PERF-001 through AC-PERF-005

**Total:** 80 explicit acceptance criteria

---

## Compliance Levels

### Level 1: Core Compliance (MUST)
All criteria marked as **MUST** - Required for basic Fitz client

### Level 2: Production Ready (SHOULD)
All MUST + SHOULD criteria - Recommended for production deployments

### Level 3: Full Compliance
All criteria including performance and edge cases

---

## Notes

- Criteria are written in **Given-When-Then** format for clarity
- Each criterion is independently testable
- Error codes reference CLIENT.md specification
- Timing requirements use reasonable defaults (adjust per deployment)
- Permission syntax follows format: `domain://realm/area/resource#access`

**Last Updated:** February 3, 2026
