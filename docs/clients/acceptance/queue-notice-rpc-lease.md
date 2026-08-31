
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

### AC-QUEUE-009: Unified availability-watch registration contract

**MUST** apply the shared subscription contract to Queue availability watches
**Given:** An authenticated session
**When:** The client subscribes or unsubscribes using exact, `*`, `**`,
overlapping, or wildcard-realm Queue patterns capable of matching three segments
**Then:**

- Notifications contain the matching identifier and exact concrete three-segment Queue route
- Duplicate original registration strings are idempotent and checked before the 128-wildcard cap
- Exact registration succeeds at the cap; the 129th distinct wildcard returns 4011
- Wrong schemes, empty segments, partial wildcards, and impossible depths return 4010 on TCP and WebSocket
- Matching and disconnect cleanup remain isolated by `RouteFamily`
- ENQUEUE, EXTEND, and COMPLETE continue to require concrete routes
- RESERVE accepts exact routes and valid whole-segment Queue patterns
- Exact RESERVE retains `[message_id][lease_token][body]` per item; wildcard
  RESERVE prefixes every item with the matched concrete route used for EXTEND or COMPLETE

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
**When:** Client sends `Subscribe(route="rpc://prod/users/validate", max_concurrent=32)`
**Then:**

- Server returns success
- Client is registered as worker
- Client receives REQUEST frames for this route
- `max_concurrent` values outside `1..=1024` are rejected
- Whole-segment `*` and `**` are accepted in any registration segment, including realm
- Credit belongs to the registration and is shared across every concrete route it matches
- Overlapping exact and wildcard registrations are equal, independent candidates
- A session may hold at most 128 wildcard RPC registrations; duplicate registration is idempotent

### AC-RPC-002: RPC Call and Response

**MUST** complete RPC request-response cycle
**Given:** Worker registered for `rpc://prod/users/validate`
**When:**

1. Caller sends `Call(route="rpc://prod/users/validate", payload="user:123", timeout=5s)`
2. Worker receives REQUEST(route="rpc://prod/users/validate", correlation_id=<16 bytes>, payload)
3. Worker sends `Reply(correlation_id, result="valid")`
   **Then:**

- Caller receives response within timeout
- Response payload matches worker's reply
- correlation_id is fixed 16 bytes (UUID)
- Caller does not receive a successful submit ACK before the worker response

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
**Given:** Worker registered for `rpc://prod/users/validate`
**When:**

1. Worker sends `Unsubscribe(route="rpc://prod/users/validate")`
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

### AC-LEASE-011: Exact and patterned watch registration

**MUST** accept exact and wildcard Lease subscriptions over the shared
depth-three grammar
**Given:** An authenticated session
**When:** The client subscribes to `lease://realm/area/resource` or a
whole-segment `*`/`**` selector such as `lease://acme/renderers/*`
**Then:**

- Duplicate subscribe (same session, same original selector string) returns
  the same identifier
- Unsubscribe is idempotent and disconnect removes every watch, exact and
  wildcard, owned by that session
- Notifications carry the subscription identifier and the exact concrete
  Lease route that changed, never the pattern
- Partial wildcards (`lock*`), wrong schemes, empty segments, and missing or
  extra segments return 5010 for SUBSCRIBE and UNSUBSCRIBE on TCP and
  WebSocket; whole-segment `*`/`**` selectors that can match a three-segment
  route are accepted
- Wildcard Lease registrations share the 128-per-session wildcard quota with
  every other wildcard-capable domain; exact registrations do not count
  against it
- Observing a route through a wildcard subscription never grants, renews, or
  releases it — only exact ACQUIRE/EXTEND/RELEASE change ownership

### AC-LEASE-012: Notification coverage of held-lease membership changes

**MUST** notify matching watches whenever the held-lease set changes
**Given:** A session subscribed to a route or a matching wildcard selector
**When:** That lease is immediately acquired, granted to a queued waiter,
released, expires, or its owning session disconnects
**Then:**

- Exactly one NOTIFY is delivered per matching registration for each such
  change, carrying the exact concrete route
- A successful RENEW does **not** emit a NOTIFY by default: the held set and
  holder did not change, and QUERY/LIST already reflect the new expiry
- A failed, fenced, or merely queued ACQUIRE does not emit a NOTIFY; only the
  waiter's own response reflects that outcome

### AC-LEASE-013: Patterned LIST inventory

**MUST** return the current held-lease inventory matching a selector
**Given:** A session with read permission covering a selector's complete
match set, and zero or more currently held leases matching it
**When:** The client sends `List(pattern, cursor=None, limit)`
**Then:**

- The response is one page of currently held, non-expired leases matching
  the selector; pending waiters are never included
- Each item reports its exact route, logical `owner_id`, opaque
  `holder_incarnation` (never the raw session ID), `acquired_at`,
  `expires_in_secs`, and `renewals`
- One live session's `holder_incarnation` is identical across every lease it
  holds; a different session using the same `owner_id` gets a different
  `holder_incarnation`, and a reconnect gets a new one
- A page at or under the default/requested limit returns `next_cursor=None`;
  a larger match set returns an opaque cursor bound to this selector, family,
  and broker lifetime
- Continuing with that cursor and the same selector returns the remaining
  items exactly once each (no duplicates, no omissions), even if other
  clients acquire, release, or let leases expire while the scan is in
  progress
- Reusing a cursor with a different selector, a different `RouteFamily`, an
  unknown/evicted snapshot ID, or after a broker restart returns error code
  `5011` (Invalid List Cursor) rather than silently narrowing or restarting
  the read
- A pattern that fails the shared grammar (partial wildcard, wrong scheme,
  wrong depth, empty segment) returns error code `5012` (Invalid List
  Pattern) before any inventory is scanned
- `LIST` items can never be turned into an owned Lease handle; only exact
  ACQUIRE/EXTEND/RELEASE change ownership

## Schedule Domain
