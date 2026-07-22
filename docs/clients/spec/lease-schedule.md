### Lease Domain (Ephemeral Exclusive Coordination with Queueing)

**Purpose:** In-memory exclusive leases for single-broker coordination with optional FIFO queue-based acquisition, process-local fencing tokens, and configurable wait timeouts.

**Key Concepts:**
- **Lease**: A scarce resource identified by route (lease://realm/area/resource)
- **Fencing Token (`u64`)**: Server-generated opaque value that is only meaningful within the current broker process
- **Owner ID**: String identifier for the holder
- **TTL (Time-to-Live)**: Server-enforced expiration in seconds
- **Queueing**: Optional FIFO wait for availability with timeout
- **Route Partitioned**: Each route has independent lease state

#### Message Types

| Type | Name    | Semantics |
| ---: | ------- | --- |
|  400 | ACQUIRE | Request exclusive ownership with optional wait |
|  401 | RENEW   | Extend lease expiration by issuing new token |
|  402 | RELEASE | Relinquish lease, grant to next waiter |
|  403 | QUERY   | Inspect current holder and waiter count |
|  407 | SUBSCRIBE | Register a live watch on a lease route |
|  408 | UNSUBSCRIBE | Remove a live watch |
|  409 | NOTIFY | Server-to-client lease change notification |

#### ACQUIRE Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  ttl_secs
[u32 BE]  wait_seconds (optional, defaults to 0)
```

**Parameters:**
- `route`: Lease identity (e.g., `lease://realm/area/leader`)
- `owner_id`: String identifier for owner (e.g., `"node-1"`)
- `ttl_secs`: Server-enforced expiration duration (e.g., `60`)
- `wait_seconds`: Max time to wait if lease held by other owner
  - `0` (or omitted): Immediate fail if unavailable → response `HeldByOther`
  - `1-30`: Queue and wait up to N seconds → response `Queued` or `Timeout`
  - Server enforces max 30 seconds per request

**Response (status=0, success):**
```
[u8]     0 (status)
[u8]     response_type (0=Acquired, 1=AlreadyHeld, 2=Queued, 3=AlreadyQueued)
[u64 BE] fencing_token
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### RENEW Request

```
[u32 BE]  route_len
[bytes]   route
[u32 BE]  owner_id_len
[bytes]   owner_id
[u64 BE]  fencing_token
[u64 BE]  ttl_secs
```

**Design Notes:**
- `fencing_token` MUST match current holder; mismatch returns `Fenced` error
- Issues new token on successful renewal
- Fails if lease expired or held by different owner

**Response (status=0, success):**
```
[u8]     0 (status)
[u64 BE] new_fencing_token
```

**Response (status=1, error):**
```
[u8]     1 (status)
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
```

**Design Notes:**
- `fencing_token` MUST match current holder
- Failing token prevents zombie holders from releasing
- Server FIFO-grants next waiter (if any) immediately

**Response (status=0, success):**
```
[u8]     0 (status)
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### QUERY Request

```
[u32 BE]  route_len
[bytes]   route
```

**Response (status=0, lease free):**
```
[u8]     0 (status)
[u8]     0 (has_holder=false)
[u32 BE] pending_waiters (always 0 when free)
```

**Response (status=0, lease held):**
```
[u8]     0 (status)
[u8]     1 (has_holder=true)
[u32 BE] owner_id_len
[bytes]  owner_id
[u64 BE] ttl_remaining_secs
[u32 BE] pending_waiters (count of clients waiting in queue)
```

**Response (status=1, error):**
```
[u8]     1 (status)
[u32 BE] error_len
[bytes]  error_msg
```

#### SUBSCRIBE Request

```
[u32 BE]  route_len
[bytes]   route
```

**Design Notes:**
- Watches are exact-route subscriptions on `lease://{realm}/{area}/{resource}`
- Duplicate subscribe calls for the same `(session, route)` return the existing `subscription_id`
- Subscriptions are session-scoped and are removed automatically on disconnect

**Response (status=0, success):**
```
[u8]     0 (status)
[u64 BE] subscription_id
```

#### UNSUBSCRIBE Request

```
[u32 BE]  route_len
[bytes]   route
```

**Design Notes:**
- Unsubscribe is idempotent; removing a missing watch still returns success

**Response (status=0, success):**
```
[u8]     0 (status)
```

#### NOTIFY (409) — Server to Client

```
[u64 BE]  subscription_id
[u32 BE]  route_len
[bytes]   route
[u32 BE]  payload_len (= 0 today)
```

**Design Notes:**
- `NOTIFY` is emitted when a watched lease changes because of release, expiry, or disconnect cleanup
- The payload is currently empty; the route identifies which lease changed
- Delivery is best-effort and is never acknowledged or retried

#### Response Types (Detailed)

**Successful Responses:**

| Response | Meaning | Next Action |
|----------|---------|-----------|
| `Acquired { token }` | Lease granted immediately | Proceed with work; renew before TTL expiry |
| `AlreadyHeld { token }` | Already own this lease (idempotent) | No-op; renew/release as needed |
| `Queued { token }` | Waiting for lease in FIFO queue | Watch for async `Acquired` message from server |
| `AlreadyQueued { token }` | Already waiting for same lease | No-op; continue waiting |
| `Renewed { token }` | Lease TTL extended with new token | Use new token for future renew/release |
| `Released` | Lease released successfully | Lease available; next waiter (if any) receives async `Acquired` |
| `Status { owner, token, ttl_secs, pending }` | Lease holder & queue info | Read-only; useful for debugging |

**Error Responses:**

| Error | Meaning | Cause | Recovery |
|-------|---------|-------|----------|
| `HeldByOther` | Immediate rejection | `wait_seconds=0` and lease held by other | Retry with `wait_seconds=N` or back off |
| `Timeout` | Wait expired | Waited full `wait_seconds`, still unavailable | Retry with longer timeout or try different resource |
| `QueueFull` | Too many waiters | >100 pending acquirers on lease | Back off; DoS prevention limit reached |
| `NotHeld` | Not lease owner | Renew/Release without ownership | Check current holder via QUERY |
| `Fenced` | Token mismatch | `fencing_token` doesn't match server | Acquire fresh lease; old lease was released |
| `Expired` | Lease expired | Query on released lease (no holder) | Lease available; acquire fresh |
| `NotFound` | Lease doesn't exist | Route never acquired before | New lease; acquire automatically creates |

#### Queueing Semantics

**FIFO Ordering:**
- When lease unavailable and `wait_seconds > 0`, client enters queue
- Waiters served in strict FIFO order (first-come, first-served)
- No fairness guarantees across multiple attempts or clients

**Wait Timeout:**
- Client-specified `wait_seconds` (1-30) limits how long to wait
- Server counts down; if lease becomes available within timeout → `Acquired` (async)
- If lease still held after timeout → `Timeout` response (async)
- Client should NOT assume timeout fires at exactly `wait_seconds`; treat as "approximately" N seconds

**Server Constraints:**
- Max queue depth: 100 pending acquirers per lease (return `QueueFull` to reject)
- Max wait time: 30 seconds per request (server enforces)
- Auto-expiry: Pending waiters discarded after their timeout expires

**Deferred Response Behavior:**
- ACQUIRE with `wait_seconds > 0` returns `Queued` immediately
- Later, server sends async `Acquired` or `Timeout` when state changes
- Client must listen for these out-of-band responses on connection

**Grant on Release/Expiry:**
- When current holder releases or lease expires, next FIFO waiter automatically gets `Acquired` response
- All other waiters remain queued or time out

**Lease Transitions:**
```
                ACQUIRE
                   |
                   v
         ┌─────────────────┐
         |   FREE LEASE    |
         └─────────────────┘
              ^         |
              |         | wait_seconds > 0
              |         v
          RELEASE  ┌──────────┐
           (async) |  QUEUED  |
             ^     └──────────┘
             |          |
           Next  (on expiry or release)
           Waiter|
             returns Acquired (async)
```

#### Usage Scenarios (with Pseudocode)

**Scenario 1: Immediate Acquire (Fast Path)**

```python
# Try to acquire with no wait
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-1",
    ttl_secs=60,
    wait_seconds=0  # Explicit: fail immediately if unavailable
)

if response.type == "Acquired":
    token = response.token
    # Proceed with exclusive work
    perform_critical_section()
    
    # Release when done
    client.lease_release(
        route="lease://prod/app/counter",
        owner_id="client-1",
        fencing_token=token
    )
elif response.type == "HeldByOther":
    print(f"Lease held by {response.current_owner}, cannot proceed")
    # Backoff or try alternate path
```

**Scenario 2: Queue-Wait Acquire (Contended Resource)**

```python
# Try to acquire with optional wait
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-1",
    ttl_secs=60,
    wait_seconds=10  # Wait up to 10 seconds
)

if response.type == "Queued":
    print("Lease unavailable; waiting in queue...")
    # Server will send async "Acquired" when lease available
    # OR async "Timeout" if not available within 10 seconds
    
    # Listen for async messages from server
    while True:
        msg = client.receive_async()
        if msg.type == "Acquired":
            token = msg.token
            print("Lease granted!")
            perform_critical_section()
            client.lease_release(...)
            break
        elif msg.type == "Timeout":
            print("Wait timeout; lease still unavailable")
            # Retry with longer timeout or abandon
            break
```

**Scenario 3: Timeout During Wait**

```python
# Acquire with short timeout
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-2",
    ttl_secs=60,
    wait_seconds=2  # Wait max 2 seconds
)

if response.type == "Queued":
    # Wait for async response (timeout or acquire)
    msg = client.receive_async(timeout=3)  # Client-side safety timeout
    
    if msg.type == "Timeout":
        print("Could not acquire within 2 seconds")
        # Abandon attempt or retry with backoff
    elif msg.type == "Acquired":
        # Late grant; proceed
        pass
```

**Scenario 4: Queue Full (Rejection)**

```python
# Many clients contending; queue at capacity
response = client.lease_acquire(
    route="lease://prod/app/counter",
    owner_id="client-99",
    ttl_secs=60,
    wait_seconds=10
)

if response.type == "QueueFull":
    print(f"Too many waiters ({response.pending_count}); rejected")
    # Server gate to prevent cascading waits
    # Backoff exponentially or use alternate strategy
    time.sleep(random_backoff())
    retry_acquire_or_failover()
```

**Scenario 5: Release & Grant Next Waiter**

```python
# Client-1 holds lease; Client-2 is oldest waiter
# Client-1 releases
client.lease_release(
    route="lease://prod/app/counter",
    owner_id="client-1",
    fencing_token=token_from_client_1
)
# Response: Released (immediate)

# Server automatically grants Client-2 (oldest waiter)
# Client-2's async listener receives:
msg = client.receive_async()  # From server
# msg.type == "Acquired"
# msg.token = new_token
# Client-2 can now proceed with work
```

**Scenario 6: Renew While Holding**

```python
# Client has lease; renew before expiry
token = initial_token_from_acquire()

response = client.lease_renew(
    route="lease://prod/app/counter",
    owner_id="client-1",
    fencing_token=token,
    ttl_secs=60
)

if response.type == "Renewed":
    new_token = response.token
    # Continue work; use new_token for next renew/release
    perform_more_work()
    
    # Later, renew again or release
    client.lease_release(
        route="lease://prod/app/counter",
        owner_id="client-1",
        fencing_token=new_token
    )
elif response.type == "Fenced":
    print(f"Token mismatch; lease no longer ours (held by {response.current_holder})")
    # Abort and acquire fresh lease if needed
```

#### Common Patterns & Best Practices

**Idempotent Acquire:**
- Calling ACQUIRE when already holding the lease returns `AlreadyHeld` (safe re-issue)
- Useful for "set desired state" patterns

**Fencing Against Zombies:**
- RENEW and RELEASE require matching `fencing_token`
- Prevents stale holder (expired but still executing) from affecting current holder
- Token changes on each RENEW, ensuring linear ordering

**Linearizability:**
- Each token change represents a serialization point
- Holders with newer tokens always supersede older tokens
- Safe for strong consistency coordination

**Timeout Tuning:**
- **Short waits (1-2s):** Fast-fail scenarios; high throughput, less fairness
- **Medium waits (5-10s):** Balanced; allows real contention, bounded latency
- **Long waits (20-30s):** High fairness; higher latency for unavailable resources
- **No wait (wait_seconds=0):** Pessimistic locking; immediate fail-fast

**Queue Monitoring:**
- Use QUERY to inspect `pending_waiters` count
- High count → high contention; consider resource partitioning or load shedding
- Helps detect cascading waits or application logic issues

**Client-Side Timeouts:**
- Pair server-side `wait_seconds` with client-side receive timeout
- Prevents client hang if async message lost/delayed
- Example: `receive_async(timeout_ms=max(wait_seconds+2, 5000))`

#### Semantics

- **Self-Contained Operations**: Every ACQUIRE/RENEW/RELEASE includes full route
- **Mutual Exclusion**: Only one owner holds a lease at a time
- **TTL-based Expiry**: Expired leases automatically released; next waiter (if any) granted
- **Route Partitioned**: Different routes have independent lease state and queues
- **In-Memory**: Lost on broker restart (use for coordination, not durability)
- **FIFO Fairness**: Waiters served in order; no starvation within single lease

##### Fencing Token

**`fencing_token` is server-generated opaque `u64` value:**
- Prevents stale commands from affecting current holder or leaked state
- Clients MUST treat as cookie (no prediction, caching, or reuse)
- Generated fresh at ACQUIRE time
- Changes on each successful RENEW
- Restart resets the token lineage; tokens are not durable or cluster-wide
- Validated by server on RENEW/RELEASE; mismatch → `Fenced` error

**Use Case Example:**
- Client-A acquires lease (token T1), gets delayed
- Lease expires; Client-B acquires lease (token T2)
- Client-A wakes up, tries RELEASE with T1
- Server rejects: token T1 ≠ current T2 → Error `Fenced`
- Client-A knows it lost the lease; Client-B's lease protected

#### Error Codes (5xxx)

- 5001 = ERR_LEASE_HELD (immediate rejection with `wait_seconds=0`)
- 5002 = ERR_LEASE_HELD_BY_OTHER (detailed: `current_owner` provided)
- 5003 = ERR_INVALID_FENCE (token mismatch on RENEW/RELEASE)
- 5004 = ERR_LEASE_EXPIRED (lease no longer valid)
- 5005 = ERR_LEASE_NOT_FOUND (route never acquired)
- 5006 = ERR_QUEUE_FULL (too many pending waiters)
- 5007 = ERR_INVALID_OWNER (owner_id mismatch on RENEW/RELEASE)
- 5008 = ERR_WAIT_OUT_OF_RANGE (wait_seconds > 30)

#### Acceptance Tests

- acquire succeeds on free lease, returns `Acquired`
- acquire fails on held lease with `wait_seconds=0`, returns `HeldByOther`
- acquire with `wait_seconds>0` returns `Queued`, later receives `Acquired` when available
- acquire with short `wait_seconds` receives `Timeout` if unavailable
- multiple waiters receive `Acquired` in FIFO order as lease released
- renew with valid token extends TTL and issues new token
- renew with invalid token fails with `Fenced`
- release with valid token releases, grants next waiter
- release with invalid token fails with `Fenced`
- expired lease acquirable by new owner (FIFO waiters bypass expiry)
- query shows holder, TTL, and pending_waiters count
- idempotent acquire (already holding) returns `AlreadyHeld`
- queue-full rejection at 101st concurrent waiter

### Schedule Domain (Delayed/Recurring Tasks)

**Purpose:** Durable scheduling of delayed tasks and recurring jobs.

#### Message Types

| Type | Name        | Direction                  |
| ---: | ----------- | -------------------------- |
|  700 | CREATE      | Client → Server            |
|  701 | CANCEL      | Client → Server            |
|  702 | LIST        | Client → Server            |
|  703 | SUBSCRIBE   | Client → Server            |
|  704 | UNSUBSCRIBE | Client → Server            |
|  705 | NOTIFY      | Server → Client (delivery) |

#### CREATE Request

**Wire Format:**
```
[u32 BE]  route_len
[bytes]   route (e.g., "schedule://realm/area/resource/operation")
[u32 BE]  cron_len
[bytes]   cron (UTF-8 cron expression, 5-field format)
[u8]      delivery_mode (0=broadcast, 1=single)
[u32 BE]  payload_len
[bytes]   payload (arbitrary bytes to deliver on notification)

Response (success=0):
  [u8]     0

Response (error=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Route serves as the unique schedule identifier (upsert behavior)
- Creating a schedule with an existing route updates that schedule
- Payload is arbitrary binary data delivered to subscribers on notification
- Delivery mode is required. Unknown values return error code 7008.
- `broadcast` attempts every connected exact-route subscriber. `single` fairly
  rotates one accepted live handoff across connected exact-route subscribers.
- Both modes are ephemeral downstream delivery. No subscriber or all rejected
  handoffs still complete the occurrence without backlog or retry.
- Invalid cron expression returns error code 7002

#### CANCEL Request

**Wire Format:**
```
[u32 BE]  route_len
[bytes]   route

Response (success=0):
  [u8]     0

Response (error=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Canceling a nonexistent schedule succeeds (idempotent)
- Cancel prevents all future executions
- Already-running notifications may still be delivered

#### LIST Request

**Wire Format:**
```
optional:
  [u8]     1 (offset present)
  [u64 BE] offset
  [u8]     1 (limit present)
  [u64 BE] limit

Response (success):
  [u8]     0 (status=success)
  [u64 BE] total_count
  repeat 0..N:
    [u8]     1 (has_entry=true)
    [u32 BE] route_len
    [bytes]  route
    [u32 BE] cron_len
    [bytes]  cron
    [u8]     delivery_mode (0=broadcast, 1=single)
    [u32 BE] payload_len
    [bytes]  payload
  [u8]     0 (has_entry=false, end sentinel)

Response (error):
  [u8]     1 (status=error)
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Omitting the payload defaults to `offset=0, limit=100`
- `limit=0` means "all remaining entries from offset"
- LIST is scoped to the current route family and returns a single response payload, not a multi-frame stream

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
- Boot-load into Schedule actors before schedule-domain traffic reaches that family
- Execution resumes at the next scheduled time
- Missed schedules (broker down at scheduled time) are skipped
- No catch-up or backfill for missed executions
- Schedule subscriptions and notifications remain session-scoped live delivery only

#### Usage Example

```python (notification-only model)
client.schedule_create(
    route="schedule://prod/app/reminders/send",
    cron="0 9 * * 1",  # Every Monday at 9 AM
    delivery_mode="broadcast",
    payload=b"weekly-reminder-config"
)

# Subscribe to schedule notifications
client.schedule_subscribe(
    route="schedule://prod/app/reminders/send"
)

# Receive notification when schedule fires (Message Type 705)
# Server sends: SCHEDULE_NOTIFY(subscription_id, payload)

# List schedules
schedules = client.schedule_list()

# Cancel schedule
client.schedule_cancel(
    route="schedule://prod/app/reminders/send"
)
```uses route as identity:

- `CREATE`: `[route_len][route][cron_len][cron][mode][payload_len][payload]`
- `LIST`: optional `[offset][limit]`, returning one response with `total_count` plus entry sentinels
- `CANCEL`: `[route_len][route]`

#### Semantics

- **Route-Based Identity**: Routes uniquely identify schedules (CREATE is upsert)
- **Durability**: Schedules persist across broker restarts
- **Notification-Only**: When schedules fire, SCHEDULE_NOTIFY (705) is sent to subscribers
- **Recurring**: Interval-based recurring tasks (cron-like)
- **Cancellation**: Cancels future runs; already-delivered notifications cannot be revoked
- **Realm Scoped**: Schedules isolated per realm

##### Execution Model (Notification-Only)

When a schedule fires, the broker performs **one action**:

**SCHEDULE_NOTIFY (705):** In broadcast mode the broker attempts every connected
exact-route subscriber. In single mode it attempts subscribers in round-robin
order until one accepts. The notification wire payload is unchanged.

**Client observability:** Clients observe schedule execution by subscribing to schedule routes via `SCHEDULE_SUBSCRIBE` to receive `SCHEDULE_NOTIFY` when schedules fire.

**Payload semantics:** The payload is opaque to Fitz — clients can encode configuration, task identifiers, or any data needed to handle the notification. Common patterns:
- JSON-encoded task config
- Protobuf-serialized parameters  
- Simple string identifiers
- Arbitrary binary data

**Client observability:** Clients have two options for observing schedule execution:
- **Direct:** Subscribe to the schedule route via `SCHEDULE_SUBSCRIBE` to receive `SCHEDULE_NOTIFY` when the schedule fires
- **Indirect:** Subscribe to the target resource via the appropriate domain (e.g., Notice SUBSCRIBE for Notice targets, Queue RESERVE for Queue targets)

#### Error Codes (7xxx)

- 7001 = ERR_SCHEDULE_NOT_FOUND
- 7002 = ERR_INVALID_CRON (informational - cancel is idempotent)
- 7002 = ERR_INVALID_CRON
- 7003 = ERR_SCHEDULE_LIMIT
- 7004 = ERR_PARSE_ERROR
- 7005 = ERR_INVALID_ROUTE
- 7006 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 7007 = ERR_SUBSCRIPTION_LIMIT
- 7008 = ERR_INVALID_DELIVERY_MODE

#### Acceptance Tests

- create schedules task with cron expression
- create on existing route updates (upsert)
- cancel prevents future notifications
- cancel on nonexistent route succeeds (idempotent)
- list returns all created schedules
- schedule persists across broker restart
- subscribers receive SCHEDULE_NOTIFY when schedule fires
- invalid cron expression rejected with 7002
#### Schedule SUBSCRIBE (703)

Subscribe to schedule fire notifications for an exact schedule route.

```
[u32 BE]  route_len
[bytes]   route
Response (status=0):
  [u8]     0
  [u8]     1
  [u64 BE] subscription_id
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Route Example:**
- `schedule://realm/area/resource/operation` — specific schedule fires

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same route returns the same `subscription_id`
- Client is responsible for local multiplexing when multiple handlers share the same route
- Wildcard schedule subscribe is invalid; subscriptions require the exact schedule route
- When the schedule fires, the server sends SCHEDULE_NOTIFY (705) with subscription_id and payload; the client matches notifications to the route they subscribed with

#### Schedule UNSUBSCRIBE (704)

Unsubscribe from schedule fire notifications.

```
[u32 BE]  route_len
[bytes]   route
Response (status=0):
  [u8]     0
Response (status=1):
  [u8]     1
  [u32 BE] error_len
  [bytes]  error_msg
```

**Design Notes:**
- Client sends the original exact route string used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent route returns success

#### Schedule NOTIFY (705) — Server to Client

Server pushes a schedule fire notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  payload_len
[bytes]   payload (the schedule's configured payload bytes)
```

**Design Notes:**
- `subscription_id` tells the client which subscription matched; the subscription already identifies the exact route
- Payload is the raw payload bytes configured when the schedule was created
- Client demultiplexes to local handlers registered for that `subscription_id`
- Delivery is best-effort; notifications may be dropped under backpressure
