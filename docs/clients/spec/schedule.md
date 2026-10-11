# Schedule Domain Spec

Split from the former `lease-schedule.md`; the Lease domain is in [lease.md](lease.md).

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
|  706 | CREATE_BATCH | Client → Server (broker extension) |
|  707 | LIST_V2     | Client → Server (broker extension) |

Codes 700–705 are the canonical cross-client surface. The current broker also
advertises 706 and 707 in its protocol manifest. They are additive extensions:
clients MUST use LIST 702 for portable pagination and MUST NOT substitute the
cursor-shaped LIST_V2 707 for canonical LIST.

#### Error Envelopes

Production clients depend on the exact error layouts below. Do not change
either one inside an existing record; a new error field must travel in its own
record so existing clients keep decoding unchanged.

- Errors the Schedule domain itself returns for CREATE (700), CANCEL (701),
  SUBSCRIBE (703), UNSUBSCRIBE (704), CREATE_BATCH (706), and LIST_V2 (707)
  are uncoded: `[u8 1][u32 BE error_len][error_msg]`. On these operations an
  invalid cron, an unknown delivery mode, an invalid subscription pattern, and
  either subscription registration limit are distinguished only by `error_msg`.
  The SUBSCRIBE (703) limit messages are exact and stable:
  - `wildcard subscription limit exceeded (128 per session)` when a wildcard
    SUBSCRIBE finds the session already holding 128 wildcard registrations.
    This check runs first, so a wildcard SUBSCRIBE that reaches both limits
    still receives this message.
  - `total subscription limit exceeded (1024 per session)` when the session
    already holds 1,024 registrations of any kind and the wildcard limit does
    not apply.
- Errors the Schedule domain returns for LIST (702) are coded:
  `[u8 1][u32 BE error_code][u32 BE error_len][error_msg]`.
- Errors the broker raises before the request reaches the Schedule domain are
  coded on every Schedule message type: 7009 unauthorized, 7010 busy (never
  accepted, safe to resend), and 7011 timeout or unavailable (outcome unknown).

A client can tell the two status=1 layouts apart by length. The uncoded layout
has a total length of `5 + error_len` read at offset 1; the coded layout has a
total length of `9 + error_len` read at offset 5.

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
- Payload is arbitrary binary data delivered to matching live registrations on notification
- Delivery mode is required. Unknown values are rejected with an uncoded error.
- `broadcast` attempts every connected matching registration. `single` fairly
  rotates one accepted live handoff across matching registrations for the
  concrete fired route.
- Both modes are ephemeral downstream delivery. No match or all rejected
  handoffs still complete the occurrence without backlog or retry.
- An invalid cron expression is rejected with an uncoded error

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
  [u32 BE] error_code
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Omitting the payload defaults to `offset=0, limit=100`
- `limit=0` requests all remaining entries from `offset`, but the response is
  still one TLV value bounded by the wire frame limit and MAY return fewer
  entries than exist, regardless of what `limit` requested. There is no
  `has_more` flag on this response: detect truncation by comparing the
  returned entry count to `total_count`. If `offset + entries_returned <
  total_count`, more entries remain; continue by re-issuing LIST with
  `offset += entries_returned` (same `limit`) until the count is exhausted.
  Every entry sits at a stable index for the duration of an unchanging
  definition set, so this offset advance is safe.
- LIST is scoped to the current route family and each call returns exactly one
  response payload (never a multi-frame stream), but that payload may be a
  partial page per the truncation rule above

#### Broker Extensions

- `CREATE_BATCH` (706) encodes `[u32 entry_count]` followed by the CREATE fields
  for each entry and returns the same plain success/error envelope as CREATE.
- `LIST_V2` (707) encodes `[optional string continuation][optional u64 limit]`
  and returns its versioned cursor page. It exists for broker compatibility;
  portable clients use canonical offset/limit LIST (702). A continuation
  cursor resumes after the route it names, even if that schedule has since
  been cancelled. A cursor that lacks this family's `schedule-list-v1:<family>:`
  prefix, or has nothing after it (including an empty cursor), is not an
  error; it returns the first page.

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

When day-of-month and day-of-week are both restricted, standard cron OR
semantics apply: a date matches when either field matches. When either field is
`*`, the other field controls the date match. Brokers MUST reject expressions
that cannot match a date during a complete Gregorian 400-year cycle, such as
`0 0 31 2 *`.

**Examples:**

- `0 9 * * 1` = 9:00 AM every Monday
- `*/5 * * * *` = Every 5 minutes
- `0 */2 * * *` = Every 2 hours
- `0 9-17/2 * * *` = Every 2 hours from 9 AM-5 PM
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
# Server sends: SCHEDULE_NOTIFY(subscription_id, exact_route, payload)

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
- **Notification-Only**: When schedules fire, SCHEDULE_NOTIFY (705) is attempted for matching live registrations
- **Recurring**: Interval-based recurring tasks (cron-like)
- **Cancellation**: Cancels future runs; already-delivered notifications cannot be revoked
- **Realm Scoped**: Schedules isolated per realm

##### Execution Model (Notification-Only)

When a schedule fires, the broker performs **one action**:

**SCHEDULE_NOTIFY (705):** In broadcast mode the broker attempts every connected
matching registration. In single mode it attempts matching registrations in
registration order from a per-concrete-route round-robin cursor until one accepts.
The notification wire payload is `[subscription_id][exact_route][payload]`.

| Mode | Live subscriber result | Occurrence result |
|---|---|---|
| `broadcast` | none | No notification; acknowledge and advance. |
| `broadcast` | all accept | Attempt and hand off to all; acknowledge and advance. |
| `broadcast` | some or all reject | Attempt all once; keep accepted handoffs, do not retry rejected handoffs; acknowledge and advance. |
| `single` | none | No notification; acknowledge and advance. |
| `single` | a candidate accepts | Try in cursor order, stop after the first accepted handoff, advance past it; acknowledge and advance. |
| `single` | all reject | Try every candidate once, advance the cursor, acknowledge and advance. |

“Accepts” means the broker's in-process router accepted the handoff. Schedule
notifications have no consumer acknowledgement. Subscriptions accept strict
whole-segment `*` and `**` patterns, including wildcard realm. Overlapping
patterns remain distinct. Matching stays in the same route family; subscriptions
and the round-robin cursor are lost
on restart. A persisted pending claim may therefore be attempted again after a
restart, but it does not provide exactly-once delivery.

The broker does not wait for a subscriber or create a retry window. Doing so
would make live subscriber availability create a work backlog and duplicate
Queue's reservation, retry, and consumer-acknowledgement responsibilities. Use
Queue when an occurrence must remain available for eventual processing.

**Client observability:** Clients observe schedule execution by registering exact
or wildcard Schedule patterns via `SCHEDULE_SUBSCRIBE` and receiving
`SCHEDULE_NOTIFY` when a matching occurrence fires.

**Payload semantics:** The payload is opaque to Fitz — clients can encode configuration, task identifiers, or any data needed to handle the notification. Common patterns:
- JSON-encoded task config
- Protobuf-serialized parameters  
- Simple string identifiers
- Arbitrary binary data

#### Error Codes (7xxx)

- 7001 = ERR_SCHEDULE_NOT_FOUND
- 7002 = ERR_INVALID_CRON
- 7003 = ERR_SCHEDULE_LIMIT
- 7004 = ERR_PARSE_ERROR
- 7005 = ERR_INVALID_TARGET
- 7006 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 7007 = ERR_SUBSCRIPTION_LIMIT
- 7008 = ERR_INVALID_DELIVERY_MODE
- 7010 = ERR_BACKEND_ERROR
- 7011 = ERR_TIMEOUT

`ERR_BACKEND_ERROR` reports transient broker backend unavailability or
saturation. It is distinct from `ERR_PARSE_ERROR`: clients must not tell callers
that their cron or payload is malformed when the broker could not service an
otherwise valid request. Clients may classify 7010 as retryable, subject to the
operation's normal replay-safety rules.

`ERR_TIMEOUT` reports that the broker accepted the command but did not finish it
before its deadline. The outcome is unknown: the command may still apply. It is
deliberately NOT retryable, because 7010 means the request was declined and is
safe to re-send, whereas re-sending after 7011 can apply the same create or
cancel twice. A client that knows its operation is idempotent may still retry
deliberately; an automatic `IsRetryable` retry must not.

#### Acceptance Tests

- create schedules task with cron expression
- create on existing route updates (upsert)
- cancel prevents future notifications
- cancel on nonexistent route succeeds (idempotent)
- list returns all created schedules
- schedule persists across broker restart
- matching live registrations receive SCHEDULE_NOTIFY when schedule fires
- invalid cron expression rejected with 7002
#### Schedule SUBSCRIBE (703)

Subscribe to schedule fire notifications for an exact route or strict
whole-segment `*`/`**` route pattern.

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

**Route Examples:**
- `schedule://realm/area/resource/operation` — specific schedule fires
- `schedule://realm/area/*/run` — every matching `run` occurrence
- `schedule://**` — every schedule occurrence visible in this `RouteFamily`

**Semantics:**
- Subscriptions are **session-scoped** — all subscriptions are lost on disconnect
- Idempotent: re-subscribing to the same pattern returns the same `subscription_id`
- Client is responsible for local multiplexing when multiple handlers share the same route
- Wildcards must occupy complete segments. Patterns that cannot match a concrete
  four-segment Schedule route are rejected with an uncoded error.
- A session may retain at most 1,024 Schedule registrations total and 128
  wildcard registrations; overflow is rejected with an uncoded error whose
  exact `error_msg` is listed under Error Envelopes.
- Matching never crosses `RouteFamily` boundaries; overlapping registrations
  remain distinct.
- When the schedule fires, the server sends SCHEDULE_NOTIFY (705) with
  `subscription_id`, the exact fired route, and payload.

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
- Client sends the original route pattern used in SUBSCRIBE
- Idempotent: unsubscribing a non-existent route returns success

#### Schedule NOTIFY (705) — Server to Client

Server pushes a schedule fire notification to a subscriber.

```
[u64 BE]  subscription_id
[u32 BE]  exact_route_len
[bytes]   exact_route
[u32 BE]  payload_len
[bytes]   payload (the schedule's configured payload bytes)
```

**Design Notes:**
- `subscription_id` tells the client which registration matched
- `exact_route` identifies the concrete schedule occurrence, including when the
  registration was a wildcard pattern
- Payload is the raw payload bytes configured when the schedule was created
- Client demultiplexes to local handlers registered for that `subscription_id`
- Delivery is best-effort; notifications may be dropped under backpressure
