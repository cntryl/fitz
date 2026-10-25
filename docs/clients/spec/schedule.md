# Schedule Domain Spec

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

Codes 700–705 are the canonical cross-client surface. The broker also
advertises 706 in its protocol manifest as an additive extension. Identifier
707 was `LIST_V2` before 0.3.0; its cursor form is now `LIST` (702) and the
identifier is never reused.

#### Error Envelopes

Every Schedule error, on every verb, uses the common envelope from
[error-codes.md](error-codes.md):
`[u8 1][u16 BE code][u8 class][u8 effect][u32 BE error_len][error_msg]`.
Clients branch on `code` and `class`, never on `error_msg`. The conditions a
Schedule client sees most:

- 7051 `INVALID_CRON`, 7054 `INVALID_DELIVERY_MODE`, 7053 `INVALID_TARGET`,
  and 7008 `BAD_REQUEST` for a payload the verb contract rejects.
- 7003 `INVALID_PATTERN` for a SUBSCRIBE or UNSUBSCRIBE selector the grammar
  rejects.
- 7004 `REGISTRATION_LIMIT` when a SUBSCRIBE would exceed 128 wildcard or
  1,024 total registrations for the session. The wildcard check runs first.
- 7052 `DEFINITION_LIMIT` when CREATE would exceed the definition cap.
- 7050 `SCHEDULE_NOT_FOUND` for CANCEL or RUN on an unknown definition.
- 7001 `UNAUTHORIZED`, 7005 `BUSY` (never accepted, safe to resend), and 7007
  `TIMEOUT` (accepted, outcome unknown) are raised by ingress before the
  domain sees the request.

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
  [u16 BE] code
  [u8]     class
  [u8]     effect
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Route serves as the unique schedule identifier (upsert behavior)
- Creating a schedule with an existing route updates that schedule
- Payload is arbitrary binary data delivered to matching live registrations on notification
- Delivery mode is required. Unknown values are rejected with 7054 `INVALID_DELIVERY_MODE`.
- `broadcast` attempts every connected matching registration. `single` fairly
  rotates one accepted live handoff across matching registrations for the
  concrete fired route.
- Both modes are ephemeral downstream delivery. No match or all rejected
  handoffs still complete the occurrence without backlog or retry.
- An invalid cron expression is rejected with 7051 `INVALID_CRON`

#### CANCEL Request

**Wire Format:**
```
[u32 BE]  route_len
[bytes]   route

Response (success=0):
  [u8]     0

Response (error=1):
  [u8]     1
  [u16 BE] code
  [u8]     class
  [u8]     effect
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
optional (empty payload = first page, default limit):
  [u8]     cursor present (0 or 1)
  [string] cursor (if present; opaque continuation from a previous page)
  [u8]     limit present (0 or 1)
  [u64 BE] limit (if present; default 100, at most 1000)

Response (success):
  [u8]     0 (status=success)
  [u8]     1 (page version)
  [u8]     has_more (0 or 1)
  [u8]     continuation present (0 or 1)
  [string] continuation (if present; send it as the next cursor)
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
  [u16 BE] code
  [u8]     class
  [u8]     effect
  [u32 BE] error_len
  [bytes]  error_msg
```

**Semantics:**
- Entries are returned in route order, scoped to the current route family.
- A page holds at most `limit` entries and never more than fit in one
  response message, so a page MAY be shorter than `limit`. `has_more=1` says
  more entries remain and `continuation` is the cursor that resumes after the
  last entry on the page; send it unchanged. A page with `has_more=0` is the
  last.
- A continuation resumes after the route it names even if that schedule has
  since been cancelled. A cursor from another family or an unrecognised cursor
  is not an error; it returns the first page.
- `limit` above 1000 answers 7008 `BAD_REQUEST`. A `limit` of 0 is treated
  as 1.
- Each call returns exactly one response message, never a multi-frame stream.

#### Broker Extensions

- `CREATE_BATCH` (706) encodes `[u32 entry_count]` followed by the CREATE fields
  for each entry and returns the same plain success/error envelope as CREATE.

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

All cron fields are evaluated in UTC. Schedule definitions do not select a
local timezone, and daylight-saving transitions do not alter their cadence.

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
- `LIST`: optional `[cursor][limit]`, returning one route-ordered page with `has_more`, a continuation, and entry sentinels
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

- 7050 = ERR_SCHEDULE_NOT_FOUND
- 7051 = ERR_INVALID_CRON
- 7052 = ERR_DEFINITION_LIMIT
- 7008 = ERR_BAD_REQUEST
- 7053 = ERR_INVALID_TARGET
- 7003 = ERR_INVALID_SUBSCRIPTION_PATTERN
- 7004 = ERR_SUBSCRIPTION_LIMIT
- 7054 = ERR_INVALID_DELIVERY_MODE
- 7005 = ERR_BUSY
- 7006 = ERR_BACKEND_ERROR (accepted operation, outcome indeterminate)
- 7007 = ERR_TIMEOUT

`ERR_BUSY` (7005) means the broker declined the request before the Schedule
domain saw it; nothing applied and re-sending after a backoff is safe. It is
distinct from `ERR_BAD_REQUEST`: clients must not tell callers that their cron
or payload is malformed when the broker could not service an otherwise valid
request.

`ERR_TIMEOUT` (7007) reports that the broker accepted the command but did not
finish it before its deadline. Its envelope carries effect INDETERMINATE: the
command may still apply, so re-sending can apply the same create or cancel
twice. A client that knows its operation is idempotent may still retry
deliberately; an automatic `IsRetryable` retry must not.

#### Acceptance Tests

- create schedules task with cron expression
- create on existing route updates (upsert)
- cancel prevents future notifications
- cancel on nonexistent route succeeds (idempotent)
- list returns all created schedules
- schedule persists across broker restart
- matching live registrations receive SCHEDULE_NOTIFY when schedule fires
- invalid cron expression rejected with 7051
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
  [u16 BE] code
  [u8]     class
  [u8]     effect
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
  four-segment Schedule route are rejected with 7003 `INVALID_PATTERN`.
- A session may retain at most 1,024 Schedule registrations total and 128
  wildcard registrations; overflow is rejected with 7004 `REGISTRATION_LIMIT`.
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
  [u16 BE] code
  [u8]     class
  [u8]     effect
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
