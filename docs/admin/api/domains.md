## Domain-Specific Endpoints

Admin domain routes are mounted under `/api/v1/{route_family}/{domain}/...`.
Every domain read or mutation must name a concrete provisioned route family;
the former domain-first `/api/v1/{domain}/...` paths are removed and return
404. Wildcard admin sessions may use `/api/v1/all/{domain}/...` only for
aggregate reads. Paths under `/admin/{domain}/...` or
`/api/v1/admin/{domain}/...` are not mounted.

In the endpoint examples below, insert `/{route_family}` between `/api/v1` and
the domain name. Query parameters formerly used only to select a route family
are no longer accepted as a substitute for that path segment.

### KV Domain
All KV admin responses separate durable committed data from live transaction
coordination. Committed values persist according to storage commit semantics,
but open transactions shown here are current-process in-memory state only. They
disappear on disconnect cleanup or broker restart and do not imply durable
transaction recovery.

#### Get KV Resource
```
GET /api/v1/kv/realms/{realm}/areas/{area}/resources/{resource}
```
`transactions_active` counts only live session-scoped transactions for the
current broker process. It resets on disconnect cleanup or broker restart.

**Response**:
```json
{
  "realm": "prod",
  "area": "app",
  "resource": "users",
  "transactions_active": 1
}
```
#### List KV Transactions For A Resource
```
GET /api/v1/kv/realms/{realm}/areas/{area}/resources/{resource}/transactions
```
`tx_id` is a session-scoped runtime handle for the currently running broker
process. It is not a durable recovery token, and the listed transactions do not
survive disconnect or restart.

**Response**:
```json
{
  "transactions": [
    {
      "tx_id": 1234567890,
      "realm": "prod",
      "area": "app",
      "resource": "users",
      "mode": "ReadWrite",
      "started_at": "2026-01-31T10:30:00Z",
      "operations_count": 5,
      "idle_seconds": 12
    }
  ]
}
```
### Stream Domain
Stream admin responses combine durable committed stream metadata with live
current-process append-session counts. Committed streams remain visible after
restart because `offset`, `watermark`, and `size_bytes` come from durable
metadata. `sessions_active` counts only live append sessions on the current
broker process and resets on disconnect cleanup or broker restart. Consumer
cursors remain client-managed; there are no durable broker-side cursor groups.

#### List Stream Resources In An Area
```
GET /api/v1/stream/realms/{realm}/areas/{area}/resources
```
**Response**:
```json
{
  "realm": "prod",
  "area": "events",
  "resources": [
    { "resource": "orders", "committed_event_count": 384922, "size_bytes": 52847392, "sessions_active": 3 },
    { "resource": "payments", "committed_event_count": 812, "size_bytes": 98304, "sessions_active": 0 }
  ]
}
```
Concrete-family paths report only that family. `/api/v1/all/...` sums committed
event counts, storage bytes, and live append sessions for identical resource
paths across accessible families.

#### Get Stream Resource Detail
```
GET /api/v1/stream/realms/{realm}/areas/{area}/resources/{resource}
```
**Response**:
```json
{
  "realm": "prod",
  "area": "events",
  "resource": "orders",
  "offset": 384921,
  "watermark": 384921,
  "size_bytes": 52847392,
  "sessions_active": 3
}
```
#### Notes
- `offset` is the last committed resource offset.
- `watermark` is the highest committed visible offset for that resource.
- `size_bytes` is derived from durable committed-byte metadata.
- `sessions_active` is a live append-session count only; it is not a durable writer inventory.
- Stream subscriptions remain session-scoped best-effort delivery and are not represented as durable admin state.
### Notice Domain
All Notice admin responses reflect live in-memory broker state only. Notice subscriptions are session-scoped, disappear on disconnect, and are not restored after broker restart.

The Notice resource list includes `subscriptions_active`,
`notifications_received`, and `publishes_per_minute`. These are current-process
counters and rates, not durable delivery history. `/all/` sums identical paths
across accessible families.

#### Get Notice Resource Detail
```
GET /api/v1/notice/realms/{realm}/areas/{area}/resources/{resource}
```
`subscriptions_active` counts only current in-memory subscriptions matching this
resource. It resets on disconnect cleanup and broker restart.

**Response**:
```json
{
  "realm": "prod",
  "area": "events",
  "resource": "orders",
  "subscriptions_active": 3
}
```

#### List Active Resource Subscriptions
```
GET /api/v1/notice/realms/{realm}/areas/{area}/resources/{resource}/subscriptions
```
`created_at` is the time the current in-memory subscription was created.
`notifications_received` is a live delivery counter for the current in-memory
subscription and resets when the client reconnects or the broker restarts.

**Response**:
```json
{
  "subscriptions": [
    {
      "subscription_id": 42,
      "session_id": "sess_abc123",
      "realm": "prod",
      "pattern": "notice://prod/events/**",
      "created_at": "2026-01-31T10:30:00Z",
      "notifications_received": 1847
    }
  ]
}
```

#### Search Notice Delivery Observations
```
GET /api/v1/{route_family}/notice/deliveries?realm={realm}&area={area}&resource={resource}&q={query}&limit={limit}
```
This route requires a concrete route family, either as the path segment above or
as `route_family` when using `/api/v1/notice/deliveries`. It returns current
broker observations for active subscriptions and live route counters.

#### Notice Statistics
```
GET /api/v1/notice/stats
```
These values are point-in-time in-memory statistics for the running broker instance.

**Response**:
```json
{
  "subscriptions_active": 312,
  "routes_registered": 67,
  "publishes_total": 1847392,
  "publishes_per_second": 95,
  "fanout_ratio": 4.2
}
```
### Queue Domain
Queue admin responses reflect only the current broker's warm in-memory actor state unless otherwise noted. Queue data remains durable according to the configured queue write policy, but warm resource counts and live lease rows can disappear after disconnect cleanup, idle actor eviction, or broker restart until traffic rehydrates that queue.

#### List Queue Resources Under An Area
```
GET /api/v1/queue/realms/{realm}/areas/{area}/resources
```

#### Get Queue Resource Detail
```
GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}?family={family}
```
`family` is optional on read routes. When omitted, queue detail aggregates warm state across route families that share the same `{realm}/{area}/{resource}` on the current broker. When provided, the response is filtered to that exact queue identity.

`messages_ready`, `messages_delayed`, `messages_inflight`, `messages_dead_lettered`, and `messages_total` are point-in-time counts for the current broker only. They are not a durable catalog of every accepted queue in storage.

**Response**:
```json
{
  "realm": "prod",
  "area": "jobs",
  "resource": "emails",
  "messages_ready": 1847,
  "messages_delayed": 12,
  "messages_inflight": 67,
  "messages_dead_lettered": 4,
  "messages_total": 1930,
  "oldest_message_age_seconds": 0
}
```

#### List Live Queue Inflight Entries For A Resource
```
GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/inflight?family={family}
```
`family` is optional. When provided, only inflight entries for that exact queue identity are returned.

`inflight_token` and `session_id` describe live in-memory inflight ownership only. They are dropped on disconnect cleanup, invalidated on expiry, and never survive broker restart.

**Response**:
```json
{
  "inflight": [
    {
      "message_id": 123456,
      "family": 1,
      "realm": "prod",
      "area": "jobs",
      "resource": "emails",
      "inflight_token": "987654321",
      "session_id": "12345",
      "expires_at": "2026-01-31T10:35:00Z",
      "attempts": 2
    }
  ]
}
```

#### List Queue Dead Letters For A Resource
```
GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters?family={family}
```
`family` is optional on reads. Dead-letter rows remain durably stored, but this endpoint only exposes DLQ rows for queue actors that are currently warm on this broker.

**Response**:
```json
{
  "messages": [
    {
      "message_id": 123456,
      "family": 1,
      "realm": "prod",
      "area": "jobs",
      "resource": "emails",
      "dead_lettered_at": "2026-01-31T10:35:00Z",
      "attempts": 3,
      "reason": "max_attempts_exceeded"
    }
  ]
}
```

#### Replay Queue Dead Letter
```
POST /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}/replay?family={family}
```
`family` is required for destructive queue actions because queue identity includes route family. On success the retained DLQ row is moved back to ready state, its attempts counter resets, and the endpoint returns `204 No Content`.

#### Purge Queue Dead Letter
```
DELETE /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}?family={family}
```
`family` is required. On success the retained DLQ row is permanently removed from storage and the endpoint returns `204 No Content`.

### RPC Domain
All RPC admin endpoints expose live in-memory state for the current broker instance only. Worker registrations and pending requests disappear on disconnect or broker restart and are not durable recovery queues.
The broker updates this read model as a coalesced operational snapshot, so very recent subscribe, unsubscribe, timeout, and cleanup events can lag briefly in admin responses. Treat these endpoints as near-live diagnostics, not strongly consistent reads of the hot path.

The RPC resource list unions worker and pending-only routes and reports
`workers_registered`, `requests_pending`, and nullable
`slowest_worker_average_latency_ms`. Latency remains null until a worker has
handled a request. `/all/` sums counts and takes the slowest available latency
across accessible families.

#### List Operations For A Resource
```
GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations
```
**Response**:
```json
{
  "realm": "prod",
  "area": "compute",
  "resource": "tasks",
  "operations": [{ "operation": "heavy-task" }]
}
```

#### Get RPC Operation Detail
```
GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}
```
The counts are point-in-time in-memory values for the running broker process.

**Response**:
```json
{
  "realm": "prod",
  "area": "compute",
  "resource": "tasks",
  "operation": "heavy-task",
  "workers_registered": 2,
  "requests_pending": 1,
  "slowest_worker_average_latency_ms": 145.0
}
```

#### List Registered Workers For An Operation
```
GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}/workers
```
**Response**:
```json
{
  "workers": [
    {
      "session_id": "sess_abc123",
      "realm": "prod",
      "route": "rpc://prod/compute/tasks/heavy-task",
      "registered_at": "2026-01-31T10:00:00Z",
      "requests_handled": 1847,
      "average_latency_ms": 145
    }
  ]
}
```

#### List Pending Requests
```
GET /api/v1/rpc/pending
```
Pending requests shown here are only those still tracked in memory by the running broker. A restart clears this list immediately.

**Response**:
```json
{
  "requests": [
    {
      "correlation_id": "0123456789abcdef",
      "route": "rpc://prod/compute/tasks/heavy-task",
      "submitted_at": "2026-01-31T10:34:50Z",
      "age_seconds": 10,
      "worker_session_id": "sess_abc123"
    }
  ]
}
```

#### Search RPC Call Observations
```
GET /api/v1/{route_family}/rpc/calls?realm={realm}&area={area}&resource={resource}&operation={operation}&q={query}&limit={limit}
```
This route requires a concrete route family. It combines current pending
requests and worker registrations into a single near-live diagnostic list.

**Response**:
```json
{
  "route_family": 1,
  "limit": 100,
  "observations": [
    {
      "route_family": 1,
      "realm": "prod",
      "area": "compute",
      "resource": "tasks",
      "operation": "heavy-task",
      "route": "rpc://prod/compute/tasks/heavy-task",
      "correlation_id": "0123456789abcdef",
      "state": "pending",
      "age_seconds": 10
    }
  ]
}
```
#### RPC Statistics
```
GET /api/v1/rpc/stats
```
`workers_registered` and `requests_pending` are point-in-time in-memory counts for the running broker process. They reset on restart and should not be interpreted as durable backlog or recovery state.
Like the worker and pending endpoints, these counters are served from the current broker's coalesced admin snapshot and can lag the latest in-flight mutations briefly.

**Response**:
```json
{
  "workers_registered": 34,
  "requests_pending": 12,
  "requests_completed_total": 184739,
  "requests_timed_out_total": 42,
  "operations_per_second": 42,
  "average_latency_ms": 125
}
```
### Lease Domain
All Lease admin responses reflect live in-memory state for the current broker process only. Lease ownership disappears on disconnect cleanup or broker restart, and `fencing_token` values are process-local rather than durable or cross-node identifiers.

The Lease resource list unions owned and waiter-only resources and reports
`active_leases`, `waiters`, and `oldest_lease_age_seconds`. `/all/` sums counts
and takes the oldest live age across accessible families.

#### Get Lease Resource Detail
```
GET /api/v1/lease/realms/{realm}/areas/{area}/resources/{resource}
```
`active_leases` counts only leases currently tracked in memory for this
resource. It resets on disconnect cleanup and broker restart.

**Response**:
```json
{
  "realm": "prod",
  "area": "locks",
  "resource": "job-executor",
  "active_leases": 1,
  "oldest_lease_age_seconds": 42
}
```

#### Search Active Leases And Waiters
```
GET /api/v1/{route_family}/lease/search?realm={realm}&area={area}&resource={resource}&owner={owner}&state={state}&limit={limit}
```
`acquired_at` and `expires_at` describe the current in-memory lease window only.
Fencing tokens are valid only within the running broker process and reset after
restart. This route requires a concrete route family.

**Response**:
```json
{
  "route_family": 1,
  "limit": 100,
  "items": [
    {
      "route_family": 1,
      "realm": "prod",
      "area": "locks",
      "resource": "job-executor",
      "state": "owned",
      "owner_id": "worker-1",
      "owner_session_id": "12345",
      "expires_at": "2026-01-31T10:35:00Z",
      "acquired_at": "2026-01-31T10:30:00Z",
      "renewals": 5,
      "pending_waiters": 0
    }
  ]
}
```
#### Lease Statistics
```
GET /api/v1/lease/stats
```
These values are point-in-time in-memory counts for the running broker process and should not be interpreted as durable recovery state.

**Response**:
```json
{
  "leases_active": 18,
  "leases_acquired_total": 8456,
  "leases_expired_total": 342,
  "operations_per_second": 5
}
```
### Schedule Domain
Schedule definitions are durable and are preloaded into per-family Schedule actors during broker boot. Admin schedule views therefore reflect persisted definitions before any schedule-domain traffic reaches that family. Schedule notifications and subscriptions remain live session-scoped delivery only, and `last_run` / `executions_total` are still non-authoritative placeholders in this round.

The Schedule resource list reports enabled definitions as `schedules_active`,
durable `pending_claims`, and the earliest enabled `next_run`. Disabled-only
resources remain listed with a null `next_run`; pending-only resources are also
included. `/all/` sums counts and selects the earliest enabled run across
accessible families.

#### Get Schedule Resource
```
GET /api/v1/schedule/realms/{realm}/areas/{area}/resources/{resource}
```
**Response**:
```json
{
  "realm": "prod",
  "area": "jobs",
  "resource": "cleanup",
  "enabled": true,
  "cron": "0 2 * * *",
  "next_run": "2026-02-01T02:00:00Z",
  "executions_total": 0
}
```

If multiple operations exist under the same resource, `cron` is omitted and `next_run` is the earliest next durable fire among that resource's persisted schedules.
