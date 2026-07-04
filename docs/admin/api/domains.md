## Domain-Specific Endpoints
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
    { "resource": "orders" },
    { "resource": "payments" }
  ]
}
```

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

#### List Active Subscriptions
```
GET /admin/notice/subscriptions?realm={realm}&route_pattern={pattern}
```
`created_at` is the time the current in-memory subscription was created. `notifications_received` is a live delivery counter for the current in-memory subscription and resets when the client reconnects or the broker restarts.

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
#### List Routes with Subscriber Counts
```
GET /admin/notice/routes?realm={realm}
```
`publishes_total` and `publishes_per_minute` describe live broker-observed activity for the current process lifetime. They are not durable replay or history counters.

**Response**:
```json
{
  "routes": [
    {
      "route": "notice://prod/events/orders",
      "subscribers": 23,
      "publishes_total": 8456,
      "publishes_per_minute": 45
    }
  ]
}
```
#### Notice Statistics
```
GET /admin/notice/stats?realm={realm}
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
#### Force Cancel Subscription (Admin)
```
POST /admin/notice/subscriptions/{subscription_id}/cancel
```
Force-removes an active in-memory notice subscription from the current broker instance. This endpoint does not cancel durable state and has no effect after the owning session disconnects.

**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
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

#### List Registered Workers
```
GET /admin/rpc/workers?realm={realm}
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
GET /admin/rpc/pending?realm={realm}
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
#### RPC Statistics
```
GET /admin/rpc/stats?realm={realm}
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
#### Cancel Request (Admin)
```
POST /admin/rpc/requests/{correlation_id}/cancel
```
**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
### Lease Domain
All Lease admin responses reflect live in-memory state for the current broker process only. Lease ownership disappears on disconnect cleanup or broker restart, and `fencing_token` values are process-local rather than durable or cross-node identifiers.

#### List Active Leases
```
GET /admin/lease/leases?realm={realm}
```
`acquired_at` and `expires_at` describe the current in-memory lease window only. `fencing_token` is valid only within the running broker process and resets after restart.

**Response**:
```json
{
  "leases": [
    {
      "realm": "prod",
      "area": "locks",
      "resource": "job-executor",
      "owner_session_id": "sess_abc123",
      "acquired_at": "2026-01-31T10:30:00Z",
      "expires_at": "2026-01-31T10:35:00Z",
      "renewals": 5,
      "fencing_token": 42
    }
  ]
}
```
#### Lease Statistics
```
GET /admin/lease/stats?realm={realm}
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
#### Force Release Lease (Admin)
```
POST /admin/lease/leases/{lease_id}/release
```
Force-releases an active in-memory lease on the current broker instance only. This endpoint does not recover or revoke durable state, and it has no effect after the owning session has already disconnected or the broker has restarted.

**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
### Schedule Domain
Schedule definitions are durable and are preloaded into per-family Schedule actors during broker boot. Admin schedule views therefore reflect persisted definitions before any schedule-domain traffic reaches that family. Schedule notifications and subscriptions remain live session-scoped delivery only, and `last_run` / `executions_total` are still non-authoritative placeholders in this round.

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
