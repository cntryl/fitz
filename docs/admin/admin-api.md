# Fitz Admin API Specification
**Purpose**: Runtime visibility, metrics, and operational control for Fitz broker.
**Protocol**: HTTP REST API (coexists with WebSocket data plane on same port)  
**Port**: Same as data plane (default: 8080)  
**Route Structure**:  
  - `/` - Single Page Application (SPA) static files
  - `/api/v1/*` - Admin REST API endpoints
  - `/metrics` - Prometheus metrics endpoint
  - `/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz` - Kubernetes and load balancer probes
  - `/ws` - WebSocket upgrade for data plane
**Data Plane**: WebSocket upgrade on `/ws`, TCP on same port (protocol detection)  
**Authentication**:  
  - `/` - No authentication (SPA public access)
  - `/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz` - No authentication (for load balancer health checks)
  - `/metrics` - **Requires authentication** (JWT or API key)  
  - `/api/v1/*` - **Requires authentication** (JWT or API key with admin permissions)
## Design Principles
1. **Read-heavy**: Most operations are queries for visibility
2. **Safe by default**: Dangerous operations (force rollback, cancel) require explicit confirmation
3. **Realm-scoped**: Queries that expose realm filters operate on the application-defined realm label used in Fitz routes and resources, never on `route_family`
4. **Prometheus-compatible**: Metrics endpoint follows Prometheus format
5. **Minimal dependencies**: No external monitoring system required for basic visibility
6. **SPA-first**: Web interface served at root, all API routes namespaced
## Route Structure
```
/                          → SPA (index.html)
/assets/*                  → SPA static assets (JS, CSS, images)
/ws                        → WebSocket upgrade (data plane)
/livez                     → Kubernetes liveness probe
/targetz                   → Orchestrator target health gate for handoff
/healthz                   → Deployment-safe health gate (mirrors readiness)
/readyz                    → Kubernetes readiness probe
/startupz                  → Kubernetes startup probe
/metrics                   → Prometheus metrics (auth required)
/api/v1/stats              → Global broker statistics (auth required)
/api/v1/kv/stats           → KV domain statistics (auth required)
/api/v1/stream/stats       → Stream domain statistics (auth required)
/api/v1/notice/stats       → Notice domain statistics (auth required)
/api/v1/queue/stats        → Queue domain statistics (auth required)
/api/v1/rpc/stats          → RPC domain statistics (auth required)
/api/v1/lease/stats        → Lease domain statistics (auth required)
```
**Authentication Rules**:
- SPA (`/`, `/assets/*`) - Public access
- Health probes (`/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz`) - Public access (for K8s/load balancers)
- Metrics (`/metrics`) - Requires JWT Bearer token
- Admin API (`/api/v1/*`) - Requires JWT Bearer token with admin scope
## Global Endpoints
### Kubernetes Probes
#### Liveness Probe
```
GET /livez
```
**Authentication**: None (public endpoint for kubelet)
**Purpose**: Indicates if the application is alive and should be restarted if unhealthy.
**Response**: 
- `200 OK` - Application is alive
- `503 Service Unavailable` - Application is stuck/deadlocked, should be restarted
```json
{
  "status": "ok"
}
```
**Criteria**: 
- Runtime is responsive
- No critical failures (panics, deadlocks)
- Does NOT check downstream dependencies
#### Orchestrator Target Health Gate
```
GET /targetz
```
**Authentication**: None (public endpoint for ECS/ALB target groups and Kubernetes probes)
**Purpose**: Orchestrator target health gate for single-writer handoff. This endpoint proves the HTTP listener is running and the process is not draining, but it intentionally does not require the Midge writer lease. Use this for ECS target group health checks or Kubernetes readiness probes when running one active Fitz instance with rolling replacement.
**Do not use this as**:
- A data-plane readiness signal. WebSocket upgrades and TCP sessions still require `/healthz`/`/readyz` readiness.
- A storage health signal. `storage_writer_lease` may be `not_ready` while `/targetz` returns `200`.
**Response**:
- `200 OK` - HTTP target may participate in ECS handoff
- `503 Service Unavailable` - Target is draining or shutting down
```json
{
  "status": "ready",
  "checks": {
    "http_listener": "ok",
    "accepting_target_traffic": "ok",
    "data_plane_ready": "not_ready",
    "storage_writer_lease": "not_ready"
  }
}
```
**ECS use**:
- Configure the ALB target group health check path to `/targetz`.
- Keep `/healthz` and `/readyz` for strict data-plane readiness and operator checks.
- Use ECS rolling deployment settings that allow overlap, for example `minimumHealthyPercent=100` and `maximumPercent=200`, so the replacement target can become ALB-healthy before the old task releases the writer lease.
**Kubernetes use**:
- Configure `startupProbe` and `livenessProbe` to `/livez`.
- Configure `readinessProbe` to `/targetz` for rolling active/passive handoff, and keep `/readyz` or `/healthz` for strict data-plane checks outside the Service readiness gate.
- Use a rolling update with `maxSurge: 1` and `maxUnavailable: 0` for a single-replica Deployment when the new Pod must become Service-ready before Kubernetes terminates the old writer Pod.
- Set `terminationGracePeriodSeconds` longer than `FITZ_DRAIN_GRACE_SECONDS`.
Relevant docs: [ECS rolling deployments](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/deployment-type-ecs.html), [ECS service health and deployment parameters](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service_definition_parameters.html), [ALB target group deregistration delay](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/edit-target-group-attributes.html), [Kubernetes probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/), [Kubernetes rolling updates](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), and [Kubernetes Pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/).
#### Data-Plane Health Gate
```
GET /healthz
```
**Authentication**: None (public endpoint for load balancers)
**Purpose**: Strict data-plane health gate. This intentionally mirrors readiness so traffic does not arrive before the broker has acquired the active single-writer storage lease or after shutdown starts.
**Use this when**:
- Your platform can check only one unauthenticated HTTP endpoint before routing traffic
- You need a single-writer-safe cutover signal for startup, lease handoff, and shutdown
**Do not use this as**:
- A liveness restart signal. `/healthz` is expected to return `503` during startup and shutdown handoff.
- A replacement for Kubernetes liveness. Use `/livez` for restart decisions.
**Response**:
- `200 OK` - Ready to accept traffic
- `503 Service Unavailable` - Not ready, remove from load balancer
```json
{
  "status": "ready",
  "checks": {
    "storage": "ok",
    "storage_writer_lease": "ok",
    "domains_initialized": "ok",
    "auth_configuration": "ok",
    "startup_complete": "ok",
    "accepting_traffic": "ok"
  }
}
```
**Criteria**:
- Storage engine initialized
- Active single-writer storage lease acquired
- Domain actors started
- Auth configuration validated during boot
- Listener startup completed
- Not in shutdown handoff
#### Readiness Probe
```
GET /readyz
```
**Authentication**: None (public endpoint for kubelet)
**Purpose**: Indicates if the application is ready to accept traffic. This currently uses the same readiness contract as `/healthz`, but is intended for native readiness probes while `/healthz` is the external traffic-gate alias.
**Response**: 
- `200 OK` - Ready to accept traffic
- `503 Service Unavailable` - Not ready, remove from load balancer
```json
{
  "status": "ready",
  "checks": {
    "storage": "ok",
    "storage_writer_lease": "ok",
    "domains_initialized": "ok",
    "auth_configuration": "ok",
    "startup_complete": "ok",
    "accepting_traffic": "ok"
  }
}
```
**Criteria**: 
- Storage engine initialized and holding the active single-writer lease
- All domain actors started
- Auth configuration validated during boot
- TCP/WebSocket listeners bound
- Not in shutdown handoff
- Ready to process requests
#### Probe Selection
- Use `/livez` to decide when Fitz should be restarted.
- Use `/targetz` for ECS/ALB target health when a replacement task must become target-healthy before it can acquire the writer lease.
- Use `/targetz` as the Kubernetes `readinessProbe` only for active/passive rolling handoff where the app must reject WS/TCP until strict data-plane readiness.
- Use `/readyz` when your orchestrator supports a distinct readiness probe.
- Use `/healthz` for strict data-plane traffic admission when your platform can tolerate stopping the old writer before the new target is healthy.
- Use `/startupz` to suppress premature liveness checks during long startup windows.
#### Startup Probe
```
GET /startupz
```
**Authentication**: None (public endpoint for kubelet)
**Purpose**: Indicates if the application has completed startup. Prevents premature liveness checks during slow startup.
**Response**: 
- `200 OK` - Startup complete
- `503 Service Unavailable` - Still starting up
```json
{
  "status": "started",
  "startup_time_seconds": 2.5
}
```
**Criteria**: 
- All initialization complete
- Domain actors ready
- Listeners bound
### Health Check (Legacy)
```
GET /health
```
**Authentication**: None (public endpoint for load balancers)
**Purpose**: Legacy alias for `/healthz`.
### Metrics (Prometheus Format)
```
GET /metrics
```
**Authentication**: Required (JWT or API key)
**Response**: Prometheus text format
```
# HELP fitz_connections_total Total number of active connections
# TYPE fitz_connections_total gauge
fitz_connections_total 142
# HELP fitz_messages_received_total Total messages received
# TYPE fitz_messages_received_total counter
fitz_messages_received_total 1847392
# HELP fitz_messages_sent_total Total messages sent
# TYPE fitz_messages_sent_total counter
fitz_messages_sent_total 1847390
# HELP fitz_message_latency_seconds Message processing latency
# TYPE fitz_message_latency_seconds histogram
fitz_message_latency_seconds_bucket{le="0.001"} 1500000
fitz_message_latency_seconds_bucket{le="0.01"} 1800000
fitz_message_latency_seconds_bucket{le="0.1"} 1847000
...
```
### Runtime Stats (Human-Readable)
```
GET /api/v1/stats
```
**Response**:
```json
{
  "broker": {
    "uptime_seconds": 86400,
    "connections": 142,
    "sessions": 142,
    "realms": ["prod", "staging", "dev"],
    "messages_per_second": 450
  },
  "domains": {
    "kv": {
      "transactions_active": 23,
      "keys_total": 12847,
      "operations_per_second": 120
    },
    "stream": {
      "streams_active": 45,
      "events_total": 384921,
      "operations_per_second": 85
    },
    "notice": {
      "subscriptions_active": 312,
      "publishes_per_second": 95
    },
    "queue": {
      "messages_pending": 1847,
      "inflight_active": 67,
      "operations_per_second": 78
    },
    "rpc": {
      "workers_registered": 34,
      "requests_pending": 12,
      "operations_per_second": 42
    },
    "lease": {
      "leases_active": 18,
      "operations_per_second": 5
    },
    "schedule": {
      "schedules_active": 56,
      "executions_per_minute": 23
    }
  }
}
```
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
## Sessions & Connections
### List Active Sessions
```
GET /admin/sessions
```
Active-session snapshots expose broker-local session identity, resolved
`route_family`, and the identity claim/value used for route-family resolution.
They do not expose or synthesize a session realm; realm remains part of Fitz
routes and resource identities.

**Response**:
```json
{
  "sessions": [
    {
      "session_id": "sess_abc123",
      "route_family": 2,
      "subject": "auth0|user123",
      "identity_claim": "org_id",
      "identity_value": "org_xyz",
      "connected_at": "2026-01-31T10:00:00Z",
      "idle_seconds": 45,
      "messages_received": 1847,
      "messages_sent": 1845,
      "transport": "websocket",
      "remote_addr": "192.168.1.100:54321"
    }
  ]
}
```
### Close Session (Admin)
```
POST /admin/sessions/{session_id}/close
```
**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
## Implementation Status
### ✅ Implemented Endpoints
**Health Probes (No Auth)**:
- `GET /livez` - Liveness probe
- `GET /targetz` - Orchestrator target health gate
- `GET /healthz` - Deployment-safe health gate
- `GET /readyz` - Readiness probe
- `GET /startupz` - Startup probe
- `GET /health` - Legacy health check
**Metrics (Auth Required)**:
- `GET /metrics` - Prometheus metrics
**Global Stats (Admin Auth)**:
- `GET /api/v1/stats` - Global broker and domain statistics
**Domain Stats (Admin Auth)**:
- `GET /api/v1/kv/stats` - KV domain statistics
- `GET /api/v1/stream/stats` - Stream domain statistics
- `GET /api/v1/notice/stats` - Notice domain statistics
- `GET /api/v1/queue/stats` - Queue domain statistics
- `GET /api/v1/rpc/stats` - RPC domain statistics
- `GET /api/v1/lease/stats` - Lease domain statistics
- `GET /api/v1/schedule/stats` - Schedule domain statistics
**List Endpoints (Admin Auth)** - current surface plus remaining follow-up:
- `GET /api/v1/kv/realms/{realm}/areas/{area}/resources/{resource}` - Get live KV resource detail
- `GET /api/v1/kv/realms/{realm}/areas/{area}/resources/{resource}/transactions` - List live session-scoped KV transactions for a resource
  - KV transaction snapshots are current-process only. They disappear on disconnect cleanup and are not restored after broker restart.
- `GET /api/v1/stream/realms/{realm}/areas/{area}/resources` - List stream resources in an area
- `GET /api/v1/stream/realms/{realm}/areas/{area}/resources/{resource}` - Get stream resource detail
  - Stream resource detail combines durable committed metadata with the current broker's live append-session count. It does not represent durable consumer cursors or broker-restored sessions.
- `GET /api/v1/admin/notice/subscriptions?realm={realm}&route_pattern={pattern}` - List subscriptions
- `GET /api/v1/admin/notice/routes?realm={realm}` - List routes with subscriber counts
- `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}` - Get warm Queue resource detail
- `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/inflight` - List live queue inflight entries for a resource
- `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters` - List retained DLQ rows for a resource
  - Queue resource and lease snapshots are current-process only. They can disappear after disconnect cleanup, idle actor eviction, or broker restart until traffic rehydrates the queue.
  - Queue detail and list routes accept an optional `family` query parameter. Replay and purge require it.
- `POST /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}/replay?family={family}` - Replay a retained DLQ row
- `DELETE /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}?family={family}` - Purge a retained DLQ row
- `GET /api/v1/admin/rpc/workers?realm={realm}` - List registered RPC workers
- `GET /api/v1/admin/rpc/pending?realm={realm}` - List pending RPC requests
- `GET /api/v1/admin/lease/leases?realm={realm}` - List active in-memory leases
  - Lease snapshots are live only. They disappear on disconnect cleanup and are not restored after broker restart.
- `GET /api/v1/schedule/realms/{realm}/areas/{area}/resources/{resource}` - Get durable Schedule resource detail
  - Schedule definitions are durable and boot-loaded. Notification delivery remains live only, and `last_run` / `executions_total` are placeholders rather than durable execution history.
- `GET /api/v1/admin/sessions?realm={realm}` - List active sessions
### 🚧 To Be Implemented
**Admin Commands (Admin Auth + X-Confirm Header)**:
- `POST /api/v1/admin/kv/transactions/{tx_id}/rollback` - Force rollback transaction
- `POST /api/v1/admin/notice/subscriptions/{subscription_id}/cancel` - Cancel subscription
- `POST /api/v1/admin/rpc/requests/{correlation_id}/cancel` - Cancel RPC request
- `POST /api/v1/admin/lease/leases/{lease_id}/release` - Force release lease
- `POST /api/v1/admin/schedule/schedules/{schedule_id}/trigger` - Trigger schedule manually
- `POST /api/v1/admin/sessions/{session_id}/close` - Close session
**Pagination Support**:
- Add `?limit=` and `?offset=` query parameters to list endpoints
**Domain Integration**:
- Each domain needs to implement methods to provide list data
- KV: Live transaction snapshots are exposed per resource and reflect only active in-memory session-scoped state
- Stream: Rebuild resource detail from durable metadata and expose live append-session counts separately
- Notice: Track subscriptions and routes, expose via admin query
- Queue: Track queue depths and leases, expose via admin query
- RPC: Track workers and pending requests, expose via admin query
- Lease: Track active leases, expose via admin query
- Sessions: Track active sessions, expose via admin query
## Implementation Notes
### Metrics Collection
Each domain should maintain lightweight counters:
```rust
pub struct DomainMetrics {
    // Counters (monotonic)
    operations_total: AtomicU64,
    errors_total: AtomicU64,
    
    // Gauges (current value)
    active_count: AtomicU64,
    
    // Histograms (latency tracking)
    latency_histogram: HistogramVec,
}
```
### Admin Actor Pattern
Create a dedicated `AdminActor` that:
1. Receives HTTP requests from admin REST handler
2. Queries domain actors for stats (via internal message passing)
3. Aggregates responses
4. Returns JSON
```rust
pub enum AdminQuery {
    KvStats { realm: Option<String> },
    KvTransactions { realm: Option<String> },
    NoticeSubscriptions { realm: Option<String>, pattern: Option<String> },
    // ... etc
}
pub enum AdminCommand {
    RollbackTransaction { tx_id: u64 },
    CancelSubscription { subscription_id: u64 },
    ExpireLease { lease_id: u64 },
    // ... etc
}
```
### Safety Considerations
1. **Authentication**: 
   - `/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz`, `/health` - No auth (for kubelet/load balancers)
   - `/metrics` - Requires JWT or API key (prevents information disclosure)
   - `/admin/*` - Requires JWT or API key with `admin:read` permission
2. **Authorization**: 
   - Admin queries (GET) require `admin:read` permission
   - Admin commands (POST) require `admin:write` permission
3. **Confirmation Header**: Dangerous operations require `X-Confirm: true` header
4. **Audit Logging**: All admin commands should be logged with timestamp, user, and action
5. **Rate Limiting**: Prevent abuse of admin queries (especially metrics scraping)
### Probe Implementation
```rust
use hyper::{Body, Response, StatusCode};
use serde_json::json;
pub async fn handle_liveness() -> Result<Response<Body>, Infallible> {
    // Check if runtime is responsive
    // Return 503 only if deadlocked/panicked
    let response = json!({ "status": "ok" });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}
pub async fn handle_readiness(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    // Check if ready to accept traffic
    if !runtime.is_ready_for_traffic() {
        let response = json!({
            "status": "not_ready",
            "checks": {
                "storage": if runtime.is_storage_ready() { "ok" } else { "not_ready" },
                "storage_writer_lease": if runtime.is_storage_ready() { "ok" } else { "not_ready" },
                "domains_initialized": if runtime.are_domains_ready() { "ok" } else { "not_ready" },
                "auth_configuration": if runtime.is_auth_config_ready() { "ok" } else { "not_ready" },
                "startup_complete": if runtime.is_startup_complete() { "ok" } else { "not_ready" },
                "accepting_traffic": if !runtime.is_shutting_down() { "ok" } else { "not_ready" }
            }
        });
        return Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }
    
    let response = json!({
        "status": "ready",
        "checks": {
            "storage": "ok",
            "storage_writer_lease": "ok",
            "domains_initialized": "ok",
            "auth_configuration": "ok",
            "startup_complete": "ok",
            "accepting_traffic": "ok"
        }
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}
pub async fn handle_startup(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    // Check if startup complete
    if !runtime.is_startup_complete() {
        let response = json!({ "status": "starting" });
        return Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }
    
    let response = json!({
        "status": "started",
        "startup_time_seconds": runtime.startup_duration().as_secs_f64()
    });
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}
```
### Performance Considerations
1. **Caching**: Cache stats with 1-second TTL to avoid overwhelming domain actors
2. **Pagination**: List endpoints should support `?limit=` and `?offset=`
3. **Filtering**: All list endpoints should support realm filtering
4. **Async**: Admin queries should not block domain actors
## Example Implementation
### Admin HTTP Handler
```rust
use hyper::{Body, Request, Response, StatusCode};
use serde_json::json;
async fn handle_admin(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    
    // Parse query params
    let query = req.uri().query();
    let realm = parse_realm_filter(query);
    
    match path {
        "/admin/stats" => {
            let stats = get_global_stats(runtime).await;
            json_response(stats)
        }
        "/admin/kv/stats" => {
            let stats = get_kv_stats(runtime, realm).await;
            json_response(stats)
        }
        "/admin/kv/transactions" => {
            let txs = get_kv_transactions(runtime, realm).await;
            json_response(txs)
        }
        "/admin/notice/subscriptions" => {
            let subs = get_notice_subscriptions(runtime, realm).await;
            json_response(subs)
        }
        _ => Ok(not_found()),
    }
}
fn json_response<T: Serialize>(data: T) -> Result<Response<Body>, Infallible> {
    let json = serde_json::to_string(&data).unwrap();
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(json))
        .unwrap())
}
fn unauthorized() -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .body(Body::from("Unauthorized"))
        .unwrap()
}
fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not Found"))
        .unwrap()
}
```
```
### Domain Stats Trait
```rust
pub trait DomainStats {
    fn get_stats(&self, realm: Option<&str>) -> DomainStatsSnapshot;
    fn get_active_items(&self, realm: Option<&str>) -> Vec<ActiveItem>;
}
impl DomainStats for KvActor {
    fn get_stats(&self, realm: Option<&str>) -> DomainStatsSnapshot {
        // Return current transaction count, key count, etc.
    }
}
```
## Recommended Deployment
1. **Admin API on same port** as data plane (e.g., 8080) for cloud platform compatibility
2. **Path-based routing**:
   - `/livez` - Kubernetes liveness probe (no auth)
   - `/targetz` - Orchestrator target health gate (no auth)
   - `/healthz` - Deployment-safe health gate (no auth)
   - `/readyz` - Kubernetes readiness probe (no auth)
   - `/startupz` - Kubernetes startup probe (no auth)
   - `/health` - Legacy health check (no auth)
   - `/ws` - WebSocket upgrade for data plane
   - `/metrics` - Prometheus metrics (**requires auth**)
   - `/admin/*` - Admin API (**requires auth + admin permissions**)
3. **TCP connections** use protocol detection on same port (raw TCP vs HTTP)
4. **Enable Prometheus scraping** at `/metrics`
5. **Dashboard**: Use Grafana to visualize metrics
6. **Alerting**: Set up alerts on key metrics (high latency, error rates)
### Cloud Platform Compatibility
Works with single-port constraints:
- ✅ **Azure Container Apps** - Single port with HTTP/WebSocket
- ✅ **Google Cloud Run** - Single port (HTTP/WebSocket)
- ✅ **AWS App Runner** - Single port
- ✅ **Kubernetes** - Single service port with proper probes
- ✅ **Docker Compose** - Simple port mapping
### Kubernetes Deployment Example
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: fitz
spec:
  replicas: 1
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxSurge: 1
      maxUnavailable: 0
  selector:
    matchLabels:
      app: fitz
  template:
    metadata:
      labels:
        app: fitz
    spec:
      containers:
      - name: fitz
        image: fitz:latest
        ports:
        - containerPort: 8080
          name: http
        
        # Startup probe - confirms the HTTP process is alive without waiting
        # for the single-writer lease.
        startupProbe:
          httpGet:
            path: /livez
            port: 8080
          initialDelaySeconds: 0
          periodSeconds: 2
          timeoutSeconds: 1
          failureThreshold: 30  # 60 seconds max startup time
        
        # Liveness probe - restart if unhealthy
        livenessProbe:
          httpGet:
            path: /livez
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 3
        
        # Readiness probe - controls Service endpoints for rolling handoff.
        # Use /readyz separately when checking strict data-plane readiness.
        readinessProbe:
          httpGet:
            path: /targetz
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
          timeoutSeconds: 2
          failureThreshold: 2
        
        env:
        - name: RUST_LOG
          value: "info"
        
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
apiVersion: v1
kind: Service
metadata:
  name: fitz
spec:
  type: ClusterIP
  selector:
    app: fitz
  ports:
  - port: 8080
    targetPort: 8080
    name: http
```
### Example Hyper Router
```rust
use hyper::{Body, Request, Response, StatusCode, Method};
use hyper::service::{make_service_fn, service_fn};
use std::convert::Infallible;
pub async fn handle_request(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    let method = req.method();
    
    match (method, path) {
        // Kubernetes probes (no auth)
        (&Method::GET, "/livez") => handle_liveness().await,
        (&Method::GET, "/targetz") => handle_targetz(runtime).await,
        (&Method::GET, "/healthz") => handle_healthz(runtime).await,
        (&Method::GET, "/readyz") => handle_readiness(runtime).await,
        (&Method::GET, "/startupz") => handle_startup(runtime).await,
        (&Method::GET, "/health") => handle_health(runtime).await,
        
        // Metrics (requires auth)
        (&Method::GET, "/metrics") => {
            if !check_auth(&req).await {
                return Ok(unauthorized());
            }
            handle_metrics(runtime).await
        }
        
        // Admin API (requires auth + admin permission)
        (&Method::GET, path) if path.starts_with("/admin/") => {
            if !check_admin_auth(&req).await {
                return Ok(unauthorized());
            }
            handle_admin(req, runtime).await
        }
        
        // WebSocket upgrade
        (&Method::GET, "/ws") => handle_websocket_upgrade(req).await,
        
        _ => Ok(not_found()),
    }
}
pub async fn serve(addr: SocketAddr, runtime: Arc<Runtime>) {
    let runtime = Arc::clone(&runtime);
    
    let make_svc = make_service_fn(move |_conn| {
        let runtime = Arc::clone(&runtime);
        async move {
            Ok::<_, Infallible>(service_fn(move |req| {
                handle_request(req, Arc::clone(&runtime))
            }))
        }
    });
    
    let server = hyper::Server::bind(&addr).serve(make_svc);
    
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}
```
fn admin_router() -> Router {
    Router::new()
        .route("/stats", get(stats))
        .route("/kv/stats", get(kv_stats))
        .route("/kv/transactions", get(kv_transactions))
        .route("/notice/subscriptions", get(notice_subscriptions))
        // ... etc
        .layer(AuthLayer::new()) // Require JWT auth
}
```
### Protocol Detection
On port 8080:
1. **HTTP GET/POST** → Check path:
   - `/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz`, `/health` → Health probes (no auth)
   - `/metrics` → Metrics (check JWT/API key)
   - `/admin/*` → Admin API (check JWT/API key + admin permission)
   - `/ws` with Upgrade header → WebSocket handler
2. **Raw TCP** (binary data, no HTTP headers) → TCP frame handler
## Minimal Initial Implementation
Start with these essential endpoints:
### Phase 1: Health & Observability
1. `GET /livez` - Kubernetes liveness probe
2. `GET /targetz` - Orchestrator target health gate
3. `GET /healthz` - Deployment-safe health gate
4. `GET /readyz` - Kubernetes readiness probe
5. `GET /startupz` - Kubernetes startup probe
6. `GET /health` - Legacy health check
7. `GET /metrics` - Prometheus metrics (with auth)
8. `GET /admin/stats` - Human-readable overview
### Phase 2: Domain Visibility
9. `GET /admin/kv/stats` - KV visibility
10. `GET /admin/notice/subscriptions` - Notice visibility
11. `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}` - Queue warm-state visibility
12. `GET /admin/sessions` - Active connections
### Phase 3: Admin Commands
13. Domain-specific commands (rollback, cancel, release) with `X-Confirm: true`
