## Design Principles
1. **Read-heavy**: Most operations are queries for visibility
2. **Safe by default**: Dangerous operations (force rollback, cancel) require explicit confirmation
3. **Realm-scoped**: Queries that expose realm filters operate on the application-defined realm label used in Fitz routes and resources, never on `route_family`
4. **Explicit metrics boundaries**: Raw Prometheus is dedicated and unauthenticated; authenticated admin metrics are structured JSON and family-scoped
5. **Minimal dependencies**: No external monitoring system required for basic visibility
6. **SPA-first**: Web interface served at root, all API routes namespaced
## Route Structure
```
/                          → SPA (index.html)
/assets/*                  → SPA static assets (JS, CSS, images)
/ws                        → WebSocket upgrade (data plane)
/livez                     → liveness probe
/targetz                   → Scheduling/orchestration health for a non-serving standby
/healthz                   → Deployment-safe health gate (mirrors readiness)
/readyz                    → native readiness probe
/startupz                  → startup probe
/metrics                   → Prometheus metrics on FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT (unauthenticated)
/api/v1/{family}/metrics   → Structured metrics for one authorized family
/api/v1/all/metrics        → Broker-wide structured metrics (wildcard authority)
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
- Health probes (`/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz`) - Public access for orchestrators and traffic managers; `/targetz` is not a customer traffic-admission signal
- Dedicated Prometheus (`FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics`) - No admin cookie or mutation routes
- Admin API (`/api/v1/*`) - Requires a cookie-backed admin session; broker-global reads and drain require wildcard authority
## Global Endpoints
### Probes
#### Liveness Probe
```
GET /livez
```
**Authentication**: None
**Purpose**: Indicates if the application is alive and should be restarted if unhealthy.
**Response**: 
- `200 OK` - Application is alive
- `503 Service Unavailable` - An initialized domain has permanently failed after exhausting supervised restarts.
```json
{
  "status": "ok"
}
```
**Criteria**: 
- Runtime is responsive
- No initialized domain has permanently failed after supervised restart exhaustion
- Does NOT check downstream dependencies
#### Orchestrator Scheduling Health Gate
```
GET /targetz
```
**Authentication**: None
**Purpose**: Scheduling/orchestration health for single-writer handoff. This endpoint proves the HTTP listener is running and the process is not draining, but it intentionally does not require the Midge writer lease. Use it only through a control path that can observe a standby without routing customer traffic to it.
**Do not use this as**:
- A data-plane readiness signal. WebSocket upgrades and TCP sessions still require `/healthz`/`/readyz` readiness.
- A storage health signal. `storage_writer_lease` may be `not_ready` while `/targetz` returns `200`.
- The health check for a customer-facing ALB target group. ALB routes to healthy targets, so a lease-waiting standby would receive traffic that it must reject.
**Response**:
- `200 OK` - Process may participate in an orchestrated handoff without serving traffic
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
**Handoff use**:
- Keep the waiting replacement outside the customer traffic route while an orchestrator observes `/targetz`.
- Use `/healthz` for customer ALB target-group health and `/readyz` for native readiness.
- Drain the old writer before waiting for `/healthz` on the replacement and admitting customer traffic.
- A standard one-target-group ECS rolling deployment cannot perform this sequence with zero downtime. Use a separate standby/controller, blue-green target groups with a custom lifecycle cutover, or a stop-first deployment that accepts downtime.
- Expected writer-lease contention retries indefinitely with capped exponential backoff; do not use `/healthz`, `/readyz`, or `/startupz` as a liveness restart signal for the waiting task.
- Use 90 seconds as the termination-grace baseline with the default 25-second drain grace, then increase it for peak session cleanup, domain teardown, and the configured backend/provider worst case. In-flight work is joined before storage release, so it can extend shutdown beyond that baseline.
#### Data-Plane Health Gate
```
GET /healthz
```
**Authentication**: None (public endpoint for load balancers)
**Purpose**: Strict data-plane health gate. This intentionally mirrors readiness so traffic does not arrive before the broker has acquired the active single-writer storage lease or after shutdown starts.
**Use this when**:
- Your platform can check only one unauthenticated HTTP endpoint before routing traffic
- You need a single-writer-safe cutover signal for startup, lease handoff, and shutdown

With a standard one-target-group ECS rolling deployment, strict `/healthz`
correctly prevents premature traffic but also prevents the replacement from
becoming healthy while the old task retains the writer lease. Zero-downtime
handoff therefore requires separate standby orchestration or a blue-green/custom
lifecycle cutover; stop-first deployment is the downtime-accepting fallback.

**Do not use this as**:
- A liveness restart signal. `/healthz` is expected to return `503` during startup and shutdown handoff.
- A replacement for liveness. Use `/livez` for restart decisions.
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

Fitz continues monitoring Midge lease health after readiness first succeeds. If
lease renewal becomes unhealthy, Fitz withdraws `/targetz`, `/healthz`, and
`/readyz` and terminates through the fatal shutdown path. It does not remain
ready while waiting to reacquire the writer lease.
#### Readiness Probe
```
GET /readyz
```
**Authentication**: None
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
- Use `/targetz` only for scheduling/orchestration of a standby that is not on the customer traffic route.
- Never use `/targetz` as the health check for the customer-facing ALB target group.
- Use `/readyz` when your orchestrator supports a distinct readiness probe.
- Use `/healthz` for strict customer traffic admission.
- Use `/startupz` to suppress premature liveness checks during long startup windows.

#### Shutdown Modes

- SIGTERM and `POST /api/v1/runtime/drain` are planned shutdowns. After startup completes, they withdraw orchestration health and readiness, wait `FITZ_DRAIN_GRACE_SECONDS`, and then clean up sessions, listeners, domains, and Midge.
- Ctrl-C and fatal actor or active writer-lease-health failures withdraw target and readiness probes and start cleanup immediately without the configured drain delay.
- A standby waiting for the writer lease has no admitted data-plane sessions and skips the drain delay for every shutdown trigger. If Midge open or recovery is already in flight, Fitz must join it before exit; the underlying operation is not cooperatively cancellable.
#### Startup Probe
```
GET /startupz
```
**Authentication**: None
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
### Metrics

Raw Prometheus is served only by the dedicated listener configured with
`FITZ_METRICS_BIND_ADDR` (default `127.0.0.1`) and `FITZ_METRICS_PORT` (default
`9090`). The main authenticated listener returns `404` for `/metrics`.

For the admin UI and operator tooling, use the structured JSON endpoints:

```
GET /api/v1/{family}/metrics
GET /api/v1/all/metrics
```

The family endpoint contains only samples attributable to the requested family.
The `all` endpoint contains broker-global samples and requires wildcard route
family authority.

### Raw Prometheus Format
```
GET /metrics
```
**Listener**: `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT`
**Authentication**: None; keep this listener private to the scrape network.
**Response**: Prometheus text format

Scrapes read an in-process Stream metrics projection initialized during startup
and advanced by successful commits and persisted watermark updates. They do not
scan durable Stream inventory or enqueue admin work on a family actor, so a slow
storage backend cannot turn observability polling into data-plane backpressure.

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
