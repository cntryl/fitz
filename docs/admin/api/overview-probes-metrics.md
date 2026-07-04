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
/livez                     → liveness probe
/targetz                   → Orchestrator target health gate for handoff
/healthz                   → Deployment-safe health gate (mirrors readiness)
/readyz                    → native readiness probe
/startupz                  → startup probe
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
- Health probes (`/livez`, `/targetz`, `/healthz`, `/readyz`, `/startupz`) - Public access for orchestrators and traffic managers
- Metrics (`/metrics`) - Requires JWT Bearer token
- Admin API (`/api/v1/*`) - Requires JWT Bearer token with admin scope
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
**Authentication**: None
**Purpose**: Orchestrator target health gate for single-writer handoff. This endpoint proves the HTTP listener is running and the process is not draining, but it intentionally does not require the Midge writer lease. Use this when a replacement process must become target-eligible before the old writer releases storage ownership.
**Do not use this as**:
- A data-plane readiness signal. WebSocket upgrades and TCP sessions still require `/healthz`/`/readyz` readiness.
- A storage health signal. `storage_writer_lease` may be `not_ready` while `/targetz` returns `200`.
**Response**:
- `200 OK` - HTTP target may participate in handoff
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
- Configure target eligibility to `/targetz`.
- Keep `/healthz` and `/readyz` for strict data-plane readiness and operator checks.
- Allow overlap so the replacement target can become eligible before the old process releases the writer lease.
- Set process termination grace longer than `FITZ_DRAIN_GRACE_SECONDS`.
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
- Use `/targetz` when a replacement process must become target-eligible before it can acquire the writer lease.
- Use `/targetz` for active/passive rolling handoff only when the app must reject WS/TCP until strict data-plane readiness.
- Use `/readyz` when your orchestrator supports a distinct readiness probe.
- Use `/healthz` for strict data-plane traffic admission when your platform can tolerate stopping the old writer before the new target is healthy.
- Use `/startupz` to suppress premature liveness checks during long startup windows.
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
