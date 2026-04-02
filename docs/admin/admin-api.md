# Fitz Admin API Specification
**Purpose**: Runtime visibility, metrics, and operational control for Fitz broker.
**Protocol**: HTTP REST API (coexists with WebSocket data plane on same port)  
**Port**: Same as data plane (default: 8080)  
**Route Structure**:  
  - `/` - Single Page Application (SPA) static files
  - `/api/v1/admin/*` - Admin REST API endpoints
  - `/metrics` - Prometheus metrics endpoint
  - `/healthz`, `/readyz`, `/startupz` - Kubernetes health probes
  - `/ws` - WebSocket upgrade for data plane
**Data Plane**: WebSocket upgrade on `/ws`, TCP on same port (protocol detection)  
**Authentication**:  
  - `/` - No authentication (SPA public access)
  - `/healthz`, `/readyz`, `/startupz` - No authentication (for load balancer health checks)  
  - `/metrics` - **Requires authentication** (JWT or API key)  
  - `/api/v1/admin/*` - **Requires authentication** (JWT or API key with admin permissions)
## Design Principles
1. **Read-heavy**: Most operations are queries for visibility
2. **Safe by default**: Dangerous operations (force rollback, cancel) require explicit confirmation
3. **Realm-scoped**: All queries can be filtered by realm for multi-tenancy
4. **Prometheus-compatible**: Metrics endpoint follows Prometheus format
5. **Minimal dependencies**: No external monitoring system required for basic visibility
6. **SPA-first**: Web interface served at root, all API routes namespaced
## Route Structure
```
/                          → SPA (index.html)
/assets/*                  → SPA static assets (JS, CSS, images)
/ws                        → WebSocket upgrade (data plane)
/healthz                   → Kubernetes liveness probe
/readyz                    → Kubernetes readiness probe
/startupz                  → Kubernetes startup probe
/metrics                   → Prometheus metrics (auth required)
/api/v1/admin/stats        → Global broker statistics (auth required)
/api/v1/admin/kv/stats     → KV domain statistics (auth required)
/api/v1/admin/stream/stats → Stream domain statistics (auth required)
/api/v1/admin/notice/stats → Notice domain statistics (auth required)
/api/v1/admin/queue/stats  → Queue domain statistics (auth required)
/api/v1/admin/rpc/stats    → RPC domain statistics (auth required)
/api/v1/admin/lease/stats  → Lease domain statistics (auth required)
```
**Authentication Rules**:
- SPA (`/`, `/assets/*`) - Public access
- Health probes (`/healthz`, `/readyz`, `/startupz`) - Public access (for K8s/load balancers)
- Metrics (`/metrics`) - Requires JWT Bearer token
- Admin API (`/api/v1/admin/*`) - Requires JWT Bearer token with admin scope
## Global Endpoints
### Kubernetes Probes
#### Liveness Probe
```
GET /healthz
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
#### Readiness Probe
```
GET /readyz
```
**Authentication**: None (public endpoint for kubelet)
**Purpose**: Indicates if the application is ready to accept traffic.
**Response**: 
- `200 OK` - Ready to accept traffic
- `503 Service Unavailable` - Not ready, remove from load balancer
```json
{
  "status": "ready",
  "checks": {
    "storage": "ok",
    "domains_initialized": "ok"
  }
}
```
**Criteria**: 
- Storage engine initialized
- All domain actors started
- TCP/WebSocket listeners bound
- Ready to process requests
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
**Purpose**: General health check for non-Kubernetes environments.
**Response**: 200 OK if healthy, 503 Service Unavailable if degraded
```json
{
  "status": "healthy",
  "uptime_seconds": 86400,
  "version": "0.1.0"
}
```
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
GET /admin/stats
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
      "leases_active": 67,
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
#### List Active Transactions
```
GET /admin/kv/transactions?realm={realm}
```
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
#### KV Statistics
```
GET /admin/kv/stats?realm={realm}
```
**Response**:
```json
{
  "transactions_active": 23,
  "transactions_committed_total": 1847392,
  "transactions_rolled_back_total": 1247,
  "keys_total": 12847,
  "keys_by_realm": {
    "prod": 8456,
    "staging": 3219,
    "dev": 1172
  },
  "operations_per_second": {
    "get": 85,
    "put": 23,
    "scan": 12
  }
}
```
#### Force Rollback Transaction (Admin)
```
POST /admin/kv/transactions/{tx_id}/rollback
```
**Headers**: `X-Confirm: true` (required for safety)
**Response**: 200 OK or 404 Not Found
### Stream Domain
#### List Active Streams
```
GET /admin/stream/streams?realm={realm}
```
**Response**:
```json
{
  "streams": [
    {
      "realm": "prod",
      "area": "events",
      "resource": "orders",
      "offset": 384921,
      "watermark": 384921,
      "size_bytes": 52847392,
      "sessions_active": 3
    }
  ]
}
```
#### Stream Statistics
```
GET /admin/stream/stats?realm={realm}
```
**Response**:
```json
{
  "streams_active": 45,
  "events_total": 384921,
  "events_by_realm": {
    "prod": 284921,
    "staging": 80000,
    "dev": 20000
  },
  "operations_per_second": {
    "append": 65,
    "read": 20
  }
}
```
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
#### List Queues with Depths
```
GET /admin/queue/queues?realm={realm}
```
**Response**:
```json
{
  "queues": [
    {
      "realm": "prod",
      "area": "jobs",
      "resource": "emails",
      "messages_ready": 1847,
      "messages_leased": 67,
      "messages_total": 1914,
      "oldest_message_age_seconds": 120
    }
  ]
}
```
#### List Active Leases
```
GET /admin/queue/leases?realm={realm}
```
**Response**:
```json
{
  "leases": [
    {
      "message_id": 123456,
      "realm": "prod",
      "area": "jobs",
      "resource": "emails",
      "lease_token": "tok_xyz789",
      "session_id": "sess_abc123",
      "expires_at": "2026-01-31T10:35:00Z",
      "attempts": 2
    }
  ]
}
```
#### Queue Statistics
```
GET /admin/queue/stats?realm={realm}
```
**Response**:
```json
{
  "messages_pending": 1847,
  "messages_leased": 67,
  "leases_expired_total": 342,
  "operations_per_second": {
    "enqueue": 45,
    "reserve": 42,
    "complete": 40
  }
}
```
#### Force Expire Lease (Admin)
```
POST /admin/queue/leases/{lease_id}/expire
```
**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
### RPC Domain
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
      "route": "rpc://prod/compute/heavy-task",
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
**Response**:
```json
{
  "requests": [
    {
      "correlation_id": "0123456789abcdef",
      "route": "rpc://prod/compute/heavy-task",
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
#### List Active Leases
```
GET /admin/lease/leases?realm={realm}
```
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
**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
### Schedule Domain
#### List Schedules
```
GET /admin/schedule/schedules?realm={realm}
```
**Response**:
```json
{
  "schedules": [
    {
      "realm": "prod",
      "area": "jobs",
      "resource": "cleanup",
      "cron": "0 2 * * *",
      "next_run": "2026-02-01T02:00:00Z",
      "last_run": "2026-01-31T02:00:00Z",
      "executions_total": 365,
      "enabled": true
    }
  ]
}
```
#### Schedule Statistics
```
GET /admin/schedule/stats?realm={realm}
```
**Response**:
```json
{
  "schedules_active": 56,
  "schedules_enabled": 54,
  "schedules_disabled": 2,
  "executions_per_minute": 23,
  "executions_total": 1847392
}
```
#### Trigger Schedule Manually (Admin)
```
POST /admin/schedule/schedules/{schedule_id}/trigger
```
**Headers**: `X-Confirm: true`
**Response**: 200 OK or 404 Not Found
## Sessions & Connections
### List Active Sessions
```
GET /admin/sessions?realm={realm}
```
**Response**:
```json
{
  "sessions": [
    {
      "session_id": "sess_abc123",
      "realm": "prod",
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
- `GET /healthz` - Liveness probe
- `GET /readyz` - Readiness probe
- `GET /startupz` - Startup probe
- `GET /health` - Legacy health check
**Metrics (Auth Required)**:
- `GET /metrics` - Prometheus metrics
**Global Stats (Admin Auth)**:
- `GET /api/v1/admin/stats` - Global broker and domain statistics
**Domain Stats (Admin Auth)**:
- `GET /api/v1/admin/kv/stats` - KV domain statistics
- `GET /api/v1/admin/stream/stats` - Stream domain statistics
- `GET /api/v1/admin/notice/stats` - Notice domain statistics
- `GET /api/v1/admin/queue/stats` - Queue domain statistics
- `GET /api/v1/admin/rpc/stats` - RPC domain statistics
- `GET /api/v1/admin/lease/stats` - Lease domain statistics
- `GET /api/v1/admin/schedule/stats` - Schedule domain statistics
**List Endpoints (Admin Auth)** - Infrastructure added, domain implementation pending:
- `GET /api/v1/admin/kv/transactions?realm={realm}` - List active KV transactions
- `GET /api/v1/admin/stream/streams?realm={realm}` - List active streams
- `GET /api/v1/admin/notice/subscriptions?realm={realm}&route_pattern={pattern}` - List subscriptions
- `GET /api/v1/admin/notice/routes?realm={realm}` - List routes with subscriber counts
- `GET /api/v1/admin/queue/queues?realm={realm}` - List queues with depths
- `GET /api/v1/admin/queue/leases?realm={realm}` - List active queue leases
- `GET /api/v1/admin/rpc/workers?realm={realm}` - List registered RPC workers
- `GET /api/v1/admin/rpc/pending?realm={realm}` - List pending RPC requests
- `GET /api/v1/admin/lease/leases?realm={realm}` - List active in-memory leases
  - Lease snapshots are live only. They disappear on disconnect cleanup and are not restored after broker restart.
- `GET /api/v1/admin/schedule/schedules?realm={realm}` - List schedules
- `GET /api/v1/admin/sessions?realm={realm}` - List active sessions
### 🚧 To Be Implemented
**Admin Commands (Admin Auth + X-Confirm Header)**:
- `POST /api/v1/admin/kv/transactions/{tx_id}/rollback` - Force rollback transaction
- `POST /api/v1/admin/notice/subscriptions/{subscription_id}/cancel` - Cancel subscription
- `POST /api/v1/admin/queue/leases/{lease_id}/expire` - Force expire lease
- `POST /api/v1/admin/rpc/requests/{correlation_id}/cancel` - Cancel RPC request
- `POST /api/v1/admin/lease/leases/{lease_id}/release` - Force release lease
- `POST /api/v1/admin/schedule/schedules/{schedule_id}/trigger` - Trigger schedule manually
- `POST /api/v1/admin/sessions/{session_id}/close` - Close session
**Pagination Support**:
- Add `?limit=` and `?offset=` query parameters to list endpoints
**Domain Integration**:
- Each domain needs to implement methods to provide list data
- KV: Track active transactions, expose via admin query
- Stream: Track active streams, expose via admin query
- Notice: Track subscriptions and routes, expose via admin query
- Queue: Track queue depths and leases, expose via admin query
- RPC: Track workers and pending requests, expose via admin query
- Lease: Track active leases, expose via admin query
- Schedule: Track schedules, expose via admin query
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
   - `/healthz`, `/readyz`, `/startupz`, `/health` - No auth (for kubelet/load balancers)
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
    if !runtime.storage_initialized() || !runtime.domains_ready() {
        let response = json!({
            "status": "not_ready",
            "checks": {
                "storage": if runtime.storage_initialized() { "ok" } else { "not_ready" },
                "domains_initialized": if runtime.domains_ready() { "ok" } else { "not_ready" }
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
            "domains_initialized": "ok"
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
    if !runtime.startup_complete() {
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
   - `/healthz` - Kubernetes liveness probe (no auth)
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
  replicas: 3
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
        
        # Startup probe - gives app time to initialize
        startupProbe:
          httpGet:
            path: /startupz
            port: 8080
          initialDelaySeconds: 0
          periodSeconds: 2
          timeoutSeconds: 1
          failureThreshold: 30  # 60 seconds max startup time
        
        # Liveness probe - restart if unhealthy
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
          timeoutSeconds: 2
          failureThreshold: 3
        
        # Readiness probe - remove from service if not ready
        readinessProbe:
          httpGet:
            path: /readyz
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
        (&Method::GET, "/healthz") => handle_liveness().await,
        (&Method::GET, "/readyz") => handle_readiness(runtime).await,
        (&Method::GET, "/startupz") => handle_startup(runtime).await,
        (&Method::GET, "/health") => handle_health().await,
        
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
   - `/healthz`, `/readyz`, `/startupz`, `/health` → Health probes (no auth)
   - `/metrics` → Metrics (check JWT/API key)
   - `/admin/*` → Admin API (check JWT/API key + admin permission)
   - `/ws` with Upgrade header → WebSocket handler
2. **Raw TCP** (binary data, no HTTP headers) → TCP frame handler
## Minimal Initial Implementation
Start with these essential endpoints:
### Phase 1: Health & Observability
1. `GET /healthz` - Kubernetes liveness probe
2. `GET /readyz` - Kubernetes readiness probe  
3. `GET /startupz` - Kubernetes startup probe
4. `GET /health` - Legacy health check
5. `GET /metrics` - Prometheus metrics (with auth)
6. `GET /admin/stats` - Human-readable overview
### Phase 2: Domain Visibility
7. `GET /admin/kv/stats` - KV visibility
8. `GET /admin/notice/subscriptions` - Notice visibility
9. `GET /admin/queue/queues` - Queue depths
10. `GET /admin/sessions` - Active connections
### Phase 3: Admin Commands
11. Domain-specific commands (rollback, cancel, expire) with `X-Confirm: true`
