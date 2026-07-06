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
    // Return 503 only after an initialized domain exhausts supervised restarts.
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
