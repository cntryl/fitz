## Sessions & Status

Admin API routes are mounted under `/api/v1`; `/admin/...` paths are browser
routes, not API routes. Raw Prometheus is served separately on the configured
`FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT` listener.

### List Active Sessions
```
GET /api/v1/sessions
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
      "session_id": "12345",
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

There is no session detail or session-close endpoint in the current router.
Operator tooling should not call `/admin/sessions/{id}/close` or
`/api/v1/admin/sessions/{id}/close`; those paths are not implemented.

## Implemented Surface

### Health Probes
- `GET /livez` - Liveness probe.
- `GET /targetz` - Scheduling/orchestration health for a non-serving standby; not a customer load-balancer admission gate.
- `GET /healthz` - Deployment-safe health gate.
- `GET /readyz` - Readiness probe.
- `GET /startupz` - Startup probe.
- `GET /health` - Legacy health alias.

### Session And Feature Routes
- `POST /api/v1/session` - Admin login.
- `GET /api/v1/session` - Current admin session.
- `DELETE /api/v1/session` - Admin logout.
- `GET /api/v1/features` - Admin feature metadata.

### Runtime And Observability Routes
- `GET /api/v1/{family}/metrics` - Structured metrics for one authorized family.
- `GET /api/v1/all/metrics` - Broker-wide structured metrics; wildcard authority required.
- `POST /api/v1/runtime/drain` - Begin the planned runtime drain and continue automatically through session, listener, domain, and storage shutdown after the configured grace.
- `GET /api/v1/stats` - Global broker and domain statistics.
- `GET /api/v1/{family}/stats` - Family-scoped broker and domain statistics.
- `GET /api/v1/topology` - Messaging topology.
- `GET /api/v1/{family}/topology` - Family-scoped messaging topology.
- `GET /api/v1/troubleshooting` - Global troubleshooting guidance.
- `GET /api/v1/{family}/troubleshooting` - Family-scoped troubleshooting guidance.
- `GET /api/v1/search` - Cross-domain admin search.
- `GET /api/v1/sessions` - Active sessions.
- `GET /api/v1/{family}/sessions` - Active sessions for one authorized family.

### Domain Routes
Domain routes use `/api/v1/{domain}/...` for aggregate reads. A concrete route
family can be selected with `/api/v1/{route_family}/{domain}/...`; wildcard
admin sessions may use `/api/v1/all/{domain}/...` for aggregate reads.

- `GET /api/v1/{domain}/stats` - Domain statistics.
- `GET /api/v1/{domain}/realms` - Realm collection.
- `GET /api/v1/{domain}/realms/{realm}/areas` - Area collection.
- `GET /api/v1/{domain}/realms/{realm}/areas/{area}/resources` - Resource collection.
- `GET /api/v1/{domain}/realms/{realm}/areas/{area}/resources/{resource}` - Resource detail.
- `GET /api/v1/{domain}/realms/{realm}/areas/{area}/resources/{resource}/events` - Resource timeline events.
- `GET /api/v1/kv/realms/{realm}/areas/{area}/resources/{resource}/transactions` - Live KV transactions.
- `GET /api/v1/stream/realms/{realm}/areas/{area}/resources/{resource}/records` - Stream records.
- `GET /api/v1/schedule/realms/{realm}/areas/{area}/resources/{resource}/executions` - Schedule executions.
- `GET /api/v1/notice/realms/{realm}/areas/{area}/resources/{resource}/subscriptions` - Live Notice subscriptions.
- `GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations` - RPC operations.
- `GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}` - RPC operation detail.
- `GET /api/v1/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}/workers` - RPC operation workers.
- `GET /api/v1/rpc/pending` - Live pending RPC requests.
- `GET /api/v1/{route_family}/lease/search` - Lease search.
- `GET /api/v1/{route_family}/notice/deliveries` - Notice delivery observations.
- `GET /api/v1/{route_family}/rpc/calls` - RPC call observations.
- `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/inflight` - Live queue inflight entries.
- `GET /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters` - Queue dead letters.
- `POST /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}/replay?family={family}` - Replay a queue dead letter.
- `DELETE /api/v1/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}?family={family}` - Purge a queue dead letter.

The current router does not implement admin commands for forcing KV rollback,
Notice subscription cancellation, RPC request cancellation, Lease release,
manual Schedule trigger, or Session close.

## Safety

Admin read routes require an authenticated admin principal unless admin auth is
explicitly open. Mutating routes also require same-origin validation. Probe
routes are intentionally unauthenticated for deployment health checks.
