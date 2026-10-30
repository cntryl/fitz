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
- `POST /api/v1/runtime/drain` - Begin the planned runtime drain and continue automatically through session, listener, domain, and storage shutdown after the configured grace. Every live session receives a `GOAWAY` control message (code 0054 `DRAINING`, the remaining grace in milliseconds, and the configured close reason) the moment the drain begins.
- `GET /api/v1/stats` - Global broker and domain statistics.
- `GET /api/v1/{family}/stats` - Family-scoped broker and domain statistics.
- `GET /api/v1/topology` - Messaging topology.
- `GET /api/v1/{family}/topology` - Family-scoped messaging topology.
- `GET /api/v1/troubleshooting` - Global troubleshooting guidance.
- `GET /api/v1/{family}/troubleshooting` - Family-scoped troubleshooting guidance.
- `GET /api/v1/search` - Cross-domain admin search.
- `GET /api/v1/sessions` - Active sessions.
- `GET /api/v1/{family}/sessions` - Active sessions for one authorized family.

Family-scoped troubleshooting (and the `diagnostics` block of family-scoped
stats and topology) analyzes only state that carries the requested route
family. Broker-wide counters, latency histograms, and router saturation are not
attributable to a family and are excluded, so a family with no hotspots reports
`healthy` with reduced confidence rather than the full confidence of the global
check.

### Domain Routes
Domain routes are always family-scoped: `/api/v1/{family}/{scheme}/...`.
`{family}` is a provisioned route-family number, or the literal `all` for a
wildcard admin session reading across families. `{scheme}` is one of `kv`,
`queue`, `notice`, `stream`, `rpc`, `lease`, or `schedule`. A bare
`/api/v1/{scheme}/...` path is not a route: the family segment is required and
the handler answers 404.

- `GET /api/v1/{family}/{scheme}/stats` - Domain statistics.
- `GET /api/v1/{family}/{scheme}/realms` - Realm collection.
- `GET /api/v1/{family}/{scheme}/realms/{realm}` - Realm detail.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas` - Area collection.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas/{area}` - Area detail.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas/{area}/resources` - Resource collection.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas/{area}/resources/{resource}` - Resource detail.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas/{area}/resources/{resource}/events` - Resource timeline events.
- `GET /api/v1/{family}/stream/realms/{realm}/watermarks` - Stream realm watermarks.
- `GET /api/v1/{family}/stream/realms/{realm}/areas/{area}/watermarks` - Stream area watermarks.
- `GET /api/v1/{family}/stream/search` - Stream search.
- `GET /api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/transactions` - Live KV transactions.
- `GET /api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/rows` - KV rows.
- `GET /api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/value` - One KV value.
- `GET /api/v1/{family}/kv/realms/{realm}/areas/{area}/resources/{resource}/prefix` - KV prefix scan.
- `GET /api/v1/{family}/stream/realms/{realm}/areas/{area}/resources/{resource}/records` - Stream records.
- `GET /api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}/executions` - Schedule executions.
- `GET /api/v1/{family}/notice/realms/{realm}/areas/{area}/resources/{resource}/subscriptions` - Live Notice subscriptions.
- `GET /api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations` - RPC operations.
- `GET /api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}` - RPC operation detail.
- `GET /api/v1/{family}/rpc/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}/workers` - RPC operation workers.
- `GET /api/v1/{family}/rpc/pending` - Live pending RPC requests.
- `GET /api/v1/{family}/schedule/missed` - Missed schedule occurrences.
- `GET /api/v1/{family}/{scheme}/realms/{realm}/areas/{area}/resources/{resource}/compare` - Resource comparison view.
- `POST /api/v1/{family}/schedule/realms/{realm}/areas/{area}/resources/{resource}/operations/{operation}/run` - Run a schedule occurrence now.
- `GET /api/v1/{family}/rpc/calls` - RPC call observations.
- `GET /api/v1/{family}/lease/search` - Lease search.
- `GET /api/v1/{family}/notice/deliveries` - Notice delivery observations.
- `GET /api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/inflight` - Live queue inflight entries.
- `GET /api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters` - Queue dead letters.
- `POST /api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}/replay` - Replay a queue dead letter.
- `DELETE /api/v1/{family}/queue/realms/{realm}/areas/{area}/resources/{resource}/dead-letters/{message_id}` - Purge a queue dead letter.

The current router does not implement admin commands for forcing KV rollback,
Notice subscription cancellation, RPC request cancellation, Lease release, or
Session close.

### Fault-Injection Routes
These routes rehearse fail-closed behavior in tests. They are compiled into the
broker but answer 404 unless the process starts with
`FITZ_DESTROYER_FAILPOINTS=enabled`. When enabled they require a wildcard
admin session and a same-origin request, exactly like the other mutating admin
routes. A successful call panics a live domain actor, or every domain actor at
once, which fails those actors closed and marks readiness unhealthy. Never
enable them on a broker that serves
traffic.

- `POST /destroyer/failpoints/{scheme}-actor-panic` - Panic every actor of one domain, where `{scheme}` is `kv`, `queue`, `notice`, `stream`, `rpc`, `lease`, or `schedule`.
- `POST /destroyer/failpoints/all-domain-actor-panic` - Panic every actor of every domain.
- `POST /destroyer/failpoints/stream-family-{family}-actor-panic` - Panic one Stream family actor.
- `POST /destroyer/failpoints/rpc-family-{family}-actor-panic` - Panic one RPC family actor.

## Safety

Admin read routes require an authenticated admin principal unless admin auth is
explicitly open. Mutating routes also require same-origin validation. Probe
routes are intentionally unauthenticated for deployment health checks.
