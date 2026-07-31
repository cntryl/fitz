## Deployment Guidance

Fitz exposes the data plane, admin UI, and probes from the HTTP listener. Raw
Prometheus metrics use the separate `FITZ_METRICS_BIND_ADDR` /
`FITZ_METRICS_PORT` listener. The TCP listener remains separate when enabled.

Recommended unauthenticated probes:

- `/livez`: process liveness and restart decisions.
- `/targetz`: scheduling/orchestration health for a non-serving standby.
- `/startupz`: startup completion.
- `/healthz`: strict data-plane traffic admission.
- `/readyz`: strict native readiness.

Authenticated surfaces:

- `/api/v1/*`
- unsafe admin mutations

The dedicated Prometheus listener exposes only `GET /metrics` and has no admin
session or mutation routes.

## Single-Active Handoff

`/targetz` is only a scheduling or orchestration signal for a controller that can
observe a waiting replacement without routing customer traffic to it. Never use
`/targetz` as the health check for the customer-facing ALB target group: it may
return `200` while `/healthz` and `/readyz` return `503`, and the task still
rejects WebSocket upgrades and TCP sessions.

A standard ECS rolling deployment with one customer target group cannot provide
zero-downtime single-writer handoff. If that target group checks `/healthz`, the
replacement cannot become healthy before the old writer releases the lease. If
it checks `/targetz`, the ALB can route customer traffic to a standby that cannot
serve it.

Use one of these deployment patterns:

1. Keep the standby outside the customer target group and use a separate controller to observe `/targetz`, drain the old writer, wait for `/healthz` on the replacement, and only then register or route traffic to it.
2. Use blue-green target groups or a custom ECS lifecycle controller that keeps the replacement off the production route until the old writer drains, the replacement acquires the lease, and `/healthz` succeeds.
3. As the simple fallback, use a stop-first ECS deployment, accept the resulting downtime, and start or admit the replacement only after the old task exits.

In every pattern, use `/healthz` for customer traffic admission, configure the
external deregistration delay close to `FITZ_DRAIN_GRACE_SECONDS`, use 90 seconds
as the termination-grace baseline with the default 25-second drain grace, then
increase it for peak session cleanup, domain teardown, and storage/provider
latency. Run a smoke check after strict readiness succeeds.

Expected writer-lease contention keeps the replacement alive indefinitely with
capped exponential backoff. SIGTERM and authenticated runtime drain requests
use graceful drain on an active broker: they start the configured grace period
and then continue automatically through session, listener, domain, and Midge
shutdown. Ctrl-C and fatal actor or active writer-lease-health failures skip the
drain delay and begin cleanup immediately. A standby has no active data-plane
sessions, so every shutdown trigger skips the active-broker drain delay.

The 90-second value is a baseline, not a shutdown upper bound. Fitz completes
session cleanup and domain joins before releasing storage, and joins an
in-flight Midge open or recovery attempt so a detached blocking task cannot
acquire the writer lease later. Slow session/domain teardown or backend/provider
latency can extend shutdown beyond that baseline.

After the broker becomes active, Fitz monitors Midge writer-lease health. A
renewal-health failure withdraws orchestration health and strict readiness and
terminates the broker through the fatal shutdown path; Fitz does not continue
serving or silently reacquire ownership in process.

## Route Handling

The admin router should keep probe, metrics, SPA, WebSocket, and API paths separate:

- `/` and `/assets/*`: SPA assets.
- `/ws`: WebSocket upgrade.
- `/livez`, `/targetz`, `/startupz`, `/healthz`, `/readyz`: public probes.
- `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics`: unauthenticated Prometheus scrape endpoint.
- `/api/v1/{family}/metrics`: authenticated structured metrics.
- `/api/v1/*`: authenticated admin API.

Do not use observability or admin read models to define domain behavior. They describe runtime state; the domain contracts remain in [../../development/domain-boundaries-spec.md](../../development/domain-boundaries-spec.md).
