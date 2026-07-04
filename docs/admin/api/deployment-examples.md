## Deployment Guidance

Fitz exposes the data plane, admin UI, probes, and metrics from the HTTP listener. The TCP listener remains separate when enabled.

Recommended unauthenticated probes:

- `/livez`: process liveness and restart decisions.
- `/targetz`: target eligibility during single-active handoff.
- `/startupz`: startup completion.
- `/healthz`: strict data-plane traffic admission.
- `/readyz`: strict native readiness.

Protected surfaces:

- `/metrics`
- `/api/v1/*`
- unsafe admin mutations

## Single-Active Handoff

For one active Fitz process at a time:

1. Use `/targetz` for target eligibility when a replacement must overlap with the old writer.
2. Keep `/healthz` or `/readyz` for strict data-plane readiness.
3. Configure the external deregistration delay close to `FITZ_DRAIN_GRACE_SECONDS`.
4. Configure termination grace longer than `FITZ_DRAIN_GRACE_SECONDS`.
5. Run a smoke check after the replacement owns the storage writer and strict readiness is healthy.

During handoff, `/targetz` may return `200` while `/healthz` and `/readyz` return `503`. That is intentional: the process is target-eligible, but WebSocket upgrades and TCP sessions remain closed until strict readiness succeeds.

## Route Handling

The admin router should keep probe, metrics, SPA, WebSocket, and API paths separate:

- `/` and `/assets/*`: SPA assets.
- `/ws`: WebSocket upgrade.
- `/livez`, `/targetz`, `/startupz`, `/healthz`, `/readyz`: public probes.
- `/metrics`: authenticated metrics.
- `/api/v1/*`: authenticated admin API.

Do not use observability or admin read models to define domain behavior. They describe runtime state; the domain contracts remain in [../../development/domain-boundaries-spec.md](../../development/domain-boundaries-spec.md).
