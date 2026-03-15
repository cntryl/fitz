# Cloud Setup

This guide defines a baseline cloud setup for Fitz.

## Goals

- Keep async edge and sync core behavior predictable.
- Isolate realms with explicit quotas.
- Expose health and metrics endpoints for automation.

## Baseline Topology

1. Fitz node group behind a load balancer.
2. Persistent volume per stateful deployment where durability is required.
3. Prometheus scraping for metrics and log shipping to central storage.

## Required Endpoints

- `/healthz` for liveness
- `/readyz` for readiness
- `/startupz` for startup gating
- `/metrics` for Prometheus
- `/ws` for WebSocket data plane

Details are in [admin/admin-api.md](../admin/admin-api.md) and [operations/observability.md](observability.md).

## Production Baseline Checklist

1. TLS enabled at ingress.
2. JWT signing keys configured and rotated.
3. Resource limits configured per pod and per realm.
4. Persistent storage configured for durability-sensitive domains.
5. Alerting configured before first customer traffic.
