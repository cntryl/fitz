# Observability

Fitz exposes process metrics, authenticated admin metrics, and health probes.
Metric names containing `tlv` are retained for compatibility; they describe
Wire2 message codec activity.

## Metrics

- `/metrics` is served by the dedicated listener configured with
  `FITZ_METRICS_BIND_ADDR` and `FITZ_METRICS_PORT`. It is unauthenticated and
  should be kept on a trusted network.
- `/api/v1/{family}/metrics` returns authenticated, family-scoped structured
  metrics. `/api/v1/all/metrics` requires wildcard authority.
- Metrics include counters, gauges, and histograms for broker, domain,
  transport, and storage activity. Metric names and labels are part of the
  admin API contract; see [admin API overview](../admin/api/overview-probes-metrics.md)
  and [admin API reference](../admin/admin-api.md).
- `fitz_admin_observation_overload_rejections_total` counts REST, MCP, and
  Prometheus observations rejected because shared admin worker capacity was
  full. MCP also exports its own admission counters. This signal describes
  admin observation load and does not by itself mean data-plane work was
  rejected.

## Health probes

- `/livez` reports process liveness and is intended for restart decisions.
- `/startupz` reports whether startup has completed.
- `/readyz` and `/healthz` report readiness to serve customer traffic.
- `/targetz` reports orchestration status for a standby and must not be used
  for customer traffic admission.

See the [probe and metrics reference](../admin/api/overview-probes-metrics.md)
for response contracts, shutdown behavior, and single-writer handoff details.
