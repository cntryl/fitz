# Fitz Operator Guide

This guide is the operational starting point for deploying, monitoring, and
replacing a Fitz broker. It summarizes the decisions operators need to make
and links to the detailed procedures for each area.

## Operating Model

- Run one active Fitz writer for each storage namespace. Do not start a second
  process against the same namespace as an active peer. A replacement must
  acquire the storage writer lease and complete recovery before it is ready.
- Give each process its own local cache path. To scale across independent
  workloads, use separate Fitz deployments, storage namespaces or prefixes,
  and cache paths. `RouteFamily` isolates traffic inside one broker; it does
  not coordinate ownership across processes.
- Sessions are live process state. After a disconnect or replacement, clients
  reconnect and rebuild subscriptions and other session-owned state.
- Fitz does not make all domains durable in the same way. Notice, RPC, and
  Lease are ephemeral; Stream and KV persist committed state according to
  their write mode; Queue persistence is best effort; Schedule persists
  definitions and pending fire claims. See [durability](../user-guides/durability.md)
  before deciding what work can be safely retried or regenerated.

## Choose and Protect Storage

| `FITZ_STORAGE_MODE` | Use | Operator expectation |
| --- | --- | --- |
| `memory` | Local development or disposable workloads | State is lost when the process exits. |
| `local` | A single host with managed persistent disk | Recovery depends on the disk and its backup policy. |
| `cloud` | Replacement across processes or hosts | Fitz uses a local cache and provider storage; readiness follows lease acquisition and recovery. |

For cloud mode, configure one stable namespace/prefix per environment and a
unique `FITZ_STORAGE_CACHE_PATH` per process. The storage writer lease defaults
to a 30-second TTL. Graceful shutdown releases it; an ungraceful failure can
make a replacement wait for lease expiry. A longer TTL extends that wait.

`FITZ_STORAGE_CLOUD_DURABILITY=background` is the default for broker-selected
durable cloud writes; `strict` waits for provider acknowledgement for those
writes. KV and Stream commits still honor the client's explicit buffered or
sync choice. Queue is always best effort, regardless of this setting: accepted
enqueues and ACKs can be lost before background persistence finishes, and lost
ACKs can cause redelivery. The Queue flush interval is a target, not a loss
guarantee.

For provider settings, lease behavior, and handoff constraints, see [cloud
storage setup](cloud-setup.md) and the [operations runbook](operations-runbook.md).

## Gate Traffic on the Right Probe

Use the probes for different decisions:

| Probe | Use it for | Do not use it for |
| --- | --- | --- |
| `/livez` | Process liveness and restart decisions | Customer traffic admission |
| `/startupz` | Confirming startup has completed | Liveness or storage durability |
| `/healthz` or `/readyz` | Customer traffic admission | Promoting a lease-waiting standby |
| `/targetz` | A separate orchestrator deciding whether a standby can be promoted | A customer-facing load balancer target check |

After startup or replacement, wait for `/healthz` or `/readyz` to return 200
before routing customer traffic. `/targetz` can succeed for a standby before
the data plane is ready. Fitz supports one active writer per storage namespace;
a normal rolling deployment through one customer target group cannot perform a
zero-downtime writer handoff. Follow the [replacement procedure](operations-runbook.md#planned-maintenance).

Example checks against a local HTTP listener:

```sh
curl --fail http://127.0.0.1:4090/startupz
curl --fail http://127.0.0.1:4090/healthz
curl --fail http://127.0.0.1:4090/readyz
```

## Watch Capacity and Storage Signals

- Scrape `/metrics` from a trusted network only. The dedicated Prometheus
  listener is unauthenticated; set `FITZ_METRICS_BIND_ADDR` and
  `FITZ_METRICS_PORT` to keep it within the monitoring network.
- Watch `fitz_admin_observation_overload_rejections_total` for REST, MCP, and
  Prometheus observations rejected because shared admin worker capacity is
  full. MCP also exposes its own admission counters. A rising count means
  reduce the rate or concurrency of admin polling and retry with bounded
  backoff; it does not by itself mean customer data-plane work was rejected.
- Watch `fitz_queue_fast_flush_failures_total` and
  `fitz_queue_admission_budget_exhausted_total` for Queue persistence pressure.
  See [observability](observability.md) and the [operations runbook](operations-runbook.md#steady-state-signals)
  for other signals and their meanings.
- Admin projections are observations, not authoritative domain state. Use
  domain responses and supported persistence/recovery procedures to establish
  state; do not treat a dashboard snapshot as a durability or delivery proof.

## Secure the Operator Surface

For deployments outside local development, use protected admin auth, verified
runtime JWTs, TLS at a trusted edge, exact public origins, and private access
to the broker and metrics listeners. Keep development secrets, open admin mode,
and local-loopback edge settings out of externally reachable deployments.
Follow [auth and browser deployment](auth-browser-deployment.md) for the
required settings and startup validation rules.

## Plan Upgrades as a Coordinated Change

Fitz 0.3.x speaks wire format 2 and cannot serve wire format 1 clients. Upgrade
the broker and all connected SDKs together; mixed generations cannot
communicate. Storage-format or Midge changes have their own compatibility and
migration requirements, so a wire-compatible release is not automatically a
storage-compatible release. Check the [migration guide](migration-guide.md),
the release's storage notes, and the [support matrix](../development/support-matrix.md)
before rollout. Keep a tested rollback path and validate probes and one
authenticated client round trip after replacement.

## Operational References

- [Operations runbook](operations-runbook.md): startup, steady-state signals,
  single-writer replacement, shutdown, and rollback.
- [Cloud storage setup](cloud-setup.md): provider settings, durability modes,
  cache paths, and writer lease.
- [Observability](observability.md): probes, metrics, and contracts.
- [Auth and browser deployment](auth-browser-deployment.md): JWT and admin
  perimeter configuration.
- [Environment variables](../user-guides/vars.md): complete runtime setting
  reference.
- [Resource limits](resource-limits.md): configured bounds and capacity behavior.
- [Durability](../user-guides/durability.md): guarantees by domain.
