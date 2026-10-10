# fitz

Fitz is a container-friendly application broker that exposes seven communication
and state primitives through one active broker process and one route model per
storage namespace.

The deployment model is intentionally simple: one active writer per storage
namespace, replaceable broker compute, and explicit workload sharding when more
capacity is needed.

**Product stage:** Fitz is pre-GA and is already used in production deployments.
Deployment status and release stage are distinct; current deployments do not
imply general availability, certification across configurations, or a
service-level commitment.

## What Fitz Provides

Fitz combines streams, queues, live fanout, RPC, KV, leases, and schedules
behind one broker process. Each domain has its own persistence, disconnect, and
restart semantics.

| Domain | Use it for | Durability model |
| --- | --- | --- |
| Notice | live fanout to connected subscribers | ephemeral |
| Stream | durable append and replay of committed history | durable according to write mode |
| KV | current authoritative state | durable on commit according to write mode |
| Queue | work delivery with reservation and redelivery | best-effort persistence; crash loss is possible |
| RPC | live request and response dispatch to registered workers | ephemeral |
| Lease | single-broker ownership coordination | ephemeral |
| Schedule | durable future timing intent | durable definitions and pending fire claims |

Durable paths use Midge-backed persistence. Local storage writes to disk.
Blob/object-backed storage uses a local cache plus provider storage, so durable
state that has reached the configured cloud durability barrier can outlive and
be recovered by a replacement broker process. A storage writer lease ensures
only one process at a time owns a storage namespace. Each domain's contract and
selected write policy determine which commits are durable.

## Deployment Tradeoffs

Fitz keeps each storage namespace single-writer instead of coordinating a broker
cluster. This reduces the coordination and operational surface for workloads that
fit on one broker instance. Container orchestrators can replace a failed process;
after the replacement acquires the storage lease and recovers persisted state, it
can serve traffic again. On an ungraceful failure, lease expiry can delay that
handoff. Clients reconnect and rebuild ephemeral session state.

When a workload outgrows one broker, scale it explicitly: assign independent
workloads or partitions to separate Fitz deployments, each with its own storage
namespace and local cache. Application routing owns that assignment; Fitz does
not automatically shard one logical broker across processes. RouteFamily remains
an isolation boundary within a broker deployment, not a cross-process shard
selector.

Fitz does not form a multi-node consensus cluster or provide a clustered
high-availability mode. Process replacement can interrupt traffic; it does not
provide:

- zero-downtime or transparent failover during process replacement
- session recovery after disconnect
- exactly-once delivery
- durable live subscription recovery
- durable RPC pending work
- crash-safe lease ownership

Sessions are ephemeral. Disconnect creates a new session, and clients must
rebuild subscriptions, workers, leases, transactions, and Stream resume
positions explicitly.

## Quick Start

Run an anonymous local broker:

```sh
docker run --rm \
  -p 4090:4090 \
  -p 4091:4091 \
  -p 9090:9090 \
  -e FITZ_AUTH_REQUIRED=false \
  -e FITZ_STORAGE_MODE=local \
  -e FITZ_STORAGE_PATH=/data \
  ghcr.io/cntryl/fitz:latest
```

`latest` and `main` are rebuilt automatically from every push to `main`, and
`develop` from every push to `develop`. For a
reproducible deployment, use an immutable SemVer image such as
`ghcr.io/cntryl/fitz:0.1.1`; publish one by dispatching the `Publish` workflow
from `main` after the release checks pass.

Check readiness:

```sh
curl http://localhost:4090/healthz
```

For repository-local development:

```sh
docker compose up --build
```

The compose file starts an authenticated broker on `4090`/`4091` and an anonymous broker on `4190`/`4191`. These defaults are for local development: ports are loopback-bound, the admin surface is local, and the authenticated broker uses the shared HS256 dev secret `dev-test-secret`.

Provider-specific development profiles use the Sqrzl storage emulator:

```sh
docker compose -f compose.s3.yml up --build       # S3-compatible emulator
docker compose -f compose.azure.yml up --build    # Azure Blob emulator
docker compose -f compose.gcs.yml up --build      # GCS emulator
```

Each profile starts the same authenticated and anonymous brokers, provisions
separate storage namespaces for them, and publishes the emulator on loopback
port `9000` for local diagnostics. The S3 profile reaches readiness with the
current emulator image. Its Azure and GCS front doors currently return HTTP 500
for missing objects, so those two profiles cannot complete Midge's first-start
recovery until that upstream emulator behavior is corrected.

## Runtime Surfaces

- HTTP root and admin UI: `http://localhost:4090/`
- WebSocket data plane: `ws://localhost:4090/ws`
- TCP data plane: `localhost:4091`
- Probes: `/livez`, `/targetz`, `/startupz`, `/healthz`, `/readyz`
- Prometheus metrics: `http://localhost:9090/metrics` on the dedicated unauthenticated listener
- Authenticated structured metrics: `/api/v1/{family}/metrics` (wildcard admins use `/api/v1/all/metrics`)

## Deployment Configuration

Use these references to configure and operate a Fitz process:

- Storage: [docs/operations/cloud-setup.md](docs/operations/cloud-setup.md)
- Auth and browser perimeter: [docs/operations/auth-browser-deployment.md](docs/operations/auth-browser-deployment.md)
- Probes and metrics: [docs/operations/observability.md](docs/operations/observability.md)
- Operations runbook: [docs/operations/operations-runbook.md](docs/operations/operations-runbook.md)
- Environment variables: [docs/user-guides/vars.md](docs/user-guides/vars.md)

Important defaults:

- `FITZ_AUTH_REQUIRED` defaults to `true`.
- `FITZ_ROUTE_FAMILIES` defaults to `1`; configure a contiguous allowlist such as `1,2,3` before serving multiple isolated families.
- `FITZ_STORAGE_MODE` accepts `memory`, `local`, or `cloud`.
- Queue persistence is always best effort; accepted enqueues and ACKs can be lost before background persistence completes.
- `FITZ_STORAGE_CLOUD_DURABILITY` accepts `background` or `strict` for broker-selected durable cloud writes.

## Documentation

Use [docs/README.md](docs/README.md) as the documentation index.

Start here:

- [docs/user-guides/overview.md](docs/user-guides/overview.md)
- [docs/user-guides/quick-start.md](docs/user-guides/quick-start.md)
- [docs/user-guides/api-guide.md](docs/user-guides/api-guide.md)
- [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md)
- [docs/clients/client-spec.md](docs/clients/client-spec.md)
