# fitz

Fitz is a single-node application broker that exposes seven communication and
state primitives through one process and one route model.

The model is simple: one broker, seven application primitives, one deployment model.

## What Fitz Provides

Fitz combines streams, queues, live fanout, RPC, KV, leases, and schedules
behind one broker process. Each domain has its own persistence, disconnect, and
restart semantics.

| Domain | Use it for | Durability model |
| --- | --- | --- |
| Notice | live fanout to connected subscribers | ephemeral |
| Stream | durable append and replay of committed history | durable according to write mode |
| KV | current authoritative state | durable on commit according to write mode |
| Queue | durable work delivery with reservation and redelivery | durable according to queue write policy |
| RPC | live request and response dispatch to registered workers | ephemeral |
| Lease | single-broker ownership coordination | ephemeral |
| Schedule | durable future timing intent | durable definitions and pending fire claims |

Durable paths use Midge-backed persistence. Local storage writes to disk. Blob/object-backed storage uses a local cache plus provider storage and keeps domain guarantees explicit: a domain is durable only when its contract and selected write policy say it is.

## Non-Goals

Fitz is single-node software. It does not provide:

- high availability or consensus
- transparent failover
- session recovery after disconnect
- exactly-once delivery
- durable live subscription recovery
- durable RPC pending work
- crash-safe lease ownership

Sessions are ephemeral. Disconnect creates a new session, and clients must rebuild subscriptions, workers, leases, transactions, and stream resume positions explicitly.

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
- `FITZ_QUEUE_WRITE_POLICY` defaults to `fast`; accepted recent queue mutations can be lost before the background flush window closes.
- `FITZ_STORAGE_CLOUD_DURABILITY` accepts `background` or `strict` for broker-selected durable cloud writes.

## Documentation

Use [docs/README.md](docs/README.md) as the documentation index.

Start here:

- [docs/user-guides/overview.md](docs/user-guides/overview.md)
- [docs/user-guides/quick-start.md](docs/user-guides/quick-start.md)
- [docs/user-guides/api-guide.md](docs/user-guides/api-guide.md)
- [docs/development/domain-boundaries-spec.md](docs/development/domain-boundaries-spec.md)
- [docs/clients/client-spec.md](docs/clients/client-spec.md)
