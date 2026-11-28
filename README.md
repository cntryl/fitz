# fitz

Fitz is a container-friendly application broker for messaging, durable history,
and application state. Each storage namespace has one active writer.

## What Fitz Provides

| Domain | Use it for | Durability model |
| --- | --- | --- |
| Notice | live fanout to connected subscribers | ephemeral |
| Stream | durable append and replay of committed history | durable according to write mode |
| KV | current authoritative state | durable on commit according to write mode |
| Queue | work delivery with reservation and redelivery | best-effort persistence; crash loss is possible |
| RPC | live request and response dispatch to registered workers | ephemeral |
| Lease | single-broker ownership coordination | ephemeral |
| Schedule | durable future timing intent | durable definitions and pending fire claims |

Midge persists durable state to local disk or provider storage with a local
cache. A replacement broker can recover persisted state after acquiring the
storage lease. Lease expiry can delay replacement after a crash. Clients
reconnect and rebuild ephemeral sessions. Cluster failover and sharding are
outside Fitz's deployment model. Assign independent workloads to separate
deployments and storage namespaces. See the [durability guide](docs/user-guides/durability.md) and
[operator guide](docs/operations/operator-guide.md) for recovery behavior.

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

For a reproducible deployment, use an immutable SemVer image published by the
repository release process.

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

Each profile starts local brokers and a provider-compatible storage emulator.
See the compose files for ports and credentials.

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
- Operator guide: [docs/operations/operator-guide.md](docs/operations/operator-guide.md)
- Auth and browser perimeter: [docs/operations/auth-browser-deployment.md](docs/operations/auth-browser-deployment.md)
- Probes and metrics: [docs/operations/observability.md](docs/operations/observability.md)
- Operations runbook: [docs/operations/operations-runbook.md](docs/operations/operations-runbook.md)
- Environment variables: [docs/user-guides/vars.md](docs/user-guides/vars.md)

Important defaults:

- `FITZ_AUTH_REQUIRED` defaults to `true`.
- `FITZ_ROUTE_FAMILIES` defaults to `1`; configure a contiguous allowlist such as `1,2,3` before serving multiple isolated families.
- `FITZ_STORAGE_MODE` accepts `memory`, `local`, or `cloud`.
- Queue is for regenerable or loss-tolerant work. Accepted enqueues and ACKs can
  be lost before background persistence completes, so an enqueue response is
  not a durable commit.
- `FITZ_STORAGE_CLOUD_DURABILITY` accepts `background` or `strict` for broker-selected durable cloud writes.

## Documentation

- [Documentation index](docs/README.md)
- [Quick start](docs/user-guides/quick-start.md)
- [API guide](docs/user-guides/api-guide.md)
- [Client specification](docs/clients/client-spec.md)
- [Contributing](CONTRIBUTING.md)
