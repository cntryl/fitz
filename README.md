# fitz

Fitz is a broker project for teams that need one place to handle common application messaging patterns.

## What Problem Fitz Solves

Many systems end up running separate tools for:

- request response traffic
- queue workloads
- event fanout
- stream style delivery
- key value coordination data

That increases operational overhead, client complexity, and integration drift across services.

Fitz is intended to provide a single broker surface for these patterns so teams can start with one deployment model and one client contract.

## What Fitz Should Be

Fitz should be:

- simple to start locally and in containers
- explicit about durability and failure behavior
- predictable to operate with health probes and metrics
- practical to adopt from multiple client languages

It currently exposes HTTP and WebSocket on 4090, TCP framed traffic on 4091, and domain surfaces for KV, queue, notice, RPC, lease, stream, and schedule.

Current status: early prototype.

## Quick Start

### Option 1: Docker Run

1. Start Fitz from GHCR with auth disabled for local onboarding:

		docker run --rm -p 4090:4090 -p 4091:4091 -e FITZ_AUTH_REQUIRED=false -e RUST_LOG=info,fitz=trace ghcr.io/cntryl/fitz:latest

2. Verify basic health from another terminal:

		curl http://localhost:4090/healthz

Expected result: HTTP 200 with a small JSON status response.

### Option 2: Docker Compose

The repository includes [compose.yml](compose.yml) with two services:

- fitz-auth: auth required on 4090 and 4091
- fitz-anon: auth disabled on 4190 and 4191

The compose file also configures admin auth for the local UI and admin API. Use these credentials when signing in:

- Username: `admin`
- Password: `pwd123`

Run both:

		docker compose up --build

If you want to use published images only, use a compose file that sets `image: ghcr.io/cntryl/fitz:latest` and removes `build`.

Quick checks:

		curl http://localhost:4090/healthz
		curl http://localhost:4190/healthz

Stop:

		docker compose down

### Option 3: Cloud Storage With Peas

[compose.cloud.yml](compose.cloud.yml) starts the same two Fitz brokers against the Peas emulator. This compose file is local-emulator-only and defaults to the S3-compatible Peas front door:

		docker compose -f compose.cloud.yml up --build

Peas provider flips stay in a compose-only environment variable:

		FITZ_PEAS_PROVIDER=peas-azure docker compose -f compose.cloud.yml up --build
		FITZ_PEAS_PROVIDER=peas-gcs docker compose -f compose.cloud.yml up --build

Peas uses `http://peas:9000` inside compose and publishes `http://127.0.0.1:9000` for local host tests. The default namespace is `fitz`, with access key `admin` and secret `easy-peasy`. Real cloud providers use explicit `FITZ_STORAGE_MODE=cloud` plus `FITZ_STORAGE_PROVIDER=...` runtime env instead of this Peas compose file.

### Minimal Compose Example

If you want a single local service, this is the smallest useful compose file:

		services:
			fitz:
				image: ghcr.io/cntryl/fitz:latest
				ports:
					- "4090:4090"
					- "4091:4091"
				environment:
					FITZ_AUTH_REQUIRED: "false"
					FITZ_STORAGE_MODE: "local"
					FITZ_STORAGE_PATH: "/data"
					RUST_LOG: "info,fitz=trace"

## Endpoints

- HTTP root and static UI: http://localhost:4090/
- WebSocket endpoint: ws://localhost:4090/ws
- TCP endpoint: localhost:4091
- Health probes: /healthz, /readyz, /startupz
- Metrics: /metrics

## Configuration Notes

- FITZ_AUTH_REQUIRED defaults to true.
- FITZ_ROUTE_FAMILIES defaults to `1`. Configure a non-empty contiguous list starting at `1`, such as `1,2,3`, before startup.
- FITZ_STORAGE_MODE can be set to `memory`, `local`, or `cloud`.
- FITZ_STORAGE_PATH is only for local disk storage and defaults to `./.fitz`.
- FITZ_STORAGE_MEMTABLE_BYTES optionally sets Midge's runtime memtable size and flush threshold in bytes. Lower values make SST flushes happen sooner for diagnostics and constrained environments.
- Cloud storage requires FITZ_STORAGE_PROVIDER plus provider-specific values. Supported providers are `peas-s3`, `peas-azure`, `peas-gcs`, `aws-s3`, `s3-compatible`, `minio`, `wasabi`, `oci-s3`, `azure-blob`, and `gcs`.
- Cloud storage uses FITZ_STORAGE_CACHE_PATH for its local cache and defaults to `./.fitz-cloud-cache`; it does not read FITZ_STORAGE_PATH.
- FITZ_STORAGE_CLOUD_DURABILITY can be `background` or `strict`; any other value is rejected. `background` keeps provider upload asynchronous; `strict` waits for provider acknowledgement for broker-selected durable cloud writes and request-level sync writes.
- Authenticated JWTs resolve to a route family server-side. Keep `FITZ_ROUTE_FAMILIES=1,2,...` as the provisioned allowlist, set `FITZ_ROUTE_FAMILY_MAP=tid-value=1,other=2`, and use `FITZ_ROUTE_FAMILY_CLAIM` to choose the default identity claim (`tid` by default).
- `FITZ_AUTH_ORG_CLAIM` optionally overrides identity lookup before `FITZ_ROUTE_FAMILY_CLAIM` when that claim is present in the token. Example: `FITZ_AUTH_ORG_CLAIM=fitz://org_id`.
- `FITZ_AUTH_CUSTOM_CLAIM` can point at a namespaced JWT object containing only `permissions`, for example `https://example.com/fitz`.
- `FITZ_AUTH_PERMISSIONS_CLAIM` optionally points to a namespaced array claim of permission strings. Example: `FITZ_AUTH_PERMISSIONS_CLAIM=fitz://permissions`.
- `FITZ_AUTH_ROLE_CLAIM` defaults to `roles` and is only used when that claim's array values are already direct Fitz permissions or recognized coarse scopes.
- Permission normalization is fixed: configured custom permission claim, top-level `permissions`, configured `FITZ_AUTH_PERMISSIONS_CLAIM` array, configured role claim array, `scp`, then `scope`.
- Provider setup is claim-driven: Auth0 can use `org_id` plus top-level `permissions` or namespaced claims via `FITZ_AUTH_ORG_CLAIM` and `FITZ_AUTH_PERMISSIONS_CLAIM`; Entra delegated uses `tid` plus `scp`; Entra app-only uses `tid` plus `roles`; Cognito uses `custom:tenant_id` or `sub` plus `scope`; Okta uses an exact custom or namespaced identity claim plus `scope`, a configured custom permissions claim, or a configured role claim array.
- FITZ_MIN_MEMORY_BYTES defaults to `134217728` and rejects smaller Linux cgroup memory limits during startup. Set it higher for production headroom, or set it to `0` only when intentionally bypassing the preflight for experiments.

## Documentation

- Full docs index: [docs/README.md](docs/README.md)
- Environment variables reference: [docs/user-guides/vars.md](docs/user-guides/vars.md)
- Architectural laws: [docs/development/architectural-laws.md](docs/development/architectural-laws.md)
- Client protocol spec: [docs/clients/client-spec.md](docs/clients/client-spec.md)
- User onboarding guides: [docs/user-guides](docs/user-guides)
- Auth0 setup: [docs/user-guides/auth0.md](docs/user-guides/auth0.md)
- Operations guides: [docs/operations](docs/operations)
- Local perf loop runner: [docs/development/perf-loop.md](docs/development/perf-loop.md)
