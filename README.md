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

Run both:

		docker compose up --build

If you want to use published images only, use a compose file that sets `image: ghcr.io/cntryl/fitz:latest` and removes `build`.

Quick checks:

		curl http://localhost:4090/healthz
		curl http://localhost:4190/healthz

Stop:

		docker compose down

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
					FITZ_STORAGE_MODE: "memory"
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
- FITZ_STORAGE_MODE can be set to memory, local, s3, gcs, or azure.
- FITZ_STORAGE_PATH defaults to ./.fitz for local-backed storage modes.

## Documentation

- Full docs index: [docs/README.md](docs/README.md)
- Client protocol spec: [docs/clients/CLIENT_SPEC.md](docs/clients/CLIENT_SPEC.md)
- User onboarding guides: [docs/user-guides](docs/user-guides)
- Operations guides: [docs/operations](docs/operations)


