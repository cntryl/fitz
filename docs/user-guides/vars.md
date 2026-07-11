# Environment Variables Reference

This page is the central reference for environment variables supported by Fitz runtime code and related deployment surfaces.

## Core Broker

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_AUTH_REQUIRED | true or false | true | Enables authenticated CONNECT JWT validation. When false, broker allows anonymous mode. |
| FITZ_HTTP_PORT | u16 port | 4090 | HTTP and WebSocket listener port. |
| FITZ_TCP_ENABLED | true or false | true | Enables the raw TCP listener. HTTP, admin routes, and WebSocket remain enabled regardless. |
| FITZ_TCP_PORT | u16 port | 4091 | Raw TCP listener port. |
| FITZ_BIND_ADDR | IP or hostname | 0.0.0.0 | Bind address for listeners. |
| FITZ_ASSUME_EXTERNAL_TLS | true or false | false | Enables TLS-dependent browser response behavior such as HSTS when TLS is terminated outside Fitz. Local development can leave this unset. |
| FITZ_WS_ALLOWED_ORIGINS | Comma-separated exact browser origins, e.g. https://app.example.com | Local loopback origins for ports 3000 and 4090 | Browser WebSocket Origin allowlist. Values are HTTP origins, not wss URLs, and must not include a path, query, fragment, or trailing slash. Public browser deployments should set this to their exact SPA origins. |
| FITZ_DRAIN_GRACE_SECONDS | Positive integer seconds | 25 | Planned redeploy drain grace. During drain, `/healthz` and `/readyz` fail and new TCP/WebSocket sessions are rejected before active sessions are closed on shutdown. Set lower than the external termination grace. |
| FITZ_DRAIN_CLOSE_REASON | Non-empty string | broker draining for redeploy | Server close reason recorded when planned drain shutdown closes active sessions. |
| FITZ_ROUTE_FAMILIES | Comma-separated u32 list, contiguous from 1 (example: 1,2,3) | 1 | Provisioned route-family allowlist accepted after identity resolution. |
| FITZ_ROUTE_FAMILY_MAP | Comma-separated identity=family mappings | Empty | Maps verified identity claim values to provisioned route-family numbers. Required when auth is enabled. |
| FITZ_ROUTE_FAMILY_CLAIM | JWT claim key | tid | Default identity claim key used for route-family resolution. |

## JWT Verification

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_JWT_JWKS_MAP | Comma-separated issuer=jwks_url pairs | Unset | Preferred runtime token verification mode when `FITZ_AUTH_REQUIRED=true`. JWKS URLs must be absolute URLs without credentials or fragments. By default, only `https` is accepted. |
| FITZ_JWT_HMAC_SECRET | Secret string | Unset | Local-development fallback. Enables HS256 tokens when JWKS is not configured. Do not use for production traffic. |
| FITZ_JWT_AUDIENCES | Comma-separated audience strings | fitz,fitz-broker | Audience allowlist used by runtime JWKS verification. |
| FITZ_JWT_AUDIENCE | Single audience string | Alias fallback | Legacy/single-value fallback if FITZ_JWT_AUDIENCES is unset. |
| FITZ_JWT_ALLOW_INSECURE_HTTP | true or false | false | Allows `http://` JWKS URLs for local mock environments when set to `true`. Never enable this outside local development. |

## Claim Normalization

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_AUTH_CUSTOM_CLAIM | Namespaced JWT object claim key | Unset | Highest-priority permission source. Claim must contain a permissions array object payload. |
| FITZ_AUTH_PERMISSIONS_CLAIM | JWT array claim key (example: fitz://permissions) | Unset | Optional permission source checked after top-level permissions and before role/scp/scope sources. |
| FITZ_AUTH_ROLE_CLAIM | JWT array claim key | roles | Role claim source used for already-normalized Fitz permissions or recognized coarse scopes. |
| FITZ_AUTH_ORG_CLAIM | JWT string claim key (example: fitz://org_id) | Unset | Optional identity override checked before FITZ_ROUTE_FAMILY_CLAIM for route-family resolution. |

Permission source precedence is fixed:
1. FITZ_AUTH_CUSTOM_CLAIM
2. Top-level permissions
3. FITZ_AUTH_PERMISSIONS_CLAIM
4. FITZ_AUTH_ROLE_CLAIM
5. scp
6. scope

Identity claim lookup precedence is fixed:
1. FITZ_AUTH_ORG_CLAIM (if configured and present)
2. FITZ_ROUTE_FAMILY_CLAIM

## Admin API Authentication

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_ADMIN_AUTH_MODE | protected or open | protected | Admin auth mode. Open mode bypasses session login. |
| FITZ_ADMIN_USERNAME | Non-empty string | Unset | Required with password hash for protected admin login. |
| FITZ_ADMIN_PASSWORD_HASH | Argon2 password hash | Unset | Required with username for protected admin login. |
| FITZ_ADMIN_SESSION_TTL_SECS | Positive integer seconds | 28800 | Admin session cookie lifetime. |
| FITZ_ADMIN_COOKIE_SECURE | true or false | true | Sets Secure cookie attribute for admin session cookie. Set false only for loopback/local non-TLS development. |
| FITZ_ADMIN_PUBLIC_ORIGIN | Exact URL origin, e.g. https://admin.example.com | Request host on local binds | Expected same-origin value for protected unsafe admin requests. Required for protected admin on non-loopback binds. Must not include a path, query, fragment, or trailing slash. |
| FITZ_ADMIN_OPEN_USERNAME | String | admin | Username exposed by open mode for admin principal identity. |

`FITZ_ADMIN_JWT_SECRET` is no longer required; Fitz generates a process-ephemeral signing key for admin session cookies when protected mode is configured. Admin session cookies therefore do not survive broker restarts.

## Local Development Baseline

The repo compose files are local-development examples only:

- `compose.yml`, `compose.cloud.yml`, and `compose.sqrzl.yml` publish only to loopback and are not production deployment manifests.
- `compose.yml` and `compose.cloud.yml` keep `fitz-auth` on `FITZ_JWT_HMAC_SECRET` by default so `docker compose up` stays the shortest successful path.
- Those same compose files keep `FITZ_ADMIN_AUTH_MODE=open` because the admin surface is loopback-only and meant for local inspection.
- The built-in loopback defaults for `FITZ_WS_ALLOWED_ORIGINS` are only for local development.

To exercise issuer/JWKS plumbing locally instead of the default HMAC flow:

- Start `docker compose -f compose.yml -f compose.jwks.yml up --build`, or layer the same overlay onto `compose.cloud.yml`.
- `compose.jwks.yml` starts a local `fitz-jwks` service and sets:
  - `FITZ_JWT_ALLOW_INSECURE_HTTP=true`
  - `FITZ_JWT_JWKS_MAP="https://fitz.mock/=http://fitz-jwks:8080/.well-known/jwks.json"`
- Tokens in that mode must use `iss=https://fitz.mock/` and the mock JWKS key material documented in [quick-start.md](quick-start.md).

## Production Baseline

For authenticated browser or API deployments outside local development:

- Set `FITZ_AUTH_REQUIRED=true`.
- Configure runtime JWT verification with `FITZ_JWT_JWKS_MAP`. Do not rely on `FITZ_JWT_HMAC_SECRET` in production.
- Set `FITZ_ASSUME_EXTERNAL_TLS=true` when TLS terminates outside Fitz.
- Set `FITZ_WS_ALLOWED_ORIGINS` to the exact public SPA origins allowed to open browser WebSockets.
- Set `FITZ_ADMIN_AUTH_MODE=protected`, `FITZ_ADMIN_PUBLIC_ORIGIN=https://admin.example.com`, and keep `FITZ_ADMIN_COOKIE_SECURE=true`.
- Expect protected-admin session cookies to expire on broker restart because the signing key is generated in memory per process.
- Keep Fitz reachable only from your TLS terminator or other trusted network boundary.

For the complete production auth and browser-perimeter checklist, see [../operations/production-auth.md](../operations/production-auth.md).

## Storage and Durability

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_STORAGE_MODE | memory, local, or cloud | local | Selects storage backend mode. |
| FITZ_STORAGE_PATH | Filesystem path | ./.fitz | Local storage path when FITZ_STORAGE_MODE=local. |
| FITZ_STORAGE_PROVIDER | Provider identifier | Unset | Required in cloud mode. Selects the configured blob/object storage backend. |
| FITZ_STORAGE_PREFIX | Prefix string | Unset | Optional object key namespace prefix in cloud mode. |
| FITZ_STORAGE_CACHE_PATH | Filesystem path | ./.fitz-cloud-cache | Local cache path for cloud-backed storage mode. |
| FITZ_STORAGE_CLOUD_DURABILITY | background or strict | background | Cloud sync behavior for broker-selected durable writes. |
| FITZ_STORAGE_MEMTABLE_BYTES | Unsigned integer byte count | Auto | Optional explicit memtable size override for embedded engine. |
| FITZ_QUEUE_WRITE_POLICY | fast, buffered, or strict | fast | Queue mutation write policy. `fast` skips WAL on the hot path and flushes dirty queue storage in the background. |
| FITZ_QUEUE_LOSS_WINDOW_MS | Positive integer millisecond count | 100 | Target background flush interval for fast queue writes. Accepted recent queue mutations can be lost before this window closes. |
| FITZ_STREAM_STORAGE_LAYOUT | promotion-frontier or aliases | promotion-frontier | Stream layout selector. Legacy aliases are accepted but normalized to promotion-frontier. |
| FITZ_MIN_MEMORY_BYTES | Unsigned integer byte count | 134217728 | Startup preflight minimum cgroup memory threshold. Set 0 to bypass memory-limit check. |

Schedule persistence follows the selected storage mode. `memory` mode uses
best-effort writes and does not promise restart recovery. Persistent local and
background-cloud modes wait for local sync; strict-cloud mode waits for the
provider acknowledgement. Definitions and unresolved fire claims have no
age-based expiry; cancellation or deletion is the explicit resolution path.

### Provider-Specific FITZ Keys

| Key | Used By | Notes |
| --- | --- | --- |
| FITZ_STORAGE_BUCKET | Bucket-shaped providers | Required when the selected provider uses bucket-style naming. |
| FITZ_STORAGE_CONTAINER | Container-shaped providers | Required when the selected provider uses container-style naming. |
| FITZ_STORAGE_ENDPOINT | Emulator/custom endpoint | Required when the selected provider needs an explicit endpoint. |
| FITZ_STORAGE_REGION | Region string | Required when the selected provider needs an explicit region. |
| FITZ_STORAGE_NAMESPACE | Namespace string | Required when the selected provider needs an explicit namespace. |
| FITZ_STORAGE_FORCE_PATH_STYLE | true or false | Path-style addressing toggle for compatible object providers. |
| FITZ_STORAGE_ACCOUNT | String | Reserved for compatibility references; not consumed by current runtime provider builders. |

## Observability and Telemetry

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_LOG_FORMAT | text or json | text | Log formatter style. |
| FITZ_LOG_LEVEL | trace, debug, info, warn, etc | info | Base log level for Fitz logger. |
| FITZ_METRICS_BIND_ADDR | IP or hostname | 127.0.0.1 | Bind address for the dedicated unauthenticated Prometheus listener. |
| FITZ_METRICS_PORT | u16 port | 9090 | Prometheus metrics endpoint port on the dedicated listener. |
| FITZ_HOT_PATH_METRICS | 1/true/yes/on to enable | false | Enables expensive hot-path attribution metrics. |
| FITZ_SERVICE_INSTANCE_ID | String | Generated UUID | Service instance metadata for tracing resources. |
| FITZ_DEPLOYMENT_ENVIRONMENT | String | unknown | Deployment environment metadata for tracing resources. |
| FITZ_OTEL_SAMPLE_RATIO | Float between 0.0 and 1.0 | 1.0 | OTEL trace sampling ratio. |

### Non-FITZ Telemetry Keys Read by Runtime

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| RUST_LOG | tracing env filter syntax | Unset | If set, takes precedence over FITZ_LOG_LEVEL filter construction. |
| OTEL_ENABLED | true or false | true | Enables OTLP export path in observability init. |
| OTEL_EXPORTER_OTLP_ENDPOINT | URL | http://localhost:4317 | OTLP collector endpoint. |

## Cloud Provider Credential and Platform Keys

These keys are read in cloud provider builders when FITZ_STORAGE_MODE=cloud.

| Key | Description |
| --- | --- |
| Provider-native region variables | Region fallback when `FITZ_STORAGE_REGION` is not set and the provider supports native region discovery. |
| Provider-native credential variables | Credential fallback when credentials are supplied through the process environment. |
| Provider-native project or account variables | Optional provider metadata when the selected backend requires it. |

## Compose-Only Convenience Key

| Key | Scope | Description |
| --- | --- | --- |
| FITZ_SQRZL_PROVIDER | compose.cloud.yml only | Chooses the local emulator front door in the compose flow. Not read directly by runtime code. |

## Removed Key

| Key | Status | Replacement |
| --- | --- | --- |
| FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY | Removed and rejected at startup | Use FITZ_ROUTE_FAMILY_MAP and claim-based identity mapping instead. |
