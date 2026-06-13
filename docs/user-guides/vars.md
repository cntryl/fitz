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
| FITZ_ROUTE_FAMILIES | Comma-separated u32 list, contiguous from 1 (example: 1,2,3) | 1 | Provisioned route-family allowlist accepted after identity resolution. |
| FITZ_ROUTE_FAMILY_MAP | Comma-separated identity=family mappings | Empty | Maps verified identity claim values to provisioned route-family numbers. Required when auth is enabled. |
| FITZ_ROUTE_FAMILY_CLAIM | JWT claim key | tid | Default identity claim key used for route-family resolution. |

## JWT Verification

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_JWT_HMAC_SECRET | Non-empty string | Unset | Enables HMAC JWT verification mode when set. |
| FITZ_JWT_JWKS_MAP | Comma-separated issuer=jwks_url pairs | Unset | Enables JWKS verification mode when HMAC secret is unset. JWKS URLs must be absolute HTTPS URLs without credentials or fragments. |
| FITZ_JWT_AUDIENCES | Comma-separated audience strings | fitz,fitz-broker | Audience allowlist used by HMAC and JWKS modes. |
| FITZ_JWT_AUDIENCE | Single audience string | Alias fallback | Legacy/single-value fallback if FITZ_JWT_AUDIENCES is unset. |

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
| FITZ_ADMIN_USERNAME | Non-empty string | Unset | Required with password hash and JWT secret for protected admin login. |
| FITZ_ADMIN_PASSWORD_HASH | Argon2 password hash | Unset | Required with username and JWT secret for protected admin login. |
| FITZ_ADMIN_JWT_SECRET | Non-empty string | Unset | Required with username and password hash to issue admin session cookies. |
| FITZ_ADMIN_SESSION_TTL_SECS | Positive integer seconds | 28800 | Admin session cookie lifetime. |
| FITZ_ADMIN_COOKIE_SECURE | true or false | true | Sets Secure cookie attribute for admin session cookie. Set false only for loopback/local non-TLS development. |
| FITZ_ADMIN_PUBLIC_ORIGIN | Exact URL origin, e.g. https://admin.example.com | Request host on local binds | Expected same-origin value for protected unsafe admin requests. Required for protected admin on non-loopback binds. Must not include a path, query, fragment, or trailing slash. |
| FITZ_ADMIN_OPEN_USERNAME | String | admin | Username exposed by open mode for admin principal identity. |

## Production Browser Baseline

For browser clients behind a TLS-terminating load balancer:

- Set `FITZ_AUTH_REQUIRED=true`.
- Set `FITZ_ASSUME_EXTERNAL_TLS=true` to emit TLS-dependent browser headers such as HSTS.
- Set `FITZ_WS_ALLOWED_ORIGINS` to the exact SPA origins allowed to open runtime WebSockets, without trailing slashes. The built-in loopback defaults are only for local development.
- Set `FITZ_ADMIN_AUTH_MODE=protected`, `FITZ_ADMIN_PUBLIC_ORIGIN=https://admin.example.com`, and keep `FITZ_ADMIN_COOKIE_SECURE=true`.
- Keep the Fitz backend port reachable only from the load balancer.
- Use short-lived runtime JWTs with narrow route permissions and reconnect with a fresh token when they expire.

## Storage and Durability

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_STORAGE_MODE | memory, local, or cloud | local | Selects storage backend mode. |
| FITZ_STORAGE_PATH | Filesystem path | ./.fitz | Local storage path when FITZ_STORAGE_MODE=local. |
| FITZ_STORAGE_PROVIDER | Provider name | Unset | Required in cloud mode. See provider list below. |
| FITZ_STORAGE_PREFIX | Prefix string | Unset | Optional object key namespace prefix in cloud mode. |
| FITZ_STORAGE_CACHE_PATH | Filesystem path | ./.fitz-cloud-cache | Local cache path for cloud-backed storage mode. |
| FITZ_STORAGE_CLOUD_DURABILITY | background or strict | background | Cloud sync behavior for broker-selected durable writes. |
| FITZ_STORAGE_MEMTABLE_BYTES | Unsigned integer byte count | Auto | Optional explicit memtable size override for embedded engine. |
| FITZ_STREAM_STORAGE_LAYOUT | promotion-frontier or aliases | promotion-frontier | Stream layout selector. Legacy aliases are accepted but normalized to promotion-frontier. |
| FITZ_MIN_MEMORY_BYTES | Unsigned integer byte count | 134217728 | Startup preflight minimum cgroup memory threshold. Set 0 to bypass memory-limit check. |

Supported FITZ_STORAGE_PROVIDER values:
- peas-s3
- peas-azure
- peas-gcs
- aws-s3
- s3-compatible
- minio
- wasabi
- oci-s3
- azure-blob
- gcs

### Provider-Specific FITZ Keys

| Key | Used By | Notes |
| --- | --- | --- |
| FITZ_STORAGE_BUCKET | S3-like and GCS providers | Required for aws-s3, s3-compatible, minio, wasabi, oci-s3, gcs. Optional with peas-s3 and peas-gcs. |
| FITZ_STORAGE_CONTAINER | Azure Blob providers | Required for azure-blob. Optional with peas-azure. |
| FITZ_STORAGE_ENDPOINT | Emulator/custom endpoint | Required for s3-compatible and minio. Optional for peas providers and other endpoint-capable providers. |
| FITZ_STORAGE_REGION | Region string | Required for wasabi and oci-s3. Required for aws-s3 unless AWS_REGION/AWS_DEFAULT_REGION is set. |
| FITZ_STORAGE_NAMESPACE | Namespace string | Required for oci-s3. |
| FITZ_STORAGE_FORCE_PATH_STYLE | true or false | Path-style addressing toggle for s3-compatible and oci-s3 providers. |
| FITZ_STORAGE_ACCOUNT | String | Reserved for compatibility references; not consumed by current runtime provider builders. |

## Observability and Telemetry

| Key | Allowed Values / Format | Default | Description |
| --- | --- | --- | --- |
| FITZ_LOG_FORMAT | text or json | text | Log formatter style. |
| FITZ_LOG_LEVEL | trace, debug, info, warn, etc | info | Base log level for Fitz logger. |
| FITZ_METRICS_PORT | u16 port | 9090 | Prometheus metrics endpoint port. |
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

| Key | Used By | Description |
| --- | --- | --- |
| AWS_REGION | aws-s3 region fallback | Region fallback when FITZ_STORAGE_REGION is not set. |
| AWS_DEFAULT_REGION | aws-s3 region fallback | Secondary region fallback when FITZ_STORAGE_REGION and AWS_REGION are unset. |
| AZURE_STORAGE_CONNECTION_STRING | azure-blob | Enables connection-string auth mode. |
| AZURE_STORAGE_ACCOUNT_NAME | azure-blob | Account name for shared key, SAS, or default credential auth. |
| AZURE_STORAGE_ACCOUNT_KEY | azure-blob | Shared key credential path. |
| AZURE_STORAGE_SAS_TOKEN | azure-blob | SAS token credential path. |
| GOOGLE_APPLICATION_CREDENTIALS | gcs | Service-account file path fallback when HMAC keys are absent. |
| GOOGLE_CLOUD_PROJECT | gcs | Optional project id enrichment for GCS provider config. |
| GCS_HMAC_ACCESS_ID | gcs | HMAC access key id; requires GCS_HMAC_SECRET too. |
| GCS_HMAC_SECRET | gcs | HMAC secret; requires GCS_HMAC_ACCESS_ID too. |

## Compose-Only Convenience Key

| Key | Scope | Description |
| --- | --- | --- |
| FITZ_PEAS_PROVIDER | compose.cloud.yml only | Chooses peas-s3, peas-azure, or peas-gcs in local Peas compose flow. Not read directly by runtime code. |

## Removed Key

| Key | Status | Replacement |
| --- | --- | --- |
| FITZ_AUTH_ALLOW_JWT_ROUTE_FAMILY | Removed and rejected at startup | Use FITZ_ROUTE_FAMILY_MAP and claim-based identity mapping instead. |
