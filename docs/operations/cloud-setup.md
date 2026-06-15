# Cloud Setup

This guide defines Fitz cloud-backed storage setup. The intended path is:

1. Start locally with Peas.
2. Pick the target provider with `FITZ_STORAGE_PROVIDER`.
3. Move to the real provider by changing environment values, not Fitz code.

## Storage Contract

`FITZ_STORAGE_MODE` accepts only:

- `memory`: ephemeral in-process storage
- `local`: local disk storage using `FITZ_STORAGE_PATH`
- `cloud`: cloud-backed storage using `FITZ_STORAGE_PROVIDER`

Legacy values such as `FITZ_STORAGE_MODE=s3`, `FITZ_STORAGE_MODE=gcs`, and `FITZ_STORAGE_MODE=azure` are rejected. Use `FITZ_STORAGE_MODE=cloud` plus a provider value instead.

Cloud mode uses a local cache plus object storage. `FITZ_STORAGE_PROVIDER` is required; Fitz does not pick a cloud provider implicitly. `FITZ_STORAGE_PATH` is only for local disk storage; cloud mode reads `FITZ_STORAGE_CACHE_PATH` and defaults it to `./.fitz-cloud-cache`.

Cloud-backed storage is tuned for write batching and lower object-store churn by default. `FITZ_STORAGE_MEMTABLE_BYTES` still takes precedence when operators want an exact runtime memtable size or flush threshold.

## Local Peas

Use the Peas compose file for local cloud storage. This file is intentionally Peas-only; do not use it for real cloud providers.

```sh
docker compose -f compose.cloud.yml up --build
```

Expect the Fitz brokers to reach readiness eventually rather than instantly on first
boot. Fitz now retries single-writer lease-held storage opens for a bounded window
before failing startup, so brief handoff races should delay `/healthz` and `/readyz`
rather than immediately crashing the new process.

The default provider is `peas-s3`. To exercise the other Peas front doors, use the compose-only `FITZ_PEAS_PROVIDER` variable:

```sh
FITZ_PEAS_PROVIDER=peas-azure docker compose -f compose.cloud.yml up --build
FITZ_PEAS_PROVIDER=peas-gcs docker compose -f compose.cloud.yml up --build
```

Peas defaults:

- Docker endpoint: `http://peas:9000`
- Host endpoint for local tests: `http://127.0.0.1:9000`
- Access key: `admin`
- Secret key: `easy-peasy`
- Bucket/container: optional; set `FITZ_STORAGE_BUCKET` or `FITZ_STORAGE_CONTAINER` only when you want a fixed namespace name

## Provider Values

Set `FITZ_STORAGE_MODE=cloud` and one of these provider values. Provider-native credential variables are not remapped except for the explicit Fitz namespace/cache/env listed here.

| Provider | Required Fitz env | Provider-native credentials |
| --- | --- | --- |
| `peas-s3` | Optional `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_ENDPOINT` | Built-in Peas `admin` / `easy-peasy` |
| `peas-azure` | Optional `FITZ_STORAGE_CONTAINER`, `FITZ_STORAGE_ENDPOINT` | Built-in Peas `admin` / `easy-peasy` |
| `peas-gcs` | Optional `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_ENDPOINT` | Built-in Peas `admin` / `easy-peasy` |
| `aws-s3` | `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_REGION` or `AWS_REGION` | AWS default environment chain |
| `s3-compatible` | `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_ENDPOINT` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional session token |
| `minio` | `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_ENDPOINT` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `wasabi` | `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_REGION` | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| `oci-s3` | `FITZ_STORAGE_BUCKET`, `FITZ_STORAGE_NAMESPACE`, `FITZ_STORAGE_REGION` | AWS-style OCI S3 credentials |
| `azure-blob` | `FITZ_STORAGE_CONTAINER` | `AZURE_STORAGE_CONNECTION_STRING`, or `AZURE_STORAGE_ACCOUNT_NAME` with key/SAS/default identity |
| `gcs` | `FITZ_STORAGE_BUCKET` | `GCS_HMAC_ACCESS_ID` plus `GCS_HMAC_SECRET`, service account file, or ADC |

Common cloud env:

- `FITZ_STORAGE_PREFIX`: object namespace prefix
- `FITZ_STORAGE_CACHE_PATH`: local cache path
- `FITZ_STORAGE_ENDPOINT`: custom endpoint or emulator endpoint
- `FITZ_STORAGE_REGION`: provider region
- `FITZ_STORAGE_FORCE_PATH_STYLE`: path-style S3-compatible addressing

Azure Blob intentionally uses `FITZ_STORAGE_CONTAINER`, not `FITZ_STORAGE_BUCKET`, and it uses `AZURE_STORAGE_ACCOUNT_NAME`, not a Fitz account alias.

## Real Provider Examples

AWS S3:

```sh
FITZ_STORAGE_MODE=cloud
FITZ_STORAGE_PROVIDER=aws-s3
FITZ_STORAGE_BUCKET=fitz-prod
FITZ_STORAGE_REGION=us-east-1
FITZ_STORAGE_PREFIX=prod
FITZ_STORAGE_CACHE_PATH=/var/lib/fitz-cloud-cache
```

S3-compatible:

```sh
FITZ_STORAGE_MODE=cloud
FITZ_STORAGE_PROVIDER=s3-compatible
FITZ_STORAGE_BUCKET=fitz-prod
FITZ_STORAGE_ENDPOINT=https://objects.example.com
FITZ_STORAGE_REGION=us-east-1
FITZ_STORAGE_FORCE_PATH_STYLE=true
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

Azure Blob:

```sh
FITZ_STORAGE_MODE=cloud
FITZ_STORAGE_PROVIDER=azure-blob
FITZ_STORAGE_CONTAINER=fitz-prod
FITZ_STORAGE_PREFIX=prod
FITZ_STORAGE_CACHE_PATH=/var/lib/fitz-cloud-cache
AZURE_STORAGE_ACCOUNT_NAME=...
AZURE_STORAGE_ACCOUNT_KEY=...
```

GCS:

```sh
FITZ_STORAGE_MODE=cloud
FITZ_STORAGE_PROVIDER=gcs
FITZ_STORAGE_BUCKET=fitz-prod
FITZ_STORAGE_PREFIX=prod
FITZ_STORAGE_CACHE_PATH=/var/lib/fitz-cloud-cache
GOOGLE_APPLICATION_CREDENTIALS=/var/run/secrets/gcp/service-account.json
GOOGLE_CLOUD_PROJECT=...
```

## Cloud Durability

`FITZ_STORAGE_CLOUD_DURABILITY` controls Fitz-selected durable cloud writes:

- `background`: default; local WAL work is committed while provider upload continues in the background.
- `strict`: waits for provider acknowledgement for broker-selected durable cloud writes and request-level sync writes.

Any other value is rejected at startup.

Queue and Schedule use this policy for server-selected durable writes. KV and Stream still honor client-selected buffered versus sync modes; when cloud strict is configured, sync maps to provider-ack writes.

This setting does not change Fitz domain semantics. Notice, RPC, and Lease remain live or ephemeral as defined by the protocol; cloud storage only backs durable domains that already persist state or history.

## Operations Checklist

1. Set `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous allowlist before startup.
2. Give each broker its own `FITZ_STORAGE_CACHE_PATH`.
3. Use a stable `FITZ_STORAGE_PREFIX` per environment, such as `dev`, `staging`, or `prod`.
4. Start Peas locally with `compose.cloud.yml`; only `FITZ_PEAS_PROVIDER=peas-s3|peas-azure|peas-gcs` is supported there.
5. Move to production by using explicit runtime env for `FITZ_STORAGE_PROVIDER`, namespace, endpoint/region, and provider credentials.
6. Configure `/livez`, `/healthz`, `/readyz`, `/startupz`, and `/metrics` monitoring before customer traffic.

Details for endpoints are in [admin/admin-api.md](../admin/admin-api.md) and [operations/observability.md](observability.md).
