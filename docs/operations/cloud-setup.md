# Cloud Storage Setup

This guide defines category-level setup for blob/object-backed Fitz storage.

## Storage Contract

`FITZ_STORAGE_MODE` accepts:

- `memory`: ephemeral in-process storage.
- `local`: local disk storage using `FITZ_STORAGE_PATH`.
- `cloud`: blob/object-backed storage using `FITZ_STORAGE_PROVIDER`.

Legacy storage-mode aliases are rejected. Use `FITZ_STORAGE_MODE=cloud` plus an explicit provider identifier.

Cloud mode uses a local cache plus provider storage. `FITZ_STORAGE_PATH` is only for local disk storage; cloud mode reads `FITZ_STORAGE_CACHE_PATH` and defaults it to `./.fitz-cloud-cache`.

## Required Shape

Set these values for cloud mode:

```sh
FITZ_STORAGE_MODE=cloud
FITZ_STORAGE_PROVIDER=<provider-identifier>
FITZ_STORAGE_PREFIX=<environment-prefix>
FITZ_STORAGE_CACHE_PATH=/var/lib/fitz-cloud-cache
```

Then provide the namespace and credentials required by your selected blob/object provider. Fitz passes provider-native credential environment through to the storage engine rather than inventing a separate secret format.

Common Fitz keys:

- `FITZ_STORAGE_BUCKET`
- `FITZ_STORAGE_CONTAINER`
- `FITZ_STORAGE_ENDPOINT`
- `FITZ_STORAGE_REGION`
- `FITZ_STORAGE_NAMESPACE`
- `FITZ_STORAGE_FORCE_PATH_STYLE`

Use only the keys required by the selected provider. `FITZ_STORAGE_BUCKET` and `FITZ_STORAGE_CONTAINER` are intentionally separate because not every backend uses bucket terminology.

## Local Emulator Flow

Use [../../compose.cloud.yml](../../compose.cloud.yml) for local blob/object storage testing against `sqrzl-emulator`. This compose file is local-only and keeps the same loopback-bound auth/admin defaults as `compose.yml`.

```sh
docker compose -f compose.cloud.yml up --build
```

Expect Fitz to reach readiness after storage startup and writer-lease acquisition. During startup handoff, `/targetz` can succeed before `/healthz` or `/readyz`; the data plane remains closed until strict readiness succeeds.

## Cloud Durability

`FITZ_STORAGE_CLOUD_DURABILITY` controls broker-selected durable cloud writes:

- `background`: default; local durable work is committed while provider upload continues in the background.
- `strict`: waits for provider acknowledgement for broker-selected durable cloud writes and request-level sync writes.

Any other value is rejected at startup.

Schedule uses this policy for server-selected durable writes. KV and Stream still honor client-selected buffered versus sync modes. Notice, RPC, and Lease remain live or ephemeral as defined by their domain contracts.

Queue has a separate hot-path policy:

- `FITZ_QUEUE_WRITE_POLICY=fast`: default; skips WAL for queue mutations and flushes dirty queue storage in the background.
- `FITZ_QUEUE_WRITE_POLICY=buffered`: uses buffered WAL writes without waiting for sync acknowledgement per queue mutation.
- `FITZ_QUEUE_WRITE_POLICY=strict`: waits for local sync writes; in cloud mode it also waits for provider acknowledgement.

`FITZ_QUEUE_LOSS_WINDOW_MS` defaults to `100` and controls the target flush interval for fast queue writes. In fast mode, accepted recent queue sends, completes, dead-letter replays, and dead-letter purges can be lost if the process or host crashes before the background flush completes.
If that loss leaves only one side of a split queue record, startup discards the
incomplete remnant with a sync write (or provider-acknowledged write in strict
cloud mode), invalidates that queue's derived indexes for authoritative rebuild,
logs the discarded message ID, and continues. Buffered and strict queue
policies continue to fail startup on the same incomplete authoritative state.

## Operations Checklist

1. Set `FITZ_ROUTE_FAMILIES=1,2,...` as a contiguous allowlist before startup.
2. Give each broker process its own `FITZ_STORAGE_CACHE_PATH`.
3. Use a stable `FITZ_STORAGE_PREFIX` per environment, such as `dev`, `staging`, or `prod`.
4. Configure provider namespace, endpoint, region, and credentials outside the image.
5. Configure `/livez`, `/targetz`, `/startupz`, `/healthz`, and `/readyz` on the HTTP listener, plus `FITZ_METRICS_BIND_ADDR:FITZ_METRICS_PORT/metrics` for Prometheus before customer traffic.
6. For single-active rolling handoff, use `/targetz` for target eligibility and keep `/readyz` or `/healthz` for strict data-plane readiness.

Endpoint details are in [../admin/admin-api.md](../admin/admin-api.md) and [observability.md](observability.md).
