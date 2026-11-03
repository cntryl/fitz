# Benchmark targets

The Cargo manifest is the source of target names and tier membership. Each
benchmark workflow is manually dispatched and uploads its report. These
workloads are diagnostic and are not a pull-request performance gate.

Tier 5 targets requiring explicit setup:

- `tier5_cloud_catalog`: use `cargo bench --bench tier5_cloud_catalog` with an
  immutable `FITZ_TEST_IMAGE`, a loopback S3-compatible provider, and disposable
  provider state. See [the workload contract](cloud-catalog-pressure.md).
- `queue_pressure`: use `cargo bench --bench queue_pressure` with an explicit
  resource envelope.
- `queue_drain_latency`: use `cargo bench --bench queue_drain_latency` with an
  explicit invocation and finite reserve and acknowledgement workload.
- `schedule_pressure`: use `cargo bench --bench schedule_pressure` with an
  explicit resource envelope.
- `kv_pressure`: use `cargo bench --bench kv_pressure` with an explicit
  resource envelope.
- `lease_pressure`: use `cargo bench --bench lease_pressure` with an explicit
  resource envelope.

Other targets are grouped by tier in `.github/workflows/bench-tier*.yml` and
registered in `Cargo.toml`.

The `tier1_hotpath_message` target also compares Wire 2 and Wire 3 core-only
frame decoding, measures a small unknown extension, and scans a valid Wire 3
payload filled with zero-length unknown entries at both the minimum configured
frame limit and the 1 MiB default limit. It also measures validation of the
maximum representable CONNECT capability list, plus construction and framing
of a default-limit extension payload with an explicit allocation budget.
