# Cloud catalog recovery workload

The `tier5_cloud_catalog` stress workload verifies cloud-backed KV recovery
from a catalog-heavy store after process termination and cache loss. It checks
exact reads and a new strict commit before and after recovery. It is a recovery
acceptance workload, not a throughput benchmark.

Run it with `cargo bench --bench tier5_cloud_catalog`. The default campaign uses
1,024, 8,192, and 32,768 small WAL segments; set `FITZ_CATALOG_SEGMENTS` to one
of those values to run one stage. The driver requires an immutable runtime image
through `FITZ_TEST_IMAGE` and a loopback S3-compatible provider.

The workload's acceptance envelope is 250 millicores, 512 MiB of memory, no
swap, a 180-second readiness deadline, and a 20-minute campaign deadline. Each
stage must pass exact readback and strict-commit checks before and after a
forced process exit and complete cache loss. Reports and failure evidence are
written under `target/fitz-stress/`.

The companion `should_recover_gib_stream_history_after_complete_cache_loss`
case exercises Stream history recovery separately. Neither case makes a
throughput claim.
