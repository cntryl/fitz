# Constrained runtime qualification

Fitz must remain usable with a fraction of a CPU and 512MiB RAM. CI qualifies a
specific local-disk envelope on native Linux AMD64 and ARM64: 0.25 CPU, 512MiB
RAM, swap disabled, and an external load generator. This is a survival and
correctness gate, not a throughput promise or a maximum-capacity claim.

The campaign probes all seven domains, enqueues 16,384 uniquely identified 4KiB
Queue messages through 64 connections, verifies every delivery and ACK, checks
that the queue is empty, and probes all seven domains again. Health must remain
ready throughout. The container must remain running with zero restarts and no
OOM kill. Every operation must finish within the campaign's 600-second deadline;
returned rejections fail this bounded qualification and are never blindly retried.

The 64MiB payload burst crosses normal Midge SST publication and checks that
storage pressure does not leave other domains unusable. Notice/RPC probes cover
live delivery; Lease covers acquire/release; Schedule covers create/cancel;
Stream and KV cover write/read; Queue covers enqueue/reserve/ACK. These probes do
not establish replay, power-loss recovery, scheduled execution, or ownership
continuity. The campaign uses anonymous connections and local storage; cloud
storage, authenticated workloads, and larger bursts require separate evidence.

Run against the exact source checkout used to build the image:

```sh
docker build -t fitz-resource-local .
docker run --detach --name fitz-resource-local \
  --cpus 0.25 --memory 512m --memory-swap 512m \
  --publish 127.0.0.1::4090 --publish 127.0.0.1::4091 \
  --env FITZ_AUTH_REQUIRED=false --env FITZ_ADMIN_AUTH_MODE=open \
  --env FITZ_STORAGE_MODE=local --env FITZ_STORAGE_PATH=/data \
  --env FITZ_LOG_LEVEL=warn --env OTEL_ENABLED=false fitz-resource-local
export FITZ_EXTERNAL_CONTAINER=fitz-resource-local
export FITZ_EXTERNAL_HTTP_ENDPOINT="http://$(docker port fitz-resource-local 4090/tcp)"
export FITZ_EXTERNAL_TCP_ADDRESS="$(docker port fitz-resource-local 4091/tcp)"
export FITZ_RESOURCE_SOURCE_SHA="$(git rev-parse HEAD)"
export FITZ_RESOURCE_REPORT_PATH=target/fitz-stress/constrained-runtime/report.json
curl --fail "$FITZ_EXTERNAL_HTTP_ENDPOINT/healthz"
cargo test --locked --test constrained_runtime \
  should_remain_usable_after_bounded_storage_pressure -- --ignored --nocapture
```

Wait for `/healthz` to report `ready` before starting the driver. The driver
checks the actual Docker limits before and after the campaign and writes its
accounting, health timings, container state, and supplied source SHA to the
report. CI also retains Docker stats and logs; on failure it retains the local
store before removing its disposable container. Docker CLI memory stats report
a working set; the container's hard memory limit includes file cache.

The Queue optimization exposed a Midge SST index bug: an unsafe trie successor
hint could hide an existing Stream epoch key after SST publication. Fitz pins
the immutable upstream fix from Midge PR 775 until a registry release contains
it. The Stream concurrency guard remains enforced. A focused actor regression
also checks Stream writes after Fast Queue forces automatic SST publication.

## S3 WAL retention and crash recovery

CI also runs four process-level campaigns against the same native runtime image
at 0.25 CPU / 512MiB / no additional swap. A digest-pinned Sqrzl container and
the external driver run outside the broker's resource limit. Fitz uses its native
S3-compatible provider with explicit strict Queue and cloud durability. Provider
failure fails the campaign; these tests never silently skip.

The retention campaign runs four enqueue/ACK cycles of 4,096 deterministic 16KiB
messages, totaling 256MiB of accepted payload. It uses the ordinary automatic
memtable and maintenance settings. Within 120 seconds of the final ACK, the
catalog segments captured during the first cycle must retire, SST objects
must exist, and both
catalog-authorized WAL and actual remote WAL objects must total at most 128MiB.
Every accepted message ID and payload is verified before ACK, and each cycle
ends with an empty queue. This measures a fixed resource/workload envelope;
it does not assert that every possible database has a universal WAL size cap.

Two separate restart campaigns first enqueue 4,096 strict 16KiB messages and
require at least 32MiB of catalog-authorized WAL to remain. Each then sends
SIGKILL, verifies exit 137 without an OOM kill, and either restarts with the same
cache or removes the container and its anonymous data volume before creating a
replacement with a fresh cache. Strict `/healthz` must report ready within 180
seconds measured from the Docker start attempt, including container startup
and the configured 59-second crashed-writer lease. All accepted
IDs and exact payloads must recover, be ACKed and leave the queue empty. The
seven-domain smoke probe must also pass after recovery. Each whole campaign
has a fixed 1,200-second deadline.

The large-WAL campaign constructs a separate catalog-authorized 640MiB backlog
with the pinned Midge frame/record codecs, then opens the ordinary Fitz image
with a completely empty cache. This backlog exceeds the broker's entire hard
memory limit. Readiness has the same 180-second deadline, and 40,960 exact KV
values must be readable through Fitz afterward. Fixture construction runs
outside the resource cap and occurs with no active writer. It qualifies large
WAL recovery; the two Queue campaigns separately prove preservation of writes
acknowledged through Fitz. Only the external driver enables the
`recovery-qualification` feature; the production image uses its normal features.

Reports retain the supplied source SHA, resolved runtime image ID, provider
digest, actual Docker resource limits, authoritative WAL catalog and remote
object inventories, readiness timings, container lifecycle, broker logs and
Docker memory samples. The driver requires Docker and AWS CLI v2. Run the
ignored `s3::` tests in `constrained_runtime` with
`--features recovery-qualification` and one test thread; `ci.yml`
contains the complete disposable provider setup and required environment.

These campaigns qualify native S3 protocol behavior against an emulator. They
do not qualify production AWS latency, an unmeasured historic backlog, or reuse
of a Fitz 0.1.0 storage prefix. Fitz 0.1.0 resolved Midge 0.1.1; current develop
pins Midge 0.3.3 revision `d5bcb607b75b1fa5cf5e4f4f910d7e49f9ebe12b`.
Its streaming recovery checkpoints, exact delete/range-tombstone retirement,
budgeted replay coverage and indexed SST candidate selection address concrete
historical retention/recovery mechanisms. Residual recovery cost remains
tracked in [Midge #754](https://github.com/cntryl/midge/issues/754), and the final
published dependency and Fitz stable release remain gated by
[Fitz #416](https://github.com/cntryl/fitz/issues/416). Follow the
[migration guide](../operations/migration-guide.md) before any production cutover.
