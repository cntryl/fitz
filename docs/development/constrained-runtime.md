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
