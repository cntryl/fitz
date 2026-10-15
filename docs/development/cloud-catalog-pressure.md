# Tier 5 cloud catalog pressure

`tier5_cloud_catalog` reproduces the catalog memory pressure observed in the
Chipmunk Fitz 0.1.0 incident. Cloud upload and startup logs reported a catalog
decode reservation of 139,236,128 bytes and a provider workspace reservation
of 34,808,008 bytes. The old decoder's `16 * serialized_bytes + 4096` charge
implies an 8,702,002-byte catalog. These are catalog allocations, not the sum
of remote WAL payload bytes.

The workload seeds separate supported-format stores with 1,024, 8,192 and
32,768 small, linked, catalog-authorized WAL segments. They overwrite only
32 logical KV keys; latest exact values are known independently of replay.
Fixture construction and upload run outside the broker's resource cap with
no active writer. The final stage must reach at least the incident's serialized
catalog size. This is an offline history fixture, not a claim that ordinary
rotation necessarily generates these segment sizes or that the deployed
0.1.0 prefix can be opened by Midge 0.4.0.

Each stage starts the ordinary immutable Fitz image at 0.25 CPU, 512 MiB RAM,
and no additional swap. It must become strictly ready within 180 seconds,
return all 32 exact values over TCP, and acknowledge a new cloud-strict KV
commit. The driver then sends SIGKILL, verifies exit 137 without an OOM kill,
removes the anonymous cache volume, and starts a successor from the same
provider store. Crash-to-ready has the same 180-second limit, including the
59-second crashed-writer lease. All values and the new strict commit must
survive. Resource admission errors, process exits, readiness failures and
incorrect data fail the workload; they are never accepted capacity boundaries.

One 1,200-second wall deadline covers all stages, including offline setup.
The framework watchdog defaults to 240 seconds because a single readiness
phase may legally exceed the usual 60-second workload watchdog. Readiness
retains its stricter independent deadline. The workload records one
`catalog_recovery_cycle` only after a stage's complete verification and orderly
cleanup. Fixture writes and individual TCP sends are not completions. Timing
excludes fixture preparation; one smoke sample makes no throughput or
statistical performance claim.
The framework may also emit its advisory single-operation throughput-shape
warning: the recorded unit is one verified recovery cycle, not a throughput
sample. The driver never substitutes a made-up operation count to avoid it.

The report retains driver HEAD/dirty state, supplied runtime source SHA,
immutable container image identity, actual resource limits, catalog JSON,
object inventory, readiness durations, exact verification counts, broker logs
and Docker memory samples. Failures retain the stopped broker/cache and the
disposable provider's store for investigation. Successful broker containers
and cache volumes are removed. Local provider buckets are retained as evidence.

The catalog case qualifies catalog-heavy cold recovery and publication.
The separate `should_recover_gib_stream_history_after_complete_cache_loss`
workload writes 1 GiB of deterministic, incompressible Stream event payloads
through real TCP sessions and cloud-strict commits. Four concurrent writers
populate 16 streams with 2,048 events of 32 KiB each. Individual read pages
contain one event to fit the protocol's 16-bit TLV length. It checks readiness after
each commit and samples the provider catalog during growth.

Before requesting an orderly checkpoint, ordinary maintenance must retire all
WAL segments observed after the first commit, leave SST objects, and reduce
remote WAL payloads to at most 128 MiB within two minutes of the last write.
The catalog must remain at most 1 MiB. These are explicit workload acceptance
limits. Both an orderly restart and a SIGKILL followed by complete cache loss
must recover all 32,768 exact event bodies. The broker retains the same CPU,
RAM, swap and readiness limits as the catalog case. One 1,200-second deadline
covers writes, maintenance, both restarts and full readback. A successful
completion counts verified records after cold recovery, never sent writes.
See also [constrained runtime qualification](constrained-runtime.md).
AWS remains outside the workload: the driver accepts only a loopback provider
endpoint and supplies disposable provider credentials to every AWS CLI call.

## Run

Build the ordinary runtime image from the source being qualified, or use an
immutable published digest whose build run identifies that source. The driver
alone enables `recovery-qualification`. Run the provider outside the broker cap:

```sh
export FITZ_TEST_IMAGE=fitz-resource-local
export FITZ_RESOURCE_SOURCE_SHA="$(git rev-parse HEAD)"
export FITZ_S3_PROVIDER_IMAGE=ghcr.io/sqrzl/sqrzl-emulator@sha256:bb5bf54895fcb3b9730665b48e0f7b602791c61c3d18e5e7a060a4dce5c90964
export FITZ_S3_TEST_ENDPOINT=http://127.0.0.1:9000
export FITZ_S3_NETWORK=fitz-catalog-stress
export FITZ_S3_REPORT_DIR=target/fitz-stress/cloud-catalog
docker network create "$FITZ_S3_NETWORK"
docker run --detach --name fitz-catalog-provider \
  --network "$FITZ_S3_NETWORK" --network-alias fitz-s3-provider \
  --publish 127.0.0.1:9000:9000 --publish 127.0.0.1:9001:9001 \
  --env SQRZL_ACCESS_KEY_ID=admin --env SQRZL_SECRET_ACCESS_KEY=easy-peasy \
  --env SQRZL_BLOBS_PATH=/app/blobs "$FITZ_S3_PROVIDER_IMAGE"
curl --fail http://127.0.0.1:9001/healthz
cargo bench --locked --bench tier5_cloud_catalog \
  --features benchkit,stress-soak,recovery-qualification -- \
  --workload should_recover_after_s3_catalog_pressure
```

The target uses the existing Tier 5 single-sample smoke defaults. It is manually
invoked and is not added to hosted workflows by this change.

Run the live Stream case with the same environment:

```sh
cargo bench --locked --bench tier5_cloud_catalog \
  --features benchkit,stress-soak,recovery-qualification -- \
  --workload should_recover_gib_stream_history_after_complete_cache_loss
```

Set `FITZ_CATALOG_SEGMENTS` to `1024`, `8192` or `32768` to diagnose just that
stage. Other values fail configuration; a selected-stage run does not qualify
the full sweep or a larger catalog.

## Initial observation

The 2026-10-15 ARM64 run used the immutable image produced for Fitz
`f1f91b891617e4326a422979927e0a1d640c19bc` by
[Containers run 38781862377](https://github.com/cntryl/fitz/actions/runs/38781862377),
manifest digest `sha256:4a944b212cfa8a48b67a793443ed550d58cafe96a7e4a511032e622665f9d837`.
Midge is the published 0.4.0 crate.

The 1,024-segment control had a 327,439-byte catalog. It reached readiness in
6.85 seconds and recovered all 32 values plus the new strict commit after
complete cache loss; crash-to-ready was 86.67 seconds.

The 8,192-segment stage had a 2,656,995-byte catalog and 3,399,680 bytes of WAL.
It exited with status 1 before readiness, without an OOM kill:

```text
Resource limit: control upload provider workspace requires 10627980 bytes
with 47961078 of 53687091 bytes already reserved
[admission-contention pool=2 release=4901967]
```

This is the same catalog-publication allocation path as the incident, at a
smaller supported-format catalog. It establishes a failing constrained
consumer case, not byte-for-byte replay of the old prefix or proof that the
underlying long-run retention defect is unchanged. The first run stopped at
this failure; its 32,768-segment stage was not executed. Keep the workload red
until the production path satisfies the declared envelope.
