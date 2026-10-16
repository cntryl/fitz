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
During active writes the catalog must remain below the incident's 8,702,002
serialized bytes; after maintenance catches up it must be at most 1 MiB.
These are explicit workload acceptance limits. Both an orderly restart and a SIGKILL followed by complete cache loss
must recover all 32,768 exact event bodies. SIGKILL and cache loss happen first,
before any driver-requested checkpoint can rescue unflushed data. The orderly
restart follows the complete cold readback. The broker retains the same CPU,
RAM, swap and readiness limits as the catalog case. One 3,600-second deadline
covers writes, maintenance, both restarts and full readback. This window bounds
the complete load campaign, not a throughput SLA; readiness remains 180 seconds
and post-load maintenance remains two minutes. A successful
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

## Development fix qualification

The follow-up native ARM64 sweep used Fitz runtime source `f1f91b89` with
Midge fix `2bafa60cde9814cc62656c1178e0af37007d3148` from
[Midge PR 887](https://github.com/cntryl/midge/pull/887). Its immutable local
image is `sha256:2eec03ebe8227fd6eb93020f2554fee01c58900f639e0b21eecf4adac5126663`.
This image uses a development path override; it is diagnostic evidence, not
qualification of a published dependency or of native AMD64.

| Catalog entries | Serialized bytes | Initial readiness | Crash-to-ready |
| --- | --- | --- | --- |
| 1,024 | 327,588 | 3.55 s | 88.51 s |
| 8,192 | 2,657,541 | 14.62 s | 89.66 s |
| 32,768 | 10,759,502 | 43.28 s | 128.01 s |

All three stages passed exact readback of 32 historical keys and the new
cloud-strict marker after complete cache loss, at the unchanged resource cap.
Reports and logs are retained under
`target/fitz-stress/cloud-catalog-publication/`.

An earlier candidate passed the first two stages but failed startup publication
at 32,768 entries because append's temporary reallocation workspace remained
charged during the following upload. A second active Midge regression reproduced
that exact failure; the qualified revision releases temporary append workspace
before encoding/upload while retaining the grown catalog's allocation charge.
The live 1 GiB Stream workload remains a separate acceptance gate.

The first live run stopped after 25,376 acknowledged events (793 MiB) because
its driver incorrectly imposed the post-load 1 MiB catalog target during active
writes. The catalog reached 1,050,260 bytes while the broker remained healthy;
this was not an admission failure or a startup reproduction. That failed report
is preserved under `target/fitz-stress/stream-gib/`. The corrected workload uses
the incident-sized guard during growth and retains the strict post-load target.

The corrected cold-first run acknowledged all 32,768 events (1 GiB) and passed
automatic retirement before any forced checkpoint: the serialized catalog was
347,556 bytes and remote WAL was 108,705,818 bytes. It then failed the unchanged
180-second crash-to-ready limit after complete cache removal. Neither cold nor
orderly full-history readback completed, so this run does not qualify recovery.
Evidence is retained under
`target/fitz-stress/stream-gib-cold-first/stream-gib-a5e3178b-dfc8-4eb0-a5ce-ae68ae70779c/`.

The successor remained running without an OOM. Midge reported storage ready
about 88 seconds after launch; subsequent Fitz domain initialization consumed
the remaining window, including a Stream admin snapshot timeout. These logs
do not establish another catalog-admission failure or its root cause.

A separate active Midge regression found that repeated publication across the
32,768-entry vector-capacity boundary still exceeds upload admission after
PR 887. Codex completed Claude's follow-up
[Midge PR 888](https://github.com/cntryl/midge/pull/888), merged into develop as
`5305a5a178c8db6c82fa7a8b89bc5a2201cb1bfb`. Adversarial review also reproduced
and fixed cache eviction on temporary decode admission refusal and uncharged
nested retirement evidence. Active native-provider regressions cover repeated
publication across the capacity boundary, startup salvage at 32,768 and 32,769
entries, and retirement refusal followed by safe retry when pressure clears.
Final consumer qualification requires the fixed published crate and a rebuilt
Fitz image. The cold-start acceptance gate above remains open.

Fitz now resolves Midge's `develop` branch through `Cargo.lock` to that exact
`5305a5a` commit. The ordinary production Dockerfile builds without a path
override. A diagnostic cold start against a copy of the preserved 1 GiB store
opened Midge successfully but failed Schedule's 120-second preload deadline.
This copied store had an expired lease and prior background compaction, so it
does not substitute for the complete live-write/crash/cache-loss campaign.

The diagnostic exposed a Fitz domain-scan bug: Stream's family-scoped payload
blobs use an empty internal realm, but the foreign-domain skip helper treated
every binary-leading key as legacy data. Queue and Schedule consequently
traversed Stream payload history. The regression
`should_skip_foreign_family_scoped_stream_payloads` fails before the fix and
passes after recognizing the leading separator as an empty realm. Companion
tests retain owned Stream payloads and unknown/legacy rows. The fix changes no
readiness deadline or resource cap. The resulting image was qualified below.

## Locked develop qualification

The native ARM64 production image built from Fitz
`1cfe2cc865d62d294b1822a454cef54af419564c` locks Midge develop to
`5305a5a178c8db6c82fa7a8b89bc5a2201cb1bfb`, without a path override. Its
immutable identity is
`sha256:6cde0c911703c8161dc0219e9cfc1601cd897e41c32b29e9f46b46550aaa199f`.
It includes the empty-realm foreign-domain scan fix described above.

| Catalog entries | Serialized bytes | Initial readiness | Crash-to-ready |
| --- | --- | --- | --- |
| 1,024 | 327,408 | 5.75 s | 88.82 s |
| 8,192 | 2,656,455 | 16.71 s | 85.40 s |
| 32,768 | 10,757,128 | 46.60 s | 122.05 s |

All three stages passed exact historical readback and recovery of the new
strict commit after SIGKILL and complete cache removal. The independent live
Stream campaign also passed: all 32,768 acknowledged 32 KiB events, totaling
1,073,741,824 bytes, were verified exactly after both restarts. Cold
crash-to-ready was 101.15 seconds; orderly readiness was 16.42 seconds. Before
any forced checkpoint, automatic retirement reached a 274,791-byte catalog,
77,263,009 remote WAL bytes and 245 SST objects within the two-minute limit.
The full live campaign completed in 1,294.64 seconds.

All five existing S3 qualification tests also passed against this image:
supported-format fixture recovery; empty-cache recovery of 674,005,451 WAL
bytes with exact KV readback; warm and cold recovery/drain of 4,096 persisted
16 KiB Queue messages; and retirement of 67,400,735 historical WAL bytes during
16,384 enqueue/ACK operations (256 MiB of Queue payload). The churn case
verified all 4,096 historical KV values, finished with three remote WAL objects
totaling 1,094 bytes, and remained ready across 158 health samples.

Docker inspections confirm 0.25 CPU, 512 MiB RAM, no extra swap, zero restarts
under load and no OOM kills. Reports are retained under
`target/fitz-stress/midge-develop-catalog/`,
`target/fitz-stress/midge-develop-stream-gib/` and
`target/fitz-stress/midge-develop-s3/`. These are bounded smoke qualification
results, not throughput or statistical performance claims. They establish the
locked develop code's behavior on native ARM64; the fixed published dependency,
final release image, native AMD64 and migration gates remain separate.
