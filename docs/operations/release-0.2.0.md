# Fitz 0.2.0 qualification record

Status: qualified candidate; the maintainer signed off 0.2.0 as stable on
2026-10-16. Publication and deployment remain separate operations.
The release blocker is [#416](https://github.com/cntryl/fitz/issues/416).

## Storage and recovery

The candidate uses the published crates.io Midge 0.4.1 package. Its lockfile
checksum is `f2ef64491bd05e19d8f8c580470422ce25f64e8b599b126ac597eb2a48955f32`.
Broker runtime source `af73e02d472bb12ff3b4bca9900aeb20a1ac3694` is tree-identical
to the source qualified before merging #486.

Native ARM64 qualification passed at 0.25 CPU, 512 MiB RAM and no extra swap:

- All 32,768 Sync Stream events, exactly 1 GiB of payload, survived both cold
  and orderly restart with exact body readback. Cold readiness took 115.86
  seconds, including the crashed writer's lease; orderly readiness took 14.53
  seconds. Automatic WAL retirement passed before any forced checkpoint.
- Catalog pressure passed at 1,024, 8,192 and 32,768 entries. The largest
  serialized catalog was 10,758,406 bytes, exceeding the incident's inferred
  size. Cold readiness remained below 180 seconds in all three stages.
- All five S3 recovery and retirement cases passed without skips, including
  674,005,451 bytes of historical WAL and complete local-cache loss.

These are bounded correctness and survival results against a pinned local
Sqrzl provider. They do not establish production AWS latency, statistical
performance, or a universal database capacity limit. On 2026-10-16 the maintainer
accepted local verification on the Apple Silicon Mac for this release, replacing
the native AMD64 host requirement. No AMD64 runtime qualification is claimed.
Workflow changes were explicitly declined. See the
[catalog report](../development/cloud-catalog-pressure.md) and
[constrained runtime procedure](../development/constrained-runtime.md).

## Client compatibility

All five current SDK main branches passed their explicitly dispatched Spec
workflows against the candidate `develop` broker image on 2026-10-16:

| SDK | Validated source | Candidate compatibility run |
| --- | --- | --- |
| .NET | `d52cb2ee54cad8b90095f64b42789ffea34a439a` | [38936654701](https://github.com/cntryl/fitz-dotnet/actions/runs/38936654701) |
| TypeScript | `3a27c37cd01fc69f344c0c5e5167efd24e607dfc` | [38936658858](https://github.com/cntryl/fitz-ts/actions/runs/38936658858) |
| Go | `b2dade7aa45ab6847f478acfd61c601d4fcd5550` | [38936663174](https://github.com/cntryl/fitz-go/actions/runs/38936663174) |
| Python | `4046dcfdba3844ba290ddd40111902bf337f00ef` | [38936667531](https://github.com/cntryl/fitz-py/actions/runs/38936667531) |
| Rust | `3458c45805649604ad07bcafb8e6d9f99fd411fc` | [38936671961](https://github.com/cntryl/fitz-rs/actions/runs/38936671961) |

These source runs do not qualify older published packages. Registry readback
found TypeScript 0.0.23 and GitHub NuGet 1.4.2 as the latest published versions.
The TypeScript package still emits the old KV COMMIT payload, so it must not
be used with the candidate's explicit commit-time persistence contract. A
deployment must build or obtain the matching client revision and follow the
coordinated wire upgrade in the [migration guide](migration-guide.md#breaking-change-explicit-kv-and-stream-commit-persistence).
SDK package publication and downstream Portia qualification are separate from
this broker release; they are not claimed by these source compatibility runs.
The Stream error-envelope generation introduced by #238 is unchanged in 0.2.0.

## Migration rehearsal

The opt-in Rust integration test
`migration::should_preserve_rollback_store_during_logical_migration` exercises
the immutable 0.1.0 image
`ghcr.io/cntryl/fitz@sha256:0686055f39f349aeec5e992e2ac19c4b02b585edd8dd7d5f7482eaba8c7e4804`
and an explicitly supplied candidate production image against disposable local
S3 storage. It exports known binary KV records and ordered Stream bodies and
metadata through the old broker, preserves every original cloud object with
SHA-256 hashes, and imports/replays into a different empty bucket and cache.
It checks orderly restart, SIGKILL with complete cache loss, byte-for-byte
preservation of the original cloud objects during import, and exact original
data through the old broker after rollback. No engine opens another version's
store.

The fixture is 16 non-expiring KV records and eight Stream events. Stream replay
creates new timestamps. This test does not preserve original TTL deadlines,
prove arbitrary key discovery, migrate Queue/Schedule state, or reconcile
application writes after export. An operator must decide the migration scope,
preserve expiration metadata and reconcile application state before cutover.
Do not reset a deployment merely because this representative fixture passes.
Follow the [migration guide](migration-guide.md#midge-040).

With the disposable provider configured as described in the constrained runtime
procedure, run the rehearsal against the supplied immutable candidate image:

```sh
cargo test --locked --features recovery-qualification --test constrained_runtime \
  migration::should_preserve_rollback_store_during_logical_migration \
  -- --ignored --exact --nocapture
```

Both broker images use the same 0.25 CPU / 512 MiB / no-extra-swap limits. The
driver writes logical exports, cloud object copies and hash inventories alongside
container identities, limits and restart timings under `FITZ_S3_REPORT_DIR`.

## Local performance qualification

The full local qualification against the original Midge 0.4.0 baseline
`f1f91b891617e4326a422979927e0a1d640c19bc` passed all 32 workloads, all eight
budgets and both unchanged-binary controls. The predeclared full confirmation
passed both controls and seven budgets, but missed WebSocket append p95:
72.31 microseconds versus 63.69 microseconds, ratio 1.1354 against the 1.10 limit.
Throughput retained 95.996% of baseline. Candidate latency captures were marked
noisy; a Teams renderer reached 175% CPU in the affected capture's host snapshot.
That observation does not prove the cause or waive the missed budget.

Both runs used the same archived executables, default sampling, three
alternating pairs per workload, identical fixture parameters and original
baseline manifest/lockfile. All native captures exited successfully, exact
correctness passed, and no compiler activity was observed in host snapshots.
The candidate source was `2c95b7a04dd9fe3885458c17742472441f449c25`.
Raw artifacts, executable hashes, plans, host snapshots and comparisons are
preserved in `/tmp/fitz-020-performance-quiet/` on the qualification Mac.

Two earlier full runs in `/tmp/fitz-020-performance/` were invalidated by
compiler interference. They also used an optional native baseline comparison
that rejected diagnostic-trusted latency baselines; this setup error was removed
for the recorded runs above. Original raw-median budgets were unchanged, and
failed captures were preserved. No baseline was refreshed. The passed
qualification alone does not satisfy the two-run publication policy.

On 2026-10-16 the maintainer accepted 0.2.0 as stable and deferred further work
to 0.2.1 and later. This is release-specific acceptance of the recorded noisy
WebSocket append p95 confirmation miss, not a passing confirmation verdict or
a change to future performance budgets. The production recovery and migration
qualification passed. No further timing campaign is required for 0.2.0.

Standard/default hosted Tier 4 run
[38936485262](https://github.com/cntryl/fitz/actions/runs/38936485262) passed all
24 jobs. All 14 primary release rows passed correctness with acceptable quality,
and the artifact-backed Rust release-row acceptance test passed. This evidence
does not replace the outstanding full confirmation requirement.

## Remaining publication gates

- Exact-head promotion checks, merge `develop` into `main` with a merge commit,
  successful main checks, then the existing Publish workflow and immutable
  multi-architecture manifest/source-tag readback.

AWS access and deployment are outside this qualification. Publication must not
be reported as deployment or as validation of an existing 0.1.0 prefix.
Deployment still requires application migration-scope review and the documented
rollback/reconciliation plan; representative broker rehearsal does not migrate
an application's live data.
