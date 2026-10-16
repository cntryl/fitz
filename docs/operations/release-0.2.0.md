# Fitz 0.2.0 qualification record

Status: candidate; stable publication and deployment have not occurred.
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

| SDK | Candidate compatibility run |
| --- | --- |
| .NET | [38936654701](https://github.com/cntryl/fitz-dotnet/actions/runs/38936654701) |
| TypeScript | [38936658858](https://github.com/cntryl/fitz-ts/actions/runs/38936658858) |
| Go | [38936663174](https://github.com/cntryl/fitz-go/actions/runs/38936663174) |
| Python | [38936667531](https://github.com/cntryl/fitz-py/actions/runs/38936667531) |
| Rust | [38936671961](https://github.com/cntryl/fitz-rs/actions/runs/38936671961) |

These source runs do not qualify older published packages. Registry readback
found TypeScript 0.0.23 and GitHub NuGet 1.4.2 as the latest published versions.
The TypeScript package still emits the old KV COMMIT payload, so it must not
be used with the candidate's explicit commit-time persistence contract.
Record and qualify the matching published SDK artifacts before broker publication;
also repeat the downstream Portia assertions required by the
[release checklist](release-checklist.md).

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

## Remaining publication gates

- Benchmark release rows and the required same-host Stream performance
  qualification, including unchanged-binary controls and the confirmation run.
  A standard Tier 4 workflow alone does not satisfy the full policy in
  [Performance Loop](../development/perf-loop.md).
- Matching SDK publication and exact published-consumer qualification.
- Review of application migration scope and rollback/reconciliation instructions.
- Exact-head promotion checks, merge `develop` into `main` with a merge commit,
  successful main checks, then the existing Publish workflow and immutable
  multi-architecture manifest/source-tag readback.

AWS access and deployment are outside this qualification. Publication must not
be reported as deployment or as validation of an existing 0.1.0 prefix.
