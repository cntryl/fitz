# Benchmark Guidelines

**Version:** 2.3
**Last Updated:** July 7, 2026
**Project:** Fitz Message Broker

Fitz benchmarks use one framework: `cntryl-stress`. Tier 1 through Tier 6 write
`cntryl-stress.v2` artifacts under `target/stress/`, and
`cntryl-tools summarize-benchmarks` turns those artifacts into
`target/bench_results.json` and `target/bench_summary.md`.

The family-ownership migration's locked before/after results are recorded in
the [SOLID remediation benchmark report](solid-remediation-benchmark-report.md).
The multi-family Stream admin refresh characterization and its clean-family
fast-path results are in the
[Stream admin refresh report](stream-admin-refresh-characterization.md).

The actor-level Schedule due-burst/control measurements, raw artifact and model
limits are recorded in the [Schedule control latency characterization](schedule-control-latency-characterization.md).

## Philosophy

Benchmarks measure real broker performance across routes, domains, and
transports. Tests prove correctness; benchmarks quantify cost, scaling, and
regression risk.

Use benchmark rows for one of these questions:

- Did a core invariant regress?
- Did customer-visible throughput regress?
- Did an algorithmic scaling curve change?
- Did a risky subsystem get slower?

Do not benchmark fake work, setup-only loops, or getters that cannot inform an
optimization decision. Keep setup outside the timed section unless the row name
explicitly says construction/setup is part of the measured behavior.

For Stream D4, keep direct rows for hot-resource append at empty and 100,000
prior events, 15/16/17 KiB and maximum-valid-event payloads, sparse and dense
locator reads, TTL churn, repeated memtable rotation, and replay before/after
compaction. A benchmark must fail on overlap, corruption, duplicate, or
missing-record errors; it must not reset a local fixture to conceal an
overlapping append.

The D4 shape target records these focused measurements:
`ttl_churn_commit_and_maintain`, `memtable_rotation_append_4k`,
`sparse_locator_read`, `dense_locator_read`, `replay_before_compaction`, and
`replay_after_compaction`. They remain characterization rows until repeated
same-host runs are stable and baseline-backed.

## Suite Split

Fitz uses the stress default profile for documented and CI Tier 1 through Tier 4
commands. The default profile is their acceptance surface even when the
summary reports `authoritative: false`; do not switch docs or CI to a release
or lab profile just to force an authoritative flag. Do not pass `--profile` in
Tier 1 through Tier 4 workflow or documentation commands.

Tier 5 and Tier 6 own long active windows and deliberately execute one measured
invocation with `--profile smoke --samples 1 --warmup-samples 0 --cooldown-samples 0`.
The smoke profile permits correctness and liveness diagnostics with one sample;
the default quality gate rejects that sample count. This explicit exception
prevents framework repetitions from multiplying the owned duration and makes no
statistical performance claim. See [Tier 5 and Tier 6 benchmarks](tier5-tier6-benchmarks.md)
for duration controls, domain scope, verification, and partial artifacts.

- **Release suite:** 30-50 baseline-backed rows that cover customer-visible
  invariants: RPC request/response, queue enqueue/dequeue/ack, stream
  append/read/replay, notice publish/fanout, schedule create/claim/ack, and
  TCP/WS integration for each domain.
- **Deep suite:** Broader nightly/manual coverage for scaling curves,
  wildcard and route-depth variants, high-cardinality registration, and rows
  under active signal review. It uses the same default stress profile; it is
  deeper coverage, not a different profile.
- **Saturation and endurance diagnostics:** Tier 5/6 owned windows use the
  explicit smoke exception, real semantic checks, and useful-progress watchdogs.
  A full-duration pass does not establish statistical performance quality.
- **Historical experiments:** One-off profiling benches that no longer answer
  an active regression, throughput, scaling, or risky-subsystem question.

The release suite is enumerated in
[`config/bench_release_ids.txt`](../../config/bench_release_ids.txt). Keep that
file small, baseline-backed, and reviewable.

## External Comparison Labels

Use rows with matching completion semantics when comparing Fitz to NATS or
another broker.

- NATS Core pub/sub send-throughput comparisons should use Notice rows with
  `mode=fire_and_forget_unacked`. These rows measure publish send completion and
  drain subscribers after the timed section; they do not imply durable or
  guaranteed delivery.
- NATS sync request/reply comparisons should use RPC rows with
  `mode=sync_single_inflight`, `completion_mode=response_wait`, and
  `inflight_per_client=1`. These rows are RTT-bound.
- High-throughput request/reply comparisons should use RPC rows with
  `mode=async_pipelined` or `mode=concurrent_pipelined`, where the benchmark
  validates every response correlation ID before counting completions.

## File Organization

| Tier | Kind | Tool | Location | Scope |
| --- | --- | --- | --- | --- |
| **Tier 1** | Hotpath | Stress micro | `benches/tier1_hotpath_*.rs` | Pure synchronous internals using `#[stress(tier = 1)]` and one named measurement. |
| **Tier 2** | Subsystem | Stress | `benches/tier2_subsystem_*.rs` | Component and domain subsystem rows using stress fixed-operation samples and explicit correctness counters. |
| **Tier 3** | System | Stress | `benches/tier3_system_*.rs` | In-process domain actor + test engine, no network. |
| **Tier 4** | Integration | Stress | `benches/tier4_{domain}_{group}.rs` | Full-stack direct/TCP/WebSocket/multiclient scenarios, split by domain and workload group. |
| **Tier 5** | Saturation and scaling | Stress | `benches/tier5_saturation.rs` | Seven domains; concurrent 1/2/4/8/16/32/64-lane stages share one total active budget. |
| **Tier 6** | Endurance | Stress | `benches/tier6_endurance.rs` | Seven domains; eight lanes accumulate the full active batch-time budget after setup, excluding verification. |

Shared helper files:

- `benches/tier2_stress.rs`: small Tier 2 counter helpers around direct stress
  context calls.
- `benches/stress_config.rs`: shared correctness counter recording and a
  benchmark-only `measure_workload` adapter for existing Tier 3/4 rows.
- `benches/stress_support/`: shared runner, domain drivers, bounded measurements,
  and partial artifact helpers for Tier 5/6 owned windows.

Tier 4 executable targets use the stable `tier4_{domain}_{group}` naming
convention (for example, `tier4_kv_gate` and `tier4_rpc_pipeline`). Support
fixtures and measurement modules use the same prefix but are not registered as
standalone Cargo benchmarks.

The manifest sets `autobenches = false`; every runnable bench target must be
listed explicitly in `Cargo.toml`.

## Benchmark Structure

Stress benchmarks follow this shape:

```rust
use cntryl_stress::{black_box, stress_main, stress, StressContext};

#[stress(tier = 1, name = "decode_one_64b", max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_decode_one_64b(ctx: &mut StressContext) {
    let frame = build_frame(64);
    let decoder = TlvDecoder::new();

    ctx.parameter("payload_size", 64);
    ctx.measure("decode_one_64b", || black_box(decoder.decode_one(black_box(&frame)).unwrap()));
}

#[stress(tier = 3)]
fn should_complete_capacity_ack_roundtrip(ctx: &mut StressContext) {
    let mut actor = build_actor();

    ctx.parameter("scenario", "capacity_ack_roundtrip");
    let iterations = ctx.measure_batch("complete_capacity_ack_roundtrip", 1, || {
        complete_one_ack_roundtrip(&mut actor);
    });
    let _ = ctx.correctness().attempted(iterations).completed(iterations);
}

stress_main!();
```

Use the narrowest direct stress API that describes the row:

- `ctx.measure("name", ...)`: one named measurement using the tier-derived mode.
- `ctx.measure_batch("name", logical_ops, ...)`: repeated logical work where
  each framework iteration performs a known operation count.
- `ctx.record_external("name", duration, completed)`: externally timed systems
  where the benchmark body owns timing and completed-operation counting.
- `ctx.record_external_outcome("name", duration, logical_unit, outcome)`: owned
  Tier 5/6 windows with observed outcomes, including zero completed operations.
- `ctx.measure_io("name", ...)`, `ctx.measure_pipeline("name", ...)`, and
  `ctx.measure_async("name", ...)`: named measurements with a specific intent.

Do not write benchmark diagnostics with `println!`, `eprintln!`, or `dbg!`.
Use readable measurement names that describe the measured behavior. The name is
part of the artifact ID, so keep the current name unless the measured workload
or a workload-defining parameter changes. Use `ctx.parameter` for fields that
define the workload identity and
`ctx.metadata` for descriptive facts that should appear in artifacts without
changing IDs. The terminal output should be the stress console report.

## Tier 1 Micro Semantics

Tier 1 rows use `#[stress(tier = 1)]`, which defaults to stress micro mode.
Use one named measurement per row. Prefer `ctx.measure("readable_name", ...)`
for single-operation rows; use batched measurement only when the row explicitly
counts repeated logical work.

Micro rows record calibrated net nanoseconds per operation. When the operation
should be allocation-free, install `cntryl_stress::stress_allocator!()` in the
bench binary and set `max_allocs_per_op = 0` and `max_bytes_per_op = 0`.
Do not add allocation budgets to rows where construction or allocation is the
behavior under review.
Rows whose measured behavior is construction, parsing, or allocation may emit
`high_allocations` diagnostics. Those warnings are advisory for that class of
row; keep allocation statistics visible and do not hide them with
`record_external` only to silence the diagnostic.

Use `cntryl_stress::black_box`, not `std::hint::black_box` directly in new
bench code.

## Stress Configuration

Tier 1 through Tier 4 commands rely on the stress default profile and omit
`--profile`. Tier 5 and Tier 6 use the documented single-invocation smoke
exception. Stress derives mode from tier:
Tier 1 is `micro`, Tier 2 is `fixed_operations`, and Tiers 3+ are
`fixed_duration`. Omit `mode` on new rows unless compatibility with older
examples requires spelling it out, and never set a mode that conflicts with the
tier.

Common arguments:

| Argument | Meaning |
| --- | --- |
| `--workload <PATTERN>` | Run one workload name/module pattern. |
| `--tier <N>` | Run one stress tier. |
| `--samples <N>` | Local diagnostic override; committed Tier 5/6 owned windows use `1`. |
| `--warmup-samples <N>` | Local diagnostic override; committed Tier 5/6 owned windows use `0`. |
| `--cooldown-samples <N>` | Local diagnostic override; committed Tier 5/6 owned windows use `0`. |
| `--operations-per-sample <N>` | Local diagnostic override for Tier 2 fixed-operation sample size. |
| `--console <MODE>` | Local diagnostic output mode. |

Tier 1 through Tier 4 local `smoke` or `lab` profile experiments remain framework
diagnostics and are not committed workflow defaults. The Tier 5/6 smoke exception
accepts correctness and liveness evidence without claiming statistical quality.

## CI and Local Workflows

Benchmark workflows are scheduled or manually dispatched and are not a pull-request performance gate.

The [Tier 5 workflow](../../.github/workflows/bench-tier5.yml) runs weekly and the
[Tier 6 workflow](../../.github/workflows/bench-tier6.yml) runs monthly, each with
seven independent domain jobs. Both require `benchkit,stress-soak`, preserve
failure logs and partial JSON alongside framework artifacts, and use the
[owned-window invocation contract](tier5-tier6-benchmarks.md).
They explicitly use `FITZ_STRESS_STORAGE_PROFILE=local_disk` for continuous
campaigns. The opt-in `memory` profile is a resource-pressure diagnostic whose
retained physical write history can exhaust the store despite bounded logical
workload state.

Run a targeted benchmark:

The opt-in `queue_drain_latency` target seeds a fresh Fast local-disk Queue,
then records separate validated client RESERVE and ACK round trips and the
actual duration of each requested 5ms pause. `FITZ_QUEUE_DRAIN_PAIRS` selects
1–1,000 messages (default 1,000); startup, seeding, draining, and empty-state
verification share one 90-second deadline. Shutdown has a separate bounded
cleanup window. This is a timing diagnostic on the current host, not a capacity
or throughput baseline, and it does not reproduce a 100,000-message backlog.
Its JSON under `target/fitz-stress/queue-drain-latency` records source and dirty
state, actual accepted/verified work, bounded per-pair timings, Queue histogram
snapshots, and cleanup outcome. Failure preserves the store and partial report;
no crash-recovery or ownership-continuity guarantee is inferred.
The failure report retains the current reserved identity and ACK state:
dispatch started, frame sent, terminal received, validated success, or validated
rejection. ACK counts require validated success; timing samples additionally
require the pause to finish. Cancellation during that pause records
`acknowledged_pause_pending_sample`. An unresolved dispatched ACK is never retried.

```bash
FITZ_QUEUE_DRAIN_PAIRS=1000 cargo bench --locked --bench queue_drain_latency \
  --features benchkit,stress-soak
```

```bash
export FITZ_LOG_LEVEL=off
export OTEL_ENABLED=false
cargo bench --quiet --bench tier1_hotpath_tlv -- --workload decode_one
cargo bench --quiet --bench tier2_subsystem_queue -- --workload ack_256_messages_primary
cargo bench --quiet --bench tier4_rpc_roundtrip -- --workload should_measure_tcp_request_response_roundtrip
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

Use `cargo bench --quiet` so Cargo build progress does not bury the stress
table. Keep the stress console mode at its default unless a local diagnostic run
needs `--console verbose`, `--console json`, or `--console markdown`.
Tier 4 benchmark binaries silence Fitz observability by default; set
`FITZ_BENCH_ALLOW_LOGS=true` only when you need transport or startup logs while
debugging a benchmark harness issue.

Run the hosted suite locally by using the command list in
[`.github/workflows/bench.yml`](../../.github/workflows/bench.yml), then summarize:

```bash
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

Do not use compile-only benchmark preflights. They compile every bench target
without producing performance signal and hide which benchmark surface is under
review.

## Stress Benchmark Contract

Tier 3 and Tier 4 stress tests must follow the
[stress benchmark contract](stress-bench-contract.md): setup outside timed
sections, real actor/domain logic inside timed sections, explicit correctness
counters, and valid direct/TCP/WebSocket/multiclient semantics.
Tier 5 and Tier 6 follow its owned-window rules and the
[domain-specific saturation and endurance scope](tier5-tier6-benchmarks.md).

## Performance Targets

Numerical targets live in
[`config/perf_targets.json`](../../config/perf_targets.json) and are mirrored in
[Performance targets](bench-targets.md). Generated IDs come from current
`cntryl-stress.v2` artifacts and use the current tool format, for example:

```text
benchmark_id|metric|scenario=...|parameter=...
```

Do not hand-convert stale legacy IDs into current targets. Regenerate targets,
release IDs, `bench-targets.md`, and the baseline from clean current stress
artifacts only.
Stress v2 benchmark IDs include the named measurement suffix exactly as the
bench records it, such as `/owning_from_route_struct_payload` or
`/complete_capacity_ack_roundtrip`. Do not churn readable names into generic
`/operation` or `/workload` suffixes unless the workload itself has changed.

## Baseline Refresh

Before a full validation or baseline refresh, remove ignored benchmark artifacts
so stale partial `latest.json` files cannot mix with the current run:

```bash
rm -rf target/stress target/bench_results.json target/bench_summary.md
```

Refresh `config/bench_baseline.json` only after a fresh full default run and the
relevant report has:

- `critical == 0`
- release `missing == 0`
- no unreviewed untrustworthy release rows
- no legacy-adapter records
- no noisy or untrustworthy release rows

The release manifest uses memory-backed TCP and WebSocket lifecycle rows for KV
and Stream so the regression signal covers the wire and domain lifecycle without
folding host filesystem scheduling into every sample. The corresponding
local-disk sync rows remain in the deep results with
`target_class = storage_characterization`; review them for storage regressions,
but do not use them to accept or reject the release suite while their WAL flush
tail is host-sensitive.

For release runs, run the artifact-backed Rust test after summarizing:

```bash
cargo test --test release_benchmark_results should_accept_current_release_benchmark_results -- --ignored --exact
```

It rejects stale `tierr-*` / `tier-*` Tier 4 artifacts and requires the 14
manifest primary throughput rows to have passing correctness and `acceptable`
(or better) quality. Paired latency records remain report-only.

After copying `target/bench_results.json` to `config/bench_baseline.json`,
summarize again and require `new == 0`, `missing == 0`, and `critical == 0`.
Never refresh the baseline from a targeted benchmark run or a partial
`target/stress/**/latest.json` artifact.

## Reviewer Checklist

- The row measures one clear behavior.
- Setup is outside timing unless setup is part of the named behavior.
- Correctness counters match actual completed work.
- Tier 1 rows use one named measurement.
- Tier 2 rows omit `mode = "fixed_duration"` and use fixed-operation timing.
- Tier 2+ rows use direct stress context APIs.
- Tier 1 through Tier 4 commands omit `--profile`; Tier 5/6 owned windows use the
  explicit smoke exception with one sample and no warmup or cooldown.
- Artifacts are current `cntryl-stress.v2`.
- Release rows are baseline-backed and stable.

## Document History

| Date | Version | Changes |
| --- | --- | --- |
| 2026-07-07 | 2.3 | Added RPC/Notice comparison labels for sync, pipelined, delivery-confirmed, and unacked benchmark rows. |
| 2026-07-06 | 2.2 | Clarified default-profile acceptance, readable measurement IDs, partial-artifact hazards, and allocation diagnostics. |
| 2026-07-05 | 2.1 | Updated benches and docs for `cntryl-stress` v2 named measurements and schema. |
| 2026-07-04 | 2.0 | Migrated all tiers to `cntryl-stress`; removed the previous adapter and Fitz profile-default helpers. |
| 2026-07-04 | 1.1 | Split benchmark workflows into release and deep suites. |
| 2025-10-20 | 1.0 | Initial version tailored for Fitz message broker. |
