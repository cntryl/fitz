# Benchmark Guidelines

**Version:** 2.0
**Last Updated:** July 4, 2026
**Project:** Fitz Message Broker

Fitz benchmarks use one framework: `cntryl-stress`. Tier 1 through Tier N write
`cntryl-stress.v1` artifacts under `target/stress/`, and
`cntryl-tools summarize-benchmarks` turns those artifacts into
`target/bench_results.json` and `target/bench_summary.md`.

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

## Suite Split

Fitz uses the stress default profile for every documented and CI benchmark
command. The default is the trustworthy release profile from `cntryl-stress`.
Do not pass `--profile` in Fitz workflow or documentation commands.

- **Release suite:** 30-50 baseline-backed rows that cover customer-visible
  invariants: RPC request/response, queue enqueue/dequeue/ack, stream
  append/read/replay, notice publish/fanout, schedule create/claim/ack, and
  TCP/WS integration for each domain.
- **Deep suite:** Broader nightly/manual coverage for scaling curves,
  storage-model experiments, wildcard and route-depth variants, high-cardinality
  registration, and noisy rows. It uses the same default stress profile; it is
  deeper coverage, not a different profile.
- **Historical experiments:** One-off profiling benches that no longer answer
  an active regression, throughput, scaling, or risky-subsystem question.

The release suite is enumerated in
[`config/bench_release_ids.txt`](../../config/bench_release_ids.txt). Keep that
file small, baseline-backed, and reviewable.

## File Organization

| Tier | Kind | Tool | Location | Scope |
| --- | --- | --- | --- | --- |
| **Tier 1** | Hotpath | Stress micro | `benches/tier1_hotpath_*.rs` | Pure synchronous internals using `#[stress_test(tier = 1)]` and `ctx.measure_micro`. |
| **Tier 2** | Subsystem | Stress | `benches/tier2_subsystem_*.rs` | Component and domain subsystem rows with explicit correctness counters. |
| **Tier 3** | System | Stress | `benches/tier3_system_*.rs` | In-process domain actor + test engine, no network. |
| **Tier 4** | Integration | Stress | `benches/tier4_integration_*.rs` | Full stack direct/TCP/WebSocket/multiclient scenarios. |

Shared helper files:

- `benches/tier2_stress.rs`: small Tier 2 counter helpers around direct stress
  context calls.
- `benches/stress_config.rs`: shared correctness counter recording only.

The manifest sets `autobenches = false`; every runnable bench target must be
listed explicitly in `Cargo.toml`.

## Benchmark Structure

Stress benchmarks follow this shape:

```rust
use cntryl_stress::{black_box, stress_main, stress_test, StressContext};

#[stress_test(tier = 1, name = "decode_one_64b", max_allocs_per_op = 0, max_bytes_per_op = 0)]
fn should_decode_one_64b(ctx: &mut StressContext) {
    let frame = build_frame(64);
    let decoder = TlvDecoder::new();

    ctx.parameter("payload_size", 64);
    ctx.measure_micro(|| black_box(decoder.decode_one(black_box(&frame)).unwrap()));
}

#[stress_test(tier = 3, mode = "fixed_duration")]
fn should_complete_capacity_ack_roundtrip(ctx: &mut StressContext) {
    let mut actor = build_actor();

    ctx.parameter("scenario", "capacity_ack_roundtrip");
    let iterations = ctx.measure_workload(|| {
        complete_one_ack_roundtrip(&mut actor);
    });
    let _ = ctx.correctness().attempted(iterations).completed(iterations);
}

stress_main!();
```

Use the narrowest direct stress API that describes the row:

- `ctx.measure_micro`: calibrated Tier 1 micro samples.
- `ctx.measure_workload`: default-profile fixed-duration or fixed-operation
  workload samples.
- `ctx.measure`: one measured operation or batch.
- `ctx.measure_for`: explicit wall-clock loops only when the row requires a
  custom local duration independent of the profile.
- `ctx.record_duration`: externally timed systems where the benchmark body owns
  timing.

Do not write benchmark diagnostics with `println!`, `eprintln!`, or `dbg!`.
Use `ctx.parameter` for fields that define the workload identity and
`ctx.metadata` for descriptive facts that should appear in artifacts without
changing IDs. The terminal output should be the stress console report.

## Tier 1 Micro Semantics

Tier 1 rows use `#[stress_test(tier = 1)]`, which defaults to stress micro mode.
Use `ctx.measure_micro` exactly once per row.

Micro rows record calibrated net nanoseconds per operation. When the operation
should be allocation-free, install `cntryl_stress::stress_allocator!()` in the
bench binary and set `max_allocs_per_op = 0` and `max_bytes_per_op = 0`.
Do not add allocation budgets to rows where construction or allocation is the
behavior under review.

Use `cntryl_stress::black_box`, not `std::hint::black_box` directly in new
bench code.

## Stress Configuration

Fitz commands rely on the stress default profile. Do not pass `--profile` in
repo docs, CI, or release/deep command lists.

Common arguments:

| Argument | Meaning |
| --- | --- |
| `--workload <PATTERN>` | Run one workload name/module pattern. |
| `--tier <N>` | Run one stress tier. |
| `--samples <N>` | Local diagnostic override for measured sample count. |
| `--warmup-samples <N>` | Local diagnostic override for warmup sample count. |
| `--console <MODE>` | Local diagnostic output mode. |

Local `smoke` or `lab` profile experiments are framework diagnostics, not Fitz
workflow defaults. Keep such commands out of committed Fitz docs and CI.

## CI and Local Workflows

Run a targeted benchmark:

```bash
export FITZ_LOG_LEVEL=warn
export OTEL_ENABLED=false
cargo bench --quiet --bench tier1_hotpath_tlv -- --workload decode_one
cargo bench --quiet --bench tier2_subsystem_queue -- --workload ack_256_messages_primary
cargo bench --quiet --bench tier4_integration_rpc -- --workload should_complete_direct_request
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

Use `cargo bench --quiet` so Cargo build progress does not bury the stress
table. Keep the stress console mode at its default unless a local diagnostic run
needs `--console verbose`, `--console json`, or `--console markdown`.

Run the release suite locally by using the release command list in
[`.github/workflows/bench.yml`](../../.github/workflows/bench.yml), then
summarize:

```bash
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

Run the deep suite locally with the deep command list in the same workflow. The
deep suite is broader coverage and still uses the stress default profile.

Do not use compile-only benchmark preflights. They compile every bench target
without producing performance signal and hide which benchmark surface is under
review.

## Stress Benchmark Contract

Tier 3 and Tier 4 stress tests must follow the
[stress benchmark contract](stress-bench-contract.md): setup outside timed
sections, real actor/domain logic inside timed sections, explicit correctness
counters, and valid direct/TCP/WebSocket/multiclient semantics.

## Performance Targets

Numerical targets live in
[`config/perf_targets.json`](../../config/perf_targets.json) and are mirrored in
[Performance targets](bench-targets.md). Generated IDs come from current
`cntryl-stress.v1` artifacts and use the current tool format, for example:

```text
benchmark_id|metric|scenario=...|parameter=...
```

Do not hand-convert stale legacy IDs into current targets. Regenerate targets,
release IDs, `bench-targets.md`, and the baseline from clean current stress
artifacts.

## Baseline Refresh

Refresh `config/bench_baseline.json` only after the relevant report has:

- `critical == 0`
- release `missing == 0`
- no unreviewed untrustworthy release rows
- no legacy-adapter records

After copying `target/bench_results.json` to `config/bench_baseline.json`,
summarize again and require `new == 0`, `missing == 0`, and `critical == 0`.

## Reviewer Checklist

- The row measures one clear behavior.
- Setup is outside timing unless setup is part of the named behavior.
- Correctness counters match actual completed work.
- Tier 1 rows use `ctx.measure_micro`.
- Tier 2+ rows use direct stress context APIs.
- Commands omit `--profile`.
- Artifacts are current `cntryl-stress.v1`.
- Release rows are baseline-backed and stable.

## Document History

| Date | Version | Changes |
| --- | --- | --- |
| 2026-07-04 | 2.0 | Migrated all tiers to `cntryl-stress`; removed the previous adapter and Fitz profile-default helpers. |
| 2026-07-04 | 1.1 | Split benchmark workflows into release and deep suites. |
| 2025-10-20 | 1.0 | Initial version tailored for Fitz message broker. |
