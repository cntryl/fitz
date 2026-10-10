# Performance Loop

Use direct test and benchmark commands for local optimization work. Keep the same command set before and after the code change so the comparison is meaningful.

## Baseline

Run the correctness checks first:

```bash
cargo fmt --all -- --check
cargo test --workspace
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings -D clippy::pedantic
```

Run the benchmark tier or target that covers the suspected hot path. Prefer a release-suite row from `config/bench_release_ids.txt` when it covers the behavior; use the deep suite for scaling curves, wildcard sweeps, high-cardinality registration, or rows under active signal review:

```bash
export FITZ_LOG_LEVEL=off
export OTEL_ENABLED=false
cargo bench --quiet --bench tier3_system_rpc -- --workload should_complete_single_response_throughput
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

For a full tier refresh, use the release or deep command lists in [Benchmark Guidelines](benchmarks.md). Do not use compile-only benchmark preflights; they compile every target without producing performance signal.

## Optimize

Make one focused change, then rerun the same correctness checks and benchmark command. Compare the regenerated `target/bench_summary.md` and `target/bench_results.json` with the baseline output you captured before the change.

For Stream storage changes, compare three-run medians on the same host. Existing
regression-gate throughput must remain at least 90% of baseline with p95 at most
110%; the maximum-valid-event write row must retain 95% throughput with p95 at
most 105%; and hot-resource append with 100,000 prior events must remain within
10% of an empty resource.

The Stream performance workflow isolates each of the eight transport workloads
in its own process capture. Memory replay therefore never follows disk writes
within a capture. Timed comparisons require all 32 workloads, fixtures, and the default sampling profile
remain required. Each workload has three alternating baseline/candidate pairs.
The maximum-event write and WebSocket exact-replay workloads also have three
alternating same-binary control pairs; both throughput and p95 ratios must lie
within 0.95–1.05. An unstable control labels the entire run
`measurement_unstable`, regardless of observed budget results.

Compilation and binary/dependency/fixture audits finish before timing. One full
qualification and one predeclared full confirmation use the same archived
executables on the same host. Both must pass every original budget and both
controls. Preserve raw quality labels, semantic checks, source SHAs, executable
hashes, locked resolution, and untimed CPU/I/O snapshots. Retain failures and
leave qualification open when controls remain unstable; additional exploratory
runs do not replace the declared pair.

Dependency comparisons with unchanged benchmark fixtures retain the baseline's
original Cargo manifest and lockfile. Both libraries and every benchmark must
have fresh compiler artifacts from the expected source paths. If the compiler's
release source inputs match and all benchmark executables are byte-identical,
the workflow records `binary_equivalent` with provenance and takes no timing
samples. This records artifact equivalence, not a new measured performance
result. Any different executable requires both full timing runs and all existing
budgets and controls. Reused binaries for changed compiled source remain an error.
Pair individual disk-size rows directly across source versions. The Stream
qualification workflow also requires an unchanged-binary maximum-event control
to stay within 5% for throughput and p95; retain failed comparisons.
Flush pending measurement-host I/O before each trial and record preparation
and I/O pressure separately from unchanged benchmark samples.

## Selection Rules

Use [config/perf_targets.json](../../config/perf_targets.json) and [Performance targets](bench-targets.md) to choose optimization candidates. Prefer the scenario furthest over its operational target inside the relevant bucket, then use stretch-target distance and current `mean_us` to break ties.

Rows outside the release suite can still justify product work when they show a hard miss, but keep that slice scoped to one row and promote it into release gating only after the row is stable and baseline-backed.

Tier 4 benchmark binaries silence Fitz observability by default so the stress console stays readable. Set `FITZ_BENCH_ALLOW_LOGS=true` only when you need transport or startup logs during harness debugging.
