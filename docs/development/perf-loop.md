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
export FITZ_LOG_LEVEL=warn
export OTEL_ENABLED=false
cargo bench --quiet --bench tier3_system_rpc -- --workload should_complete_single_response_throughput
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

For a full tier refresh, use the release or deep command lists in [Benchmark Guidelines](benchmarks.md#ci-and-local-workflows). Do not use compile-only benchmark preflights; they compile every target without producing performance signal.

## Optimize

Make one focused change, then rerun the same correctness checks and benchmark command. Compare the regenerated `target/bench_summary.md` and `target/bench_results.json` with the baseline output you captured before the change.

## Selection Rules

Use [config/perf_targets.json](../../config/perf_targets.json) and [Performance targets](bench-targets.md) to choose optimization candidates. Prefer the scenario furthest over its operational target inside the relevant bucket, then use stretch-target distance and current `mean_us` to break ties.

Rows outside the release suite can still justify product work when they show a hard miss, but keep that slice scoped to one row and promote it into release gating only after the row is stable and baseline-backed.
