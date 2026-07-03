# Performance Loop

Use direct test and benchmark commands for local optimization work. Keep the same command set before and after the code change so the comparison is meaningful.

## Baseline

Run the correctness checks first:

```bash
cargo test --workspace
cargo test test_guidelines_compliance
cntryl-tools validate-tests
```

Run the benchmark tier or target that covers the suspected hot path:

```bash
export FITZ_LOG_LEVEL=warn
export OTEL_ENABLED=false
cargo bench --no-run
cargo bench --bench tier3_system_rpc -- --runs 5 --warmup 1
cntryl-tools summarize-benchmarks --product-name Fitz --report-title "Fitz Benchmark Report"
```

For a full tier refresh, use the commands in [Benchmark Guidelines](benchmarks.md#stress-configuration-tier-3-and-4).

## Optimize

Make one focused change, then rerun the same correctness checks and benchmark command. Compare the regenerated `target/bench_summary.md` and `target/bench_results.json` with the baseline output you captured before the change.

## Selection Rules

Use [config/perf_targets.json](../../config/perf_targets.json) and [Performance targets](bench-targets.md) to choose optimization candidates. Prefer the scenario furthest over its operational target inside the relevant bucket, then use stretch-target distance and current `mean_us` to break ties.
