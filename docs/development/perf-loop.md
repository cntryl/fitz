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

The Tier 4 workflow runs the registered Stream workloads directly through
`cntryl-stress`. Native artifacts retain sampling, correctness, quality, and
environment data. Use the framework's baseline comparison and
`cntryl-tools summarize-benchmarks` for performance analysis; do not launch
comparison campaigns from integration tests or repository-owned scripts.

The six memory transport workloads declare native `max_regression_pct = 10`
budgets; maximum-event disk writes declare `5`. With a compatible raw native
baseline, `cntryl-stress` applies these budgets to throughput and the paired
p95 latency measurement. Sampling stays at the framework default. No baseline
means these relative budgets are unevaluated, and statistically inconclusive
comparisons do not establish a regression. These guards use the framework's
environment, trust, quality, and confidence checks; the committed summary
baseline is not a raw native baseline. Keep accepted raw baselines separate
from current output artifacts and supply them with the native `--baseline`
option. Paired latency rows remain report-only in the configured release
summary; explicit native budgets also apply when doing this raw comparison.

The former bespoke Stream comparison workflow is retired. Ordinary tier runs
and the two configured Stream release rows do not enforce its complete
same-host qualification policy. The existing blocked qualification remains
blocked; a scheduled benchmark pass does not satisfy it or refresh the baseline.

That outstanding qualification requires all 32 workloads, identical fixtures,
the default sampling profile, and three alternating baseline/candidate pairs
per workload on a stable Linux host. Maximum-event writes and WebSocket exact
replay also require three alternating unchanged-binary control pairs with
throughput and p95 ratios within 0.95–1.05. Unstable controls invalidate the
performance conclusion regardless of observed budget results.

One full qualification and one predeclared full confirmation must use the same
archived executables; both must pass the budgets above and both controls.
Finish compilation and fixture/dependency audits before timing. Preserve source
SHAs, locked dependency resolution, executable hashes, raw quality labels,
semantic checks, failed captures, and untimed host CPU/I/O observations.
Dependency comparisons must retain the baseline's original manifest and lockfile.
Byte-identical outputs alone do not qualify a source change. A performance
host and shared-tool support for this policy are still required; no new timing
qualification or artifact-equivalence verdict is claimed by this cleanup.

For Fitz 0.2.0, the maintainer accepted local Apple Silicon Mac verification on
2026-10-16 instead of requiring a separate Linux host. This changes the accepted
host for that release only. The workload set, sampling, alternating pairs,
controls, confirmation and performance budgets above remain unchanged.

## Selection Rules

Use [config/perf_targets.json](../../config/perf_targets.json) and [Performance targets](bench-targets.md) to choose optimization candidates. Prefer the scenario furthest over its operational target inside the relevant bucket, then use stretch-target distance and current `mean_us` to break ties.

Rows outside the release suite can still justify product work when they show a hard miss, but keep that slice scoped to one row and promote it into release gating only after the row is stable and baseline-backed.

Tier 4 benchmark binaries silence Fitz observability by default so the stress console stays readable. Set `FITZ_BENCH_ALLOW_LOGS=true` only when you need transport or startup logs during harness debugging.
