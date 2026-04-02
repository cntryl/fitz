/// Stress configuration for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// **Bench defaults**
/// - `BENCH_RUNS`: Number of measurement runs per stress test (default: 5). Use lower (e.g. 3) for CI.
/// - `BENCH_WARMUP`: Number of warmup runs before measurement (default: 1).
///
/// The bench binaries themselves consume these settings via `cargo bench -- --runs ... --warmup ...`.
///
/// **set_elements(N)** in each `#[stress_test]`: N is the explicit batch size for one timed
/// iteration inside `ctx.measure_for(Duration::from_secs(5), || { ... })`. The target measured
/// run is 5 seconds, with 3 seconds as the minimum acceptable floor, and N must match the
/// logical number of meaningful operations performed in that batch so throughput
/// (batch_size / elapsed_time) reported by `cargo fitz-tools benchmark-summary` is interpretable.
///
/// If a scenario has a natural transport or fanout scope, add tags like `measurement_scope`
/// and `batch_size` so the report can distinguish direct, transport, and delivery cost.
pub struct BenchConfig {
    pub runs: usize,
    pub warmup: usize,
}

impl Default for BenchConfig {
    fn default() -> Self {
        let runs = std::env::var("BENCH_RUNS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(5);

        let warmup = std::env::var("BENCH_WARMUP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        BenchConfig { runs, warmup }
    }
}
