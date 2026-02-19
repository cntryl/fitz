/// Stress configuration for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// Default: 5 runs with 1 warmup (CI-friendly, ~10-30s per suite)
/// Override: BENCH_RUNS and BENCH_WARMUP environment variables

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
