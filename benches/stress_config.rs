/// Stress configuration for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// **Bench defaults**
/// - `BENCH_RUNS`: Number of measurement runs per stress test.
/// - `BENCH_WARMUP`: Number of warmup runs before measurement.
///
/// The stress harness consumes run/warmup settings directly via `stress_main!()`.
///
/// - `BENCH_MEASURE_SECS`: Duration in whole seconds passed to `ctx.measure_for` (default: 3).
///   Use `BenchConfig::default().measure_duration` instead of hardcoding `Duration::from_secs(5)`.
///
/// **set_elements(N)** in each `#[stress_test]`: N is the explicit batch size for one timed
/// iteration inside `ctx.measure_for(cfg.measure_duration, || { ... })`. The default measured
/// run is 3 seconds, which is usually enough to smooth scheduler noise without making Tier 3/4
/// suites drag, and N must match the
/// logical number of meaningful operations performed in that batch so throughput
/// (batch_size / elapsed_time) reported by `cntryl-tools summarize-benchmarks`
/// with Fitz report overrides is interpretable.
///
/// If a scenario has a natural transport or fanout scope, add tags like `measurement_scope`
/// and `batch_size` so the report can distinguish direct, transport, and delivery cost.
#[allow(dead_code)]
pub struct BenchConfig {
    /// Duration passed to `ctx.measure_for`. Controlled by `BENCH_MEASURE_SECS` (default: 3).
    pub measure_duration: std::time::Duration,
}

impl Default for BenchConfig {
    fn default() -> Self {
        let measure_secs = std::env::var("BENCH_MEASURE_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3);

        BenchConfig {
            measure_duration: std::time::Duration::from_secs(measure_secs),
        }
    }
}

