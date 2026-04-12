/// Stress configuration for Fitz tier3 and tier4 benchmarks
///
/// Tier 3: System-level (domain + plumbing, single family/concurrent access patterns)
/// Tier 4: Integration-level (full TCP/WS to domain, complete pipeline)
///
/// **Bench defaults**
/// - `BENCH_RUNS`: Number of measurement runs per stress test (default: 3). Use lower (e.g. 2) for CI.
/// - `BENCH_WARMUP`: Number of warmup runs before measurement (default: 1).
///
/// The bench binaries themselves consume these settings via `cargo bench -- --runs ... --warmup ...`.
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
    pub runs: usize,
    pub warmup: usize,
    /// Duration passed to `ctx.measure_for`. Controlled by `BENCH_MEASURE_SECS` (default: 3).
    pub measure_duration: std::time::Duration,
}

impl Default for BenchConfig {
    fn default() -> Self {
        let runs = std::env::var("BENCH_RUNS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(3);

        let warmup = std::env::var("BENCH_WARMUP")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1);

        let measure_secs = std::env::var("BENCH_MEASURE_SECS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(3);

        BenchConfig {
            runs,
            warmup,
            measure_duration: std::time::Duration::from_secs(measure_secs),
        }
    }
}

/// Create a BenchRunnerConfig from environment variables with correct precedence.
///
/// This bypasses the stress_main!() macro's hardcoded defaults (runs=1, warmup=0)
/// and properly respects environment variables. Use this for manual `BenchRunner` setup
/// in tier3/tier4 benchmarks.
///
/// Precedence: CLI args > env vars > defaults
/// - BENCH_RUNS (default: 3)
/// - BENCH_WARMUP (default: 1)
///
/// **Example:**
/// ```ignore
/// fn main() {
///     let config = stress_config::bench_runner_config_from_env();
///     let mut runner = cntryl_stress::BenchRunner::with_config("my-suite", config);
///     // ... build and run tests
/// }
/// ```
pub fn bench_runner_config_from_env() -> cntryl_stress::BenchRunnerConfig {
    let cfg = BenchConfig::default();
    cntryl_stress::BenchRunnerConfig::new()
        .runs(cfg.runs)
        .warmup(cfg.warmup)
        .verbose(true)
}

/// Macro to replace stress_main!() that properly honors BENCH_RUNS and BENCH_WARMUP env vars.
///
/// The default stress_main!() macro has hardcoded CLI defaults (runs=1, warmup=0) that
/// override environment variables. This macro creates a proper main() that uses env vars.
///
/// **Usage:** Replace `stress_main!();` at end of file with `stress_main_with_env!();`
macro_rules! stress_main_with_env {
    () => {
        fn main() {
            use cntryl_stress::run_with_options;

            let cfg = $crate::stress_config::BenchConfig::default();
            let opts = cntryl_stress::StressRunnerOptions::new()
                .runs(cfg.runs)
                .warmup(cfg.warmup)
                .verbose(true);

            run_with_options(opts);
        }
    };
}

