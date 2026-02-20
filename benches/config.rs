use criterion::Criterion;
use std::time::Duration;

/// Shared criterion configuration for all benchmarks.
/// Set `BENCH_QUICK=1` for CI or fast local iteration (fewer samples, shorter times).
pub fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(100))
        .measurement_time(Duration::from_millis(500))
        .sample_size(10)
}
