use criterion::Criterion;
use std::time::Duration;

/// Shared criterion configuration for all benchmarks
pub fn criterion_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50)
}
