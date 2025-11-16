/// Criterion configuration helper optimized for fast feedback loops during optimization
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(500))
        .confidence_level(0.80)
        .significance_level(0.20)
        .noise_threshold(0.10)
        .nresamples(10_000)
        .without_plots()
}
