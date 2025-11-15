/// Criterion configuration helper optimized for fast feedback loops during optimization
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_millis(500))
        .warm_up_time(Duration::from_millis(100))
        .confidence_level(0.95)
        .significance_level(0.05)
        .noise_threshold(0.05)
        .nresamples(50_000)
        .without_plots()
}
