/// Criterion configuration helper optimized for fast feedback loops during optimization
use criterion::Criterion;
use std::time::Duration;

pub fn criterion_config() -> Criterion {
    Criterion::default()
        .sample_size(10)
        .measurement_time(Duration::from_millis(120))
        .warm_up_time(Duration::from_millis(30))
        .confidence_level(0.80)
        .significance_level(0.20)
        .noise_threshold(0.10)
        .nresamples(1000)
        .without_plots()
}
