//! Criterion configuration helper with tier-based tuning.
//!
//! Usage in benchmarks:
//! ```
//! #[path = "./criterion_config.rs"] mod criterion_config;
//! use criterion_config::criterion_config_for_tier1; // or criterion_config_for_tier2
//! criterion_group!(name = my_bench;
//!     config = criterion_config_for_tier1();
//!     targets = bench_fn);
//! ```
//!
//! NOTE: For Tier1 and Tier2, set `SamplingMode::Flat` on the benchmark group:
//! `group.sampling_mode(SamplingMode::Flat)`.
//!
//! Local developers can tune Tier1/Tier2 runs with environment variables. The defaults stay short so
//! local iteration remains snappy, and you can stretch the window when you need steadier numbers.

use criterion::Criterion;
use std::time::Duration;

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    let millis = std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default_ms);

    Duration::from_millis(millis)
}

fn env_usize(name: &str, default_value: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default_value)
}

fn env_f64(name: &str, default_value: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(default_value)
}

#[allow(dead_code)]
#[must_use]
pub fn criterion_config_for_tier1() -> Criterion {
    // ---------------------------------------------------------------
    // Tier 1 — Hotpath (ns → µs)
    //
    // Ultra-tight loops: bloom probe, cache lookup, TLV parse.
    // Goal: stable sub-microsecond signals with a fast local developer loop.
    // These defaults are tuned for local iteration; use env vars to widen the window when
    // you need more stability or are debugging noisy measurements.
    // ---------------------------------------------------------------
    Criterion::default()
        .warm_up_time(env_duration_ms("BENCH_TIER1_WARMUP_MS", 100))
        .measurement_time(env_duration_ms("BENCH_TIER1_MEASUREMENT_MS", 500))
        .sample_size(env_usize("BENCH_TIER1_SAMPLE_SIZE", 12))
        .noise_threshold(env_f64("BENCH_TIER1_NOISE_THRESHOLD", 0.05))
        .without_plots()
}

#[allow(dead_code)]
#[must_use]
pub fn criterion_config_for_tier2() -> Criterion {
    // ---------------------------------------------------------------
    // Tier 2 — Subsystem (µs → ms)
    //
    // Component-level latencies: memtable insert, block read, WAL append.
    // Used very frequently during perf tuning, so the defaults stay short and predictable.
    // ---------------------------------------------------------------
    Criterion::default()
        .warm_up_time(env_duration_ms("BENCH_TIER2_WARMUP_MS", 150))
        .measurement_time(env_duration_ms("BENCH_TIER2_MEASUREMENT_MS", 700))
        .sample_size(env_usize("BENCH_TIER2_SAMPLE_SIZE", 10))
        .noise_threshold(env_f64("BENCH_TIER2_NOISE_THRESHOLD", 0.05))
        .without_plots()
}
