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
pub fn criterion_config_for_tier1() -> Criterion {
    // ---------------------------------------------------------------
    // Tier 1 — Hotpath (ns → µs)
    //
    // Ultra-tight loops: bloom probe, cache lookup, TLV parse.
    // Goal: stable sub-microsecond signals.
    // Windows has higher system jitter, so we use:
    // - Longer warmup (CPU ramp-up, cache warmth)
    // - Longer measurement window (average out timer noise)
    // - More samples (statistical stability)
    // - Looser noise threshold (accept Windows jitter)
    // ---------------------------------------------------------------
    Criterion::default()
        .warm_up_time(env_duration_ms("BENCH_TIER1_WARMUP_MS", 400))
        .measurement_time(env_duration_ms("BENCH_TIER1_MEASUREMENT_MS", 1200))
        .sample_size(env_usize("BENCH_TIER1_SAMPLE_SIZE", 25))
        .noise_threshold(env_f64("BENCH_TIER1_NOISE_THRESHOLD", 0.04))
        .without_plots()
}

#[allow(dead_code)]
pub fn criterion_config_for_tier2() -> Criterion {
    // ---------------------------------------------------------------
    // Tier 2 — Subsystem (µs → ms)
    //
    // Component-level latencies: memtable insert, block read, WAL append.
    // Used very frequently during perf tuning.
    // ---------------------------------------------------------------
    Criterion::default()
        .warm_up_time(env_duration_ms("BENCH_TIER2_WARMUP_MS", 500))
        .measurement_time(env_duration_ms("BENCH_TIER2_MEASUREMENT_MS", 1500))
        .sample_size(env_usize("BENCH_TIER2_SAMPLE_SIZE", 20))
        .noise_threshold(env_f64("BENCH_TIER2_NOISE_THRESHOLD", 0.04))
        .without_plots()
}
