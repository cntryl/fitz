//! Schedule domain tier 1 hotpath benchmarks
//!
//! Pure cron parsing and matching operations
//! Target: <1 µs per operation

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use fitz::domains::schedule::CronSchedule;
use chrono::{TimeZone, Utc};

#[path = "config.rs"]
mod config;

fn bench_cron_parse_every_minute(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("parse_every_minute", |b| {
        b.iter(|| CronSchedule::parse(black_box("* * * * *")))
    });

    group.finish();
}

fn bench_cron_parse_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("parse_complex_expression", |b| {
        b.iter(|| CronSchedule::parse(black_box("0,30 9,12,18 * * 1,2,3,4,5")))
    });

    group.finish();
}

fn bench_cron_parse_with_step(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("parse_with_step_syntax", |b| {
        b.iter(|| CronSchedule::parse(black_box("*/15 */6 * * *")))
    });

    group.finish();
}

fn bench_cron_matching(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_match");
    group.sampling_mode(SamplingMode::Flat);

    let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
    let dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap();

    group.bench_function("match_datetime", |b| {
        b.iter(|| cron.matches_dt(black_box(&dt)))
    });

    group.finish();
}

fn bench_cron_non_match(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_match");
    group.sampling_mode(SamplingMode::Flat);

    let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
    let dt = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();

    group.bench_function("non_match_datetime", |b| {
        b.iter(|| cron.matches_dt(black_box(&dt)))
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_cron_parse_every_minute,
        bench_cron_parse_complex,
        bench_cron_parse_with_step,
        bench_cron_matching,
        bench_cron_non_match
}
criterion_main!(benches);
