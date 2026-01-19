//! Schedule domain tier 1 hotpath benchmarks
//!
//! Pure cron parsing and matching operations
//! Target: <1 µs per operation

use chrono::{TimeZone, Utc};
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::schedule::CronSchedule;

#[path = "config.rs"]
mod config;

fn bench_cron_parse_every_minute(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_every_minute", |b| {
        b.iter(|| CronSchedule::parse(black_box("* * * * *")))
    });

    group.finish();
}

fn bench_cron_parse_complex(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_complex_expression", |b| {
        b.iter(|| CronSchedule::parse(black_box("0,30 9,12,18 * * 1,2,3,4,5")))
    });

    group.finish();
}

fn bench_cron_parse_with_step(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_with_step_syntax", |b| {
        b.iter(|| CronSchedule::parse(black_box("*/15 */6 * * *")))
    });

    group.finish();
}

fn bench_cron_parse_with_ranges(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_hotpath_parse");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("parse_with_range_syntax", |b| {
        b.iter(|| CronSchedule::parse(black_box("0-30 9-17 * * 1-5")))
    });

    group.finish();
}

fn bench_cron_matching(c: &mut Criterion) {
    // Setup OUTSIDE benchmark loop
    let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
    let dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap();

    let mut group = c.benchmark_group("schedule_hotpath_match");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("match_datetime", |b| {
        b.iter(|| cron.matches_dt(black_box(&dt)))
    });

    group.finish();
}

fn bench_cron_non_match(c: &mut Criterion) {
    // Setup OUTSIDE benchmark loop
    let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
    let dt = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap();

    let mut group = c.benchmark_group("schedule_hotpath_match");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("non_match_datetime", |b| {
        b.iter(|| cron.matches_dt(black_box(&dt)))
    });

    group.finish();
}

fn bench_cron_parse_every_minute_str(c: &mut Criterion) {
    // Precompute strings once
    let exprs = vec![
        "* * * * *",
        "0 * * * *",
        "*/15 * * * *",
        "0 9-17 * * 1-5",
        "0,15,30,45 * * * *",
    ];

    let mut group = c.benchmark_group("schedule_hotpath_parse_batch");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(exprs.len() as u64));

    group.bench_function("parse_batch_5_expressions", |b| {
        b.iter(|| {
            for expr in &exprs {
                let _ = CronSchedule::parse(black_box(expr));
            }
        })
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
        bench_cron_parse_with_ranges,
        bench_cron_matching,
        bench_cron_non_match,
        bench_cron_parse_every_minute_str
}
criterion_main!(benches);
