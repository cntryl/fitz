//! Schedule domain tier 4 integration benchmarks
//!
//! Full system pipeline with local disk storage (realistic)
//! Measures operation latency through engine routing + domain handling with durable storage
//! Includes domain context creation overhead, routing, and actual disk I/O
//!
//! Uses local disk-backed Midge engine to reflect production conditions

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_local_bench_store;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage};
use fitz::runtime::routing::RouteFamily;
use std::time::Duration;

#[path = "config.rs"]
mod config;

fn bench_full_pipeline_create_cancel(c: &mut Criterion) {
    // Complete pipeline: Create actor, create schedule, cancel schedule
    // Measures total overhead with disk-backed storage
    let mut group = c.benchmark_group("schedule_integration_full_pipeline_create_cancel");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_millis(500));

    group.bench_function("create_actor_create_schedule_cancel", |b| {
        b.iter_batched(
            create_local_bench_store,
            |(store, _temp_dir)| {
                let mut actor = ScheduleActor::new(
                    RouteFamily::new(1),
                    store,
                    cntryl_midge::WriteOptions::buffered(),
                );

                let route = "schedule://integration/jobs/task001".to_string();

                // Create schedule
                actor.handle(black_box(ScheduleMessage::Create {
                    route: route.clone(),
                    cron: "* * * * *".to_string(),
                    payload: Bytes::from_static(b"integration_payload"),
                }));

                // Cancel schedule
                actor.handle(black_box(ScheduleMessage::Cancel { route }));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_full_pipeline_batch_create(c: &mut Criterion) {
    // Realistic batch creation scenario with disk-backed storage
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    );

    let mut group = c.benchmark_group("schedule_integration_batch_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    group.bench_function("create_10_schedules", |b| {
        let mut counter = 0;
        b.iter(|| {
            for i in 0..10 {
                let route = format!("schedule://integration/batch/task{:06}", counter + i);
                let cron = match i % 4 {
                    0 => "* * * * *", // Every minute
                    1 => "0 * * * *", // Every hour
                    2 => "0 0 * * *", // Daily
                    _ => "0 2 1 * *", // Monthly at 2am on 1st
                };

                actor.handle(black_box(ScheduleMessage::Create {
                    route,
                    cron: cron.to_string(),
                    payload: Bytes::from(format!("batch_payload_{}", i)),
                }));
            }
            counter += 10;
        });
    });

    group.finish();
}

fn bench_full_pipeline_create_list(c: &mut Criterion) {
    // Realistic workflow: Create schedules then list them
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    );

    // Setup: Create several schedules
    for i in 0..50 {
        let route = format!("schedule://integration/list/task{:03}", i);
        actor.handle(ScheduleMessage::Create {
            route,
            cron: "0 * * * *".to_string(),
            payload: Bytes::from(format!("setup_payload_{}", i)),
        });
    }

    let mut group = c.benchmark_group("schedule_integration_create_list");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(51));

    group.bench_function("create_one_then_list_all", |b| {
        let mut counter = 100;
        b.iter(|| {
            // Create new schedule
            let route = format!("schedule://integration/list/new{:06}", counter);
            actor.handle(black_box(ScheduleMessage::Create {
                route,
                cron: "0 0 * * *".to_string(),
                payload: Bytes::from_static(b"new_schedule"),
            }));
            counter += 1;

            // List all schedules (now 51 total)
            actor.handle(black_box(ScheduleMessage::List));
        });
    });

    group.finish();
}

fn bench_full_pipeline_mixed_workload(c: &mut Criterion) {
    // Realistic mixed workload: Create, list, cancel operations
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    );

    // Setup: Create initial schedules
    for i in 0..30 {
        let route = format!("schedule://integration/mixed/task{:03}", i);
        actor.handle(ScheduleMessage::Create {
            route,
            cron: "* * * * *".to_string(),
            payload: Bytes::from(format!("mixed_payload_{}", i)),
        });
    }

    let mut group = c.benchmark_group("schedule_integration_mixed_workload");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(4));

    group.bench_function("create_list_cancel_list", |b| {
        let mut counter = 100;
        b.iter(|| {
            // Create new schedule
            let route = format!("schedule://integration/mixed/new{:06}", counter);
            actor.handle(black_box(ScheduleMessage::Create {
                route: route.clone(),
                cron: "0 0 * * *".to_string(),
                payload: Bytes::from_static(b"mixed_new"),
            }));

            // List all schedules
            actor.handle(black_box(ScheduleMessage::List));

            // Cancel the schedule we just created
            actor.handle(black_box(ScheduleMessage::Cancel { route }));

            // List again to verify removal
            actor.handle(black_box(ScheduleMessage::List));

            counter += 1;
        });
    });

    group.finish();
}

fn bench_full_pipeline_cron_patterns(c: &mut Criterion) {
    // Benchmark different cron pattern complexity
    let (store, _temp_dir) = create_local_bench_store();
    let mut actor = ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    );

    let patterns = vec![
        ("simple", "* * * * *"),          // All wildcards
        ("hourly", "0 * * * *"),          // Specific minute
        ("daily", "0 0 * * *"),           // Specific hour
        ("monthly", "0 0 1 * *"),         // Specific day
        ("complex", "*/15 9-17 * * 1-5"), // Ranges and steps
    ];

    let mut group = c.benchmark_group("schedule_integration_cron_patterns");
    group.sampling_mode(SamplingMode::Flat);

    for (pattern_name, cron) in patterns {
        group.bench_function(pattern_name, |b| {
            let mut counter = 0;
            b.iter(|| {
                let route = format!(
                    "schedule://integration/cron/{}/task{:06}",
                    pattern_name, counter
                );
                actor.handle(black_box(ScheduleMessage::Create {
                    route,
                    cron: cron.to_string(),
                    payload: Bytes::from_static(b"cron_pattern_test"),
                }));
                counter += 1;
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_full_pipeline_create_cancel,
        bench_full_pipeline_batch_create,
        bench_full_pipeline_create_list,
        bench_full_pipeline_mixed_workload,
        bench_full_pipeline_cron_patterns
}
criterion_main!(benches);
