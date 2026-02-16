//! Tier 1 (Hotpath) benchmarks for Schedule domain
//!
//! Measures raw schedule actor performance:
//! - create_schedule() with precomputed routes/crons
//! - delete_schedule()
//! - list_defs()
//! - scan_and_fire()
//!
//! No TLV encoding, no session layer, no router overhead.
//! Pure domain logic microbenching.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, SamplingMode};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use bytes::Bytes;

#[path = "../benches/config.rs"]
mod config;

/// Create a test schedule actor for benchmarking
fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

/// Precompute schedule routes for benchmarking (deterministic)
fn precompute_routes(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| format!("schedule://acme/jobs/task{:06}", i))
        .collect()
}

/// Precompute cron expressions (varied but deterministic)
fn precompute_crons(count: usize) -> Vec<String> {
    let patterns = [
        "* * * * *",         // Every minute
        "0 * * * *",         // Every hour
        "0 0 * * *",         // Daily
        "0 0 * * 0",         // Weekly
        "0 2 1 * *",         // Monthly
        "*/5 * * * *",       // Every 5 minutes
        "0 */6 * * *",       // Every 6 hours
        "0 9-17 * * 1-5",    // Business hours
    ];
    
    (0..count)
        .map(|i| patterns[i % patterns.len()].to_string())
        .collect()
}

/// Precompute payloads (small, fixed size)
fn precompute_payloads(count: usize) -> Vec<Bytes> {
    (0..count)
        .map(|i| Bytes::from(format!("payload-{:06}", i)))
        .collect()
}

/// Benchmark: Create single schedule
fn bench_create_schedule_single(c: &mut Criterion) {
    let mut actor = create_test_actor();
    let routes = precompute_routes(1);
    let crons = precompute_crons(1);
    let payloads = precompute_payloads(1);
    
    let mut group = c.benchmark_group("schedule_create_single");
    group.sampling_mode(SamplingMode::Flat);
    
    group.bench_function("create", |b| {
        b.iter(|| {
            let _response = actor.handle(black_box(ScheduleMessage::Create {
                route: routes[0].clone(),
                cron: crons[0].clone(),
                payload: payloads[0].clone(),
            }));
        });
    });
    
    group.finish();
}

/// Benchmark: Create schedules (batch)
fn bench_create_schedule_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_create_batch");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [10, 100, 1000] {
        let routes = precompute_routes(count);
        let crons = precompute_crons(count);
        let payloads = precompute_payloads(count);
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter(|| {
                let mut actor = create_test_actor();
                for i in 0..count {
                    let _response = actor.handle(black_box(ScheduleMessage::Create {
                        route: routes[i].clone(),
                        cron: crons[i].clone(),
                        payload: payloads[i].clone(),
                    }));
                }
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Delete schedule
fn bench_delete_schedule(c: &mut Criterion) {
    let routes = precompute_routes(100);
    let crons = precompute_crons(100);
    let payloads = precompute_payloads(100);
    
    let mut group = c.benchmark_group("schedule_delete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("delete", |b| {
        b.iter(|| {
            // Setup: Create schedule
            let mut actor = create_test_actor();
            actor.handle(ScheduleMessage::Create {
                route: routes[0].clone(),
                cron: crons[0].clone(),
                payload: payloads[0].clone(),
            });
            
            // Measure: Delete
            let _response = actor.handle(black_box(ScheduleMessage::Cancel {
                route: routes[0].clone(),
            }));
        });
    });
    
    group.finish();
}

/// Benchmark: List schedules with varying counts
fn bench_list_schedules(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_list");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [0, 10, 100, 1000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            // Setup: Create N schedules
            let mut actor = create_test_actor();
            let routes = precompute_routes(count);
            let crons = precompute_crons(count);
            let payloads = precompute_payloads(count);
            
            for i in 0..count {
                actor.handle(ScheduleMessage::Create {
                    route: routes[i].clone(),
                    cron: crons[i].clone(),
                    payload: payloads[i].clone(),
                });
            }
            
            // Measure: List
            b.iter(|| {
                let _response = actor.handle(black_box(ScheduleMessage::List));
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Scan and fire with varying schedule counts
fn bench_scan_and_fire(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_scan_and_fire");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [10, 100, 1000] {
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            // Setup: Create N schedules (none ready to fire yet)
            let mut actor = create_test_actor();
            let routes = precompute_routes(count);
            let crons = precompute_crons(count);
            let payloads = precompute_payloads(count);
            
            for i in 0..count {
                actor.handle(ScheduleMessage::Create {
                    route: routes[i].clone(),
                    cron: crons[i].clone(),
                    payload: payloads[i].clone(),
                });
            }
            
            // Measure: Scan (no schedules ready, so measures overhead)
            b.iter(|| {
                let _fired = actor.scan_and_fire();
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Upsert (CREATE on existing route)
fn bench_upsert_schedule(c: &mut Criterion) {
    let route = "schedule://acme/jobs/recurring".to_string();
    let crons = precompute_crons(2);
    let payloads = precompute_payloads(2);
    
    let mut group = c.benchmark_group("schedule_upsert");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("upsert", |b| {
        b.iter(|| {
            // Setup: Create initial schedule
            let mut actor = create_test_actor();
            actor.handle(ScheduleMessage::Create {
                route: route.clone(),
                cron: crons[0].clone(),
                payload: payloads[0].clone(),
            });
            
            // Measure: Upsert (overwrite)
            let _response = actor.handle(black_box(ScheduleMessage::Create {
                route: route.clone(),
                cron: crons[1].clone(),
                payload: payloads[1].clone(),
            }));
        });
    });
    
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_create_schedule_single,
        bench_create_schedule_batch,
        bench_delete_schedule,
        bench_list_schedules,
        bench_scan_and_fire,
        bench_upsert_schedule,
}
criterion_main!(benches);
