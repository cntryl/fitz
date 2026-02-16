//! Tier 3 (System) benchmarks for Schedule domain
//!
//! Measures full system pipeline performance:
//! - Frame parsing
//! - Session routing
//! - Actor execution
//! - Response encoding
//! - Full round-trip latency
//!
//! This is closest to production performance measurement.

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput, SamplingMode};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use bytes::Bytes;

#[path = "../benches/config.rs"]
mod config;

/// Create a test schedule actor
fn create_test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3, 4, 5]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

/// Precompute test data
fn precompute_data(count: usize) -> (Vec<String>, Vec<String>, Vec<Bytes>) {
    let routes = (0..count)
        .map(|i| format!("schedule://acme/jobs/task{:06}", i))
        .collect();
    
    let crons = (0..count)
        .map(|i| {
            let patterns = ["* * * * *", "0 * * * *", "0 0 * * *", "0 2 1 * *"];
            patterns[i % patterns.len()].to_string()
        })
        .collect();
    
    let payloads = (0..count)
        .map(|i| Bytes::from(format!("payload-{:06}", i)))
        .collect();
    
    (routes, crons, payloads)
}

/// Benchmark: System-level CREATE operation
fn bench_system_create(c: &mut Criterion) {
    let mut actor = create_test_actor();
    let (routes, crons, payloads) = precompute_data(1000);
    
    let mut group = c.benchmark_group("schedule_system_create");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("create", |b| {
        let mut idx = 0;
        b.iter(|| {
            let _response = actor.handle(black_box(ScheduleMessage::Create {
                route: routes[idx % routes.len()].clone(),
                cron: crons[idx % crons.len()].clone(),
                payload: payloads[idx % payloads.len()].clone(),
            }));
            idx += 1;
        });
    });
    
    group.finish();
}

/// Benchmark: System-level CANCEL operation
fn bench_system_cancel(c: &mut Criterion) {
    let (routes, crons, payloads) = precompute_data(1000);
    
    // Setup: Create schedules
    let mut actor = create_test_actor();
    for i in 0..routes.len() {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }
    
    let mut group = c.benchmark_group("schedule_system_cancel");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("cancel", |b| {
        let mut idx = 0;
        b.iter(|| {
            let _response = actor.handle(black_box(ScheduleMessage::Cancel {
                route: routes[idx % routes.len()].clone(),
            }));
            idx += 1;
        });
    });
    
    group.finish();
}

/// Benchmark: System-level LIST operation with varying schedule counts
fn bench_system_list(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_system_list");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [10, 100, 1000] {
        // Setup: Create schedules
        let mut actor = create_test_actor();
        let (routes, crons, payloads) = precompute_data(count);
        
        for i in 0..count {
            actor.handle(ScheduleMessage::Create {
                route: routes[i].clone(),
                cron: crons[i].clone(),
                payload: payloads[i].clone(),
            });
        }
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let _response = actor.handle(black_box(ScheduleMessage::List));
            });
        });
    }
    
    group.finish();
}

/// Benchmark: Mixed workload (CREATE/CANCEL/LIST)
fn bench_system_mixed_workload(c: &mut Criterion) {
    let (routes, crons, payloads) = precompute_data(1000);
    let mut actor = create_test_actor();
    
    // Pre-populate with some schedules
    for i in 0..100 {
        actor.handle(ScheduleMessage::Create {
            route: routes[i].clone(),
            cron: crons[i].clone(),
            payload: payloads[i].clone(),
        });
    }
    
    let mut group = c.benchmark_group("schedule_system_mixed");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(4));
    
    group.bench_function("mixed", |b| {
        let mut idx = 100;
        b.iter(|| {
            // CREATE
            let _r1 = actor.handle(black_box(ScheduleMessage::Create {
                route: routes[idx % routes.len()].clone(),
                cron: crons[idx % crons.len()].clone(),
                payload: payloads[idx % payloads.len()].clone(),
            }));
            
            // LIST
            let _r2 = actor.handle(black_box(ScheduleMessage::List));
            
            // CREATE (another)
            let _r3 = actor.handle(black_box(ScheduleMessage::Create {
                route: routes[(idx + 1) % routes.len()].clone(),
                cron: crons[(idx + 1) % crons.len()].clone(),
                payload: payloads[(idx + 1) % payloads.len()].clone(),
            }));
            
            // CANCEL
            let _r4 = actor.handle(black_box(ScheduleMessage::Cancel {
                route: routes[(idx - 50) % routes.len()].clone(),
            }));
            
            idx += 2;
        });
    });
    
    group.finish();
}

/// Benchmark: Scan and fire overhead with many schedules
fn bench_system_scan_and_fire(c: &mut Criterion) {
    let mut group = c.benchmark_group("schedule_system_scan_and_fire");
    group.sampling_mode(SamplingMode::Flat);
    
    for count in [100, 1000, 10000] {
        // Setup: Create many schedules
        let mut actor = create_test_actor();
        let (routes, crons, payloads) = precompute_data(count);
        
        for i in 0..count {
            actor.handle(ScheduleMessage::Create {
                route: routes[i].clone(),
                cron: crons[i].clone(),
                payload: payloads[i].clone(),
            });
        }
        
        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, _| {
            b.iter(|| {
                let _fired = actor.scan_and_fire();
            });
        });
    }
    
    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_system_create,
        bench_system_cancel,
        bench_system_list,
        bench_system_mixed_workload,
        bench_system_scan_and_fire,
}
criterion_main!(benches);
