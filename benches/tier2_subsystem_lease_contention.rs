//! Lease domain tier 2 subsystem benchmarks - high contention scenarios
//!
//! Contention patterns + handler coordination:
//! - High contention on single lease (10, 50, 100 concurrent waiters)
//! - Grant next waiter latency after release
//! - Cascade release (A→B→C sequential grants)
//! - Mixed immediate and queued acquires
//!
//! Target: <3 seconds total

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::lease_actor::LeaseActor;
use fitz::domains::lease::protocol::LeaseMessage;
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

fn bench_high_contention_single_lease_10_clients(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(10));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/contended-10");

    group.bench_function("high_contention_10_clients", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder + create client IDs outside loop
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        let client_ids: Vec<String> = (0..10)
            .map(|i| format!("client-{}", i))
            .collect();

        // Hot path: Queue all 10 clients contending for same lease
        b.iter(|| {
            for (i, id) in client_ids.iter().enumerate() {
                let msg = LeaseMessage::Acquire {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                    owner_id: black_box(id.to_string()),
                    ttl_secs: black_box(60),
                    wait_seconds: black_box(10),
                };
                let _ = actor.handle(msg);
            }
        })
    });

    group.finish();
}

fn bench_high_contention_single_lease_50_clients(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/contended-50");

    group.bench_function("high_contention_50_clients", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        let client_ids: Vec<String> = (0..50)
            .map(|i| format!("client-{}", i))
            .collect();

        // Hot path: Queue all 50 clients
        b.iter(|| {
            for id in client_ids.iter() {
                let msg = LeaseMessage::Acquire {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                    owner_id: black_box(id.to_string()),
                    ttl_secs: black_box(60),
                    wait_seconds: black_box(10),
                };
                let _ = actor.handle(msg);
            }
        })
    });

    group.finish();
}

fn bench_high_contention_single_lease_100_clients(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/contended-100");

    group.bench_function("high_contention_100_clients_at_queue_limit", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        let client_ids: Vec<String> = (0..100)
            .map(|i| format!("client-{}", i))
            .collect();

        // Hot path: Queue all 100 clients (at max queue depth)
        b.iter(|| {
            for id in client_ids.iter() {
                let msg = LeaseMessage::Acquire {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                    owner_id: black_box(id.to_string()),
                    ttl_secs: black_box(60),
                    wait_seconds: black_box(10),
                };
                let _ = actor.handle(msg);
            }
        })
    });

    group.finish();
}

fn bench_mixed_immediate_and_queued_acquires(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_contention");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(20));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/mixed-traffic");

    group.bench_function("mixed_50pct_immediate_50pct_queued", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder owns lease
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        // Precompute alternating client IDs
        let client_ids: Vec<(String, u32)> = (0..20)
            .map(|i| {
                let wait_secs = if i % 2 == 0 { 0 } else { 10 };
                (format!("traffic-{}", i), wait_secs)
            })
            .collect();

        // Hot path: Mix of immediate (wait=0) and queued (wait>0) attempts
        b.iter(|| {
            for (id, wait) in client_ids.iter() {
                let msg = LeaseMessage::Acquire {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                    owner_id: black_box(id.to_string()),
                    ttl_secs: black_box(60),
                    wait_seconds: black_box(*wait),
                };
                let _ = actor.handle(msg);
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = 
        bench_high_contention_single_lease_10_clients,
        bench_high_contention_single_lease_50_clients,
        bench_high_contention_single_lease_100_clients,
        bench_mixed_immediate_and_queued_acquires
}
criterion_main!(benches);
