//! Lease domain tier 3 system benchmarks - queue scaling at load
//!
//! System-level queue behavior under sustained load:
//! - Queue depth scaling (10, 50, 100 waiters)
//! - Lease turnover with large backlog
//! - Query response time with deep queue
//!
//! Target: <10 seconds total

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::domains::lease::lease_actor::LeaseActor;
use fitz::domains::lease::protocol::LeaseMessage;
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

fn bench_queue_depth_throughput_10_waiters(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queue_scaling");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/queue-scale-10");

    group.bench_function("queue_depth_10_waiters", |b| {
        b.iter_batched(
            || {
                let mut actor = LeaseActor::new(Arc::new(Default::default()));

                // Setup: Holder + 10 waiters already queued
                let holder_msg = LeaseMessage::Acquire {
                    family_id: family,
                    route: route.clone(),
                    owner_id: "holder-0".to_string(),
                    ttl_secs: 60,
                    wait_seconds: 0,
                };
                let _ = actor.handle(holder_msg);

                for i in 0..10 {
                    let waiter_msg = LeaseMessage::Acquire {
                        family_id: family,
                        route: route.clone(),
                        owner_id: format!("waiter-{}", i),
                        ttl_secs: 60,
                        wait_seconds: 30,
                    };
                    let _ = actor.handle(waiter_msg);
                }

                actor
            },
            |mut actor| {
                // Measure: Single additional operation in deep queue
                let msg = LeaseMessage::Query {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                };
                let _ = actor.handle(msg);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_queue_depth_throughput_50_waiters(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queue_scaling");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/queue-scale-50");

    group.bench_function("queue_depth_50_waiters", |b| {
        b.iter_batched(
            || {
                let mut actor = LeaseActor::new(Arc::new(Default::default()));

                // Setup: Holder + 50 waiters
                let holder_msg = LeaseMessage::Acquire {
                    family_id: family,
                    route: route.clone(),
                    owner_id: "holder-0".to_string(),
                    ttl_secs: 60,
                    wait_seconds: 0,
                };
                let _ = actor.handle(holder_msg);

                for i in 0..50 {
                    let waiter_msg = LeaseMessage::Acquire {
                        family_id: family,
                        route: route.clone(),
                        owner_id: format!("waiter-{}", i),
                        ttl_secs: 60,
                        wait_seconds: 30,
                    };
                    let _ = actor.handle(waiter_msg);
                }

                actor
            },
            |mut actor| {
                // Measure: Single operation with 50 waiters pending
                let msg = LeaseMessage::Query {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                };
                let _ = actor.handle(msg);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_queue_depth_throughput_100_waiters(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queue_scaling");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/queue-scale-100-max");

    group.bench_function("queue_depth_100_waiters_at_max", |b| {
        b.iter_batched(
            || {
                let mut actor = LeaseActor::new(Arc::new(Default::default()));

                // Setup: Holder + 100 waiters (at max queue depth)
                let holder_msg = LeaseMessage::Acquire {
                    family_id: family,
                    route: route.clone(),
                    owner_id: "holder-0".to_string(),
                    ttl_secs: 60,
                    wait_seconds: 0,
                };
                let _ = actor.handle(holder_msg);

                for i in 0..100 {
                    let waiter_msg = LeaseMessage::Acquire {
                        family_id: family,
                        route: route.clone(),
                        owner_id: format!("waiter-{}", i),
                        ttl_secs: 60,
                        wait_seconds: 30,
                    };
                    let _ = actor.handle(waiter_msg);
                }

                actor
            },
            |mut actor| {
                // Measure: Single operation at max queue depth
                let msg = LeaseMessage::Query {
                    family_id: black_box(family),
                    route: black_box(route.clone()),
                };
                let _ = actor.handle(msg);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_lease_turnover_with_backlog(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queue_scaling");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(50));

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/turnover-backlog");

    group.bench_function("lease_turnover_with_50_waiter_backlog", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: 50 clients already waiting
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "initial-holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let holder_resp = actor.handle(holder_msg);
        let mut holder_token = 0u64;
        if let fitz::domains::lease::protocol::LeaseResponse::Acquired { fencing_token } = holder_resp {
            holder_token = fencing_token;
        }

        let client_ids: Vec<String> = (0..50)
            .map(|i| format!("backlog-{}", i))
            .collect();

        for id in client_ids.iter() {
            let msg = LeaseMessage::Acquire {
                family_id: family,
                route: route.clone(),
                owner_id: id.to_string(),
                ttl_secs: 60,
                wait_seconds: 30,
            };
            let _ = actor.handle(msg);
        }

        // Hot path: Holder releases (triggers domain processing to grant next waiter)
        b.iter(|| {
            let msg = LeaseMessage::Release {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("initial-holder".to_string()),
                fencing_token: black_box(holder_token),
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets = 
        bench_queue_depth_throughput_10_waiters,
        bench_queue_depth_throughput_50_waiters,
        bench_queue_depth_throughput_100_waiters,
        bench_lease_turnover_with_backlog
}
criterion_main!(benches);
