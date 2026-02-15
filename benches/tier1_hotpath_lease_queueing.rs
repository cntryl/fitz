//! Lease domain tier 1 hotpath benchmarks - queueing scenarios
//!
//! Pure service-level queueing operation latency
//! - Immediate acquire (fast path)
//! - Immediate rejection (held by other, wait_seconds=0)
//! - Queue enqueue cost (first waiter)
//! - Query with pending waiters
//!
//! Target: <1 µs per operation

use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode};
use fitz::domains::lease::lease_actor::LeaseActor;
use fitz::domains::lease::protocol::{LeaseMessage, LeaseResponse};
use fitz::runtime::routing::{Route, RouteFamily};
use std::sync::Arc;

#[path = "config.rs"]
mod config;

fn bench_lease_acquire_when_free(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/free-lock");

    group.bench_function("acquire_when_free", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-X".to_string()),
                ttl_secs: black_box(60),
                wait_seconds: black_box(0),
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

fn bench_lease_acquire_immediate_rejection(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/held-lock");

    group.bench_function("acquire_immediate_rejection_held_by_other", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder acquires lease outside hot loop
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        // Hot path: Rejection on immediate acquire attempt
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-reject".to_string()),
                ttl_secs: black_box(60),
                wait_seconds: black_box(0),
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

fn bench_lease_acquire_enqueue_cost(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/queue-lock");

    group.bench_function("acquire_enqueue_first_waiter", |b| {
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

        // Hot path: Enqueue first waiter with wait_seconds
        let mut waiter_counter = 0;
        b.iter(|| {
            waiter_counter += 1;
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box(format!("waiter-{}", waiter_counter)),
                ttl_secs: black_box(60),
                wait_seconds: black_box(10),  // Request wait
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

fn bench_lease_query_pending_waiters(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/query-lock");

    group.bench_function("query_with_pending_waiters_count", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder + 5 waiters
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        for i in 0..5 {
            let waiter_msg = LeaseMessage::Acquire {
                family_id: family,
                route: route.clone(),
                owner_id: format!("waiter-{}", i),
                ttl_secs: 60,
                wait_seconds: 10,
            };
            let _ = actor.handle(waiter_msg);
        }

        // Hot path: Query
        b.iter(|| {
            let msg = LeaseMessage::Query {
                family_id: black_box(family),
                route: black_box(route.clone()),
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

fn bench_idempotent_acquire_already_held(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/idempotent-lock");

    group.bench_function("idempotent_acquire_already_held", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Client holds lease
        let hold_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "client-1".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(hold_msg);

        // Hot path: Same client tries to acquire again (idempotent)
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("client-1".to_string()),
                ttl_secs: black_box(60),
                wait_seconds: black_box(0),
            };
            let _ = actor.handle(msg);
        })
    });

    group.finish();
}

fn bench_idempotent_acquire_already_queued(c: &mut Criterion) {
    let mut group = c.benchmark_group("lease_queueing");
    group.sampling_mode(SamplingMode::Flat);

    let family = RouteFamily::new(1);
    let route = Route::new("lease://bench/app/idempotent-queue-lock");

    group.bench_function("idempotent_acquire_already_queued", |b| {
        let mut actor = LeaseActor::new(Arc::new(Default::default()));

        // Setup: Holder + waiter already in queue
        let holder_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "holder".to_string(),
            ttl_secs: 60,
            wait_seconds: 0,
        };
        let _ = actor.handle(holder_msg);

        let waiter_msg = LeaseMessage::Acquire {
            family_id: family,
            route: route.clone(),
            owner_id: "waiter-1".to_string(),
            ttl_secs: 60,
            wait_seconds: 10,
        };
        let _ = actor.handle(waiter_msg);

        // Hot path: Same waiter tries to acquire again (already queued)
        b.iter(|| {
            let msg = LeaseMessage::Acquire {
                family_id: black_box(family),
                route: black_box(route.clone()),
                owner_id: black_box("waiter-1".to_string()),
                ttl_secs: black_box(60),
                wait_seconds: black_box(10),
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
        bench_lease_acquire_when_free,
        bench_lease_acquire_immediate_rejection,
        bench_lease_acquire_enqueue_cost,
        bench_lease_query_pending_waiters,
        bench_idempotent_acquire_already_held,
        bench_idempotent_acquire_already_queued
}
criterion_main!(benches);
