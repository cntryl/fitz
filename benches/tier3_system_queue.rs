use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_bench_queue_actor;
use fitz::domains::queue::{QueueActor, QueueKey};
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 3: SYSTEM PRESSURE BENCHMARKS
//
// Target: Measure FULL SYSTEM throughput under realistic scenarios
// Goal: Prove world-class sustained performance (50k+ msg/sec aggregate)
// Patterns: Multi-actor simulation, sustained load, realistic workload mixes
//
// These benchmarks simulate production patterns with multiple conceptual
// "actors" (simulated via sequential calls) and sustained pressure.
// ============================================================================

fn bench_capacity_sustained_load(c: &mut Criterion) {
    // SUSTAINED LOAD (capacity) - 100 ops per iteration (50 enqueue + 50 reserve)
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"sustained load message");

    let mut group = c.benchmark_group("queue_capacity_system_sustained");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 operations per iteration

    group.bench_function("queue_capacity_sustained_100ops_enqueue_reserve", |b| {
        b.iter(|| {
            for _ in 0..50 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            for _ in 0..50 {
                let _ = actor.handle_reserve(black_box(30), black_box(Some(1)));
            }
        })
    });

    group.finish();
}

fn bench_capacity_mixed_workload(c: &mut Criterion) {
    // MIXED WORKLOAD (capacity) - heterogeneous workload mix
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut actor = create_bench_queue_actor("bench", "system", "queue", Some(3)); // max_attempts=3
    let payload = Bytes::from_static(b"mixed workload message");

    let mut group = c.benchmark_group("queue_capacity_system_mixed_workload");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100));

    group.bench_function("queue_capacity_mixed_70i_20d_10dlq", |b| {
        b.iter(|| {
            for _ in 0..70 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            for _ in 0..20 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(Some(5)));
            }

            for _ in 0..10 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            let _ = actor.handle_reserve(black_box(1), black_box(Some(10)));
        })
    });

    group.finish();
}

fn bench_capacity_cold_start_recovery(c: &mut Criterion) {
    // COLD START RECOVERY (capacity) - measure recovery time after restart
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut group = c.benchmark_group("queue_capacity_system_cold_start");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 messages recovered

    group.bench_function("queue_capacity_recover_100_messages", |b| {
        b.iter_batched(
            || {
                let queue_key = QueueKey {
                    family: RouteFamily::new(1),
                    realm: "bench".to_string(),
                    area: "recovery".to_string(),
                    resource: "queue".to_string(),
                };

                let store = Arc::new(
                    cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
                        .expect("Failed to open in-memory store"),
                );
                let mut actor =
                    QueueActor::new(RouteFamily::new(1), queue_key.clone(), store.clone(), None);

                let payload = Bytes::from_static(b"recovery message");
                for _ in 0..100 {
                    let _ = actor.handle_enqueue(payload.clone(), None);
                }

                drop(actor);
                (store, queue_key)
            },
            |(store, queue_key)| {
                let actor = QueueActor::new(RouteFamily::new(1), queue_key, store, None);
                black_box(actor);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_capacity_high_contention(c: &mut Criterion) {
    // HIGH CONTENTION (capacity) - oscillating queue depth scenario
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");

    let mut group = c.benchmark_group("queue_capacity_system_high_contention");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(2)); // enqueue + reserve

    group.bench_function("queue_capacity_oscillating_1msg", |b| {
        b.iter(|| {
            let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            let reserve_resp = actor.handle_reserve(black_box(30), black_box(Some(1)));

            if let fitz::domains::queue::QueueResponse::Reserved { messages } = reserve_resp {
                if !messages.is_empty() {
                    let _ = actor
                        .handle_complete(black_box(messages[0].id), black_box(messages[0].token));
                }
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_capacity_sustained_load,
        bench_capacity_mixed_workload,
        bench_capacity_cold_start_recovery,
        bench_capacity_high_contention,
}
criterion_main!(benches);
