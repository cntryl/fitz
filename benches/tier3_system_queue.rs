use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_bench_queue_actor;
use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;

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

fn bench_sustained_load_throughput(c: &mut Criterion) {
    //! SUSTAINED LOAD THROUGHPUT - Measure throughput under continuous load
    //!
    //! Scenario: Single queue, continuous enqueue + reserve batch
    //! Pattern: Sustained production load (no burst, steady state)
    //!
    //! Measures:
    //! - Steady-state throughput (msg/sec)
    //! - No GC pauses (Rust advantage)
    //! - In-memory write throughput (Midge)
    //! - Memory stability under load

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"sustained load message");

    let mut group = c.benchmark_group("system_queue_sustained");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100)); // 100 operations per iteration

    group.bench_function("sustained_100ops_enqueue_reserve", |b| {
        b.iter(|| {
            // Batch of 50 enqueues + 50 reserves (100 ops total)
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

fn bench_mixed_workload_realistic(c: &mut Criterion) {
    //! MIXED WORKLOAD REALISTIC - Measure throughput under realistic mix
    //!
    //! Scenario: 70% immediate, 20% delayed, 10% DLQ (realistic production mix)
    //! Pattern: Real-world queue usage (retries, scheduled tasks, failed messages)
    //!
    //! Measures:
    //! - Throughput under heterogeneous workload
    //! - Delayed message processing impact
    //! - DLQ policy overhead
    //! - BinaryHeap + VecDeque + HashMap coordination

    let mut actor = create_bench_queue_actor("bench", "system", "queue", Some(3)); // max_attempts=3
    let payload = Bytes::from_static(b"mixed workload message");

    let mut group = c.benchmark_group("system_queue_mixed_workload");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100)); // 100 messages per iteration

    group.bench_function("70_immediate_20_delayed_10_dlq", |b| {
        b.iter(|| {
            // 70 immediate messages
            for _ in 0..70 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            // 20 delayed messages
            for _ in 0..20 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(Some(5)));
            }

            // 10 messages that will cycle through DLQ path (reserve with short lease)
            for _ in 0..10 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }
            // Reserve with short lease to simulate expiry path
            let _ = actor.handle_reserve(black_box(1), black_box(Some(10)));
        })
    });

    group.finish();
}

fn bench_cold_start_recovery(c: &mut Criterion) {
    //! COLD START RECOVERY - Measure recovery time after restart
    //!
    //! Scenario: Pre-fill queue, drop actor, respawn, measure recovery
    //! Pattern: Crash recovery simulation (in-memory state reconstruction)
    //!
    //! Measures:
    //! - Midge load time (in-memory recovery)
    //! - Memory reconstruction cost
    //! - Time to first reserve after restart
    //! - Recovery throughput

    let mut group = c.benchmark_group("system_queue_cold_start");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(100)); // 100 messages recovered

    group.bench_function("recover_100_messages", |b| {
        b.iter_batched(
            || {
                // Setup: Create queue, fill with 100 messages, then drop
                let queue_key = QueueKey {
                    family: RouteFamily::new(1),
                    realm: "bench".to_string(),
                    area: "recovery".to_string(),
                    resource: "queue".to_string(),
                };

                // Use in-memory storage for benchmark speed
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

                drop(actor); // Simulate crash

                (store, queue_key)
            },
            |(store, queue_key)| {
                // Measure: Respawn actor (loads from Midge)
                let actor = QueueActor::new(RouteFamily::new(1), queue_key, store, None);

                black_box(actor);
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_high_contention_scenario(c: &mut Criterion) {
    //! HIGH CONTENTION SCENARIO - Measure performance under extreme contention
    //!
    //! Scenario: Small queue, high enqueue/reserve rate
    //! Pattern: Worst-case contention (queue often empty/full)
    //!
    //! Measures:
    //! - Performance when queue oscillates between empty and full
    //! - VecDeque performance under rapid push/pop
    //! - HashMap churn under rapid insert/remove

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");

    let mut group = c.benchmark_group("system_queue_high_contention");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(2)); // enqueue + reserve

    group.bench_function("oscillating_queue_depth", |b| {
        b.iter(|| {
            // Enqueue
            let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));

            // Immediate reserve (queue goes empty)
            let reserve_resp = actor.handle_reserve(black_box(30), black_box(Some(1)));

            // Complete if we got a message
            if let QueueResponse::Reserved { messages } = reserve_resp {
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
        bench_sustained_load_throughput,
        bench_mixed_workload_realistic,
        bench_cold_start_recovery,
        bench_high_contention_scenario,
}
criterion_main!(benches);
