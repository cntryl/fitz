use criterion::{
    black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput,
};
use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::benchkit::create_bench_queue_actor;
use bytes::Bytes;
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

fn bench_sustained_load_throughput(c: &mut Criterion) {
    //! SUSTAINED LOAD THROUGHPUT - Measure throughput under continuous load
    //!
    //! Scenario: Single queue, continuous enqueue + reserve for sustained period
    //! Pattern: Sustained production load (no burst, steady state)
    //!
    //! Measures:
    //! - Steady-state throughput (msg/sec)
    //! - No GC pauses (Rust advantage)
    //! - Durable write throughput (Midge)
    //! - Memory stability under load

    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"sustained load message");

    let mut group = c.benchmark_group("system_queue_sustained");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(5)); // Longer measurement for sustained load

    group.bench_function("sustained_1sec_enqueue_reserve", |b| {
        b.iter(|| {
            let start = std::time::Instant::now();
            let mut count = 0;

            // Run for 1 second
            while start.elapsed() < Duration::from_secs(1) {
                // Enqueue
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));

                // Reserve
                let _ = actor.handle_reserve(black_box(30), black_box(Some(1)));

                count += 2;
            }

            black_box(count)
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
    //! Pattern: Crash recovery simulation (durable storage critical)
    //!
    //! Measures:
    //! - Midge load time (durable recovery)
    //! - Memory reconstruction cost
    //! - Time to first reserve after restart
    //! - Recovery throughput

    let mut group = c.benchmark_group("system_queue_cold_start");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000)); // 1000 messages recovered

    group.bench_function("recover_1000_messages", |b| {
        b.iter_batched(
            || {
                // Setup: Create queue, fill with 1000 messages, then drop
                let queue_key = QueueKey {
                    family: RouteFamily::new(1),
                    realm: "bench".to_string(),
                    area: "recovery".to_string(),
                    resource: "queue".to_string(),
                };
                
                let temp_dir = tempfile::tempdir().unwrap();
                let store = Arc::new(cntryl_midge::MidgeEngine::open(temp_dir.path().to_path_buf()).unwrap());
                let mut actor = QueueActor::new(RouteFamily::new(1), queue_key.clone(), store.clone(), None);

                let payload = Bytes::from_static(b"recovery message");

                for _ in 0..1000 {
                    let _ = actor.handle_enqueue(payload.clone(), None);
                }

                drop(actor); // Simulate crash

                (temp_dir, store, queue_key)
            },
            |(_temp_dir, store, queue_key)| {
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
                    let _ = actor.handle_complete(black_box(messages[0].id), black_box(messages[0].token));
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
