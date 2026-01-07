use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
use fitz::runtime::routing::RouteFamily;
use bytes::Bytes;
use std::sync::Arc;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 2: SUBSYSTEM STRESS BENCHMARKS
//
// Target: Measure CONTENTION and LOAD under realistic patterns
// Goal: 50k-100k msg/sec per queue under stress
// Patterns: High throughput, batch processing, churn scenarios
//
// These benchmarks measure actor performance under sustained load.
// ============================================================================

/// Helper to create a QueueActor with temporary storage
fn create_queue_actor(max_attempts: Option<u32>) -> QueueActor {
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "stress".to_string(),
        resource: "queue".to_string(),
    };
    
    let temp_dir = tempfile::tempdir().unwrap();
    let store = Arc::new(cntryl_midge::MidgeEngine::open(temp_dir.path().to_path_buf()).unwrap());
    QueueActor::new(RouteFamily::new(1), queue_key, store, max_attempts)
}

fn bench_enqueue_reserve_complete_loop(c: &mut Criterion) {
    //! ENQUEUE + RESERVE + COMPLETE LOOP - Measure full queue cycle under stress
    //!
    //! Pattern: Sustained load with full message lifecycle
    //! Stress: Maximum message throughput through all operations
    //!
    //! Measures:
    //! - Full cycle throughput (enqueue → reserve → complete)
    //! - Actor performance under sustained load
    //! - Memory stability under churn

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"test message");

    let mut group = c.benchmark_group("subsystem_queue_churn");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(3)); // enqueue + reserve + complete = 1 cycle

    group.bench_function("enqueue_reserve_complete_cycle", |b| {
        b.iter(|| {
            // Enqueue
            let enqueue_resp = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            let _msg_id = match enqueue_resp {
                QueueResponse::Enqueued { id } => id,
                _ => panic!("Expected Enqueued"),
            };

            // Reserve
            let reserve_resp = actor.handle_reserve(black_box(30), black_box(Some(1)));
            let (id, token) = match reserve_resp {
                QueueResponse::Reserved { messages } if !messages.is_empty() => {
                    (messages[0].id, messages[0].token)
                },
                _ => return, // Skip if empty
            };

            // Complete
            let _ = actor.handle_complete(black_box(id), black_box(token));
        })
    });

    group.finish();
}

fn bench_batch_reserve_stress(c: &mut Criterion) {
    //! BATCH RESERVE STRESS - Measure reserve throughput under batch load
    //!
    //! Pattern: Pre-fill queue, then sustained batch reserves
    //! Stress: Maximum reservation throughput
    //!
    //! Measures:
    //! - Batch reserve throughput at 1, 10, 100 batch sizes
    //! - VecDeque bulk pop efficiency
    //! - HashMap bulk insert efficiency

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"test message");

    // Pre-fill queue with 10000 messages
    for _ in 0..10000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("subsystem_queue_batch_reserve");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for batch_size in [1, 10, 100] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let _ = actor.handle_reserve(
                        black_box(30),
                        black_box(Some(size as usize)),
                    );
                })
            },
        );
    }

    group.finish();
}

fn bench_lease_churn_stress(c: &mut Criterion) {
    //! LEASE CHURN STRESS - Measure lease expiration under load
    //!
    //! Pattern: Reserve with short lease, let expire, repeat
    //! Stress: Maximum lease expiration throughput
    //!
    //! Measures:
    //! - Lease expiration handling throughput
    //! - Requeue performance under churn
    //! - Timer heap performance under load

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"test message");

    // Pre-fill queue
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("subsystem_queue_lease_churn");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_expire_cycle", |b| {
        b.iter(|| {
            // Reserve with very short lease (will expire quickly)
            let _ = actor.handle_reserve(black_box(1), black_box(Some(1)));
            
            // Simulate time passage (in real usage, timer would fire)
            std::thread::sleep(std::time::Duration::from_millis(10));
            
            // Process expired timers
            actor.process_expired_timers();
        })
    });

    group.finish();
}

fn bench_delayed_message_stress(c: &mut Criterion) {
    //! DELAYED MESSAGE STRESS - Measure delayed message throughput
    //!
    //! Pattern: Enqueue with delays, then process firing
    //! Stress: Maximum delayed message throughput
    //!
    //! Measures:
    //! - Delayed enqueue throughput
    //! - BinaryHeap performance under load
    //! - Delayed → ready transition cost

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"delayed message");

    let mut group = c.benchmark_group("subsystem_queue_delayed");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("enqueue_delayed_fire", |b| {
        b.iter(|| {
            // Enqueue with short delay
            let _ = actor.handle_enqueue(
                black_box(payload.clone()),
                black_box(Some(1)),  // 1-second delay
            );
            
            // Process delayed messages (simulate firing)
            actor.process_delayed_messages();
        })
    });

    group.finish();
}

fn bench_dlq_threshold_stress(c: &mut Criterion) {
    //! DLQ THRESHOLD STRESS - Measure DLQ policy under load
    //!
    //! Pattern: Reserve → expire repeatedly until DLQ threshold hit
    //! Stress: Maximum redelivery attempt throughput
    //!
    //! Measures:
    //! - Attempt tracking overhead
    //! - DLQ threshold detection cost
    //! - Midge delete cost on DLQ

    let mut actor = create_queue_actor(Some(3)); // max_attempts=3
    let payload = Bytes::from_static(b"test message");

    // Pre-fill queue
    for _ in 0..100 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("subsystem_queue_dlq");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_expire_dlq_cycle", |b| {
        b.iter(|| {
            // Reserve with short lease (will expire and increment attempts)
            let _ = actor.handle_reserve(black_box(1), black_box(Some(1)));
            
            // Simulate expiry
            std::thread::sleep(std::time::Duration::from_millis(10));
            actor.process_expired_timers();
        })
    });

    group.finish();
}

fn bench_high_volume_enqueue(c: &mut Criterion) {
    //! HIGH-VOLUME ENQUEUE - Measure sustained enqueue throughput
    //!
    //! Pattern: Continuous enqueuing without reserves
    //! Stress: Maximum write throughput to Midge
    //!
    //! Measures:
    //! - Durable write throughput
    //! - VecDeque scaling under load
    //! - Memory growth characteristics

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"high volume message");

    let mut group = c.benchmark_group("subsystem_queue_high_volume");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1000));

    group.bench_function("enqueue_1000_messages", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }
        })
    });

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_enqueue_reserve_complete_loop,
        bench_batch_reserve_stress,
        bench_lease_churn_stress,
        bench_delayed_message_stress,
        bench_dlq_threshold_stress,
        bench_high_volume_enqueue,
}
criterion_main!(benches);
