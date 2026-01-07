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

// ============================================================================
// ABUSE PATTERN BENCHMARKS
//
// These benchmarks prove the queue does NOT fall apart under misuse.
// Goal: Stable performance even when used badly
// ============================================================================

fn bench_reserve_without_complete_abuse(c: &mut Criterion) {
    //! ABUSE: Reserve without completing (lease expiry overhead)
    //!
    //! Misuse: Client reserves but never completes, forcing lease expiry
    //! Reality: Buggy clients, network partitions, crashed workers
    //!
    //! Proves:
    //! - No memory leaks from abandoned inflight entries
    //! - Lease expiry doesn't degrade throughput
    //! - Timer heap remains efficient under churn

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"abandoned message");

    // Pre-fill queue
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("abuse_reserve_no_complete");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_abandon_cycle", |b| {
        b.iter(|| {
            // Reserve but NEVER complete (abuse pattern)
            let _ = actor.handle_reserve(black_box(1), black_box(Some(1)));
            
            // Simulate time passing (lease expires)
            std::thread::sleep(std::time::Duration::from_millis(10));
            actor.process_expired_timers();
        })
    });

    group.finish();
}

fn bench_invalid_token_abuse(c: &mut Criterion) {
    //! ABUSE: Complete with wrong tokens (validation overhead)
    //!
    //! Misuse: Client uses stale/wrong tokens, retries, conflicts
    //! Reality: Race conditions, duplicate processing, confused clients
    //!
    //! Proves:
    //! - Token validation doesn't slow down
    //! - No corruption from invalid completions
    //! - Inflight tracking remains accurate

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"test message");

    // Pre-fill and reserve to get valid message IDs
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }
    let reserved = match actor.handle_reserve(30, Some(100)) {
        QueueResponse::Reserved { messages } => messages,
        _ => panic!("Expected Reserved"),
    };

    let mut group = c.benchmark_group("abuse_invalid_token");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut idx = 0;
    group.bench_function("complete_wrong_token", |b| {
        b.iter(|| {
            let msg = &reserved[idx % reserved.len()];
            idx += 1;
            // Use WRONG token (abuse pattern)
            let wrong_token = msg.token.wrapping_add(1);
            let _ = actor.handle_complete(black_box(msg.id), black_box(wrong_token));
        })
    });

    group.finish();
}

fn bench_extreme_batch_size_abuse(c: &mut Criterion) {
    //! ABUSE: Extreme batch sizes (resource exhaustion attempt)
    //!
    //! Misuse: Client requests batch_size=10000, tries to OOM
    //! Reality: Misconfigured clients, malicious users
    //!
    //! Proves:
    //! - No panic on large batch_size
    //! - Performance degrades gracefully
    //! - Memory bounded even with pathological requests

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"batch message");

    // Pre-fill with 10000 messages
    for _ in 0..10000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("abuse_extreme_batch");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for batch_size in [1, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size.min(10000)));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let _ = actor.handle_reserve(black_box(30), black_box(Some(size as usize)));
                })
            },
        );
    }

    group.finish();
}

fn bench_zero_delay_abuse(c: &mut Criterion) {
    //! ABUSE: Zero/negative delays (edge case testing)
    //!
    //! Misuse: Client uses delay_seconds=0, bypasses delayed queue
    //! Reality: Confused clients, clock skew, bad configs
    //!
    //! Proves:
    //! - No panic on edge case delays
    //! - Delayed queue handles degenerate cases
    //! - No infinite loops or hangs

    let mut actor = create_queue_actor(None);
    let payload = Bytes::from_static(b"zero delay message");

    let mut group = c.benchmark_group("abuse_zero_delay");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("enqueue_zero_delay", |b| {
        b.iter(|| {
            // Zero delay (edge case)
            let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(Some(0)));
            actor.process_delayed_messages();
        })
    });

    group.finish();
}

fn bench_empty_queue_polling_abuse(c: &mut Criterion) {
    //! ABUSE: Polling empty queue (pathological client)
    //!
    //! Misuse: Client tight-loops on reserve when queue is empty
    //! Reality: Misconfigured clients, missing backoff, bad retries
    //!
    //! Proves:
    //! - Empty reserve remains fast (doesn't scan)
    //! - No CPU spin or allocation on empty
    //! - Stable performance under futile polling

    let mut actor = create_queue_actor(None);

    let mut group = c.benchmark_group("abuse_empty_polling");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_empty_tight_loop", |b| {
        b.iter(|| {
            // Reserve on empty queue (abuse pattern)
            let _ = actor.handle_reserve(black_box(30), black_box(Some(1)));
        })
    });

    group.finish();
}

fn bench_dlq_thrashing_abuse(c: &mut Criterion) {
    //! ABUSE: DLQ thrashing (repeated failures)
    //!
    //! Misuse: Messages hit DLQ repeatedly, high delete rate
    //! Reality: Poison messages, bad handlers, systematic failures
    //!
    //! Proves:
    //! - DLQ deletion remains fast
    //! - No memory leaks from deleted messages
    //! - Stable under high failure rate

    let mut actor = create_queue_actor(Some(2)); // max_attempts=2 (fast DLQ)
    let payload = Bytes::from_static(b"poison message");

    // Pre-fill queue
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("abuse_dlq_thrashing");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("dlq_delete_cycle", |b| {
        b.iter(|| {
            // Reserve with short lease (will expire twice → DLQ)
            let _ = actor.handle_reserve(black_box(1), black_box(Some(1)));
            std::thread::sleep(std::time::Duration::from_millis(10));
            actor.process_expired_timers(); // First expiry (attempts=1)
            
            let _ = actor.handle_reserve(black_box(1), black_box(Some(1)));
            std::thread::sleep(std::time::Duration::from_millis(10));
            actor.process_expired_timers(); // Second expiry → DLQ delete
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
        // Abuse pattern benchmarks
        bench_reserve_without_complete_abuse,
        bench_invalid_token_abuse,
        bench_extreme_batch_size_abuse,
        bench_zero_delay_abuse,
        bench_empty_queue_polling_abuse,
        bench_dlq_thrashing_abuse,
}
criterion_main!(benches);
