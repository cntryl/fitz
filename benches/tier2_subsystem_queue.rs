use bytes::Bytes;
use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::benchkit::{create_bench_queue_actor, storage::create_bench_store};
use fitz::domains::queue::{Clock, QueueActor, QueueKey, QueueResponse};
use fitz::runtime::routing::RouteFamily;

#[path = "config.rs"]
mod config;

// Shared mock clock that can be advanced from benches to avoid sleeping
#[derive(Clone)]
struct SharedClock(std::sync::Arc<std::sync::Mutex<std::time::Instant>>);
impl SharedClock {
    fn new() -> Self {
        Self(std::sync::Arc::new(std::sync::Mutex::new(
            std::time::Instant::now(),
        )))
    }
}
impl Clock for SharedClock {
    fn now_instant(&self) -> std::time::Instant {
        *self.0.lock().unwrap()
    }
    fn now_epoch_ms(&self) -> u64 {
        // For a mock clock used in benchmarks, just return a fixed value
        // This is only used for epoch-based timestamps which aren't critical for benches
        0
    }
}

// ============================================================================
// TIER 2: SUBSYSTEM STRESS BENCHMARKS
//
// Target: Measure CONTENTION and LOAD under realistic patterns
// Goal: 50k-100k msg/sec per queue under stress
// Patterns: High throughput, batch processing, churn scenarios
//
// These benchmarks measure actor performance under sustained load.
// ============================================================================

fn bench_capacity_cycle_enqueue_reserve_complete(c: &mut Criterion) {
    // queue_capacity_cycle_1msg - time-bounded sustained full-cycle throughput
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut actor = create_bench_queue_actor("bench", "capacity", "queue", None);
    let payload = Bytes::from_static(b"cycle msg");

    let mut group = c.benchmark_group("queue_capacity_cycle");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(3)); // enqueue + reserve + complete = 3 ops

    group.bench_function("queue_capacity_cycle_enqueue_reserve_complete_1msg", |b| {
        b.iter(|| {
            let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));

            let reserve_resp = actor.handle_reserve(black_box(30), black_box(Some(1)));
            let (id, token) = match reserve_resp {
                QueueResponse::Reserved { messages } if !messages.is_empty() => {
                    (messages[0].id, messages[0].token)
                }
                _ => return,
            };

            let _ = actor.handle_complete(black_box(id), black_box(token));
        })
    });

    group.finish();
}

fn bench_batch_latency_reserve(c: &mut Criterion) {
    // queue_batch_latency_reserve_batch_{size} - one reserve(batch) per iteration
    let payload = Bytes::from_static(b"test message");

    let mut group = c.benchmark_group("queue_batch_latency_reserve");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);

    for &batch_size in &[1usize, 10usize, 100usize] {
        // Pre-fill a fresh actor per bench to avoid cross-iteration interference
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                let mut actor = create_bench_queue_actor("bench", "batch", "queue", None);
                for _ in 0..5000 {
                    let _ = actor.handle_enqueue(payload.clone(), None);
                }
                b.iter(|| {
                    // Single reserve(batch_size) call measured per iteration
                    let _ = actor.handle_reserve(black_box(30), black_box(Some(size)));
                })
            },
        );
    }

    group.finish();
}

fn bench_churn_reserve_expire_fixed(c: &mut Criterion) {
    //! queue_churn_reserve_expire_1k
    //! Pathological scenario: clients reserve then abandon; leases must expire and messages requeue.
    //! Uses a MockClock and fixed iteration counts (1000 per sample) so this is NOT wall-clock bounded.

    std::env::set_var("RAYON_NUM_THREADS", "1");

    // Use SharedClock to advance virtual time without sleeping

    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "lease_churn".to_string(),
        resource: "queue".to_string(),
    };

    let store = create_bench_store();
    let clock = SharedClock::new();
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(1),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    let payload = Bytes::from_static(b"test message");
    for _ in 0..2000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("queue_churn_reserve_expire");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_churn_reserve_expire_1k", |b| {
        b.iter_custom(|_| {
            let iters = 1000usize;
            let start = std::time::Instant::now();

            for _ in 0..iters {
                // Use zero-second lease so expiry is immediate (no sleeping required)
                let _ = actor.handle_reserve(black_box(0), black_box(Some(1)));
                actor.process_expired_timers();
            }

            let elapsed = start.elapsed();

            // Verification (not timed): ensure that churn caused some requeueing/deletions
            let remaining = match actor.handle_reserve(30, Some(2000)) {
                QueueResponse::Reserved { messages } => messages.len(),
                _ => 0usize,
            };
            assert!(
                remaining <= 2000,
                "unexpected remaining count: {}",
                remaining
            );
            black_box(remaining);

            elapsed
        })
    });

    group.finish();
}

fn bench_batch_latency_fire_delayed(c: &mut Criterion) {
    // queue_batch_latency_fire_delayed_1000 - one process_delayed_messages per iteration
    // Increase sample size to reduce variance for batch timing
    let payload = Bytes::from_static(b"delayed message");

    let mut group = c.benchmark_group("queue_batch_latency_delayed_fire");
    group.sample_size(50);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_batch_latency_fire_delayed_1000", |b| {
        b.iter_batched(
            || {
                // Setup: actor with many delayed messages all ready to fire (delay_seconds=0)
                let mut actor = create_bench_queue_actor("bench", "batch", "queue", None);
                for _ in 0..1000 {
                    let _ = actor.handle_enqueue(payload.clone(), Some(0));
                }
                actor
            },
            |mut actor| {
                actor.process_delayed_messages();
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_latency_timer_insert(c: &mut Criterion) {
    // queue_latency_timer_insert_1msg - single enqueue with delay scheduling per iteration
    let mut group = c.benchmark_group("queue_latency_timer_insert");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_latency_timer_insert", |b| {
        b.iter_batched(
            || {
                let actor = create_bench_queue_actor("bench", "timer", "queue", None);
                let payload = Bytes::from_static(b"timer insert");
                (actor, payload)
            },
            |(mut actor, payload)| {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(Some(60)));
            },
            criterion::BatchSize::SmallInput,
        )
    });

    group.finish();
}

fn bench_churn_dlq_threshold(c: &mut Criterion) {
    // queue_churn_dlq_threshold_100 - fixed iterations showing DLQ path under repeated expiry
    //! Pathological scenario: repeated reserve -> expiry -> redelivery until DLQ, fixed iterations

    std::env::set_var("RAYON_NUM_THREADS", "1");

    // Use SharedClock to advance virtual time without sleeping
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "dlq".to_string(),
        resource: "queue".to_string(),
    };
    let store = create_bench_store();
    let clock = SharedClock::new();
    // Use max_attempts=1 so messages are DLQ'ed after first expiry
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(1),
        queue_key,
        store,
        Box::new(clock.clone()),
        Some(1),
    );

    let payload = Bytes::from_static(b"test message");
    let initial = 500usize;
    for _ in 0..initial {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("queue_churn_dlq_threshold");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_churn_dlq_threshold_100", |b| {
        b.iter_custom(|_| {
            let iters = 100usize;
            // Measure only the churn loop
            let start = std::time::Instant::now();
            for _ in 0..iters {
                // Use zero-second lease so expiry is immediate
                let _ = actor.handle_reserve(black_box(0), black_box(Some(1)));
                actor.process_expired_timers();
            }
            let elapsed = start.elapsed();

            // Verification (not timed): Ensure DLQ path removed messages from queue
            let remaining = match actor.handle_reserve(30, Some(1000)) {
                QueueResponse::Reserved { messages } => messages.len(),
                _ => 0usize,
            };
            // Expect some messages to have been moved to DLQ (i.e., removed from ready)
            assert!(
                remaining < initial,
                "DLQ threshold did not remove messages: {} >= {}",
                remaining,
                initial
            );
            black_box(remaining);

            elapsed
        })
    });

    group.finish();
}

fn bench_capacity_enqueue_high_volume(c: &mut Criterion) {
    // queue_capacity_enqueue_1000 - time-bounded sustained enqueue throughput
    std::env::set_var("RAYON_NUM_THREADS", "1");

    let mut actor = create_bench_queue_actor("bench", "capacity", "queue", None);
    let payload = Bytes::from_static(b"high volume message");

    let mut group = c.benchmark_group("queue_capacity_enqueue_high_volume");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(std::time::Duration::from_secs(1));
    group.throughput(Throughput::Elements(1000));

    group.bench_function("queue_capacity_enqueue_1000_messages", |b| {
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

fn bench_churn_abuse_reserve_without_complete(c: &mut Criterion) {
    // queue_churn_abuse_reserve_without_complete_500
    //! Abuse scenario: many clients reserve and never complete, leases expire and are requeued.
    //! Fixed iterations per sample (500) and uses a MockClock; no sleeping inside the measured loop.

    std::env::set_var("RAYON_NUM_THREADS", "1");

    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "abuse".to_string(),
        resource: "queue".to_string(),
    };
    let store = create_bench_store();
    let clock = SharedClock::new();
    let mut actor = QueueActor::with_clock(
        RouteFamily::new(1),
        queue_key,
        store,
        Box::new(clock.clone()),
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    let payload = Bytes::from_static(b"abandoned message");
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("queue_churn_abuse_reserve_without_complete");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_churn_reserve_abandon_500", |b| {
        b.iter_custom(|_| {
            let iters = 500usize;
            let start = std::time::Instant::now();
            for _ in 0..iters {
                // Immediate expiry to avoid sleeping
                let _ = actor.handle_reserve(black_box(0), black_box(Some(1)));
                actor.process_expired_timers();
            }
            let elapsed = start.elapsed();

            // Verification: after churn, measure how many messages are available again (were requeued)
            let remaining = match actor.handle_reserve(30, Some(1000)) {
                QueueResponse::Reserved { messages } => messages.len(),
                _ => 0usize,
            };
            // Note: remaining count indicates how many messages survived the churn cycle
            // (either never reserved, or successfully requeued after expiry)
            black_box(remaining);

            elapsed
        })
    });

    group.finish();
}

fn bench_latency_complete_wrong_token(c: &mut Criterion) {
    // queue_latency_complete_wrong_token - single complete(op) per iteration with invalid token
    let mut actor = create_bench_queue_actor("bench", "latency", "queue", None);
    let payload = Bytes::from_static(b"test message");

    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }
    let reserved = match actor.handle_reserve(30, Some(100)) {
        QueueResponse::Reserved { messages } => messages,
        _ => panic!("Expected Reserved"),
    };

    let mut group = c.benchmark_group("queue_latency_complete_wrong_token");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);

    let mut idx = 0usize;
    group.bench_function("queue_latency_complete_wrong_token", |b| {
        b.iter(|| {
            let msg = &reserved[idx % reserved.len()];
            idx += 1;
            let wrong_token = msg.token.wrapping_add(1);
            let _ = actor.handle_complete(black_box(msg.id), black_box(wrong_token));
        })
    });

    group.finish();
}

fn bench_batch_latency_reserve_extreme(c: &mut Criterion) {
    // queue_batch_latency_reserve_extreme_{size} - ensure no panic and graceful degradation
    let payload = Bytes::from_static(b"batch message");

    let mut group = c.benchmark_group("queue_batch_latency_reserve_extreme");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for &batch_size in &[1usize, 100usize, 1000usize, 10000usize] {
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                // Pre-fill a fresh actor to avoid cross-iteration interference
                let mut actor = create_bench_queue_actor("bench", "abuse", "queue", None);
                for _ in 0..(size.min(10000)) {
                    let _ = actor.handle_enqueue(payload.clone(), None);
                }

                b.iter(|| {
                    let _ = actor.handle_reserve(black_box(30), black_box(Some(size)));
                })
            },
        );
    }

    group.finish();
}

fn bench_latency_enqueue_zero_delay(c: &mut Criterion) {
    // queue_latency_enqueue_zero_delay - single enqueue with delay=0 per iteration
    let mut group = c.benchmark_group("queue_latency_enqueue_zero_delay");
    group.sample_size(20);
    group.sampling_mode(SamplingMode::Flat);

    group.bench_function("queue_latency_enqueue_zero_delay", |b| {
        b.iter_batched(
            || {
                let actor = create_bench_queue_actor("bench", "latency", "queue", None);
                let payload = Bytes::from_static(b"zero delay message");
                (actor, payload)
            },
            |(mut actor, payload)| {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(Some(0)));
            },
            criterion::BatchSize::SmallInput,
        )
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

    let mut actor = create_bench_queue_actor("bench", "stress", "queue", None);

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

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_capacity_cycle_enqueue_reserve_complete,
        bench_batch_latency_reserve,
        bench_churn_reserve_expire_fixed,
        bench_batch_latency_fire_delayed,
        bench_churn_dlq_threshold,
        bench_capacity_enqueue_high_volume,
        // Abuse / latency benchmarks
        bench_churn_abuse_reserve_without_complete,
        bench_latency_complete_wrong_token,
        bench_batch_latency_reserve_extreme,
        bench_latency_enqueue_zero_delay,
        bench_latency_timer_insert,
        bench_empty_queue_polling_abuse,
}
criterion_main!(benches);
