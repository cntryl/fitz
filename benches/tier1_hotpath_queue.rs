use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
use fitz::domains::queue::QueueResponse;
use fitz::benchkit::create_bench_queue_actor;
use bytes::Bytes;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 1: HOT PATH MICROBENCHMARKS
//
// Target: Measure PURE actor operations WITHOUT scheduler/mailbox overhead
// Goal: <5Âµs p50 for enqueue, <10Âµs p50 for reserve, <5Âµs p50 for complete
// Throughput: 100k-300k msg/sec per queue
//
// These benchmarks call actor methods directly to measure the hot path.
// ============================================================================

fn bench_enqueue_only(c: &mut Criterion) {
    //! ENQUEUE ONLY - Measure pure enqueue throughput
    //!
    //! Target: <5Âµs p50 latency, 200k+ msg/sec throughput
    //!
    //! Measures:
    //! - Serialization cost [attempts:4][visible_at_ms:8][body_len:4][body]
    //! - Midge durable write cost
    //! - VecDeque push cost
    //! - NO reservation or completion

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"test message payload");

    let mut group = c.benchmark_group("hotpath_queue_enqueue");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("enqueue_no_delay", |b| {
        b.iter(|| {
            let _result = actor.handle_enqueue(
                black_box(payload.clone()),
                black_box(None), // No delay
            );
        })
    });

    group.finish();
}

fn bench_reserve_only_empty(c: &mut Criterion) {
    //! RESERVE ONLY (EMPTY QUEUE) - Measure reserve on empty queue
    //!
    //! Target: <1Âµs p50 latency (fast path: empty check)
    //!
    //! Measures:
    //! - Empty queue check cost
    //! - NO serialization, NO Midge reads
    //! - Fastest possible reserve path

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);

    let mut group = c.benchmark_group("hotpath_queue_reserve");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_empty_queue", |b| {
        b.iter(|| {
            let _result = actor.handle_reserve(
                black_box(30),         // lease_seconds
                black_box(Some(1)),    // batch_size
            );
        })
    });

    group.finish();
}

fn bench_reserve_only_full(c: &mut Criterion) {
    //! RESERVE ONLY (FULL QUEUE) - Measure reserve on pre-filled queue
    //!
    //! Target: <10Âµs p50 latency (includes VecDeque pop, HashMap insert)
    //!
    //! Measures:
    //! - VecDeque pop_front cost
    //! - HashMap insert cost (inflight tracking)
    //! - Token generation cost
    //! - NO Midge reads (already in memory)

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"test message payload");

    // Pre-fill queue with 1000 messages
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("hotpath_queue_reserve");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("reserve_full_queue", |b| {
        b.iter(|| {
            let _result = actor.handle_reserve(
                black_box(30),         // lease_seconds
                black_box(Some(1)),    // batch_size
            );
        })
    });

    group.finish();
}

fn bench_complete_only(c: &mut Criterion) {
    //! COMPLETE ONLY - Measure completion throughput
    //!
    //! Target: <5Âµs p50 latency
    //!
    //! Measures:
    //! - HashMap remove cost (inflight tracking)
    //! - Token validation cost
    //! - Midge delete cost (durable removal)

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"test message payload");

    // Pre-fill queue and reserve messages to generate tokens
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    // Reserve 1000 messages to get tokens
    let reserved = match actor.handle_reserve(30, Some(1000)) {
        QueueResponse::Reserved { messages } => messages,
        _ => panic!("Expected Reserved response"),
    };
    let tokens: Vec<(u64, u64)> = reserved.iter().map(|r| (r.id.as_u64(), r.token)).collect();

    let mut group = c.benchmark_group("hotpath_queue_complete");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    let mut token_idx = 0;
    group.bench_function("complete_with_token", |b| {
        b.iter(|| {
            let (id, token) = tokens[token_idx % tokens.len()];
            token_idx += 1;
            let _result = actor.handle_complete(
                black_box(fitz::domains::queue::MessageId::new(id)),
                black_box(token),
            );
        })
    });

    group.finish();
}

fn bench_delayed_enqueue_fire(c: &mut Criterion) {
    //! DELAYED ENQUEUE + FIRE - Measure delayed message processing
    //!
    //! Target: <10Âµs p50 latency (includes BinaryHeap pop + VecDeque push)
    //!
    //! Measures:
    //! - BinaryHeap pop cost (delayed queue)
    //! - VecDeque push cost (ready queue)
    //! - Timestamp comparison overhead
    //! - NO Midge reads (already in memory)

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"delayed message");

    // Enqueue 1000 delayed messages (all ready to fire)
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), Some(0)); // delay_seconds=0
    }

    let mut group = c.benchmark_group("hotpath_queue_delayed");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("fire_delayed_messages", |b| {
        b.iter(|| {
            // Process expired delayed messages (moves from BinaryHeap to VecDeque)
            actor.process_delayed_messages();
        })
    });

    group.finish();
}

fn bench_lease_expiry_requeue(c: &mut Criterion) {
    //! LEASE EXPIRY + REQUEUE - Measure lease expiration handling
    //!
    //! Target: <10Âµs p50 latency
    //!
    //! Measures:
    //! - BinaryHeap pop cost (timers)
    //! - HashMap remove cost (inflight)
    //! - VecDeque push cost (requeue to ready)
    //! - Midge update cost (increment attempts)

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"test message");

    // Pre-fill and reserve to create expiring leases
    for _ in 0..1000 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }
    let _ = actor.handle_reserve(1, Some(1000)); // 1-second lease

    // Wait for leases to expire (simulate time passage)
    std::thread::sleep(std::time::Duration::from_secs(2));

    let mut group = c.benchmark_group("hotpath_queue_expiry");
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(1));

    group.bench_function("requeue_expired_lease", |b| {
        b.iter(|| {
            // Process expired timers (moves from inflight to ready)
            actor.process_expired_timers();
        })
    });

    group.finish();
}

fn bench_batch_reserve_scaling(c: &mut Criterion) {
    //! BATCH RESERVE SCALING - Measure reserve throughput vs batch size
    //!
    //! Target: Linear scaling up to batch_size=100
    //!
    //! Measures:
    //! - Reserve cost scaling with batch_size 1, 10, 100
    //! - VecDeque bulk pop efficiency
    //! - HashMap bulk insert efficiency

    let mut actor = create_bench_queue_actor("bench", "test", "queue", None);
    let payload = Bytes::from_static(b"test message");

    let mut group = c.benchmark_group("hotpath_queue_batch_reserve");
    group.sampling_mode(SamplingMode::Flat);

    for batch_size in [1, 10, 100] {
        // Pre-fill queue for this batch size
        for _ in 0..1000 {
            let _ = actor.handle_enqueue(payload.clone(), None);
        }

        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let _result = actor.handle_reserve(
                        black_box(30),
                        black_box(Some(size as usize)),
                    );
                })
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = config::criterion_config();
    targets =
        bench_enqueue_only,
        bench_reserve_only_empty,
        bench_reserve_only_full,
        bench_complete_only,
        bench_delayed_enqueue_fire,
        bench_lease_expiry_requeue,
        bench_batch_reserve_scaling,
}
criterion_main!(benches);
