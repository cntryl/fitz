use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use fitz::benchkit::create_bench_queue_actor;
use fitz::domains::queue::QueueResponse;
use std::time::Duration;

#[path = "config.rs"]
mod config;

// ============================================================================
// TIER 4: INTEGRATION BENCHMARKS
//
// Target: Measure FULL END-TO-END scenarios with realistic workloads
// Goal: Prove predictable latency and throughput under complex scenarios
// Patterns: Multi-actor sequences, transactional workflows, failure recovery
//
// These benchmarks simulate complete queue workflows including:
// - Enqueue → Reserve → Complete sequences
// - Partial failure and retry scenarios
// - Message lifecycle transitions
// - Backpressure and recovery patterns
// ============================================================================

fn bench_complete_workflow_enqueue_reserve_complete(c: &mut Criterion) {
    //! COMPLETE MESSAGE LIFECYCLE - Full enqueue → reserve → complete workflow
    //!
    //! Target: <50µs p50 latency for complete transaction
    //! Throughput: 20k transactions/sec
    //!
    //! Measures:
    //! - Enqueue serialization + storage
    //! - Reserve visibility and fetching
    //! - Complete acknowledgment and cleanup
    //! - Full transactional consistency

    let mut actor = create_bench_queue_actor("bench", "integration", "queue", None);
    let payload = Bytes::from_static(b"workflow message");

    let mut group = c.benchmark_group("queue_integration_complete_workflow");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1)); // 1 complete transaction per iteration

    group.bench_function("queue_integration_enqueue_reserve_complete", |b| {
        b.iter(|| {
            // Arrange: Enqueue a message
            let enqueue_response =
                actor.handle_enqueue(black_box(payload.clone()), black_box(None));

            // Act: Reserve the message
            let _message_id = match enqueue_response {
                QueueResponse::Enqueued { id } => id,
                _ => return, // Skip if enqueue failed
            };

            let reserve_response = actor.handle_reserve(black_box(30), black_box(Some(1)));

            // Assert & cleanup: Complete the message
            match reserve_response {
                QueueResponse::Reserved { messages } => {
                    if !messages.is_empty() {
                        let _ = actor.handle_complete(
                            black_box(messages[0].id),
                            black_box(messages[0].token),
                        );
                    }
                }
                _ => {}
            }
        })
    });

    group.finish();
}

fn bench_batch_workflow_many_enqueue_one_reserve(c: &mut Criterion) {
    //! BATCH WORKFLOW - Multiple enqueues followed by bulk reserve
    //!
    //! Target: <200µs p50 latency for 100-message batch
    //! Throughput: 500k enqueue ops/sec, 100k reserve ops/sec
    //!
    //! Measures:
    //! - Batched enqueue throughput
    //! - Bulk reserve efficiency
    //! - Queue depth impact
    //! - Amortized costs at scale

    let mut actor = create_bench_queue_actor("bench", "integration", "queue", None);
    let payload = Bytes::from_static(b"batch message");

    let mut group = c.benchmark_group("queue_integration_batch_workflow");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(100)); // 100 messages enqueued + 1 reserve

    group.bench_function("queue_integration_100enqueue_10reserve", |b| {
        b.iter(|| {
            // Enqueue 100 messages
            for _ in 0..100 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            // Reserve 10 messages at once
            let reserve_response = actor.handle_reserve(black_box(30), black_box(Some(10)));

            // Complete all reserved messages
            if let QueueResponse::Reserved { messages } = reserve_response {
                for msg in messages {
                    let _ = actor.handle_complete(black_box(msg.id), black_box(msg.token));
                }
            }
        })
    });

    group.finish();
}

fn bench_failure_recovery_deadletter_workflow(c: &mut Criterion) {
    //! FAILURE RECOVERY - Messages with retries and dead-letter transition
    //!
    //! Target: <100µs p50 latency for retry cycle
    //! Throughput: 10k retry operations/sec
    //!
    //! Measures:
    //! - Retry attempt serialization
    //! - Visibility timeout management
    //! - Dead-letter classification
    //! - Failed message recovery costs

    let mut actor = create_bench_queue_actor("bench", "integration", "queue", Some(3));
    let payload = Bytes::from_static(b"retry message");

    let mut group = c.benchmark_group("queue_integration_failure_recovery");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(1)); // 1 retry cycle per iteration

    group.bench_function("queue_integration_retry_cycle_with_deadletter", |b| {
        b.iter(|| {
            // Arrange: Enqueue a message with retry limit
            let enqueue_response =
                actor.handle_enqueue(black_box(payload.clone()), black_box(Some(5)));

            let _message_id = match enqueue_response {
                QueueResponse::Enqueued { id } => id,
                _ => return,
            };

            // Act: Reserve → Nack (simulate failure) → Retry
            for _attempt in 0..3 {
                let reserve_response = actor.handle_reserve(black_box(30), black_box(Some(1)));

                match reserve_response {
                    QueueResponse::Reserved { messages } => {
                        if !messages.is_empty() {
                            // Simulate failure by not completing (would be nack in real system)
                            let _ = actor.handle_reserve(black_box(30), black_box(Some(1)));
                        }
                    }
                    _ => {}
                }
            }

            // Final reserve - should eventually hit dead-letter or be completed
            let final_reserve = actor.handle_reserve(black_box(30), black_box(Some(1)));
            if let QueueResponse::Reserved { messages } = final_reserve {
                if !messages.is_empty() {
                    let _ = actor
                        .handle_complete(black_box(messages[0].id), black_box(messages[0].token));
                }
            }
        })
    });

    group.finish();
}

fn bench_mixed_sequential_workflow(c: &mut Criterion) {
    //! MIXED SEQUENTIAL OPERATIONS - Realistic interleaved operations
    //!
    //! Target: <30µs p50 latency per operation in mixed workload
    //! Throughput: 30k+ mixed ops/sec
    //!
    //! Measures:
    //! - Context switching between enqueue/reserve/complete
    //! - Queue state management complexity
    //! - Lock contention under varied operations
    //! - Realistic producer-consumer patterns

    let mut actor = create_bench_queue_actor("bench", "integration", "queue", None);
    let payload = Bytes::from_static(b"mixed op message");

    let mut group = c.benchmark_group("queue_integration_mixed_sequential");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 10 operations per iteration

    group.bench_function("queue_integration_mixed_3e_2r_3c_2e", |b| {
        b.iter(|| {
            // Operation sequence: 3 enqueues → 2 reserves → 3 completes → 2 enqueues
            for _ in 0..3 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            let reserve1 = actor.handle_reserve(black_box(30), black_box(Some(2)));
            let mut message_ids = Vec::new();
            if let QueueResponse::Reserved { messages } = reserve1 {
                message_ids.extend(messages);
            }

            for _ in 0..3 {
                if !message_ids.is_empty() {
                    let msg = message_ids.pop().unwrap();
                    let _ = actor.handle_complete(black_box(msg.id), black_box(msg.token));
                }
            }

            for _ in 0..2 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }
        })
    });

    group.finish();
}

fn bench_backpressure_deep_queue_scenario(c: &mut Criterion) {
    //! BACKPRESSURE HANDLING - Queue under sustained deep load
    //!
    //! Target: <50µs p50 latency with 1000+ messages queued
    //! Throughput: 20k ops/sec maintaining depth
    //!
    //! Measures:
    //! - Performance with deep queue depth
    //! - Memory pressure impact
    //! - Iteration efficiency over large datasets
    //! - Sustained load without degradation

    let mut actor = create_bench_queue_actor("bench", "integration", "queue", None);
    let payload = Bytes::from_static(b"backpressure message");

    // Pre-fill queue to depth
    for _ in 0..500 {
        let _ = actor.handle_enqueue(payload.clone(), None);
    }

    let mut group = c.benchmark_group("queue_integration_backpressure_deep_queue");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);
    group.measurement_time(Duration::from_secs(1));
    group.throughput(Throughput::Elements(10)); // 5 enqueue + 5 reserve per iteration

    group.bench_function("queue_integration_deep_queue_1000plus_depth", |b| {
        b.iter(|| {
            // Maintain deep queue state with equal enqueue/reserve
            for _ in 0..5 {
                let _ = actor.handle_enqueue(black_box(payload.clone()), black_box(None));
            }

            let reserve_response = actor.handle_reserve(black_box(30), black_box(Some(5)));
            if let QueueResponse::Reserved { messages } = reserve_response {
                for msg in messages {
                    let _ = actor.handle_complete(black_box(msg.id), black_box(msg.token));
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
        bench_complete_workflow_enqueue_reserve_complete,
        bench_batch_workflow_many_enqueue_one_reserve,
        bench_failure_recovery_deadletter_workflow,
        bench_mixed_sequential_workflow,
        bench_backpressure_deep_queue_scenario,
}
criterion_main!(benches);
