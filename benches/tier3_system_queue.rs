use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::create_bench_queue_actor;
use fitz::domains::queue::{QueueActor, QueueKey};
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;

// Queue domain tier 3 system benchmarks using stress
//
// Sustained queue operations under realistic scenarios.
// Tests enqueue, reserve, and complete operations at system scale.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

#[stress_test]
fn should_complete_capacity_sustained_load(ctx: &mut StressContext) {
    ctx.set_elements(100); // 50 enqueue + 50 reserve
    ctx.tag("scenario", "sustained_load");

    // Setup: Create actor and precompute payloads outside measurement
    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"sustained load message");

    // Precompute 50 payload instances to avoid clones in hot path
    let payloads: Vec<Bytes> = (0..50).map(|_| payload.clone()).collect();

    let batch_50: Vec<(Bytes, Option<u64>)> = payloads
        .iter()
        .take(50)
        .map(|p| (p.clone(), None))
        .collect();
    ctx.measure(|| {
        let _ = actor.handle_send_batch(&batch_50);
        for _ in 0..50 {
            let _ = actor.handle_receive(30, Some(1));
        }
    });
}

#[stress_test]
fn should_complete_capacity_mixed_workload(ctx: &mut StressContext) {
    ctx.set_elements(100); // 70 + 20 + 10 enqueues
    ctx.tag("scenario", "mixed_workload");

    // Setup: Create actor and precompute payloads outside measurement
    let mut actor = create_bench_queue_actor("bench", "system", "queue", Some(3));
    let payload = Bytes::from_static(b"mixed workload message");

    // Precompute 100 payload instances to avoid clones in hot path
    let payloads: Vec<Bytes> = (0..100).map(|_| payload.clone()).collect();

    let batch_mixed: Vec<(Bytes, Option<u64>)> = payloads
        .iter()
        .take(70)
        .map(|p| (p.clone(), None))
        .chain(
            payloads
                .iter()
                .skip(70)
                .take(20)
                .map(|p| (p.clone(), Some(5))),
        )
        .chain(payloads.iter().skip(90).take(10).map(|p| (p.clone(), None)))
        .collect();
    ctx.measure(|| {
        let _ = actor.handle_send_batch(&batch_mixed);
        let _ = actor.handle_receive(1, Some(10));
    });
}

#[stress_test]
fn should_complete_capacity_cold_start_recovery(ctx: &mut StressContext) {
    ctx.set_elements(100); // 100 messages recovered
    ctx.tag("scenario", "cold_start_recovery");

    // Setup: Create store and pre-populate with messages
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

    // Pre-populate with 100 messages
    let mut pre_actor = QueueActor::new(
        RouteFamily::new(1),
        queue_key.clone(),
        store.clone(),
        None,
        fitz::utils::idempotency::global_dedup_store(),
    );

    let payload = Bytes::from_static(b"recovery message");
    for _ in 0..100 {
        let _ = pre_actor.handle_send(payload.clone(), None);
    }
    drop(pre_actor);

    // Measure: Recover actor from populated store
    ctx.measure(|| {
        let _actor = QueueActor::new(
            RouteFamily::new(1),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::global_dedup_store(),
        );
    });
}

#[stress_test]
fn should_complete_capacity_high_contention(ctx: &mut StressContext) {
    ctx.set_elements(100); // 50 enqueue + 50 reserve on one hot queue
    ctx.tag("scenario", "high_contention");

    // Setup: One actor (one hot queue), same batch pattern as sustained_load for comparable throughput
    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");
    let payloads: Vec<Bytes> = (0..50).map(|_| payload.clone()).collect();
    let batch_50: Vec<(Bytes, Option<u64>)> = payloads.iter().map(|p| (p.clone(), None)).collect();

    ctx.measure(|| {
        let _ = actor.handle_send_batch(&batch_50);
        for _ in 0..50 {
            let _ = actor.handle_receive(30, Some(1));
        }
    });
}

stress_main!();
