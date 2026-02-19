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

    ctx.measure(|| {
        // 50 enqueue operations using precomputed payloads
        for i in 0..50 {
            let _ = actor.handle_enqueue(payloads[i].clone(), None);
        }

        // 50 reserve operations
        for _ in 0..50 {
            let _ = actor.handle_reserve(30, Some(1));
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

    ctx.measure(|| {
        // 70 immediate enqueues
        for i in 0..70 {
            let _ = actor.handle_enqueue(payloads[i].clone(), None);
        }

        // 20 delayed enqueues (delay=5)
        for i in 70..90 {
            let _ = actor.handle_enqueue(payloads[i].clone(), Some(5));
        }

        // 10 more enqueues
        for i in 90..100 {
            let _ = actor.handle_enqueue(payloads[i].clone(), None);
        }

        // 1 reserve
        let _ = actor.handle_reserve(1, Some(10));
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
        let _ = pre_actor.handle_enqueue(payload.clone(), None);
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
    ctx.set_elements(2); // enqueue + reserve
    ctx.tag("scenario", "high_contention");

    // Setup: Create actor and payload outside measurement
    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");

    ctx.measure(|| {
        // Enqueue
        let _ = actor.handle_enqueue(payload.clone(), None);

        // Reserve
        let reserve_resp = actor.handle_reserve(30, Some(1));

        // Complete if message exists
        if let fitz::domains::queue::QueueResponse::Reserved { messages } = reserve_resp {
            if !messages.is_empty() {
                let _ = actor.handle_complete(messages[0].id, messages[0].token);
            }
        }
    });
}

stress_main!();
