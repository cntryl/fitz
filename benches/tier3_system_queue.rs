use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::{
    build_queue_subscribe, create_bench_queue_actor, create_bench_queue_sink,
    extract_single_tlv_field, register_session_counting_sink, route_frame, CountingSink,
};
use fitz::domains::queue::{QueueActor, QueueKey};
use fitz::protocol::frame::ChannelId;
use fitz::runtime::envelope::Envelope;
use fitz::runtime::router::{MailboxSink, Router};
use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz::runtime::DomainPublishEvent;
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::Arc;

const CLIENT_SESSION_ID: u64 = 1;

fn setup_queue_sink() -> (Arc<Router>, RouteFamily, RouteAddress, Arc<CountingSink>) {
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let sink = create_bench_queue_sink(router.clone());
    router.register_domain_pattern("queue", sink as Arc<dyn MailboxSink>);
    let (source, inbox) = register_session_counting_sink(&router, family, CLIENT_SESSION_ID);
    (router, family, source, inbox)
}

fn subscribe_queue(
    router: &Arc<Router>,
    family: RouteFamily,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    pattern: &str,
) {
    let subscribe_frame = build_queue_subscribe(pattern);
    let (msg_type, payload) = extract_single_tlv_field(&subscribe_frame);
    route_frame(
        router.as_ref(),
        source,
        destination,
        session_id,
        ChannelId::Pub,
        msg_type,
        payload,
        family,
    )
    .expect("queue subscribe");
}

// Queue domain tier 3 system benchmarks using stress
//
// Sustained queue operations under realistic scenarios.
// Tests enqueue, reserve, and complete operations at system scale.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

#[stress_test]
fn should_complete_capacity_sustained_load(ctx: &mut StressContext) {
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
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _ = actor.handle_send_batch(&batch_50);
        for _ in 0..50 {
            let _ = actor.handle_receive(30, Some(1));
        }
    });
    ctx.set_elements(100 * iterations as u64); // 50 enqueue + 50 reserve
}

#[stress_test]
fn should_complete_capacity_mixed_workload(ctx: &mut StressContext) {
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
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _ = actor.handle_send_batch(&batch_mixed);
        let _ = actor.handle_receive(1, Some(10));
    });
    ctx.set_elements(100 * iterations as u64); // 70 + 20 + 10 enqueues
}

#[stress_test]
fn should_complete_capacity_cold_start_recovery(ctx: &mut StressContext) {
    ctx.tag("scenario", "cold_start_recovery");

    // Setup: Create store and pre-populate with messages
    let queue_key = QueueKey {
        family: RouteFamily::new(1),
        realm: "bench".to_string(),
        area: "recovery".to_string(),
        resource: "queue".to_string(),
    };

    let store = create_test_engine_with_cfs(vec![1]);

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
    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _actor = QueueActor::new(
            RouteFamily::new(1),
            queue_key.clone(),
            store.clone(),
            None,
            fitz::utils::idempotency::global_dedup_store(),
        );
    });
    ctx.set_elements(100 * iterations as u64); // 100 messages recovered
}

#[stress_test]
fn should_complete_capacity_high_contention(ctx: &mut StressContext) {
    ctx.tag("scenario", "high_contention");

    // Setup: One actor (one hot queue), same batch pattern as sustained_load for comparable throughput
    let mut actor = create_bench_queue_actor("bench", "system", "queue", None);
    let payload = Bytes::from_static(b"contention message");
    let payloads: Vec<Bytes> = (0..50).map(|_| payload.clone()).collect();
    let batch_50: Vec<(Bytes, Option<u64>)> = payloads.iter().map(|p| (p.clone(), None)).collect();

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _ = actor.handle_send_batch(&batch_50);
        for _ in 0..50 {
            let _ = actor.handle_receive(30, Some(1));
        }
    });
    ctx.set_elements(100 * iterations as u64); // 50 enqueue + 50 reserve on one hot queue
}

#[stress_test]
fn should_complete_publish_fanout_with_subscribers(ctx: &mut StressContext) {
    ctx.tag("scenario", "publish_fanout");

    let (router, family, source, _inbox) = setup_queue_sink();
    let route = "queue://bench/system/fanout";
    let publish_route = Route::new(route);

    for session_id in 2..18 {
        subscribe_queue(&router, family, &source, route, session_id, route);
    }

    let publish_event = DomainPublishEvent::new(
        family,
        publish_route.clone(),
        Bytes::from_static(b"queue fanout payload"),
    );

    let iterations = ctx.measure_for(std::time::Duration::from_secs(5), || {
        let _ = router.route(Envelope::new(
            RouteAddress::new(family, publish_route.clone()),
            publish_event.clone(),
        ));
    });
    ctx.set_elements(10 * iterations as u64);
}

stress_main!();

