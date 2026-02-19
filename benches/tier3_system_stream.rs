// Stream domain tier 3 system benchmarks using stress
//
// Append, read, batch write, multi-area concurrent, and offset tracking.
// Tests sustained stream operations at system scale.
// sustained_append: in-session append only (no commit); for durable throughput use batch_write.
// Target 2M+ durable msgs/sec requires batched commit (batch_write scenario or store batching).
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::benchkit::create_bench_stream_actor;
use fitz::domains::stream::protocol::StreamMessage;
use fitz::prelude::Actor;
use fitz::runtime::routing::{Route, RouteFamily};

#[stress_test]
fn should_complete_append_sustained_load(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "sustained_append");

    // Setup: Stream actor ready for appends
    let (mut actor, mut actor_ctx) = create_bench_stream_actor("bench", "system", "append");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://bench/system/append/append".to_string());
    let begin = StreamMessage::Begin {
        family_id: family,
        route,
        expected_offset: 0,
        ingest_metadata: None,
    };
    actor.receive(begin, &mut actor_ctx);
    let payload = Bytes::from_static(b"sustained append event");

    ctx.measure(|| {
        actor.receive(
            StreamMessage::Append {
                session_id: 1,
                body: payload.clone(),
                metadata: None,
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_read_scan_throughput(ctx: &mut StressContext) {
    ctx.set_elements(100); // 100 events scanned
    ctx.tag("scenario", "read_scan");

    // Setup: Stream actor ready for reads
    let (mut actor, mut actor_ctx) = create_bench_stream_actor("bench", "system", "read");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://bench/system/read/read".to_string());

    ctx.measure(|| {
        actor.receive(
            StreamMessage::Read {
                family_id: family,
                route: route.clone(),
                from_offset: 0,
                limit: 100,
                max_bytes: None,
            },
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_batch_write_operations(ctx: &mut StressContext) {
    ctx.set_elements(100); // 100 events per batch
    ctx.tag("scenario", "batch_write");

    // Setup: Stream actor ready for batch writes
    let (mut actor, mut actor_ctx) = create_bench_stream_actor("bench", "system", "batch");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://bench/system/batch/append".to_string());
    actor.receive(
        StreamMessage::Begin {
            family_id: family,
            route,
            expected_offset: 0,
            ingest_metadata: None,
        },
        &mut actor_ctx,
    );
    let payload = Bytes::from_static(b"batch event");

    ctx.measure(|| {
        for _ in 0..100 {
            actor.receive(
                StreamMessage::Append {
                    session_id: 1,
                    body: payload.clone(),
                    metadata: None,
                },
                &mut actor_ctx,
            );
        }
    });
}

#[stress_test]
fn should_complete_multiarea_concurrent_writes(ctx: &mut StressContext) {
    ctx.set_elements(10); // 1 write per area
    ctx.tag("scenario", "multiarea_writes");

    // Setup: 10 stream actors for different areas
    let mut actors: Vec<_> = (0..10)
        .map(|i| create_bench_stream_actor("bench", "system", &format!("area{}", i)))
        .collect();
    for (index, (actor, actor_ctx)) in actors.iter_mut().enumerate() {
        let route = Route::new(format!("stream://bench/system/area{}/append", index));
        actor.receive(
            StreamMessage::Begin {
                family_id: RouteFamily::new(1),
                route,
                expected_offset: 0,
                ingest_metadata: None,
            },
            actor_ctx,
        );
    }
    let payload = Bytes::from_static(b"concurrent write");

    ctx.measure(|| {
        for (actor, actor_ctx) in actors.iter_mut() {
            actor.receive(
                StreamMessage::Append {
                    session_id: 1,
                    body: payload.clone(),
                    metadata: None,
                },
                actor_ctx,
            );
        }
    });
}

#[stress_test]
fn should_complete_offset_tracking_overhead(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "offset_tracking");

    // Setup: Stream actor ready for tail queries
    let (mut actor, mut actor_ctx) = create_bench_stream_actor("bench", "system", "offset");
    let family = RouteFamily::new(1);
    let route = Route::new("stream://bench/system/offset/append".to_string());

    ctx.measure(|| {
        actor.receive(
            StreamMessage::Last {
                family_id: family,
                route: route.clone(),
            },
            &mut actor_ctx,
        );
    });
}

stress_main!();
