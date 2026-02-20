// Notice domain tier 3 system benchmarks using stress
//
// Concurrent fanout, pattern matching, and subscriber lifecycle.
// Tests sustained notification delivery with multiple subscribers.
// Tier3 measures fanout + delivery to TestSink (includes sink overhead); in-proc
// Criterion matcher benchmarks are sink-free and report match cost only.
//
// Each test measures a single operation with all setup/teardown outside the measurement loop.
// Target: ops/sec via set_elements(count)

use bytes::Bytes;
use cntryl_stress::{stress_main, stress_test, StressContext};
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::{addr, make_router, route, session_id, TestSink};
use std::sync::Arc;

fn setup_notice_actor(
    subscriber_count: usize,
    pattern: &str,
) -> (NoticeRouteActor, Context<NoticeRouteActor>) {
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let family = RouteFamily::new(1);
    let mut actor = NoticeRouteActor::new(family);

    for i in 0..subscriber_count {
        let subscriber = addr(&format!("notice://realm/area/sub{}", i));
        router.register(subscriber.clone(), sink.clone());
    }

    let router = Arc::new(router);
    let mut ctx = Context::new(addr("notice://realm/area/ctx"), router.clone());

    for i in 0..subscriber_count {
        let subscriber = addr(&format!("notice://realm/area/sub{}", i));
        let subscribe =
            SubscribeMessage::new(family, route(pattern), session_id(i as u64 + 1), subscriber);
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
    }

    (actor, ctx)
}

#[stress_test]
fn should_complete_fanout_sustained_load(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "sustained_fanout");

    // Setup: Actor with pre-subscribed parties
    let (mut actor, mut actor_ctx) = setup_notice_actor(1, "notice://realm/area/orders/*");
    let payload = Bytes::from_static(b"sustained fanout message");
    let publish = PublishMessage::new(
        RouteFamily::new(1),
        route("notice://realm/area/orders/create"),
        payload,
    );

    ctx.measure(|| {
        actor.receive(
            NotificationMessage::Publish(publish.clone()),
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_pattern_matching_scaling(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "pattern_matching");

    // Setup: Actor with pattern subscriptions
    let (mut actor, mut actor_ctx) = setup_notice_actor(1, "notice://realm/area/**");
    let payload = Bytes::from_static(b"pattern match message");
    let publish = PublishMessage::new(
        RouteFamily::new(1),
        route("notice://realm/area/orders/created"),
        payload,
    );

    ctx.measure(|| {
        actor.receive(
            NotificationMessage::Publish(publish.clone()),
            &mut actor_ctx,
        );
    });
}

#[stress_test]
fn should_complete_fanout_high_subscriber_count(ctx: &mut StressContext) {
    ctx.set_elements(1);
    ctx.tag("scenario", "high_subscriber_count");

    // Setup: Actor ready for high subscriber fanout
    let (mut actor, mut actor_ctx) = setup_notice_actor(100, "notice://realm/area/orders/*");
    let payload = Bytes::from_static(b"high subscriber fanout");
    let publish = PublishMessage::new(
        RouteFamily::new(1),
        route("notice://realm/area/orders/create"),
        payload,
    );

    ctx.measure(|| {
        actor.receive(
            NotificationMessage::Publish(publish.clone()),
            &mut actor_ctx,
        );
    });
}

stress_main!();
