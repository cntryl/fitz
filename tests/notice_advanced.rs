//! Consolidated notice advanced tests
//!
//! Combines:
//! - notice_scale_shape.rs: Scale and shape invariants
//! - notice_e2e_fanout.rs: Fanout correctness with overlapping subscriptions
//! - notice_e2e_scale.rs: End-to-end scale tests

use bytes::Bytes;
use fitz::domains::notice::protocol::{
    NotificationMessage, PublishMessage, SubscribeMessage, UnsubscribeAllMessage,
};
use fitz::domains::notice::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::{RouteAddress, RouteFamily};
use fitz::testkit::notice::{addr, make_router, route, session_id, TestSink};
use std::sync::Arc;

// ===== Tier 2: Scale & Shape Invariants =====

#[test]
fn should_scale_linearly_with_subscription_count() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let small_n = 8usize;
    let large_n = 32usize; // 4x small_n

    let family = *addr("notice://realm/area/scale/0").family();

    let mut small_sinks = Vec::new();
    for i in 0..small_n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/scale/s{}_x", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/scale/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        small_sinks.push(sink);
    }

    let mut large_sinks = Vec::new();
    for i in 0..large_n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/scale/l{}_x", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/scale/*"),
            session_id((i + 100) as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        large_sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/scale/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/scale/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    let small_total: usize = small_sinks.iter().map(|s| s.count()).sum();
    let large_total: usize = large_sinks.iter().map(|s| s.count()).sum();

    // Assert
    assert_eq!(small_total, small_n);
    assert_eq!(large_total, large_n);
    assert!(large_total >= small_total * 4, "expected large >= 4*small");
}

#[test]
fn should_not_exhibit_quadratic_fanout_growth() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let sizes = [10usize, 20usize, 40usize];
    let family = *addr("notice://realm/area/q/0").family();

    for &n in sizes.iter() {
        let mut sinks = Vec::new();
        for i in 0..n {
            let sink = Arc::new(TestSink::new());
            let sub = addr(&format!("notice://realm/area/q/s{}_{}", n, i));
            router.register(sub.clone(), sink.clone());
            let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
            let subscribe = SubscribeMessage::new(
                family,
                route("notice://realm/area/q/*"),
                session_id(i as u64 + 1),
                sub.clone(),
            );
            actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
            sinks.push(sink);
        }

        // Act
        let mut pubctx = Context::new(addr("notice://realm/area/q/p"), Arc::new(router.clone()));
        let pubmsg =
            PublishMessage::new(family, route("notice://realm/area/q/p"), Bytes::from("x"));
        actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

        let total: usize = sinks.iter().map(|s| s.count()).sum();
        // Assert
        assert_eq!(
            total, n,
            "expected exactly {} deliveries for {} subscriptions",
            n, n
        );
    }
}

#[test]
fn should_handle_large_subscription_sets_without_failure() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 1000usize; // intentionally large but reasonable for CI
    let family = *addr("notice://realm/area/scale_big/0").family();
    let mut sinks = Vec::new();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/scale_big/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/scale_big/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/scale_big/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/scale_big/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    let total: usize = sinks.iter().map(|s| s.count()).sum();
    assert_eq!(total, n);
}

// ===== Tier 2: Fanout E2E Tests (overlapping subscriptions) =====

#[test]
fn should_deliver_notifications_to_overlapping_subscriptions_e2e() {
    // Arrange
    let router = make_router();

    let sink1 = Arc::new(TestSink::new());
    let sub1 = addr("notice://realm/area/users/recv1");
    router.register(sub1.clone(), sink1.clone());

    let sink2 = Arc::new(TestSink::new());
    let sub2 = addr("notice://realm/area/users/recv2");
    router.register(sub2.clone(), sink2.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    // Subscribe: one wildcard, one more specific overlapping pattern
    let family = *sub1.family();
    let mut ctx1 = Context::new(sub1.clone(), Arc::new(router.clone()));
    let subscribe1 = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/*"),
        session_id(1),
        sub1.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe1), &mut ctx1);

    let mut ctx2 = Context::new(sub2.clone(), Arc::new(router.clone()));
    let subscribe2 = SubscribeMessage::new(
        family,
        route("notice://realm/area/*/recv2"),
        session_id(2),
        sub2.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe2), &mut ctx2);

    // Act
    let mut pubctx = Context::new(sub1.clone(), Arc::new(router.clone()));
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv2"),
        Bytes::from("overlap"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    assert_eq!(sink1.count(), 1);
    assert_eq!(sink2.count(), 1);
}

#[test]
fn should_not_duplicate_deliveries_for_duplicate_subscriptions() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let sub = addr("notice://realm/area/users/recv");
    router.register(sub.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let family = *sub.family();
    let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/*"),
        session_id(1),
        sub.clone(),
    );
    // Subscribe twice with identical params
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        Bytes::from("dup"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 1);
}

// ===== Tier 2: Scale E2E Tests =====

#[test]
fn should_handle_1k_subscriptions_end_to_end() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 1000usize;
    let mut sinks = Vec::new();
    let family = *addr("notice://realm/area/scale_e2e/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/scale_e2e/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/scale_e2e/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/scale_e2e/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/scale_e2e/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    let total: usize = sinks.iter().map(|s| s.count()).sum();
    assert_eq!(total, n);
}

#[test]
fn should_handle_5k_subscriptions_without_failure_end_to_end() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 5000usize;
    let mut sinks = Vec::new();
    let family = *addr("notice://realm/area/scale_e2e_big/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/scale_e2e_big/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/scale_e2e_big/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/scale_e2e_big/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/scale_e2e_big/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    let total: usize = sinks.iter().map(|s| s.count()).sum();
    assert_eq!(total, n);
}

// ===== Contract: RouteFamily isolation and disconnect cleanup =====

#[test]
fn should_not_fanout_notice_across_route_families_given_same_pattern() {
    // Arrange — two actors, one per family
    let router = make_router();
    let family1 = RouteFamily::new(1);
    let family2 = RouteFamily::new(2);

    let mut actor1 = NoticeRouteActor::new(family1);
    let mut actor2 = NoticeRouteActor::new(family2);

    // Subscribe a sink in family 1
    let sink1 = Arc::new(TestSink::new());
    let sub1 = addr("notice://realm/area/iso/recv1"); // addr() uses family 1
    router.register(sub1.clone(), sink1.clone());
    let mut ctx1 = Context::new(sub1.clone(), Arc::new(router.clone()));
    actor1.receive(
        NotificationMessage::Subscribe(SubscribeMessage::new(
            family1,
            route("notice://realm/area/iso/*"),
            session_id(1),
            sub1.clone(),
        )),
        &mut ctx1,
    );

    // Subscribe a sink in family 2 with the same wildcard pattern
    let sink2 = Arc::new(TestSink::new());
    let sub2 = RouteAddress::new(family2, route("notice://realm/area/iso/recv2"));
    router.register(sub2.clone(), sink2.clone());
    let mut ctx2 = Context::new(sub2.clone(), Arc::new(router.clone()));
    actor2.receive(
        NotificationMessage::Subscribe(SubscribeMessage::new(
            family2,
            route("notice://realm/area/iso/*"),
            session_id(2),
            sub2.clone(),
        )),
        &mut ctx2,
    );

    // Act — publish only into actor1 (family 1)
    let mut pubctx = Context::new(addr("notice://realm/area/iso/pub"), Arc::new(router.clone()));
    actor1.receive(
        NotificationMessage::Publish(PublishMessage::new(
            family1,
            route("notice://realm/area/iso/pub"),
            Bytes::from("msg"),
        )),
        &mut pubctx,
    );

    // Assert — family 1 delivers; family 2 is fully isolated
    assert_eq!(sink1.count(), 1);
    assert_eq!(sink2.count(), 0);
}

#[test]
fn should_not_deliver_notice_after_disconnect_cleanup_given_publish_race() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let sub = addr("notice://realm/area/race/recv");
    router.register(sub.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let family = *sub.family();
    let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));

    actor.receive(
        NotificationMessage::Subscribe(SubscribeMessage::new(
            family,
            route("notice://realm/area/race/*"),
            session_id(1),
            sub.clone(),
        )),
        &mut ctx,
    );

    // Simulate disconnect cleanup (UnsubscribeAll)
    actor.receive(
        NotificationMessage::UnsubscribeAll(UnsubscribeAllMessage::new(session_id(1), sub.clone())),
        &mut ctx,
    );

    // Act — publish races with cleanup (arrives after)
    actor.receive(
        NotificationMessage::Publish(PublishMessage::new(
            family,
            route("notice://realm/area/race/recv"),
            Bytes::from("late"),
        )),
        &mut ctx,
    );

    // Assert — no delivery after session was cleaned up
    assert_eq!(sink.count(), 0);
}
