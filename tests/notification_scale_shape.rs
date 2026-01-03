use std::sync::Arc;

use bytes::Bytes;
use fitz::domains::notification::actor::NoticeRouteActor;
use fitz::domains::notification::protocol::{
    NotificationMessage, PublishMessage, SubscribeMessage,
};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;

mod common;
use common::harness_notification::{addr, make_router, route, session_id, TestSink};

// This file asserts scale & shape invariants: relative growth and failure modes.
// It MUST NOT assert absolute durations or performance claims.


#[test]
fn should_scale_linearly_with_subscription_count() {
    // Arrange: create two sizes and register subscriptions
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let small_n = 8usize;
    let large_n = 32usize; // 4x small_n

    let family = *addr("notify://realm/area/scale/0").family();

    let mut small_sinks = Vec::new();
    for i in 0..small_n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/scale/s{}_x", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notify://realm/area/scale/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        small_sinks.push(sink);
    }

    let mut large_sinks = Vec::new();
    for i in 0..large_n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/scale/l{}_x", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notify://realm/area/scale/*"),
            session_id((i + 100) as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        large_sinks.push(sink);
    }

    // Act: publish once and count deliveries
    let mut pubctx = Context::new(
        addr("notify://realm/area/scale/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/scale/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    let small_total: usize = small_sinks.iter().map(|s| s.count()).sum();
    let large_total: usize = large_sinks.iter().map(|s| s.count()).sum();

    // Assert: deliveries scale linearly (large ≈ 4 * small)
    assert_eq!(small_total, small_n);
    assert_eq!(large_total, large_n);
    assert!(large_total >= small_total * 4, "expected large >= 4*small");
}

#[test]
fn should_not_exhibit_quadratic_fanout_growth() {
    // Arrange: create increasing subscription sizes and assert deliveries match subscribers (no explosion)
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let sizes = [10usize, 20usize, 40usize];
    let family = *addr("notify://realm/area/q/0").family();

    for &n in sizes.iter() {
        let mut sinks = Vec::new();
        for i in 0..n {
            let sink = Arc::new(TestSink::new());
            let sub = addr(&format!("notify://realm/area/q/s{}_{}", n, i));
            router.register(sub.clone(), sink.clone());
            let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
            let subscribe = SubscribeMessage::new(
                family,
                route("notify://realm/area/q/*"),
                session_id(i as u64 + 1),
                sub.clone(),
            );
            actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
            sinks.push(sink);
        }

        // Act
        let mut pubctx = Context::new(addr("notify://realm/area/q/p"), Arc::new(router.clone()));
        let pubmsg =
            PublishMessage::new(family, route("notify://realm/area/q/p"), Bytes::from("x"));
        actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

        let total: usize = sinks.iter().map(|s| s.count()).sum();
        // Assert: exactly N deliveries, no quadratic growth
        assert_eq!(
            total, n,
            "expected exactly {} deliveries for {} subscriptions",
            n, n
        );
    }
}

#[test]
fn should_handle_large_subscription_sets_without_failure() {
    // Arrange: create a large set of subscriptions to ensure system doesn't panic
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 1000usize; // intentionally large but reasonable for CI
    let family = *addr("notify://realm/area/scale_big/0").family();
    let mut sinks = Vec::new();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/scale_big/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notify://realm/area/scale_big/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act - publish once
    let mut pubctx = Context::new(
        addr("notify://realm/area/scale_big/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/scale_big/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    let total: usize = sinks.iter().map(|s| s.count()).sum();
    assert_eq!(total, n);
}
