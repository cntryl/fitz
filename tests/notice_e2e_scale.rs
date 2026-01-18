use fitz::testkit::notice::*;

// This file asserts end-to-end scale: does not fall over and delivers correct counts under load.
// These tests must be robust and non-flaky; avoid timing assertions.

use bytes::Bytes;
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use std::sync::Arc;

#[test]
fn should_handle_1k_subscriptions_end_to_end() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 1000usize;
    let mut sinks = Vec::new();
    let family = *addr("notify://realm/area/scale_e2e/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/scale_e2e/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notify://realm/area/scale_e2e/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act - publish once
    let mut pubctx = Context::new(
        addr("notify://realm/area/scale_e2e/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/scale_e2e/p"),
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
    let family = *addr("notify://realm/area/scale_e2e_big/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/scale_e2e_big/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notify://realm/area/scale_e2e_big/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act - publish once
    let mut pubctx = Context::new(
        addr("notify://realm/area/scale_e2e_big/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/scale_e2e_big/p"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    let total: usize = sinks.iter().map(|s| s.count()).sum();
    assert_eq!(total, n);
}
