use std::sync::Arc;

use bytes::Bytes;
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;

use fitz::testkit::notice::{addr, make_router, session_id, TestSink};

// This file asserts notification semantics: verifies delivery rules (who receives notifications).
// It MUST NOT test implementation details such as matcher internals or data structures.

#[test]
fn should_deliver_notification_to_exact_matching_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notify://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv"),
        Bytes::from("hello"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 1);
}

#[test]
fn should_not_deliver_notification_when_no_subscription_matches() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notify://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act - publish to a different route
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/other/recv"),
        Bytes::from("hello"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 0);
}

#[test]
fn should_deliver_notification_to_all_matching_subscriptions() {
    // Arrange
    let router = make_router();

    let sink1 = Arc::new(TestSink::new());
    let sub1 = addr("notify://realm/area/users/recv1");
    router.register(sub1.clone(), sink1.clone());

    let sink2 = Arc::new(TestSink::new());
    let sub2 = addr("notify://realm/area/users/recv2");
    router.register(sub2.clone(), sink2.clone());

    let sink3 = Arc::new(TestSink::new());
    let sub3 = addr("notify://realm/area/users/recv3");
    router.register(sub3.clone(), sink3.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    // All three subscribe to a wildcard that matches the published route
    let family = *sub1.family();
    let mut ctx1 = Context::new(sub1.clone(), Arc::new(router.clone()));
    let subscribe1 = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(1),
        sub1.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe1), &mut ctx1);

    let mut ctx2 = Context::new(sub2.clone(), Arc::new(router.clone()));
    let subscribe2 = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(2),
        sub2.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe2), &mut ctx2);

    let mut ctx3 = Context::new(sub3.clone(), Arc::new(router.clone()));
    let subscribe3 = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(3),
        sub3.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe3), &mut ctx3);

    // Act
    let mut pubctx = Context::new(sub1.clone(), Arc::new(router.clone()));
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv1"),
        Bytes::from("hey"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    assert_eq!(sink1.count(), 1);
    assert_eq!(sink2.count(), 1);
    assert_eq!(sink3.count(), 1);
}

#[test]
fn should_not_duplicate_delivery_for_same_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notify://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    // Subscribe twice with identical session, subscriber and pattern
    let subscribe = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv"),
        Bytes::from("hello"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert - since duplicate subscribes are idempotent, we expect a single delivery
    assert_eq!(sink.count(), 1);
}

#[test]
fn should_deliver_notifications_to_overlapping_subscriptions() {
    // Arrange
    let router = make_router();
    let sink1 = Arc::new(TestSink::new());
    let sub1 = addr("notify://realm/area/users/recv1");
    router.register(sub1.clone(), sink1.clone());

    let sink2 = Arc::new(TestSink::new());
    let sub2 = addr("notify://realm/area/users/recv2");
    router.register(sub2.clone(), sink2.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let family = *sub1.family();
    let mut ctx1 = Context::new(sub1.clone(), Arc::new(router.clone()));
    let subscribe1 = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(1),
        sub1.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe1), &mut ctx1);

    let mut ctx2 = Context::new(sub2.clone(), Arc::new(router.clone()));
    let subscribe2 = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/*/recv2"),
        session_id(2),
        sub2.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe2), &mut ctx2);

    // Act - publish to a route that matches both patterns
    let mut pubctx = Context::new(sub1.clone(), Arc::new(router.clone()));
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv2"),
        Bytes::from("overlap"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert - both sinks receive a delivery
    assert_eq!(sink1.count(), 1);
    assert_eq!(sink2.count(), 1);
}

#[test]
fn should_deliver_multiple_notifications_independently() {
    // Arrange
    let router = make_router();

    let sink_a = Arc::new(TestSink::new());
    let a = addr("notify://realm/area/a");
    router.register(a.clone(), sink_a.clone());

    let sink_b = Arc::new(TestSink::new());
    let b = addr("notify://realm/area/b");
    router.register(b.clone(), sink_b.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let family = *a.family();
    // Subscribe each sink to its exact route so notifications fan out independently
    let mut ctx_a = Context::new(a.clone(), Arc::new(router.clone()));
    let sub_a = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/a"),
        session_id(1),
        a.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(sub_a), &mut ctx_a);

    let mut ctx_b = Context::new(b.clone(), Arc::new(router.clone()));
    let sub_b = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/b"),
        session_id(2),
        b.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(sub_b), &mut ctx_b);

    // Act - publish two different notifications
    let mut pubctx = Context::new(a.clone(), Arc::new(router.clone()));
    let pub1 = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/a"),
        Bytes::from("1"),
    );
    let pub2 = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/b"),
        Bytes::from("2"),
    );
    actor.receive(NotificationMessage::Publish(pub1), &mut pubctx);
    actor.receive(NotificationMessage::Publish(pub2), &mut pubctx);

    // Assert - each sink should receive its matching message
    assert_eq!(sink_a.count(), 1);
    assert_eq!(sink_b.count(), 1);
}
