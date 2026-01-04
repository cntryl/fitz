use fitz::testkit::notification::*;

// This file asserts E2E fanout correctness: realistic overlapping subscriptions and no duplicate deliveries.
// Keep subscriber counts modest (â‰¤64) â€” large-scale tests belong in `notification_e2e_scale.rs`.


use bytes::Bytes;
use fitz::domains::notification::route_actor::NoticeRouteActor;
use fitz::domains::notification::protocol::{
    NotificationMessage, PublishMessage, SubscribeMessage,
};
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use std::sync::Arc;

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

    // Subscribe: one wildcard, one more specific overlapping pattern
    let family = *sub1.family();
    let mut ctx1 = Context::new(sub1.clone(), Arc::new(router.clone()));
    let subscribe1 = SubscribeMessage::new(
        family,
        route("notify://realm/area/users/*"),
        session_id(1),
        sub1.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe1), &mut ctx1);

    let mut ctx2 = Context::new(sub2.clone(), Arc::new(router.clone()));
    let subscribe2 = SubscribeMessage::new(
        family,
        route("notify://realm/area/*/recv2"),
        session_id(2),
        sub2.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe2), &mut ctx2);

    // Act
    let mut pubctx = Context::new(sub1.clone(), Arc::new(router.clone()));
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/users/recv2"),
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
    let sub = addr("notify://realm/area/users/recv");
    router.register(sub.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let family = *sub.family();
    let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
    let subscribe = SubscribeMessage::new(
        family,
        route("notify://realm/area/users/*"),
        session_id(1),
        sub.clone(),
    );
    // Subscribe twice with identical params
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notify://realm/area/users/recv"),
        Bytes::from("dup"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert - deduplication means single delivery
    assert_eq!(sink.count(), 1);
}
