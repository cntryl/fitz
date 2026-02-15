use std::sync::Arc;

use bytes::Bytes;
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::runtime::actor::Actor;
use fitz::runtime::actor::Context;

use fitz::testkit::notice::{addr, make_router, session_id, TestSink};

// This file asserts the basic golden path: one subscription receives one notification.
// Keep tests simple Ã¢â‚¬â€ no fanout or scale complexity here.

/// E2E basic test: single notification delivered to single matching subscription
#[test]
fn should_deliver_single_notification_to_single_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    // proper 4-part route: {scheme}://{realm}/{area}/{resource}/{operation}
    let subscriber = addr("notify://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    // Use public API (Subscribe message) to register
    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/*"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Sanity checks (small, public-facing): there should be at least one subscription tracked
    // (We keep assertions minimal here; harness-level inspection is in unit tests)

    // Act
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/users/recv"),
        Bytes::from("hi"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 1);
}
