use std::sync::Arc;

use bytes::Bytes;
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::route_actor::NoticeRouteActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;

use fitz::testkit::notice::{addr, make_router, session_id, TestSink};

// This file asserts notification fanout math: deterministic N Ã¢â€ â€™ M delivery counts.
// It MUST NOT test performance or internal routing mechanics.

#[test]
fn should_fan_out_one_notification_to_one_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notify://realm/area/one/sink");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/one/*"),
        session_id(1),
        subscriber.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/one/sink"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 1);
}

#[test]
fn should_fan_out_one_notification_to_many_subscriptions() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let mut sinks = Vec::new();
    let family = *addr("notify://realm/area/one/0").family();

    for i in 0..5 {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/one/sink{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            fitz::testkit::notice::route("notify://realm/area/one/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notify://realm/area/one/publisher"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/one/sink3"),
        Bytes::from("payload"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    for s in sinks.iter() {
        assert_eq!(s.count(), 1);
    }
}

#[test]
fn should_fan_out_many_notifications_to_many_subscriptions() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let mut sinks = Vec::new();
    let family = *addr("notify://realm/area/many/0").family();

    for i in 0..3 {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/many/sink{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            fitz::testkit::notice::route("notify://realm/area/many/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notify://realm/area/many/pub"),
        Arc::new(router.clone()),
    );
    for n in 0..4 {
        let pubmsg = PublishMessage::new(
            family,
            fitz::testkit::notice::route(&format!("notify://realm/area/many/item{}", n)),
            Bytes::from("p"),
        );
        actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);
    }

    // Assert
    for s in sinks.iter() {
        assert_eq!(s.count(), 4);
    }
}

#[test]
fn should_produce_zero_deliveries_when_no_subscriptions_exist() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let addr = addr("notify://realm/area/none/sink");
    // register sink but do NOT subscribe
    router.register(addr.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));
    let mut ctx = Context::new(addr.clone(), Arc::new(router.clone()));

    let family = *addr.family();

    // Act
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/none/sink"),
        Bytes::from("nop"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_eq!(sink.count(), 0);
}

#[test]
fn should_produce_exactly_n_deliveries_for_n_matching_subscriptions() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(fitz::runtime::routing::RouteFamily::new(1));

    let n = 7usize;
    let mut sinks = Vec::new();
    let family = *addr("notify://realm/area/exact/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notify://realm/area/exact/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            fitz::testkit::notice::route("notify://realm/area/exact/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notify://realm/area/exact/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        fitz::testkit::notice::route("notify://realm/area/exact/s3"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    for s in sinks.iter() {
        assert_eq!(s.count(), 1);
    }
}
