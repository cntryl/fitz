//! Consolidated notice basic/unit tests
//!
//! Combines:
//! - notice_auth.rs: Authorization and permission checks
//! - notice_semantics.rs: Delivery semantics and subscription matching
//! - notice_fanout_math.rs: Fanout delivery count verification

use bytes::Bytes;
use fitz::auth::Permission;
use fitz::domains::notice::NoticeRouteActor;
use fitz::domains::notice::protocol::{NotificationMessage, PublishMessage, SubscribeMessage};
use fitz::domains::notice::session::SessionActor;
use fitz::prelude::Actor;
use fitz::runtime::actor::Context;
use fitz::runtime::routing::RouteFamily;
use fitz::session::permissions::SessionPermissions;
use fitz::session::session::SessionId;
use fitz::testkit::{TestSink, addr, make_router, route, session_id};
use std::sync::Arc;

fn assert_delivered_payloads(sink: &TestSink, expected_count: usize, expected_payload: &[u8]) {
    let delivered = sink.delivered();
    assert_eq!(delivered.len(), expected_count);

    for envelope in delivered {
        let message = envelope
            .payload::<NotificationMessage>()
            .expect("expected NotificationMessage payload");
        match message {
            NotificationMessage::Deliver(deliver) => {
                assert_eq!(deliver.payload.as_ref(), expected_payload);
            }
            other => panic!("expected Deliver variant, got {other:?}"),
        }
    }
}

// ===== Tier 1: Authorization Tests =====

#[test]
fn should_reject_unauthenticated_subscribe() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/sub");
    router.register(subscriber.clone(), sink.clone());

    let mut notice = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router));

    let session = SessionActor::new(SessionId(42), SessionPermissions::empty());

    // Act
    let res = session.subscribe(
        RouteFamily::new(1),
        route("notice://realm/area/orders/*"),
        &mut notice,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    assert_eq!(notice.subscription_count(), 0);
    assert_eq!(sink.count(), 0);
}

#[test]
fn should_reject_unauthorized_publish() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://prod/orders/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut notice = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router));

    let perms = vec![Permission::parse("notice://prod/orders/**#read").unwrap()];
    let session_perms = SessionPermissions::from_permissions(perms);
    let session = SessionActor::new(SessionId(43), session_perms);

    // Act
    let res = session.publish(
        RouteFamily::new(1),
        route("notice://prod/orders/create"),
        Bytes::from("payload"),
        &mut notice,
        &mut ctx,
    );

    // Assert
    assert!(res.is_err());
    assert_eq!(sink.count(), 0);
}

// ===== Tier 1: Delivery Semantics Tests =====

#[test]
fn should_deliver_notification_to_exact_matching_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        Bytes::from("notification"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_delivered_payloads(sink.as_ref(), 1, b"notification");
}

#[test]
fn should_not_deliver_notification_when_no_subscription_matches() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/orders/recv"),
        Bytes::from("different_route"),
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
    let sub1 = addr("notice://realm/area/users/recv1");
    router.register(sub1.clone(), sink1.clone());

    let sink2 = Arc::new(TestSink::new());
    let sub2 = addr("notice://realm/area/users/recv2");
    router.register(sub2.clone(), sink2.clone());

    let sink3 = Arc::new(TestSink::new());
    let sub3 = addr("notice://realm/area/users/recv3");
    router.register(sub3.clone(), sink3.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));

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
        route("notice://realm/area/users/*"),
        session_id(2),
        sub2.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe2), &mut ctx2);

    let mut ctx3 = Context::new(sub3.clone(), Arc::new(router.clone()));
    let subscribe3 = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/*"),
        session_id(3),
        sub3.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe3), &mut ctx3);

    // Act
    let mut pubctx = Context::new(sub1.clone(), Arc::new(router.clone()));
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        Bytes::from("broadcast"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    assert_delivered_payloads(sink1.as_ref(), 1, b"broadcast");
    assert_delivered_payloads(sink2.as_ref(), 1, b"broadcast");
    assert_delivered_payloads(sink3.as_ref(), 1, b"broadcast");
}

#[test]
fn should_not_duplicate_delivery_for_same_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/*"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe.clone()), &mut ctx);
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        Bytes::from("single"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_delivered_payloads(sink.as_ref(), 1, b"single");
}

#[test]
fn should_deliver_notifications_to_overlapping_subscriptions() {
    // Arrange
    let router = make_router();
    let sink1 = Arc::new(TestSink::new());
    let sub1 = addr("notice://realm/area/users/recv1");
    router.register(sub1.clone(), sink1.clone());

    let sink2 = Arc::new(TestSink::new());
    let sub2 = addr("notice://realm/area/users/recv2");
    router.register(sub2.clone(), sink2.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));

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
    assert_delivered_payloads(sink1.as_ref(), 1, b"overlap");
    assert_delivered_payloads(sink2.as_ref(), 1, b"overlap");
}

// ===== Tier 1: Fanout Math Tests =====

#[test]
fn should_fan_out_one_notification_to_one_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/one/sink");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/one/*"),
        session_id(1),
        subscriber.clone(),
    );
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/one/sink"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_delivered_payloads(sink.as_ref(), 1, b"x");
}

#[test]
fn should_fan_out_one_notification_to_many_subscriptions() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));

    let mut sinks = Vec::new();
    let family = *addr("notice://realm/area/one/0").family();

    for i in 0..5 {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/one/sink{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/one/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/one/publisher"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/one/sink3"),
        Bytes::from("payload"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    for s in sinks.iter() {
        assert_delivered_payloads(s.as_ref(), 1, b"payload");
    }
}

#[test]
fn should_fan_out_many_notifications_to_many_subscriptions() {
    // Arrange
    let router = make_router();
    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));

    let mut sinks = Vec::new();
    let family = *addr("notice://realm/area/many/0").family();

    for i in 0..3 {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/many/sink{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/many/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/many/pub"),
        Arc::new(router.clone()),
    );
    for n in 0..4 {
        let pubmsg = PublishMessage::new(
            family,
            route(&format!("notice://realm/area/many/item{}", n)),
            Bytes::from("p"),
        );
        actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);
    }

    // Assert
    for s in sinks.iter() {
        assert_delivered_payloads(s.as_ref(), 4, b"p");
    }
}

#[test]
fn should_produce_zero_deliveries_when_no_subscriptions_exist() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let addr_val = addr("notice://realm/area/none/sink");
    router.register(addr_val.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let mut ctx = Context::new(addr_val.clone(), Arc::new(router.clone()));

    let family = *addr_val.family();

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/none/sink"),
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
    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));

    let n = 7usize;
    let mut sinks = Vec::new();
    let family = *addr("notice://realm/area/exact/0").family();

    for i in 0..n {
        let sink = Arc::new(TestSink::new());
        let sub = addr(&format!("notice://realm/area/exact/s{}", i));
        router.register(sub.clone(), sink.clone());
        let mut ctx = Context::new(sub.clone(), Arc::new(router.clone()));
        let subscribe = SubscribeMessage::new(
            family,
            route("notice://realm/area/exact/*"),
            session_id(i as u64 + 1),
            sub.clone(),
        );
        actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);
        sinks.push(sink);
    }

    // Act
    let mut pubctx = Context::new(
        addr("notice://realm/area/exact/p"),
        Arc::new(router.clone()),
    );
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/exact/s3"),
        Bytes::from("x"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut pubctx);

    // Assert
    for s in sinks.iter() {
        assert_delivered_payloads(s.as_ref(), 1, b"x");
    }
}

// ===== Tier 1: Domain-Level Tests =====

/// E2E basic test: single notification delivered to single matching subscription
#[test]
fn should_deliver_single_notification_to_single_subscription() {
    // Arrange
    let router = make_router();
    let sink = Arc::new(TestSink::new());
    let subscriber = addr("notice://realm/area/users/recv");
    router.register(subscriber.clone(), sink.clone());

    let mut actor = NoticeRouteActor::new(RouteFamily::new(1));
    let ctx = Context::new(subscriber.clone(), Arc::new(router.clone()));

    let family = *ctx.address().family();
    let subscribe = SubscribeMessage::new(
        family,
        route("notice://realm/area/users/*"),
        session_id(1),
        subscriber.clone(),
    );
    let mut ctx = ctx;
    actor.receive(NotificationMessage::Subscribe(subscribe), &mut ctx);

    // Act
    let pubmsg = PublishMessage::new(
        family,
        route("notice://realm/area/users/recv"),
        Bytes::from("hi"),
    );
    actor.receive(NotificationMessage::Publish(pubmsg), &mut ctx);

    // Assert
    assert_delivered_payloads(sink.as_ref(), 1, b"hi");
}
