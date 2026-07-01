use super::*;
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::PayloadEncoder;
use crate::protocol::tlv::MessageType;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientChannel;
use bytes::Bytes;
use std::sync::Arc;

fn encode_lease_acquire(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(ttl_secs);
    Bytes::from(encoder.finish())
}

fn encode_lease_extend(route: &str, owner_id: &str, fencing_token: u64, ttl_secs: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(fencing_token);
    encoder.put_u64(ttl_secs);
    Bytes::from(encoder.finish())
}

fn encode_lease_release(route: &str, owner_id: &str, fencing_token: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(fencing_token);
    Bytes::from(encoder.finish())
}

fn encode_lease_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

fn receive_envelope(mailbox: &Mailbox, label: &str) -> Envelope {
    mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .unwrap_or_else(|_| panic!("{label}"))
}

fn assert_no_envelope(mailbox: &Mailbox) {
    assert!(mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(50))
        .is_err());
}

fn wait_for_lease_count(sink: &LeaseDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.lease_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.lease_count(), expected);
}

fn wait_for_subscription_count(sink: &LeaseDomainSink, expected: usize) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if sink.subscription_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(sink.subscription_count(), expected);
}

fn wait_for_admin_lease_count(
    admin_read_model: &crate::control::admin::read_model::AdminReadModel,
    expected: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(1);
    while Instant::now() < deadline {
        if admin_read_model.leases(None).len() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(admin_read_model.leases(None).len(), expected);
}

fn lease_key(family: RouteFamily, route: &str) -> LeaseKey {
    LeaseKey::from_route(family, &Route::new(route)).expect("valid lease route")
}

#[test]
fn should_create_lease_domain_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = LeaseDomainSink::new(router, admin_read_model);

    // Assert
    assert!(sink.state.active.load(Ordering::Relaxed));
    assert!(sink.is_actor_running());
}

#[test]
fn should_clear_session_state_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        lease_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(lease_route, "", 30),
            family,
        ),
    ))
    .expect("acquire lease");
    let _acquire_ack = receive_envelope(&subscriber_mailbox, "acquire ack envelope");
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        lease_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(407),
            encode_lease_subscribe(lease_route),
            family,
        ),
    ))
    .expect("subscribe lease route");
    let _subscribe_ack = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    wait_for_lease_count(&sink, 1);
    wait_for_subscription_count(&sink, 1);
    wait_for_admin_lease_count(&admin_read_model, 1);
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(sink.subscription_count(), 1);
    assert_eq!(admin_read_model.leases(None).len(), 1);
    assert_eq!(
        admin_read_model.leases(None)[0].owner_session_id,
        "session:7"
    );
    drain_mailbox(&subscriber_mailbox);

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("lease://cleanup")),
        crate::runtime::SessionCleanup { session_id },
    ))
    .expect("cleanup lease session");
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("lease://events")),
        crate::runtime::DomainPublishEvent::new(
            family,
            Route::new(lease_route),
            Bytes::from_static(b"expired"),
        ),
    ))
    .expect("deliver lease publish event");
    wait_for_lease_count(&sink, 0);
    wait_for_subscription_count(&sink, 0);
    wait_for_admin_lease_count(&admin_read_model, 0);

    // Assert
    assert_eq!(sink.lease_count(), 0);
    assert_eq!(sink.subscription_count(), 0);
    assert!(admin_read_model.leases(None).is_empty());
    assert_no_envelope(&subscriber_mailbox);
    assert!(sink.state.core.families.lock().is_empty());
}

#[test]
fn should_preserve_other_session_leases_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let route_a = "lease://acme/locks/resource-a";
    let route_b = "lease://acme/locks/resource-b";
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        RouteAddress::new(family, Route::new("inbox://session/7")),
        RouteAddress::new(family, Route::new(route_a)),
        FrameContext::new(
            7,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route_a, "", 30),
            family,
        ),
    ))
    .expect("session 7 acquire lease");
    sink.deliver(Envelope::from_route(
        RouteAddress::new(family, Route::new("inbox://session/8")),
        RouteAddress::new(family, Route::new(route_b)),
        FrameContext::new(
            8,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route_b, "", 30),
            family,
        ),
    ))
    .expect("session 8 acquire lease");
    wait_for_lease_count(&sink, 2);
    wait_for_admin_lease_count(&admin_read_model, 2);

    // Act
    sink.cleanup_session(7);
    wait_for_lease_count(&sink, 1);
    wait_for_admin_lease_count(&admin_read_model, 1);
    let leases = admin_read_model.leases(None);

    // Assert
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].resource, "resource-b");
    assert_eq!(leases[0].owner_session_id, "session:8");
}

#[test]
fn should_promote_waiter_given_extend_observes_expired_lease() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let waiter_session_id = 8;
    let lease_route = "lease://acme/locks/extend-expired";
    let key = lease_key(family, lease_route);
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let holder_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let waiter_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let router = Arc::new(Router::new());
    let waiter_mailbox = Arc::new(Mailbox::new(8));
    router.register(waiter_address.clone(), waiter_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    let holder_response = sink.state.runtime().handle_acquire(LeaseAcquireRequest {
        key: key.clone(),
        owner_session_id: session_id,
        owner_id: "owner1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
        reply_source: lease_address.clone(),
        reply_destination: Some(holder_address),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    let LeaseResponse::Acquired {
        fencing_token: holder_token,
    } = holder_response
    else {
        panic!("expected holder acquire");
    };
    let waiter_response = sink.state.runtime().handle_acquire(LeaseAcquireRequest {
        key: key.clone(),
        owner_session_id: waiter_session_id,
        owner_id: "owner2".to_string(),
        ttl_secs: 30,
        wait_seconds: 30,
        reply_source: lease_address,
        reply_destination: Some(waiter_address),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    let LeaseResponse::Queued {
        fencing_token: waiter_token,
    } = waiter_response
    else {
        panic!("expected queued waiter");
    };
    sink.state
        .core
        .leases
        .lock()
        .get_mut(&key)
        .expect("lease")
        .expiry = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .expect("past instant");

    // Act
    let response = sink
        .state
        .runtime()
        .handle_extend(&key, "owner1", holder_token, 30);
    let leases = admin_read_model.leases(None);
    let waiter_delivery = receive_envelope(&waiter_mailbox, "waiter acquired response");

    // Assert
    assert_eq!(response, LeaseResponse::Expired);
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(sink.state.runtime().pending_waiter_count(&key), 0);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].owner_session_id, "owner2");
    assert_eq!(leases[0].fencing_token, waiter_token);
    assert!(!sink
        .state
        .core
        .session_leases
        .lock()
        .contains_key(&session_id));
    assert!(sink
        .state
        .core
        .session_leases
        .lock()
        .get(&waiter_session_id)
        .is_some_and(|leases| leases.contains(&key)));
    assert!(waiter_delivery.payload::<FrameContext>().is_some());
}

#[test]
fn should_read_admin_waiters_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let lease_route = "lease://acme/locks/admin-waiter";
    let key = lease_key(family, lease_route);
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let holder_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let waiter_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model);
    let holder_response = sink.state.runtime().handle_acquire(LeaseAcquireRequest {
        key: key.clone(),
        owner_session_id: 7,
        owner_id: "owner1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
        reply_source: lease_address.clone(),
        reply_destination: Some(holder_address),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    assert!(matches!(holder_response, LeaseResponse::Acquired { .. }));
    let waiter_response = sink.state.runtime().handle_acquire(LeaseAcquireRequest {
        key,
        owner_session_id: 8,
        owner_id: "owner2".to_string(),
        ttl_secs: 30,
        wait_seconds: 30,
        reply_source: lease_address,
        reply_destination: Some(waiter_address),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    assert!(matches!(waiter_response, LeaseResponse::Queued { .. }));
    assert_eq!(sink.admin_waiters().len(), 1);

    // Act
    sink.stop();
    let command_waiters_after_stop = sink.admin_waiters();
    let direct_waiters_after_stop = sink.state.runtime().admin_waiters();

    // Assert
    assert!(command_waiters_after_stop.is_empty());
    assert_eq!(direct_waiters_after_stop.len(), 1);
}

#[test]
fn should_read_lease_live_counts_through_actor_command() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let lease_route = "lease://acme/locks/live-counts";
    let key = lease_key(family, lease_route);
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model);
    let holder_response = sink.state.runtime().handle_acquire(LeaseAcquireRequest {
        key,
        owner_session_id: session_id,
        owner_id: "owner1".to_string(),
        ttl_secs: 30,
        wait_seconds: 0,
        reply_source: lease_address.clone(),
        reply_destination: Some(subscriber_address.clone()),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    assert!(matches!(holder_response, LeaseResponse::Acquired { .. }));
    sink.deliver(Envelope::from_route(
        subscriber_address,
        lease_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(407),
            encode_lease_subscribe(lease_route),
            family,
        ),
    ))
    .expect("subscribe lease route");
    let _subscribe_ack = receive_envelope(&subscriber_mailbox, "subscribe ack envelope");
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(sink.subscription_count(), 1);

    // Act
    sink.stop_actor_for_tests();
    let live_counts = (sink.lease_count(), sink.subscription_count());

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(live_counts, (0, 0));
}

#[test]
fn should_remove_admin_lease_given_release() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        lease_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(lease_route, "", 30),
            family,
        ),
    ))
    .expect("acquire lease");
    let acquire_ack = receive_envelope(&subscriber_mailbox, "acquire ack envelope");
    let frame_ctx = acquire_ack
        .payload::<FrameContext>()
        .cloned()
        .expect("frame context");
    let fencing_token = u64::from_be_bytes([
        frame_ctx.payload[2],
        frame_ctx.payload[3],
        frame_ctx.payload[4],
        frame_ctx.payload[5],
        frame_ctx.payload[6],
        frame_ctx.payload[7],
        frame_ctx.payload[8],
        frame_ctx.payload[9],
    ]);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        lease_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(402),
            encode_lease_release(lease_route, "", fencing_token),
            family,
        ),
    ))
    .expect("release lease");
    let _release_ack = receive_envelope(&subscriber_mailbox, "release ack envelope");
    wait_for_lease_count(&sink, 0);
    wait_for_admin_lease_count(&admin_read_model, 0);

    // Assert
    assert!(admin_read_model.leases(None).is_empty());
    assert_eq!(sink.lease_count(), 0);
}

#[test]
fn should_track_admin_lease_renewals_given_extend() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        lease_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(lease_route, "", 30),
            family,
        ),
    ))
    .expect("acquire lease");
    let acquire_ack = receive_envelope(&subscriber_mailbox, "acquire ack envelope");
    let frame_ctx = acquire_ack
        .payload::<FrameContext>()
        .cloned()
        .expect("frame context");
    let fencing_token = u64::from_be_bytes([
        frame_ctx.payload[2],
        frame_ctx.payload[3],
        frame_ctx.payload[4],
        frame_ctx.payload[5],
        frame_ctx.payload[6],
        frame_ctx.payload[7],
        frame_ctx.payload[8],
        frame_ctx.payload[9],
    ]);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        lease_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(401),
            encode_lease_extend(lease_route, "", fencing_token, 30),
            family,
        ),
    ))
    .expect("extend lease");
    let _extend_ack = receive_envelope(&subscriber_mailbox, "extend ack envelope");
    wait_for_lease_count(&sink, 1);
    wait_for_admin_lease_count(&admin_read_model, 1);
    let leases = admin_read_model.leases(None);

    // Assert
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].renewals, 1);
}
