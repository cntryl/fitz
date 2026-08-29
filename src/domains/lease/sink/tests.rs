use super::*;

#[test]
fn should_confirm_lease_session_cleanup_before_reporting_delivery() {
    // Arrange
    let family = RouteFamily::new(1);
    let sink = LeaseDomainSink::new(
        Arc::new(Router::new()),
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Lease actor should block");
    let (result_tx, result_rx) = crossbeam_channel::bounded(1);

    // Act
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let result = sink.deliver_high_priority(Envelope::new(
                RouteAddress::new(family, Route::new("lease://cleanup")),
                crate::runtime::SessionCleanup { session_id: 7 },
            ));
            let _ = result_tx.send(result);
        });
        let early_result = result_rx.recv_timeout(Duration::from_millis(50));
        let returned_early = early_result.is_ok();
        release_tx.send(()).expect("release Lease actor");
        let final_result = early_result.unwrap_or_else(|_| {
            result_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("Lease cleanup result")
        });

        // Assert
        assert!(!returned_early, "cleanup returned before it executed");
        assert_eq!(final_result, Ok(()));
    });
}
use crate::dispatch::protocol::frame::ChannelId;
use crate::dispatch::protocol::frame_context::FrameContext;
use crate::dispatch::protocol::payload_codec::PayloadEncoder;
use crate::dispatch::protocol::tlv::MessageType;
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use crate::runtime::mailbox::Mailbox;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::ClientChannel;
use bytes::Bytes;
use std::sync::Arc;

mod correctness;

fn encode_lease_acquire(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(ttl_secs);
    Bytes::from(encoder.finish())
}

fn encode_waiting_lease_acquire(
    route: &str,
    owner_id: &str,
    ttl_secs: u64,
    wait_seconds: u32,
) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(ttl_secs);
    encoder.put_u32(wait_seconds);
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

fn encode_lease_query(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    Bytes::from(encoder.finish())
}

fn encode_lease_subscribe(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
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

fn lease_response_payloads(prepared: bool, frames: &[(u16, Bytes)]) -> Vec<Bytes> {
    let family = RouteFamily::new(1);
    let session_id = 7;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model);
    let mut responses = Vec::new();

    for (msg_type, payload) in frames {
        let envelope = if prepared {
            let meta = crate::runtime::ClientFrameMeta::new(
                session_id,
                ClientChannel::Lease,
                *msg_type,
                family,
            );
            let parsed = crate::dispatch::protocol::lease_codec::parse_prepared_request(
                *msg_type, family, session_id, payload,
            );
            Envelope::from_route(
                subscriber_address.clone(),
                lease_address.clone(),
                crate::domains::lease::protocol::PreparedLeaseClientRequest::new(meta, parsed),
            )
        } else {
            Envelope::from_route(
                subscriber_address.clone(),
                lease_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Lease,
                    MessageType::new(*msg_type),
                    payload.clone(),
                    family,
                ),
            )
        };

        sink.deliver(envelope).expect("deliver lease frame");
        let response = receive_envelope(&subscriber_mailbox, "lease response");
        responses.push(
            response
                .payload::<FrameContext>()
                .expect("frame response")
                .payload
                .clone(),
        );
    }

    responses
}

struct StoppingResponseSink {
    entered: crossbeam_channel::Sender<()>,
    release: crossbeam_channel::Receiver<()>,
}

impl crate::runtime::MailboxSink for StoppingResponseSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), crate::runtime::DeliveryError> {
        self.entered.send(()).expect("record stale delivery");
        self.release.recv().expect("release stale delivery");
        Err(crate::runtime::DeliveryError::ActorStopped)
    }

    fn deliver_high_priority(
        &self,
        envelope: Envelope,
    ) -> Result<(), crate::runtime::DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
fn should_reresolve_reply_route_when_resolved_lease_sink_stops() {
    // Arrange
    let family = RouteFamily::new(1);
    let reply_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let lease_address = RouteAddress::new(family, Route::new("lease://acme/locks/resource"));
    let router = Arc::new(Router::new());
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    router.register(
        reply_address.clone(),
        Arc::new(StoppingResponseSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let replacement = Arc::new(Mailbox::new(1));
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let sink = LeaseDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
    .with_metrics(metrics.clone());
    let request = Envelope::from_route(
        reply_address.clone(),
        lease_address,
        FrameContext::new(
            7,
            ChannelId::Lease,
            MessageType::new(crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE),
            encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
            family,
        ),
    );

    // Act
    sink.deliver(request).expect("deliver lease request");
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("stale sink entered");
    router.register(reply_address, replacement.clone());
    release_tx.send(()).expect("release stale sink");

    // Assert
    receive_envelope(&replacement, "replacement lease response");
    assert_eq!(
        metrics.counter_get(crate::domains::lease::metrics::METRIC_RESPONSE_DROPS_TOTAL),
        0,
        "a response accepted after route re-resolution was not dropped"
    );
}

#[test]
fn should_create_lease_domain_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = LeaseDomainSink::new(router, admin_read_model);

    // Assert
    assert!(sink.is_active_for_tests());
    assert!(sink.is_actor_running());
}

#[test]
fn should_match_fallback_for_prepared_lease_acquire() {
    // Arrange
    let frames = vec![(
        crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
        encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
    )];

    // Act
    let fallback = lease_response_payloads(false, &frames);
    let prepared = lease_response_payloads(true, &frames);

    // Assert
    assert_eq!(prepared, fallback);
}

#[test]
fn should_match_fallback_for_prepared_lease_extend() {
    // Arrange
    let frames = vec![
        (
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::RENEW,
            encode_lease_extend("lease://acme/locks/resource", "owner", 1, 60),
        ),
    ];

    // Act
    let fallback = lease_response_payloads(false, &frames);
    let prepared = lease_response_payloads(true, &frames);

    // Assert
    assert_eq!(prepared, fallback);
}

#[test]
fn should_keep_lease_actor_responsive_after_wire_ttl_rejections() {
    // Arrange
    let frames = vec![
        (
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            encode_lease_acquire("lease://acme/locks/resource", "owner", u64::MAX),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::RENEW,
            encode_lease_extend("lease://acme/locks/resource", "owner", 1, u64::MAX),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::QUERY,
            encode_lease_query("lease://acme/locks/resource"),
        ),
    ];

    // Act
    let responses = lease_response_payloads(false, &frames);

    // Assert
    assert_eq!(
        crate::dispatch::protocol::error_codes::decode_error_body(&responses[0])
            .expect("oversized acquire error")
            .0,
        crate::dispatch::protocol::error_codes::lease::ERR_BAD_REQUEST
    );
    assert_eq!(responses[1][0], 0);
    assert_eq!(
        crate::dispatch::protocol::error_codes::decode_error_body(&responses[2])
            .expect("oversized extend error")
            .0,
        crate::dispatch::protocol::error_codes::lease::ERR_BAD_REQUEST
    );
    assert_eq!(responses[3][0], 0);
}

#[test]
fn should_match_fallback_for_prepared_lease_release() {
    // Arrange
    let frames = vec![
        (
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::RELEASE,
            encode_lease_release("lease://acme/locks/resource", "owner", 1),
        ),
    ];

    // Act
    let fallback = lease_response_payloads(false, &frames);
    let prepared = lease_response_payloads(true, &frames);

    // Assert
    assert_eq!(prepared, fallback);
}

#[test]
fn should_match_fallback_for_prepared_lease_query() {
    // Arrange
    let frames = vec![
        (
            crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
            encode_lease_acquire("lease://acme/locks/resource", "owner", 30),
        ),
        (
            crate::dispatch::protocol::lease_codec::msg_type::QUERY,
            encode_lease_query("lease://acme/locks/resource"),
        ),
    ];

    // Act
    let fallback = lease_response_payloads(false, &frames);
    let prepared = lease_response_payloads(true, &frames);

    // Assert
    assert_eq!(prepared, fallback);
}

#[test]
fn should_reject_prepared_invalid_lease_route() {
    // Arrange
    let frames = vec![(
        crate::dispatch::protocol::lease_codec::msg_type::ACQUIRE,
        encode_lease_acquire("lease://acme/locks", "owner", 30),
    )];

    // Act
    let prepared = lease_response_payloads(true, &frames);

    // Assert
    let (code, message) = crate::dispatch::protocol::error_codes::decode_error_body(&prepared[0])
        .expect("invalid lease route error");
    assert_eq!(
        code,
        crate::dispatch::protocol::error_codes::lease::ERR_BAD_REQUEST
    );
    assert_eq!(message, "invalid lease route");
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
    assert!(sink.watch_families_are_empty_for_tests());
}

#[test]
fn should_reject_stale_acquire_after_disconnect_cleanup_marks_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 9;
    let lease_route = "lease://acme/locks/resource";
    let lease_address = RouteAddress::new(family, Route::new(lease_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model);

    // Act: cleanup for this session runs and completes before the stale
    // acquire below is processed - equivalent to what the high-priority
    // mailbox lane guarantees a real disconnect races against a queued
    // normal-lane request.
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("lease://cleanup")),
        crate::runtime::SessionCleanup { session_id },
    ))
    .expect("cleanup session");

    sink.deliver(Envelope::from_route(
        subscriber_address,
        lease_address,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(lease_route, "", 30),
            family,
        ),
    ))
    .expect("deliver stale acquire");
    let _ack = receive_envelope(&subscriber_mailbox, "stale acquire response envelope");

    // Assert: the stale acquire from the now-cleaned-up session is rejected
    // instead of resurrecting a lease for it.
    assert_eq!(sink.lease_count(), 0);
}

#[test]
fn should_preserve_other_session_leases_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let route_a = "lease://acme/locks/resource-a";
    let route_b = "lease://acme/locks/resource-b";
    let router = Arc::new(Router::new());
    let session_7_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let session_8_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    router.register(session_7_address.clone(), Arc::new(Mailbox::new(1)));
    router.register(session_8_address.clone(), Arc::new(Mailbox::new(1)));
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = LeaseDomainSink::new(router, admin_read_model.clone());

    sink.deliver(Envelope::from_route(
        session_7_address,
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
        session_8_address,
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
    sink.cleanup_session(7).expect("cleanup Lease session");
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

    let holder_response = sink.acquire_for_tests(LeaseAcquireRequest {
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
    let waiter_response = sink.acquire_for_tests(LeaseAcquireRequest {
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
    assert!(sink.expire_lease_for_tests(&key));

    // Act
    let response = sink.extend_for_tests(&key, "owner1", holder_token, 30);
    let leases = admin_read_model.leases(None);
    let waiter_delivery = receive_envelope(&waiter_mailbox, "waiter acquired response");

    // Assert
    assert_eq!(response, LeaseResponse::Expired);
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(sink.pending_waiter_count_for_tests(&key), 0);
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].owner_session_id, "owner2");
    assert_eq!(leases[0].fencing_token, waiter_token);
    assert!(!sink.session_leases_contain_for_tests(session_id, &key));
    assert!(sink.session_leases_contain_for_tests(waiter_session_id, &key));
    assert!(waiter_delivery.payload::<FrameContext>().is_some());
}

#[test]
fn should_not_retain_lease_when_promoted_waiter_cannot_receive_grant() {
    // Arrange
    let family = RouteFamily::new(1);
    let key = lease_key(family, "lease://acme/locks/undeliverable-waiter");
    let lease_address = RouteAddress::new(
        family,
        Route::new("lease://acme/locks/undeliverable-waiter"),
    );
    let holder_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let waiter_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let router = Arc::new(Router::new());
    let waiter_mailbox = Arc::new(Mailbox::new(1));
    router.register(waiter_address.clone(), waiter_mailbox.clone());
    waiter_mailbox
        .sender()
        .try_send(Envelope::new(waiter_address.clone(), 1_u8))
        .expect("fill waiter mailbox");
    let sink = LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let holder = sink.acquire_for_tests(LeaseAcquireRequest {
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
    let LeaseResponse::Acquired { fencing_token } = holder else {
        panic!("expected holder acquire");
    };
    let queued = sink.acquire_for_tests(LeaseAcquireRequest {
        key: key.clone(),
        owner_session_id: 8,
        owner_id: "owner2".to_string(),
        ttl_secs: 30,
        wait_seconds: 30,
        reply_source: lease_address,
        reply_destination: Some(waiter_address),
        channel: ClientChannel::Sub,
        route_family: family,
    });
    assert!(matches!(queued, LeaseResponse::Queued { .. }));
    assert!(sink.expire_lease_for_tests(&key));

    // Act
    let response = sink.extend_for_tests(&key, "owner1", fencing_token, 30);

    // Assert
    assert_eq!(response, LeaseResponse::Expired);
    assert_eq!(sink.lease_count(), 0);
    assert!(!sink.session_leases_contain_for_tests(8, &key));
}

#[test]
fn should_promote_next_waiter_when_prior_waiter_cannot_receive_grant() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "lease://acme/locks/undeliverable-first-waiter";
    let key = lease_key(family, route);
    let lease_address = RouteAddress::new(family, Route::new(route));
    let holder_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let first_waiter_address = RouteAddress::new(family, Route::new("inbox://session/8"));
    let next_waiter_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let router = Arc::new(Router::new());
    let first_waiter_mailbox = Arc::new(Mailbox::new(1));
    let next_waiter_mailbox = Arc::new(Mailbox::new(1));
    router.register(first_waiter_address.clone(), first_waiter_mailbox.clone());
    router.register(next_waiter_address.clone(), next_waiter_mailbox.clone());
    first_waiter_mailbox
        .sender()
        .try_send(Envelope::new(first_waiter_address.clone(), 1_u8))
        .expect("fill first waiter mailbox");
    let sink = LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let holder = sink.acquire_for_tests(LeaseAcquireRequest {
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
    let LeaseResponse::Acquired { fencing_token } = holder else {
        panic!("expected holder acquire");
    };
    for (session_id, owner_id, destination) in [
        (8, "owner2", first_waiter_address),
        (9, "owner3", next_waiter_address),
    ] {
        let queued = sink.acquire_for_tests(LeaseAcquireRequest {
            key: key.clone(),
            owner_session_id: session_id,
            owner_id: owner_id.to_string(),
            ttl_secs: 30,
            wait_seconds: 30,
            reply_source: lease_address.clone(),
            reply_destination: Some(destination),
            channel: ClientChannel::Sub,
            route_family: family,
        });
        assert!(matches!(queued, LeaseResponse::Queued { .. }));
    }
    assert!(sink.expire_lease_for_tests(&key));

    // Act
    let response = sink.extend_for_tests(&key, "owner1", fencing_token, 30);

    // Assert
    assert_eq!(response, LeaseResponse::Expired);
    let grant = receive_envelope(&next_waiter_mailbox, "next waiter acquired response");
    assert!(grant.payload::<FrameContext>().is_some());
    assert!(sink.session_leases_contain_for_tests(9, &key));
    assert_eq!(sink.pending_waiter_count_for_tests(&key), 0);
}

#[test]
fn should_not_retain_direct_lease_when_acquire_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let route = "lease://acme/locks/undeliverable-direct";
    let client = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    let mailbox = Arc::new(Mailbox::new(1));
    router.register(client.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(client.clone(), 1_u8))
        .expect("fill client mailbox");
    let sink = LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    // Act
    sink.deliver(Envelope::from_route(
        client,
        destination,
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(400),
            encode_lease_acquire(route, "", 30),
            family,
        ),
    ))
    .expect("deliver acquire");
    std::thread::sleep(Duration::from_millis(50));

    // Assert
    assert_eq!(sink.lease_count(), 0);
}

#[test]
fn should_not_retain_waiter_when_queued_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "lease://acme/locks/undeliverable-queued";
    let key = lease_key(family, route);
    let client = RouteAddress::new(family, Route::new("inbox://session/8"));
    let destination = RouteAddress::new(family, Route::new(route));
    let router = Arc::new(Router::new());
    let mailbox = Arc::new(Mailbox::new(1));
    router.register(client.clone(), mailbox.clone());
    mailbox
        .sender()
        .try_send(Envelope::new(client.clone(), 1_u8))
        .expect("fill client mailbox");
    let sink = LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    assert!(matches!(
        sink.acquire_for_tests(LeaseAcquireRequest {
            key: key.clone(),
            owner_session_id: 7,
            owner_id: "owner1".to_string(),
            ttl_secs: 30,
            wait_seconds: 0,
            reply_source: destination.clone(),
            reply_destination: None,
            channel: ClientChannel::Sub,
            route_family: family,
        }),
        LeaseResponse::Acquired { .. }
    ));

    // Act
    sink.deliver(Envelope::from_route(
        client,
        destination,
        FrameContext::new(
            8,
            ChannelId::Sub,
            MessageType::new(400),
            encode_waiting_lease_acquire(route, "owner2", 30, 30),
            family,
        ),
    ))
    .expect("deliver waiting acquire");
    std::thread::sleep(Duration::from_millis(50));

    // Assert
    assert_eq!(sink.lease_count(), 1);
    assert_eq!(sink.pending_waiter_count_for_tests(&key), 0);
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
    let holder_response = sink.acquire_for_tests(LeaseAcquireRequest {
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
    let waiter_response = sink.acquire_for_tests(LeaseAcquireRequest {
        key: key.clone(),
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
    let queued_waiter_count_after_stop = sink.pending_acquire_count_for_tests(&key);

    // Assert
    assert!(command_waiters_after_stop.is_empty());
    assert_eq!(queued_waiter_count_after_stop, 1);
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
    let holder_response = sink.acquire_for_tests(LeaseAcquireRequest {
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
