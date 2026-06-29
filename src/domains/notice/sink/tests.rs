use super::*;
use crate::control::admin::{NoticeRouteInfo, NoticeSubscription as AdminNoticeSubscription};
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
use crate::protocol::tlv::MessageType;
use crate::runtime::mailbox::Mailbox;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;

fn encode_notice_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_bytes(payload);
    Bytes::from(encoder.finish())
}

fn encode_notice_unsubscribe(subscription_id: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_u64(subscription_id);
    Bytes::from(encoder.finish())
}

fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

struct NoticeResponsePayload {
    status: u8,
    subscription_id: Option<u64>,
    error: Option<String>,
}

fn decode_notice_response(mailbox: &Mailbox) -> NoticeResponsePayload {
    let response_envelope = mailbox
        .receiver()
        .try_recv()
        .expect("notice response envelope");
    let response_frame = response_envelope
        .into_payload::<FrameContext>()
        .expect("notice response frame");
    let mut decoder = PayloadDecoder::new(&response_frame.payload);
    let status = decoder.get_u8().expect("notice response status");

    if status == 0 {
        let subscription_id = if decoder.remaining() > 0 {
            Some(
                decoder
                    .get_optional_u64()
                    .expect("notice response subscription id")
                    .expect("notice response subscription id value"),
            )
        } else {
            None
        };
        assert!(decoder.is_complete());
        NoticeResponsePayload {
            status,
            subscription_id,
            error: None,
        }
    } else {
        let _error_code = decoder.get_u32().expect("notice response error code");
        let error = decoder.get_string().expect("notice response error");
        assert!(decoder.is_complete());
        NoticeResponsePayload {
            status,
            subscription_id: None,
            error: Some(error),
        }
    }
}

fn subscribe_notice_pattern(
    sink: &NoticeDomainSink,
    subscriber_address: &RouteAddress,
    notice_address: &RouteAddress,
    session_id: u64,
    pattern: &str,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe(pattern),
            family,
        ),
    ))
    .expect("subscribe notice pattern");
}

fn unsubscribe_notice_pattern(
    sink: &NoticeDomainSink,
    subscriber_address: &RouteAddress,
    notice_address: &RouteAddress,
    session_id: u64,
    subscription_id: u64,
    family: RouteFamily,
) {
    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(502),
            encode_notice_unsubscribe(subscription_id),
            family,
        ),
    ))
    .expect("unsubscribe notice pattern");
}

fn assert_notice_admin_subscriptions(
    actual: &[AdminNoticeSubscription],
    expected_patterns: &[&str],
) {
    let mut actual_patterns: Vec<&str> =
        actual.iter().map(|entry| entry.pattern.as_str()).collect();
    actual_patterns.sort_unstable();

    let mut expected_patterns = expected_patterns.to_vec();
    expected_patterns.sort_unstable();

    assert_eq!(actual_patterns, expected_patterns);
}

fn assert_notice_admin_routes(actual: &[NoticeRouteInfo], expected_routes: &[&str]) {
    let mut actual_routes: Vec<&str> = actual.iter().map(|entry| entry.route.as_str()).collect();
    actual_routes.sort_unstable();

    let mut expected_routes = expected_routes.to_vec();
    expected_routes.sort_unstable();

    assert_eq!(actual_routes, expected_routes);
}

fn refresh_notice_admin_snapshot(sink: &NoticeDomainSink) {
    sink.refresh_admin_snapshot_if_dirty();
}

#[test]
fn should_create_notice_domain_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = NoticeDomainSink::new(router, admin_read_model);

    // Assert
    assert!(sink.active.load(Ordering::Relaxed));
}

#[test]
fn should_include_notice_subscription_given_flexible_route_shape() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_route = "notice://acme/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    // Act
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);
    refresh_notice_admin_snapshot(&sink);

    // Assert
    let subscriptions = admin_read_model.notice_subscriptions(None, None);
    let routes = admin_read_model.notice_routes(None);
    assert_notice_admin_subscriptions(&subscriptions, &[notice_route]);
    assert_notice_admin_routes(&routes, &[notice_route]);
    assert_eq!(subscriptions[0].realm, "acme");
}

#[test]
fn should_track_notice_publish_activity_given_matching_publish() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let publisher_session_id = 11;
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        subscriber_session_id,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);

    // Act
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            publisher_session_id,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"hello"),
            family,
        ),
    ))
    .expect("publish notice event");
    refresh_notice_admin_snapshot(&sink);

    // Assert
    let routes = admin_read_model.notice_routes(None);
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].route, notice_route);
    assert_eq!(routes[0].subscribers, 1);
    assert_eq!(routes[0].publishes_total, 1);
    assert_eq!(routes[0].publishes_per_minute, 1.0);
}

#[test]
fn should_remove_notice_subscriptions_given_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let publisher_session_id = 11;
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new(notice_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    let publisher_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register(publisher_address.clone(), publisher_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model);

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe(notice_route),
            family,
        ),
    ))
    .expect("subscribe notice route");
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);
    assert!(subscribe_response.subscription_id.is_some());

    // Act
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("notice://cleanup")),
        crate::runtime::SessionCleanup {
            session_id: subscriber_session_id,
        },
    ))
    .expect("cleanup notice subscriber");
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            publisher_session_id,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"hello"),
            family,
        ),
    ))
    .expect("publish notice event");

    // Assert
    assert_eq!(sink.subscription_count(), 0);
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
    assert!(publisher_mailbox.receiver().try_recv().is_err());
    assert!(sink.families.lock().is_empty());
}

#[test]
fn should_clear_notice_admin_snapshot_given_session_cleanup_with_mixed_subscriptions() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let exact_route = "notice://acme/app/events";
    let wildcard_route = "notice://acme/app/*";
    let notice_address = RouteAddress::new(family, Route::new(exact_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        exact_route,
        family,
    );
    let exact_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(exact_response.status, 0);

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        wildcard_route,
        family,
    );
    let wildcard_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(wildcard_response.status, 0);

    refresh_notice_admin_snapshot(&sink);

    let before_subscriptions = admin_read_model.notice_subscriptions(None, None);
    let before_routes = admin_read_model.notice_routes(None);
    assert_notice_admin_subscriptions(&before_subscriptions, &[exact_route, wildcard_route]);
    assert_notice_admin_routes(&before_routes, &[exact_route, wildcard_route]);

    // Act
    sink.unsubscribe_all_for_session(session_id);

    // Assert
    assert_eq!(sink.subscription_count(), 0);
    refresh_notice_admin_snapshot(&sink);
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
    assert!(admin_read_model.notice_routes(None).is_empty());
    assert!(sink.families.lock().is_empty());
}

#[test]
fn should_prune_notice_route_stats_after_last_subscription_is_removed() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let publisher_session_id = 11;
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new(notice_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    let publisher_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register(publisher_address.clone(), publisher_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model);

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let _subscribe_response = decode_notice_response(&subscriber_mailbox);
    sink.deliver(Envelope::from_route(
        publisher_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            publisher_session_id,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"hello"),
            family,
        ),
    ))
    .expect("publish notice event");
    assert_eq!(sink.route_stats.lock().len(), 1);
    drain_mailbox(&subscriber_mailbox);
    drain_mailbox(&publisher_mailbox);

    // Act
    sink.unsubscribe_all_for_session(session_id);
    refresh_notice_admin_snapshot(&sink);

    // Assert
    assert!(sink.route_stats.lock().is_empty());
}

#[test]
fn should_retain_other_notice_subscription_given_unsubscribe_on_same_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_session_id = 7;
    let publisher_session_id = 11;
    let removed_route = "notice://acme/app/events";
    let retained_route = "notice://acme/app/audits";
    let notice_address = RouteAddress::new(family, Route::new(removed_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(16));
    let publisher_mailbox = Arc::new(Mailbox::new(16));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register(publisher_address.clone(), publisher_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model);

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe(removed_route),
            family,
        ),
    ))
    .expect("subscribe removed notice route");
    let removed_subscribe_response = decode_notice_response(&subscriber_mailbox);
    let removed_subscription_id = removed_subscribe_response
        .subscription_id
        .expect("removed subscribe subscription id");

    sink.deliver(Envelope::from_route(
        subscriber_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe(retained_route),
            family,
        ),
    ))
    .expect("subscribe retained notice route");
    let _retained_subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(sink.subscription_count(), 2);
    drain_mailbox(&subscriber_mailbox);
    drain_mailbox(&publisher_mailbox);

    // Act
    sink.deliver(Envelope::from_route(
        subscriber_address,
        notice_address.clone(),
        FrameContext::new(
            subscriber_session_id,
            ChannelId::Sub,
            MessageType::new(502),
            encode_notice_unsubscribe(removed_subscription_id),
            family,
        ),
    ))
    .expect("unsubscribe removed notice route");
    let unsubscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(unsubscribe_response.status, 0);
    assert!(unsubscribe_response.subscription_id.is_none());
    assert_eq!(sink.subscription_count(), 1);

    sink.deliver(Envelope::from_route(
        publisher_address.clone(),
        notice_address.clone(),
        FrameContext::new(
            publisher_session_id,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(removed_route, b"removed"),
            family,
        ),
    ))
    .expect("publish removed notice event");
    assert!(publisher_mailbox.receiver().try_recv().is_err());
    assert!(subscriber_mailbox.receiver().try_recv().is_err());

    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            publisher_session_id,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(retained_route, b"retained"),
            family,
        ),
    ))
    .expect("publish retained notice event");

    // Assert
    let notify_envelope = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("retained notice notify envelope");
    let notify_frame = notify_envelope
        .into_payload::<FrameContext>()
        .expect("retained notice notify frame");
    assert_eq!(notify_frame.msg_type.as_u16(), 504);
    let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
    let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
    let notified_route = notify_decoder.get_string().expect("notify route");
    let notified_payload = notify_decoder.get_bytes().expect("notify payload");
    assert_eq!(notified_route, retained_route);
    assert_eq!(notified_payload.as_ref(), b"retained");
    assert!(notify_decoder.is_complete());

    assert!(publisher_mailbox.receiver().try_recv().is_err());
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[test]
fn should_retain_notice_admin_snapshot_entry_given_unsubscribe_of_sibling_pattern() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let removed_route = "notice://acme/app/events";
    let retained_route = "notice://acme/app/audits";
    let notice_address = RouteAddress::new(family, Route::new(removed_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        removed_route,
        family,
    );
    let removed_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(removed_response.status, 0);
    let removed_subscription_id = removed_response
        .subscription_id
        .expect("removed subscription id");

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        retained_route,
        family,
    );
    let retained_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(retained_response.status, 0);

    // Act
    unsubscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        removed_subscription_id,
        family,
    );
    let unsubscribe_response = decode_notice_response(&subscriber_mailbox);

    // Assert
    assert_eq!(unsubscribe_response.status, 0);
    assert_eq!(sink.subscription_count(), 1);

    refresh_notice_admin_snapshot(&sink);

    let subscriptions = admin_read_model.notice_subscriptions(None, None);
    let routes = admin_read_model.notice_routes(None);
    assert_notice_admin_subscriptions(&subscriptions, &[retained_route]);
    assert_notice_admin_routes(&routes, &[retained_route]);
}

#[test]
fn should_increment_delivery_drop_counter_given_failing_subscriber_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new(notice_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let sink = NoticeDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);

    let before_drops =
        crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total");

    struct FailingSink;

    impl MailboxSink for FailingSink {
        fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
            Err(DeliveryError::ActorStopped)
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    router.register(subscriber_address.clone(), Arc::new(FailingSink));

    // Act
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            11,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"dropped"),
            family,
        ),
    ))
    .expect("publish notice event");

    // Assert
    assert_eq!(
        crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total"),
        before_drops + 1
    );
    assert_eq!(sink.subscription_count(), 1);
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[test]
fn should_retry_notice_delivery_when_outbound_mailbox_is_temporarily_full() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new(notice_route));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let sink = NoticeDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);

    let retry_state = Arc::new(Mutex::new(vec![
        Err(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        }),
        Err(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        }),
        Ok(()),
    ]));

    struct RetrySink {
        state: Arc<Mutex<Vec<Result<(), DeliveryError>>>>,
    }

    impl MailboxSink for RetrySink {
        fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
            self.state.lock().remove(0)
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    router.register(
        subscriber_address.clone(),
        Arc::new(RetrySink {
            state: retry_state.clone(),
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            11,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"delayed"),
            family,
        ),
    ))
    .expect("publish notice event");

    // Assert
    assert!(retry_state.lock().is_empty());
}

#[test]
fn should_reject_wildcard_subscription_when_session_limit_is_exceeded() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION + 4));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    for pattern_index in 0..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
        let pattern = format!("notice://acme/app/{pattern_index}/*");
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            &pattern,
            family,
        );
        let response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(response.status, 0);
    }

    let overflow_pattern = "notice://acme/app/overflow/*";

    // Act
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        overflow_pattern,
        family,
    );
    let overflow_response = decode_notice_response(&subscriber_mailbox);

    // Assert
    assert_eq!(overflow_response.status, 1);
    assert_eq!(
        overflow_response.error.as_deref(),
        Some("wildcard subscription limit exceeded (128 per session)")
    );
    assert_eq!(
        sink.subscription_count(),
        MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
    );
    refresh_notice_admin_snapshot(&sink);
    assert_eq!(
        admin_read_model.notice_subscriptions(None, None).len(),
        MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
    );
}

#[test]
fn should_return_existing_subscription_id_given_idempotent_wildcard_subscribe_at_limit() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let duplicated_pattern = "notice://acme/app/dupe/*";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION + 5));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let sink = NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        duplicated_pattern,
        family,
    );
    let first_response = decode_notice_response(&subscriber_mailbox);
    let first_subscription_id = first_response
        .subscription_id
        .expect("first subscription id");

    for pattern_index in 1..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
        let pattern = format!("notice://acme/app/{pattern_index}/*");
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            &pattern,
            family,
        );
        let response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(response.status, 0);
    }

    // Act
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        duplicated_pattern,
        family,
    );
    let duplicate_response = decode_notice_response(&subscriber_mailbox);

    // Assert
    assert_eq!(duplicate_response.status, 0);
    assert_eq!(
        duplicate_response.subscription_id,
        Some(first_subscription_id)
    );
    assert_eq!(
        sink.subscription_count(),
        MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
    );
}
