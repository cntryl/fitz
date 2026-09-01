use super::*;
use crate::domains::notice::sink::delivery_worker::{
    deliver_with_retry, deliver_with_retry_for_test,
};

#[test]
fn should_retry_delivery_policy_with_payload_independent_envelope_builder() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Router::new();
    let retry_state = Arc::new(Mutex::new(vec![
        Err(DeliveryError::MailboxFull {
            capacity: 1,
            current_len: 1,
        }),
        Ok(()),
    ]));
    router.register(
        subscriber.clone(),
        Arc::new(RetrySink {
            state: retry_state.clone(),
        }),
    );

    // Act
    deliver_with_retry(&router, &subscriber, || {
        Envelope::new(subscriber.clone(), ())
    });

    // Assert
    assert!(retry_state.lock().is_empty());
}

#[test]
fn should_reresolve_subscriber_route_before_notice_retry() {
    struct BlockingRetrySink {
        attempts: Arc<std::sync::atomic::AtomicUsize>,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    }

    impl MailboxSink for BlockingRetrySink {
        fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
            let attempt = self
                .attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 {
                self.entered.send(()).expect("retry observer");
                self.release.recv().expect("retry release");
                return Err(DeliveryError::MailboxFull {
                    capacity: 1,
                    current_len: 1,
                });
            }
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    // Arrange
    let family = RouteFamily::new(1);
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    router.register(
        subscriber.clone(),
        Arc::new(BlockingRetrySink {
            attempts: attempts.clone(),
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let delivery_router = router.clone();
    let delivery_subscriber = subscriber.clone();
    let delivery = std::thread::spawn(move || {
        deliver_with_retry_for_test(
            &delivery_router,
            &delivery_subscriber,
            Duration::from_secs(1),
            || Envelope::new(delivery_subscriber.clone(), ()),
        );
    });
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("first attempt should block");
    let replacement = Arc::new(Mailbox::new(1));
    router.register(subscriber, replacement.clone());

    // Act
    release_tx.send(()).expect("release first attempt");
    delivery.join().expect("join notice retry");

    // Assert
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    replacement
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("replacement subscriber should receive retry");
}

#[test]
fn should_not_retain_subscription_when_subscribe_response_cannot_be_delivered() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let pattern = "notice://acme/app/undeliverable";
    let notice_address = RouteAddress::new(family, Route::new(pattern));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(1));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    subscriber_mailbox
        .sender()
        .try_send(Envelope::new(subscriber_address.clone(), 1_u8))
        .expect("fill subscriber mailbox");
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink =
        NoticeDomainSink::new(router, admin_read_model.clone()).with_metrics(metrics.clone());

    // Act
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        pattern,
        family,
    );

    // Assert
    assert_eq!(sink.subscription_count(), Ok(0));
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
    assert!(admin_read_model.notice_routes(None).is_empty());
    assert_eq!(
        metrics.gauge_get(crate::domains::notice::metrics::METRIC_SUBSCRIPTIONS_GAUGE),
        0
    );
}

#[test]
fn should_contain_panicking_cached_notice_sink() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Router::new();
    router.register(subscriber.clone(), Arc::new(PanickingSink));

    // Act
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        deliver_with_retry(&router, &subscriber, || {
            Envelope::new(subscriber.clone(), ())
        });
    }));

    // Assert
    assert!(result.is_ok());
}

#[test]
#[allow(clippy::too_many_lines)]
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
    assert_eq!(sink.subscription_count(), Ok(2));
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
    assert_eq!(sink.subscription_count(), Ok(1));

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
        .recv_timeout(Duration::from_secs(1))
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
    assert_eq!(sink.subscription_count(), Ok(1));

    refresh_notice_admin_snapshot(&sink);

    let subscriptions = admin_read_model.notice_subscriptions(None, None);
    let notice_routes = admin_read_model.notice_routes(None);
    assert_notice_admin_subscriptions(&subscriptions, &[retained_route]);
    assert_notice_admin_routes(&notice_routes, &[retained_route]);
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
    let deadline = Instant::now() + Duration::from_secs(1);
    while crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total")
        < before_drops + 1
        && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(
        crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total"),
        before_drops + 1
    );
    assert_eq!(sink.subscription_count(), Ok(1));
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
        Ok(()),
    ]));

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
    let deadline = Instant::now() + Duration::from_secs(1);
    while !retry_state.lock().is_empty() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert!(retry_state.lock().is_empty());
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_isolate_unrelated_family_when_notice_subscriber_blocks() {
    // Arrange
    let blocked_family = RouteFamily::new(1);
    let healthy_family = RouteFamily::new(2);
    let blocked_route = "notice://acme/blocked";
    let healthy_route = "notice://acme/healthy";
    let blocked_notice = RouteAddress::new(blocked_family, Route::new(blocked_route));
    let healthy_notice = RouteAddress::new(healthy_family, Route::new(healthy_route));
    let blocked_subscriber = RouteAddress::new(blocked_family, Route::new("inbox://session/7"));
    let healthy_subscriber = RouteAddress::new(healthy_family, Route::new("inbox://session/8"));
    let router = Arc::new(Router::new());
    let blocked_mailbox = Arc::new(Mailbox::new(4));
    let healthy_mailbox = Arc::new(Mailbox::new(4));
    router.register(blocked_subscriber.clone(), blocked_mailbox.clone());
    router.register(healthy_subscriber.clone(), healthy_mailbox.clone());
    let sink = Arc::new(NoticeDomainSink::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    ));
    subscribe_notice_pattern(
        &sink,
        &blocked_subscriber,
        &blocked_notice,
        7,
        blocked_route,
        blocked_family,
    );
    assert_eq!(decode_notice_response(&blocked_mailbox).status, 0);
    subscribe_notice_pattern(
        &sink,
        &healthy_subscriber,
        &healthy_notice,
        8,
        healthy_route,
        healthy_family,
    );
    assert_eq!(decode_notice_response(&healthy_mailbox).status, 0);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    router.register(
        blocked_subscriber,
        Arc::new(BlockingSink {
            entered: entered_tx,
            release: release_rx,
        }),
    );
    let blocked_sink = sink.clone();
    let blocked_publish = std::thread::spawn(move || {
        blocked_sink.deliver(Envelope::from_route(
            RouteAddress::new(blocked_family, Route::new("inbox://session/11")),
            blocked_notice,
            FrameContext::new(
                11,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(blocked_route, b"blocked"),
                blocked_family,
            ),
        ))
    });
    entered_rx.recv().expect("blocked delivery entered");

    // Act
    let started = Instant::now();
    let healthy_result = sink.deliver(Envelope::from_route(
        RouteAddress::new(healthy_family, Route::new("inbox://session/12")),
        healthy_notice,
        FrameContext::new(
            12,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(healthy_route, b"healthy"),
            healthy_family,
        ),
    ));
    let elapsed = started.elapsed();
    let delivered = healthy_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1));
    release_tx.send(()).expect("release blocked delivery");
    let blocked_result = blocked_publish.join().expect("join blocked publish");

    // Assert
    assert!(healthy_result.is_ok());
    assert!(blocked_result.is_ok());
    assert!(delivered.is_ok());
    assert!(elapsed < Duration::from_millis(250), "elapsed: {elapsed:?}");
}

#[test]
fn should_reject_wildcard_subscription_when_session_limit_is_exceeded() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_REGISTRATIONS_PER_SESSION + 4));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    for pattern_index in 0..MAX_WILDCARD_REGISTRATIONS_PER_SESSION {
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
        Ok(MAX_WILDCARD_REGISTRATIONS_PER_SESSION)
    );
    refresh_notice_admin_snapshot(&sink);
    assert_eq!(
        admin_read_model.notice_subscriptions(None, None).len(),
        MAX_WILDCARD_REGISTRATIONS_PER_SESSION
    );
}

#[test]
fn should_reject_exact_subscription_when_total_session_limit_is_exceeded() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(MAX_NOTICE_REGISTRATIONS_PER_SESSION + 4));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let sink = NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );

    for pattern_index in 0..MAX_NOTICE_REGISTRATIONS_PER_SESSION {
        let pattern = format!("notice://acme/app/{pattern_index}");
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            &pattern,
            family,
        );
        assert_eq!(decode_notice_response(&subscriber_mailbox).status, 0);
    }

    // Act
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        session_id,
        "notice://acme/app/overflow",
        family,
    );
    let overflow_response = decode_notice_response(&subscriber_mailbox);

    // Assert
    assert_eq!(overflow_response.status, 1);
    assert_eq!(
        overflow_response.error.as_deref(),
        Some("notice subscription limit exceeded (1024 per session)")
    );
    assert_eq!(
        sink.subscription_count(),
        Ok(MAX_NOTICE_REGISTRATIONS_PER_SESSION)
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
    let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_REGISTRATIONS_PER_SESSION + 5));
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

    for pattern_index in 1..MAX_WILDCARD_REGISTRATIONS_PER_SESSION {
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
        Ok(MAX_WILDCARD_REGISTRATIONS_PER_SESSION)
    );
}
