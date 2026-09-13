//! Fault-injection tests for the Notice PUBLISH fan-out, SUBSCRIBE-ack
//! rollback, and disconnect-cleanup races described in the 2026-09
//! correctness audit's `notice_lease` slice.

use super::*;

#[test]
fn should_deliver_to_surviving_subscriber_when_sibling_subscriber_actor_stopped() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let failing_subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let healthy_subscriber = RouteAddress::new(family, Route::new("inbox://session/8"));
    let router = Arc::new(Router::new());
    let failing_mailbox = Arc::new(Mailbox::new(8));
    let healthy_mailbox = Arc::new(Mailbox::new(8));
    router.register(failing_subscriber.clone(), failing_mailbox.clone());
    router.register(healthy_subscriber.clone(), healthy_mailbox.clone());
    let collector = crate::observability::metrics::MetricsCollector::new();
    let sink = NoticeDomain::new(
        router.clone(),
        crate::control::admin::read_model::AdminReadModel::new(),
    )
    .with_metrics(collector.clone());

    subscribe_notice_pattern(
        &sink,
        &failing_subscriber,
        &notice_address,
        7,
        notice_route,
        family,
    );
    let _ = decode_notice_response(&failing_mailbox);
    subscribe_notice_pattern(
        &sink,
        &healthy_subscriber,
        &notice_address,
        8,
        notice_route,
        family,
    );
    let _ = decode_notice_response(&healthy_mailbox);
    // Now that both subscriptions exist, swap one subscriber's live sink for
    // one that always reports ActorStopped, simulating that session's
    // transport actor having already gone away without an explicit
    // unsubscribe/cleanup having run yet.
    router.register(failing_subscriber, Arc::new(FailingSink));

    // Act
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            11,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(notice_route, b"hello"),
            family,
        ),
    ))
    .expect("publish notice event");

    // Assert
    // The healthy subscriber still gets the notification, and the
    // failure of its sibling subscriber's sink is counted exactly once, not
    // amplified or absorbed into the batch.
    let notify_envelope = healthy_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("healthy subscriber should still receive the notice");
    assert!(notify_envelope.into_payload::<FrameContext>().is_some());
    let deadline = Instant::now() + Duration::from_secs(1);
    while collector.counter_get("fitz_notice_delivery_drops_total") < 1 && Instant::now() < deadline
    {
        std::thread::yield_now();
    }
    assert_eq!(collector.counter_get("fitz_notice_delivery_drops_total"), 1);
}

#[test]
fn should_reject_undeliverable_subscribe_response_when_subscriber_sink_reports_actor_stopped() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let pattern = "notice://acme/app/undeliverable-stopped";
    let notice_address = RouteAddress::new(family, Route::new(pattern));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let router = Arc::new(Router::new());
    // Register a sink that always reports ActorStopped, unlike the
    // existing MailboxFull-based rollback coverage.
    router.register(subscriber_address.clone(), Arc::new(FailingSink));
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomain::new(router, admin_read_model.clone());

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
    // The subscription must not be retained when its own ack could
    // never reach the subscriber.
    assert_eq!(sink.subscription_count(), Ok(0));
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
}

#[test]
fn should_skip_delivery_to_subscriber_whose_own_session_cleanup_overtakes_queued_publish() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomain::new(router, admin_read_model);
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        7,
        notice_route,
        family,
    );
    let _ = decode_notice_response(&subscriber_mailbox);
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("Notice family should block");
    let request = crate::domains::notice::NoticeClientRequest::new(
        crate::runtime::ClientFrameMeta::new(11, crate::runtime::ClientChannel::Pub, 500, family),
        Ok(
            crate::domains::notice::protocol::NotificationMessage::Publish(
                crate::domains::notice::protocol::PublishMessage::new(
                    family,
                    Route::new(notice_route),
                    Bytes::from_static(b"hello"),
                ),
            ),
        ),
    );

    // Act
    // The publish is queued on the normal lane while the family is
    // blocked, then the *subscriber's own* disconnect cleanup is queued on
    // the control lane behind it. Control-lane priority means cleanup runs
    // first once the family unblocks.
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        request,
    ))
    .expect("accept notice publish");
    let cleanup = sink.enqueue_cleanup_for_tests(Envelope::new(
        RouteAddress::new(family, Route::new("notice://cleanup")),
        crate::runtime::SessionCleanup { session_id: 7 },
    ));
    cleanup.expect("enqueue subscriber cleanup");
    release_tx.send(()).expect("release Notice family");

    // Assert
    // The subscriber disconnected before its subscription could be
    // used, so the queued publish must not resurrect a delivery to it.
    assert!(subscriber_mailbox
        .receiver()
        .recv_timeout(Duration::from_millis(200))
        .is_err());
}
