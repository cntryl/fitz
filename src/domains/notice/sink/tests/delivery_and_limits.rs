use super::*;

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
        MAX_WILDCARD_REGISTRATIONS_PER_SESSION
    );
    refresh_notice_admin_snapshot(&sink);
    assert_eq!(
        admin_read_model.notice_subscriptions(None, None).len(),
        MAX_WILDCARD_REGISTRATIONS_PER_SESSION
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
        MAX_WILDCARD_REGISTRATIONS_PER_SESSION
    );
}
