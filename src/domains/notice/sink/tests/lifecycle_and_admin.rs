use super::*;

#[test]
fn should_create_notice_domain_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = NoticeDomainSink::new(router, admin_read_model);

    // Assert
    assert!(sink.is_active());
}

#[test]
fn should_reject_notice_delivery_when_managed_actor_is_stopped() {
    // Arrange
    let family = RouteFamily::new(1);
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model);

    // Act
    sink.stop_actor_for_tests();
    let result = sink.deliver(Envelope::from_route(
        subscriber_address,
        notice_address,
        FrameContext::new(
            7,
            ChannelId::Sub,
            MessageType::new(501),
            encode_notice_subscribe("notice://acme/events"),
            family,
        ),
    ));

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
    assert!(matches!(
        sink.subscription_count(),
        Err(DeliveryError::ActorStopped)
    ));
}

#[test]
fn should_route_notice_live_count_queries_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_route = "notice://acme/events";
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model);
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        7,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);
    assert_eq!(sink.subscription_count(), Ok(1));

    // Act
    sink.stop_actor_for_tests();
    let subscription_count = sink.subscription_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(
        subscription_count,
        Err(DeliveryError::ActorStopped)
    ));
}

#[test]
fn should_report_failed_notice_cleanup_when_managed_actor_is_stopped() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    sink.stop_actor_for_tests();

    // Act
    let result = sink.unsubscribe_all_for_session(7);

    // Assert
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
}

#[test]
fn should_report_timeout_when_notice_actor_is_alive_but_busy() {
    // Arrange
    let router = Arc::new(Router::new());
    let sink = NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
    let (release_tx, release_rx) = crossbeam_channel::bounded(1);
    sink.block_actor_for_tests(entered_tx, release_rx);
    entered_rx.recv().expect("blocking sink entered");

    // Act
    let result = sink.subscription_count();
    release_tx.send(()).expect("release blocking sink");

    // Assert
    assert!(sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::Timeout)));
}

#[test]
fn should_route_notice_admin_dirty_refresh_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_route = "notice://acme/events";
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let router = Arc::new(Router::new());
    let subscriber_mailbox = Arc::new(Mailbox::new(8));
    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());
    subscribe_notice_pattern(
        &sink,
        &subscriber_address,
        &notice_address,
        7,
        notice_route,
        family,
    );
    let subscribe_response = decode_notice_response(&subscriber_mailbox);
    assert_eq!(subscribe_response.status, 0);

    // Act
    sink.stop_actor_for_tests();
    sink.refresh_admin_snapshot_if_dirty();
    let subscriptions = admin_read_model.notice_subscriptions(None, None);
    let notice_routes = admin_read_model.notice_routes(None);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(subscriptions.is_empty());
    assert!(notice_routes.is_empty());
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
    let notice_routes = admin_read_model.notice_routes(None);
    assert_notice_admin_subscriptions(&subscriptions, &[notice_route]);
    assert_notice_admin_routes(&notice_routes, &[notice_route]);
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
    let notice_routes = admin_read_model.notice_routes(None);
    assert_eq!(notice_routes.len(), 1);
    assert_eq!(notice_routes[0].route, notice_route);
    assert_eq!(notice_routes[0].subscribers, 1);
    assert_eq!(notice_routes[0].publishes_total, 1);
    assert!((notice_routes[0].publishes_per_minute - 1.0).abs() < f64::EPSILON);
}

#[test]
fn should_track_one_notice_publish_given_multiple_subscribers_on_same_pattern() {
    // Arrange
    let family = RouteFamily::new(1);
    let notice_pattern = "notice://acme/app/*";
    let publish_route = "notice://acme/app/events";
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let first_subscriber = RouteAddress::new(family, Route::new("inbox://session/7"));
    let second_subscriber = RouteAddress::new(family, Route::new("inbox://session/8"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let router = Arc::new(Router::new());
    let first_mailbox = Arc::new(Mailbox::new(8));
    let second_mailbox = Arc::new(Mailbox::new(8));
    router.register(first_subscriber.clone(), first_mailbox.clone());
    router.register(second_subscriber.clone(), second_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomainSink::new(router, admin_read_model.clone());

    subscribe_notice_pattern(
        &sink,
        &first_subscriber,
        &notice_address,
        7,
        notice_pattern,
        family,
    );
    let first_response = decode_notice_response(&first_mailbox);
    assert_eq!(first_response.status, 0);
    subscribe_notice_pattern(
        &sink,
        &second_subscriber,
        &notice_address,
        8,
        notice_pattern,
        family,
    );
    let second_response = decode_notice_response(&second_mailbox);
    assert_eq!(second_response.status, 0);

    // Act
    sink.deliver(Envelope::from_route(
        publisher_address,
        notice_address,
        FrameContext::new(
            11,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(publish_route, b"hello"),
            family,
        ),
    ))
    .expect("publish notice event");
    refresh_notice_admin_snapshot(&sink);

    // Assert
    first_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("first subscriber delivery");
    second_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("second subscriber delivery");
    assert!(first_mailbox.receiver().try_recv().is_err());
    assert!(second_mailbox.receiver().try_recv().is_err());

    let notice_routes = admin_read_model.notice_routes(None);
    assert_eq!(notice_routes.len(), 1);
    assert_eq!(notice_routes[0].route, notice_pattern);
    assert_eq!(notice_routes[0].subscribers, 2);
    assert_eq!(notice_routes[0].publishes_total, 1);
    assert!((notice_routes[0].publishes_per_minute - 1.0).abs() < f64::EPSILON);
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
    assert_eq!(sink.subscription_count(), Ok(0));
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
    assert!(publisher_mailbox.receiver().try_recv().is_err());
    assert_eq!(sink.subscription_family_count(), 0);
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
    sink.unsubscribe_all_for_session(session_id)
        .expect("notice session cleanup");

    // Assert
    assert_eq!(sink.subscription_count(), Ok(0));
    refresh_notice_admin_snapshot(&sink);
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
    assert!(admin_read_model.notice_routes(None).is_empty());
    assert_eq!(sink.subscription_family_count(), 0);
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
    assert_eq!(sink.route_stats_count(), 1);
    drain_mailbox(&subscriber_mailbox);
    drain_mailbox(&publisher_mailbox);

    // Act
    sink.unsubscribe_all_for_session(session_id)
        .expect("notice session cleanup");
    refresh_notice_admin_snapshot(&sink);

    // Assert
    assert_eq!(sink.route_stats_count(), 0);
}
