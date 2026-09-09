use super::*;

#[test]
fn should_route_notice_session_cleanup_command_through_family_runtime() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 7;
    let notice_route = "notice://acme/events";
    let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let router = Arc::new(Router::new());
    let client_mailbox = Arc::new(Mailbox::new(8));
    router.register(client_address.clone(), client_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomain::new(router, admin_read_model);
    subscribe_notice_pattern(
        &sink,
        &client_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let response = decode_notice_response(&client_mailbox);
    assert_eq!(response.status, 0);
    assert_eq!(sink.subscription_count(), Ok(1));

    // Act
    sink.stop_actor_for_tests();
    let removed = sink.unsubscribe_all_for_session(session_id);
    let subscription_count = sink.subscription_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(removed, Err(DeliveryError::ActorStopped));
    assert_eq!(subscription_count, Err(DeliveryError::ActorStopped));
}

#[test]
fn should_reject_stale_subscribe_after_disconnect_cleanup_marks_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let session_id = 9;
    let notice_route = "notice://acme/events";
    let client_address = RouteAddress::new(family, Route::new("inbox://session/9"));
    let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
    let router = Arc::new(Router::new());
    let client_mailbox = Arc::new(Mailbox::new(8));
    router.register(client_address.clone(), client_mailbox.clone());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = NoticeDomain::new(router, admin_read_model);
    subscribe_notice_pattern(
        &sink,
        &client_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );
    let _ = decode_notice_response(&client_mailbox);
    assert_eq!(sink.subscription_count(), Ok(1));

    // Act: complete disconnect cleanup on the family control lane before the
    // stale normal-lane request below is processed.
    sink.enqueue_cleanup_for_tests(Envelope::new(
        RouteAddress::new(family, Route::new("notice://cleanup")),
        crate::runtime::SessionCleanup { session_id },
    ))
    .expect("enqueue and complete Notice cleanup");
    assert_eq!(sink.subscription_count(), Ok(0));

    subscribe_notice_pattern(
        &sink,
        &client_address,
        &notice_address,
        session_id,
        notice_route,
        family,
    );

    // Assert: the stale subscribe from the now-cleaned-up session is
    // rejected instead of resurrecting a subscription for it.
    let response = decode_notice_response(&client_mailbox);
    assert_eq!(response.status, 1);
    assert_eq!(response.error.as_deref(), Some("session already closed"));
    assert_eq!(sink.subscription_count(), Ok(0));
}
