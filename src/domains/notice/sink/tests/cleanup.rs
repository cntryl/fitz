use super::*;

#[test]
fn should_route_notice_session_cleanup_command_through_managed_actor() {
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
    let sink = NoticeDomainSink::new(router, admin_read_model);
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
