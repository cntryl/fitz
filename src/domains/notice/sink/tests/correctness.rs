use super::*;

fn new_correctness_notice_sink(router: Arc<Router>) -> NoticeDomainSink {
    NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    )
}

#[test]
fn should_reject_notice_request_when_source_and_destination_families_differ() {
    // Arrange
    let source_family = RouteFamily::new(2);
    let destination_family = RouteFamily::new(1);
    let source = RouteAddress::new(source_family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(destination_family, Route::new("notice://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_notice_sink(router);
    let request = crate::domains::notice::NoticeClientRequest::new(
        crate::runtime::ClientFrameMeta::new(
            7,
            crate::runtime::ClientChannel::Sub,
            501,
            destination_family,
        ),
        Ok(
            crate::domains::notice::protocol::NotificationMessage::Subscribe(
                crate::domains::notice::protocol::SubscribeMessage::new(
                    destination_family,
                    Route::new("notice://acme/app/events"),
                    crate::session::SessionId(7),
                    RouteAddress::new(destination_family, Route::new("inbox://session/7")),
                ),
            ),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver mismatched notice request");
    let response = decode_notice_response(&mailbox);

    // Assert
    assert_eq!(response.status, 1);
    assert_eq!(response.error.as_deref(), Some("route family mismatch"));
    assert_eq!(sink.subscription_family_count(), 0);
}

#[test]
fn should_reject_notice_subscription_before_allocating_family_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("notice://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_notice_sink(router);
    let request = crate::domains::notice::NoticeClientRequest::new(
        crate::runtime::ClientFrameMeta::new(7, crate::runtime::ClientChannel::Sub, 501, family),
        Ok(
            crate::domains::notice::protocol::NotificationMessage::Subscribe(
                crate::domains::notice::protocol::SubscribeMessage::new(
                    family,
                    Route::new(""),
                    crate::session::SessionId(7),
                    source.clone(),
                ),
            ),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver empty notice subscription");
    let response = decode_notice_response(&mailbox);

    // Assert
    assert_eq!(response.status, 1);
    assert_eq!(
        response.error.as_deref(),
        Some("subscription pattern must use notice://")
    );
    assert_eq!(sink.subscription_family_count(), 0);
}

#[test]
fn should_reject_notice_publish_when_decoded_family_differs_from_request() {
    // Arrange
    let family = RouteFamily::new(1);
    let other_family = RouteFamily::new(2);
    let source = RouteAddress::new(family, Route::new("inbox://session/7"));
    let destination = RouteAddress::new(family, Route::new("notice://inbound"));
    let mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(source.clone(), mailbox.clone());
    let sink = new_correctness_notice_sink(router);
    let request = crate::domains::notice::NoticeClientRequest::new(
        crate::runtime::ClientFrameMeta::new(7, crate::runtime::ClientChannel::Pub, 500, family),
        Ok(
            crate::domains::notice::protocol::NotificationMessage::Publish(
                crate::domains::notice::protocol::PublishMessage::new(
                    other_family,
                    Route::new("notice://acme/app/events"),
                    Bytes::from_static(b"event"),
                ),
            ),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(source, destination, request))
        .expect("deliver decoded-family mismatch");
    let response = decode_notice_response(&mailbox);

    // Assert
    assert_eq!(response.status, 1);
    assert_eq!(response.error.as_deref(), Some("route family mismatch"));
    assert_eq!(sink.subscription_family_count(), 0);
}
