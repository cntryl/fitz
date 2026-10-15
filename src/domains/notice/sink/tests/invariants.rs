use super::*;

#[test]
fn should_preserve_notice_subscription_after_foreign_unsubscribe_domain_invariant() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "notice://acme/events";
    let destination = RouteAddress::new(family, Route::new(route));
    let owner = RouteAddress::new(family, Route::new("inbox://session/7"));
    let stranger = RouteAddress::new(family, Route::new("inbox://session/8"));
    let owner_mailbox = Arc::new(Mailbox::new(8));
    let stranger_mailbox = Arc::new(Mailbox::new(8));
    let router = Arc::new(Router::new());
    router.register(owner.clone(), owner_mailbox.clone());
    router.register(stranger.clone(), stranger_mailbox.clone());
    let sink = NoticeDomain::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    );
    subscribe_notice_pattern(&sink, &owner, &destination, 7, route, family);
    let id = decode_notice_response(&owner_mailbox)
        .subscription_id
        .expect("subscription");

    // Act
    sink.deliver(Envelope::from_route(
        stranger.clone(),
        destination.clone(),
        FrameContext::new(
            8,
            ChannelId::Sub,
            MessageType::new(502),
            encode_notice_unsubscribe(id),
            family,
        ),
    ))
    .expect("foreign unsubscribe");
    let reply = decode_notice_response(&stranger_mailbox);
    sink.deliver(Envelope::from_route(
        stranger,
        destination,
        FrameContext::new(
            8,
            ChannelId::Sub,
            MessageType::new(500),
            encode_notice_publish(route, b"still-live"),
            family,
        ),
    ))
    .expect("publish");

    // Assert
    assert_eq!(reply.status, 0);
    assert_eq!(sink.subscription_count(), Ok(1));
    let envelope = owner_mailbox
        .receiver()
        .recv_timeout(Duration::from_secs(1))
        .expect("live delivery");
    let frame = envelope.payload::<FrameContext>().expect("notice frame");
    assert_eq!(frame.msg_type.as_u16(), 504);
    let mut decoder = PayloadDecoder::new(&frame.payload);
    assert_eq!(decoder.get_u64().expect("subscription id"), id);
    assert_eq!(decoder.get_string().expect("route"), route);
    assert_eq!(
        decoder.get_bytes().expect("payload").as_ref(),
        b"still-live"
    );
    assert!(decoder.is_complete());
    assert!(owner_mailbox.receiver().try_recv().is_err());
}
