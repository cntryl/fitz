use super::*;

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
