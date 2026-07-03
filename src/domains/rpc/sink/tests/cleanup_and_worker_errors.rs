use super::*;

#[test]
fn should_remove_only_matching_pending_given_worker_unsubscribe() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    let removed_route = Route::new("rpc://bench/system/resource/operation");
    let retained_route = Route::new("rpc://bench/system/resource/other");
    let removed_worker_addr = RouteAddress::new(family, removed_route.clone());
    let retained_worker_addr = RouteAddress::new(family, retained_route.clone());
    let removed_correlation_id = uuid::Uuid::new_v4();
    let retained_correlation_id = uuid::Uuid::new_v4();

    state
        .ensure_route_state(&removed_route)
        .register_worker(test_rpc_worker(family, &removed_route, 42));
    state
        .ensure_route_state(&retained_route)
        .register_worker(test_rpc_worker(family, &retained_route, 42));
    state.pending.track_pending(
        removed_correlation_id,
        test_pending_request(
            family,
            &removed_route,
            99,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    state.pending.track_pending(
        retained_correlation_id,
        test_pending_request(
            family,
            &retained_route,
            100,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    let cleanup = state.unregister_worker(&removed_worker_addr, 42);

    // Assert
    assert_eq!(cleanup.removed_workers, 1);
    assert_eq!(cleanup.removed_pending, 1);
    assert_eq!(cleanup.pending_len, 1);
    assert_eq!(cleanup.disconnect_deliveries.len(), 1);
    assert_eq!(
        cleanup.disconnect_deliveries[0].correlation_id,
        removed_correlation_id
    );
    assert!(
        !state.pending.pending.contains_key(&removed_correlation_id),
        "removed worker pending should no longer be tracked"
    );
    let retained_pending = state
        .pending
        .pending
        .get(&retained_correlation_id)
        .expect("retained worker pending should remain tracked");
    assert_eq!(retained_pending.worker_addr, retained_worker_addr);
    assert_eq!(state.pending.len(), 1);
    assert_eq!(state.route_count(), 1);
}

#[test]
fn should_forward_worker_disconnect_error_given_rpc_unsubscribe() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request =
        match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
            .expect("parse rpc request")
        {
            crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
            other => panic!("expected rpc request, found {other:?}"),
        };
    let unsubscribe_payload = {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_string(request_route.as_str());
        encoder.finish()
    };

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(301),
            bytes::Bytes::from(unsubscribe_payload),
            family,
        ),
    ))
    .expect("unsubscribe worker");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(sink.worker_count(), 0);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 2);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_eq!(reply_frames[0].payload[0], 0);
    assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
    assert_eq!(error_response.correlation_id, request.correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        RPC_WORKER_NOT_FOUND_ERROR,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_retain_other_worker_route_given_rpc_unsubscribe_on_same_session() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let removed_route = Route::new("rpc://bench/system/resource/operation");
    let retained_route = Route::new("rpc://bench/system/resource/other");
    let removed_addr = RouteAddress::new(family, removed_route.clone());
    let retained_addr = RouteAddress::new(family, retained_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_worker_for_tests(RpcWorker::with_stats(
        removed_addr.clone(),
        worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    sink.register_worker_for_tests(RpcWorker::with_stats(
        retained_addr.clone(),
        worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let unsubscribe_payload = {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::new();
        encoder.put_string(removed_route.as_str());
        encoder.finish()
    };
    let request_frame = crate::benchkit::build_rpc_request(retained_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request =
        match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
            .expect("parse rpc request")
        {
            crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
            other => panic!("expected rpc request, found {other:?}"),
        };
    let response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            request.correlation_id,
            bytes::Bytes::from_static(b"ok"),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        worker_source.clone(),
        removed_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(301),
            bytes::Bytes::from(unsubscribe_payload),
            family,
        ),
    ))
    .expect("unsubscribe removed worker route");
    sink.deliver(Envelope::from_route(
        request_source,
        retained_addr.clone(),
        request_ctx,
    ))
    .expect("dispatch request to retained route");
    sink.deliver(Envelope::from_route(
        worker_source,
        retained_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload),
            family,
        ),
    ))
    .expect("deliver retained worker response");

    // Assert
    assert_eq!(sink.worker_count(), 1);
    assert_eq!(sink.pending_request_count(), 0);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 2);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_eq!(reply_frames[0].payload[0], 0);
    assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
    let forwarded_response = parse_forwarded_rpc_response(&reply_frames[1]);
    assert_eq!(forwarded_response.correlation_id, request.correlation_id);
    assert_eq!(forwarded_response.seq, 0);
    assert!(forwarded_response.stream_end);
    assert_eq!(forwarded_response.body.as_ref(), b"ok");

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 3);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 301);
    assert_eq!(worker_frames[0].payload[0], 0);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[2].msg_type.as_u16(), 304);
}

#[test]
fn should_forward_worker_disconnect_error_given_rpc_session_cleanup() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_worker_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request =
        match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
            .expect("parse rpc request")
        {
            crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
            other => panic!("expected rpc request, found {other:?}"),
        };

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr,
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("rpc://cleanup")),
        crate::runtime::SessionCleanup { session_id: 42 },
    ))
    .expect("cleanup worker session");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(sink.worker_count(), 0);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 2);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_eq!(reply_frames[0].payload[0], 0);
    assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
    assert_eq!(error_response.correlation_id, request.correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        RPC_WORKER_NOT_FOUND_ERROR,
    );
}

#[test]
fn should_reject_worker_response_when_correlation_missing_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model)
            .with_request_timeout(Duration::from_millis(250)),
    );
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload,
        family,
    );
    let orphan_correlation_id = uuid::Uuid::new_v4();
    let orphan_response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            orphan_correlation_id,
            bytes::Bytes::from_static(b"wrong"),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(orphan_response_payload),
            family,
        ),
    ))
    .expect("deliver orphan response");

    // Assert
    assert_eq!(sink.pending_request_count(), 1);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&worker_frames[1]);
    assert_eq!(error_response.correlation_id, orphan_correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_CORRELATION_NOT_FOUND,
        RPC_CORRELATION_NOT_FOUND_ERROR,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_worker_response_from_non_owner_session_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let owner_worker_source = session_inbox_address(family, 42);
    let non_owner_worker_source = session_inbox_address(family, 43);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let owner_worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let non_owner_worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        request_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        owner_worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: owner_worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        non_owner_worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: non_owner_worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        owner_worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        non_owner_worker_source.clone(),
        43,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload,
        family,
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    let owner_request = owner_worker_frames
        .lock()
        .first()
        .cloned()
        .expect("owner worker request delivery");
    let owner_request = match crate::protocol::rpc_codec::parse_request(
        &owner_request,
        &owner_request.payload,
        family,
    )
    .expect("parse owner worker request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    let wrong_response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            owner_request.correlation_id,
            bytes::Bytes::from_static(b"wrong"),
        ),
    );
    sink.deliver(Envelope::from_route(
        non_owner_worker_source,
        request_addr,
        FrameContext::new(
            43,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(wrong_response_payload),
            family,
        ),
    ))
    .expect("deliver non-owner response");

    // Assert
    assert_eq!(sink.pending_request_count(), 1);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);

    let owner_worker_frames = owner_worker_frames.lock();
    assert_eq!(owner_worker_frames.len(), 1);
    assert_eq!(owner_worker_frames[0].msg_type.as_u16(), 302);

    let non_owner_worker_frames = non_owner_worker_frames.lock();
    assert_eq!(non_owner_worker_frames.len(), 1);
    assert_eq!(non_owner_worker_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&non_owner_worker_frames[0]);
    assert_eq!(error_response.correlation_id, owner_request.correlation_id);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER,
        RPC_WRONG_WORKER_ERROR,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_worker_ack_from_non_owner_session_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let owner_worker_source = session_inbox_address(family, 42);
    let non_owner_worker_source = session_inbox_address(family, 43);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let owner_worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let non_owner_worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        request_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        owner_worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: owner_worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        non_owner_worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: non_owner_worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        owner_worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        non_owner_worker_source.clone(),
        43,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload,
        family,
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source,
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    let owner_request = owner_worker_frames
        .lock()
        .first()
        .cloned()
        .expect("owner worker request delivery");
    let owner_request = match crate::protocol::rpc_codec::parse_request(
        &owner_request,
        &owner_request.payload,
        family,
    )
    .expect("parse owner worker request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    let wrong_ack_payload = crate::protocol::rpc_codec::encode_ack(&owner_request.correlation_id);
    sink.deliver(Envelope::from_route(
        non_owner_worker_source,
        request_addr,
        FrameContext::new(
            43,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(304),
            bytes::Bytes::from(wrong_ack_payload),
            family,
        ),
    ))
    .expect("deliver non-owner ack");

    // Assert
    assert_eq!(sink.pending_request_count(), 1);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);

    let owner_worker_frames = owner_worker_frames.lock();
    assert_eq!(owner_worker_frames.len(), 1);
    assert_eq!(owner_worker_frames[0].msg_type.as_u16(), 302);

    let non_owner_worker_frames = non_owner_worker_frames.lock();
    assert_eq!(non_owner_worker_frames.len(), 1);
    assert_eq!(non_owner_worker_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&non_owner_worker_frames[0]);
    assert_eq!(error_response.correlation_id, owner_request.correlation_id);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_WRONG_WORKER,
        RPC_WRONG_WORKER_ERROR,
    );
}

#[test]
fn should_drop_late_worker_response_after_requester_cleanup_without_forward_error() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let worker_source = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_source.clone(), worker_sink as Arc<dyn MailboxSink>);
    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        worker_source.clone(),
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request =
        match crate::protocol::rpc_codec::parse_request(&request_ctx, &request_payload, family)
            .expect("parse rpc request")
        {
            crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
            other => panic!("expected rpc request, found {other:?}"),
        };
    let response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            request.correlation_id,
            bytes::Bytes::from_static(b"ok"),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        request_source.clone(),
        request_addr.clone(),
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("rpc://cleanup")),
        crate::runtime::SessionCleanup { session_id: 1 },
    ))
    .expect("cleanup requester session");
    router.unregister(&request_source);
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload),
            family,
        ),
    ))
    .expect("deliver response");

    // Assert
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    let worker_frames = worker_frames.lock();
    assert!(
        worker_frames
            .iter()
            .any(|frame| frame.msg_type.as_u16() == 304),
        "expected worker ACK even when requester has disconnected"
    );
}

#[test]
fn should_remove_pending_request_on_stream_end_given_rpc_pending_table() {
    // Arrange
    let correlation_id = uuid::Uuid::new_v4();
    let caller_inbox_addr = session_inbox_address(RouteFamily::new(7), 42);
    let worker_addr = RouteAddress::new(
        RouteFamily::new(7),
        Route::new("rpc://bench/system/resource/operation"),
    );
    let mut pending = RpcPendingTable::new();
    let pending_len = pending.track_pending(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: worker_addr.route().clone(),
            caller_session_id: 42,
            caller_inbox_addr: caller_inbox_addr.clone(),
            worker_addr: worker_addr.clone(),
            worker_session_id: 77,
            worker_slot: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let result = pending.pending_for_response(&correlation_id, 77, 0, true);

    // Assert
    assert_eq!(pending_len, 1);
    match result {
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            removed_pending,
        } => {
            assert_eq!(tracked.caller_session_id, 42);
            assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
            assert_eq!(&tracked.route, worker_addr.route());
            assert_eq!(tracked.worker_slot, 0);
            assert!(removed_pending);
            assert_eq!(pending.len(), 0);
        }
        other => panic!("expected terminal response handling, found {other:?}"),
    }
}
