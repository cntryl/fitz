use super::*;

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_duplicate_live_correlation_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let worker_source = session_inbox_address(family, 42);
    let caller_one_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let caller_two_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        caller_one.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_one_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        caller_two.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_two_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        worker_source,
        42,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));
    let correlation_id = uuid::Uuid::new_v4();
    let mut payload_encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
    let caller_one_payload = crate::dispatch::protocol::rpc_codec::encode_request_into(
        &crate::domains::rpc::protocol::RpcRequest::new(
            family,
            correlation_id,
            request_route.clone(),
            bytes::Bytes::from_static(b"first"),
        ),
        &mut payload_encoder,
    );
    let caller_two_payload = crate::dispatch::protocol::rpc_codec::encode_request_into(
        &crate::domains::rpc::protocol::RpcRequest::new(
            family,
            correlation_id,
            request_route.clone(),
            bytes::Bytes::from_static(b"second"),
        ),
        &mut payload_encoder,
    );

    // Act
    sink.deliver(Envelope::from_route(
        caller_one,
        request_addr.clone(),
        FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            bytes::Bytes::from(caller_one_payload),
            family,
        ),
    ))
    .expect("deliver first request");
    sink.deliver(Envelope::from_route(
        caller_two,
        request_addr,
        FrameContext::new(
            2,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            bytes::Bytes::from(caller_two_payload),
            family,
        ),
    ))
    .expect("deliver duplicate request");

    // Assert
    assert_eq!(sink.pending_request_count(), 1);
    let caller_one_frames = caller_one_frames.lock();
    assert_eq!(caller_one_frames.len(), 0);

    let caller_two_frames = caller_two_frames.lock();
    assert_eq!(caller_two_frames.len(), 1);
    assert_rpc_terminal_code_error(
        &caller_two_frames[0],
        correlation_id,
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_DUPLICATE_CORRELATION,
        RPC_DUPLICATE_CORRELATION_ERROR,
    );

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 1);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_queue_request_when_worker_is_busy_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let worker_source = session_inbox_address(family, 42);
    let caller_one_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let caller_two_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        caller_one.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_one_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        caller_two.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_two_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let caller_one_request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        request_route.clone(),
        bytes::Bytes::from_static(b"first"),
    );
    let caller_two_request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        request_route.clone(),
        bytes::Bytes::from_static(b"second"),
    );
    let caller_one_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &caller_one_request,
            &mut encoder,
        ))
    };
    let caller_two_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &caller_two_request,
            &mut encoder,
        ))
    };

    // Act
    sink.deliver(Envelope::from_route(
        caller_one,
        request_addr.clone(),
        FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_one_payload.clone(),
            family,
        ),
    ))
    .expect("deliver first request");
    sink.deliver(Envelope::from_route(
        caller_two,
        request_addr,
        FrameContext::new(
            2,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_two_payload.clone(),
            family,
        ),
    ))
    .expect("deliver second request");

    // Assert
    assert_eq!(sink.pending_request_count(), 2);
    assert_eq!(sink.pending_table_len_for_tests(), 1);
    assert_eq!(sink.queued_request_count_for_tests(), 1);
    assert_eq!(sink.route_queued_len_for_tests(&request_route), 1);
    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 1);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[0].payload, caller_one_payload);
    let caller_one_frames = caller_one_frames.lock();
    assert_eq!(caller_one_frames.len(), 0);
    let caller_two_frames = caller_two_frames.lock();
    assert_eq!(caller_two_frames.len(), 0);
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_dispatch_queued_request_after_terminal_response_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let worker_source = session_inbox_address(family, 42);
    let caller_one_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let caller_two_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        caller_one.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_one_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        caller_two.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_two_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_source.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let caller_one_request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        request_route.clone(),
        bytes::Bytes::from_static(b"first"),
    );
    let caller_two_request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        request_route.clone(),
        bytes::Bytes::from_static(b"second"),
    );
    let caller_one_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &caller_one_request,
            &mut encoder,
        ))
    };
    let caller_two_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &caller_two_request,
            &mut encoder,
        ))
    };
    let response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            caller_one_request.correlation_id,
            bytes::Bytes::from_static(b"ok"),
        ),
    );

    // Act
    sink.deliver(Envelope::from_route(
        caller_one,
        request_addr.clone(),
        FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_one_payload.clone(),
            family,
        ),
    ))
    .expect("deliver first request");
    sink.deliver(Envelope::from_route(
        caller_two,
        request_addr.clone(),
        FrameContext::new(
            2,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_two_payload.clone(),
            family,
        ),
    ))
    .expect("deliver second request");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload),
            family,
        ),
    ))
    .expect("deliver terminal response");

    // Assert
    assert_eq!(sink.pending_request_count(), 1);
    assert_eq!(sink.pending_table_len_for_tests(), 1);
    assert_eq!(sink.queued_request_count_for_tests(), 0);
    let caller_one_frames = caller_one_frames.lock();
    assert_eq!(caller_one_frames.len(), 1);
    assert_eq!(caller_one_frames[0].msg_type.as_u16(), 303);
    let forwarded_response = parse_forwarded_rpc_response(&caller_one_frames[0]);
    assert_eq!(
        forwarded_response.correlation_id,
        caller_one_request.correlation_id,
    );
    let caller_two_frames = caller_two_frames.lock();
    assert_eq!(caller_two_frames.len(), 0);
    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[0].payload, caller_one_payload);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].payload, caller_two_payload);
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_request_when_route_queue_capacity_reached_given_rpc_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model).with_route_pending_capacity(1),
    );
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let caller_three = session_inbox_address(family, 3);
    let worker_source = session_inbox_address(family, 42);
    let caller_one_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let caller_two_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let caller_three_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        caller_one.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_one_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        caller_two.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_two_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        caller_three.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_three_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_source,
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let caller_one_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &crate::domains::rpc::protocol::RpcRequest::new(
                family,
                uuid::Uuid::new_v4(),
                request_route.clone(),
                bytes::Bytes::from_static(b"first"),
            ),
            &mut encoder,
        ))
    };
    let caller_two_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &crate::domains::rpc::protocol::RpcRequest::new(
                family,
                uuid::Uuid::new_v4(),
                request_route.clone(),
                bytes::Bytes::from_static(b"second"),
            ),
            &mut encoder,
        ))
    };
    let caller_three_request = crate::domains::rpc::protocol::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        request_route.clone(),
        bytes::Bytes::from_static(b"third"),
    );
    let caller_three_payload = {
        let mut encoder = crate::dispatch::protocol::payload_codec::PayloadEncoder::new();
        bytes::Bytes::from(crate::dispatch::protocol::rpc_codec::encode_request_into(
            &caller_three_request,
            &mut encoder,
        ))
    };

    // Act
    sink.deliver(Envelope::from_route(
        caller_one,
        request_addr.clone(),
        FrameContext::new(
            1,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_one_payload,
            family,
        ),
    ))
    .expect("deliver first request");
    sink.deliver(Envelope::from_route(
        caller_two,
        request_addr.clone(),
        FrameContext::new(
            2,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_two_payload,
            family,
        ),
    ))
    .expect("deliver second request");
    sink.deliver(Envelope::from_route(
        caller_three,
        request_addr,
        FrameContext::new(
            3,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(302),
            caller_three_payload,
            family,
        ),
    ))
    .expect("deliver third request");

    // Assert
    assert_eq!(sink.pending_request_count(), 2);
    assert_eq!(sink.pending_table_len_for_tests(), 1);
    assert_eq!(sink.queued_request_count_for_tests(), 1);
    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 1);
    let caller_one_frames = caller_one_frames.lock();
    assert_eq!(caller_one_frames.len(), 0);
    let caller_two_frames = caller_two_frames.lock();
    assert_eq!(caller_two_frames.len(), 0);
    let caller_three_frames = caller_three_frames.lock();
    assert_eq!(caller_three_frames.len(), 1);
    assert_rpc_terminal_code_error(
        &caller_three_frames[0],
        caller_three_request.correlation_id,
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        RPC_BACKPRESSURE_ERROR,
    );
}

#[test]
fn should_snapshot_queued_request_without_worker_session_id_given_rpc_admin_snapshot() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model.clone());
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://prod/api/users/get");
    let correlation_id = uuid::Uuid::new_v4();
    sink.register_registration_for_tests(test_rpc_worker(family, &route, 42));
    sink.queue_request_for_tests(
        correlation_id,
        RpcQueuedRequest::from_request(
            crate::domains::rpc::protocol::RpcRequest::new(
                family,
                correlation_id,
                route.clone(),
                bytes::Bytes::from_static(b"queued"),
            ),
            7,
            session_inbox_address(family, 7),
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    sink.sync_admin_snapshot();
    let pending = admin_read_model.rpc_pending(Some("prod"));

    // Assert
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].correlation_id, correlation_id.to_string());
    assert_eq!(pending[0].route, route.as_str());
    assert_eq!(pending[0].worker_session_id, None);
}

#[test]
fn should_reject_request_when_worker_disconnects_before_dispatch_given_missing_worker_inbox() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = session_inbox_address(family, 1);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    router.register(request_source.clone(), reply_sink as Arc<dyn MailboxSink>);
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = match crate::dispatch::protocol::rpc_codec::parse_request(
        &request_ctx,
        &request_payload,
        family,
    )
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

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(sink.worker_count(), 0);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_rpc_terminal_code_error(
        &reply_frames[0],
        request.correlation_id,
        crate::dispatch::protocol::error_codes::rpc::ERR_WORKER_NOT_FOUND,
        RPC_WORKER_NOT_FOUND_ERROR,
    );
}

#[test]
fn should_forward_response_to_original_request_source_given_noncanonical_inbox_route() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let request_addr = RouteAddress::new(family, request_route.clone());
    let request_source = RouteAddress::new(family, Route::new("inbox://session/1/custom"));
    let worker_source = RouteAddress::new(family, Route::new("inbox://session/42/custom"));
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
    sink.register_registration_for_tests(RpcWorker::with_stats(
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
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload.clone(),
        family,
    );
    let request = match crate::dispatch::protocol::rpc_codec::parse_request(
        &request_ctx,
        &request_payload,
        family,
    )
    .expect("parse rpc request")
    {
        crate::domains::rpc::protocol::RpcMessage::Request(request) => request,
        other => panic!("expected rpc request, found {other:?}"),
    };
    let response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
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
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload),
            family,
        ),
    ))
    .expect("deliver response");

    // Assert
    let reply_frames = reply_frames.lock();
    assert!(
        reply_frames
            .iter()
            .any(|frame| frame.msg_type.as_u16() == 303),
        "expected worker response on original request source route"
    );
}

#[test]
fn should_detach_caller_pending_given_rpc_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let caller_inbox_addr = session_inbox_address(family, 7);
    let mut state = RpcState::new();
    let route = Route::new("rpc://bench/system/resource/operation");
    state.register_registration(test_rpc_worker(family, &route, 42));
    let detached_correlation_id = uuid::Uuid::new_v4();
    state.pending.track_pending(
        detached_correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: route.clone(),
            caller_session_id: 7,
            caller_inbox_addr: caller_inbox_addr.clone(),
            registration_addr: RouteAddress::new(family, route.clone()),
            registration_session_id: 42,
            registration_id: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let caller_cleanup = state.cleanup_session(7);

    // Assert
    assert_eq!(caller_cleanup.removed_registrations, 0);
    assert_eq!(caller_cleanup.detached_callers, 1);
    assert_eq!(caller_cleanup.removed_pending, 0);
    assert_eq!(caller_cleanup.pending_len, 1);
    let detached_pending = state
        .pending
        .pending
        .get(&RpcCorrelationKey {
            family,
            correlation_id: detached_correlation_id,
        })
        .expect("detached pending should remain tracked");
    assert_eq!(detached_pending.dispatch_info.caller_session_id, 7);
    assert_eq!(detached_pending.dispatch_info.caller_inbox_addr, None);
    assert_eq!(
        detached_pending.worker_addr,
        RouteAddress::new(family, route)
    );
    assert_eq!(detached_pending.worker_session_id, 42);
    assert_eq!(state.pending.len(), 1);
    assert_eq!(state.route_count(), 0);
}

#[test]
fn should_remove_queued_request_given_rpc_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    let route = Route::new("rpc://bench/system/resource/operation");
    let queued_correlation_id = uuid::Uuid::new_v4();
    state.register_registration(test_rpc_worker(family, &route, 42));
    state.queue_request(
        queued_correlation_id,
        RpcQueuedRequest::from_request(
            crate::domains::rpc::protocol::RpcRequest::new(
                family,
                queued_correlation_id,
                route.clone(),
                bytes::Bytes::from_static(b"queued"),
            ),
            7,
            session_inbox_address(family, 7),
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    let caller_cleanup = state.cleanup_session(7);

    // Assert
    assert_eq!(caller_cleanup.removed_registrations, 0);
    assert_eq!(caller_cleanup.detached_callers, 0);
    assert_eq!(caller_cleanup.removed_pending, 1);
    assert_eq!(caller_cleanup.pending_len, 0);
    assert!(state.queued.is_empty());
    assert!(!state.contains_correlation(&queued_correlation_id));
    assert_eq!(state.route_count(), 0);
}

#[test]
fn should_remove_worker_entries_given_rpc_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    let route = Route::new("rpc://bench/system/resource/operation");
    state.register_registration(test_rpc_worker(family, &route, 42));

    // Act
    let worker_cleanup = state.cleanup_session(42);

    // Assert
    assert_eq!(worker_cleanup.removed_registrations, 1);
    assert_eq!(worker_cleanup.detached_callers, 0);
    assert_eq!(worker_cleanup.removed_pending, 0);
    assert_eq!(worker_cleanup.pending_len, 0);
    assert_eq!(state.route_count(), 0);
}

#[test]
fn should_remove_worker_owned_pending_given_rpc_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    let worker_route = Route::new("rpc://bench/system/resource/operation");
    state.pending.track_pending(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &worker_route,
            99,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    let worker_cleanup = state.cleanup_session(42);

    // Assert
    assert_eq!(worker_cleanup.removed_registrations, 0);
    assert_eq!(worker_cleanup.detached_callers, 0);
    assert_eq!(worker_cleanup.removed_pending, 1);
    assert_eq!(worker_cleanup.pending_len, 0);
    assert_eq!(state.pending.len(), 0);
}
