use super::*;

#[test]
fn should_retain_pending_request_before_stream_end_given_rpc_pending_table() {
    // Arrange
    let correlation_id = uuid::Uuid::new_v4();
    let caller_inbox_addr = session_inbox_address(RouteFamily::new(9), 84);
    let worker_addr = RouteAddress::new(
        RouteFamily::new(9),
        Route::new("rpc://bench/system/resource/operation"),
    );
    let mut pending = RpcPendingTable::new();
    let pending_len = pending.track_pending(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: worker_addr.route().clone(),
            caller_session_id: 84,
            caller_inbox_addr: caller_inbox_addr.clone(),
            worker_addr: worker_addr.clone(),
            worker_session_id: 99,
            worker_slot: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let result = pending.pending_for_response(&correlation_id, 99, 0, false);

    // Assert
    assert_eq!(pending_len, 1);
    match result {
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            removed_pending,
        } => {
            assert_eq!(tracked.caller_session_id, 84);
            assert_eq!(tracked.caller_inbox_addr, Some(caller_inbox_addr));
            assert_eq!(&tracked.route, worker_addr.route());
            assert_eq!(tracked.worker_slot, 0);
            assert!(!removed_pending);
            assert_eq!(pending.len(), 1);
            assert_eq!(
                pending.pending[&RpcCorrelationKey {
                    family: RouteFamily::new(1),
                    correlation_id,
                }]
                    .next_expected_seq,
                1
            );
        }
        other => panic!("expected non-terminal response handling, found {other:?}"),
    }
}

#[test]
fn should_reject_invalid_response_sequence_given_rpc_pending_table() {
    // Arrange
    let correlation_id = uuid::Uuid::new_v4();
    let caller_inbox_addr = session_inbox_address(RouteFamily::new(11), 21);
    let worker_addr = RouteAddress::new(
        RouteFamily::new(11),
        Route::new("rpc://bench/system/resource/operation"),
    );
    let mut pending = RpcPendingTable::new();
    pending.track_pending(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: worker_addr.route().clone(),
            caller_session_id: 21,
            caller_inbox_addr,
            worker_addr,
            worker_session_id: 77,
            worker_slot: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    let result = pending.pending_for_response(&correlation_id, 77, 1, false);

    // Assert
    match result {
        RpcPendingResponseDisposition::InvalidSequence {
            pending: tracked,
            expected_seq,
        } => {
            assert_eq!(tracked.caller_session_id, 21);
            assert_eq!(tracked.worker_slot, 0);
            assert_eq!(expected_seq, 0);
            assert_eq!(pending.len(), 0);
        }
        other => panic!("expected invalid sequence handling, found {other:?}"),
    }
}

#[test]
fn should_reject_out_of_order_worker_response_given_rpc_sink() {
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
    let invalid_response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            1,
            bytes::Bytes::from_static(b"gap"),
            false,
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
            bytes::Bytes::from(invalid_response_payload),
            family,
        ),
    ))
    .expect("deliver invalid response");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert_eq!(error_response.correlation_id, request.correlation_id);
    assert_eq!(error_response.seq, 0);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
    let worker_error = parse_forwarded_rpc_response(&worker_frames[1]);
    assert_eq!(worker_error.correlation_id, request.correlation_id);
    assert!(worker_error.stream_end);
    assert_rpc_code_error(
        worker_error.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );
}

#[test]
#[allow(clippy::too_many_lines)]
fn should_reject_duplicate_worker_response_chunk_given_rpc_sink() {
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
    let first_response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            0,
            bytes::Bytes::from_static(b"part-0"),
            false,
        ),
    );
    let duplicate_response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::chunk(
            request.correlation_id,
            0,
            bytes::Bytes::from_static(b"part-0-again"),
            false,
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
        worker_source.clone(),
        request_addr.clone(),
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(first_response_payload),
            family,
        ),
    ))
    .expect("deliver first response chunk");
    sink.deliver(Envelope::from_route(
        worker_source,
        request_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(duplicate_response_payload),
            family,
        ),
    ))
    .expect("deliver duplicate response chunk");

    // Assert
    assert_eq!(sink.pending_request_count(), 0);

    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 2);
    let first_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert_eq!(first_response.correlation_id, request.correlation_id);
    assert_eq!(first_response.seq, 0);
    assert!(!first_response.stream_end);
    let terminal_error = parse_forwarded_rpc_response(&reply_frames[1]);
    assert_eq!(terminal_error.correlation_id, request.correlation_id);
    assert!(terminal_error.stream_end);
    assert_rpc_code_error(
        terminal_error.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );

    let worker_frames = worker_frames.lock();
    assert_eq!(worker_frames.len(), 2);
    assert_eq!(worker_frames[0].msg_type.as_u16(), 302);
    assert_eq!(worker_frames[1].msg_type.as_u16(), 303);
    let worker_error = parse_forwarded_rpc_response(&worker_frames[1]);
    assert_eq!(worker_error.correlation_id, request.correlation_id);
    assert!(worker_error.stream_end);
    assert_rpc_code_error(
        worker_error.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_INVALID_SEQUENCE,
        RPC_INVALID_SEQUENCE_ERROR,
    );
}
