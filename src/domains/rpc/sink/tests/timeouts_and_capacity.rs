use super::*;

#[test]
pub(super) fn should_accumulate_timeout_counters_given_rpc_timeout_sweep() {
    // Arrange
    let family = RouteFamily::new(1);
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router.clone(), admin_read_model)
        .with_metrics(metrics.clone())
        .with_request_timeout(Duration::from_millis(10));
    let caller_one = session_inbox_address(family, 1);
    let caller_two = session_inbox_address(family, 2);
    let caller_sink = Arc::new(CaptureRpcFrameSink {
        frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
    });
    router.register(
        caller_one.clone(),
        caller_sink.clone() as Arc<dyn MailboxSink>,
    );
    router.register(caller_two.clone(), caller_sink as Arc<dyn MailboxSink>);
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: Route::new("rpc://bench/system/resource/a"),
            caller_session_id: 1,
            caller_inbox_addr: caller_one,
            worker_addr: RouteAddress::new(family, Route::new("rpc://bench/system/resource/a")),
            worker_session_id: 42,
            registration_id: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_millis(5),
        }),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: Route::new("rpc://bench/system/resource/b"),
            caller_session_id: 2,
            caller_inbox_addr: caller_two,
            worker_addr: RouteAddress::new(family, Route::new("rpc://bench/system/resource/b")),
            worker_session_id: 43,
            registration_id: 1,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now(),
            expires_at: Instant::now() + Duration::from_millis(5),
        }),
    );

    // Act
    sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(metrics.counter_get("rpc_request_timeouts_total"), 2);
    assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
    assert_eq!(metrics.gauge_get("rpc_pending_requests"), 0);
}

#[test]
pub(super) fn should_forward_timeout_error_given_expired_pending_request() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model)
            .with_request_timeout(Duration::from_millis(10)),
    );
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/timeout");
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
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload,
        family,
    );

    sink.deliver(Envelope::from_route(
        request_source,
        request_addr,
        request_ctx,
    ))
    .expect("deliver request");

    // Act
    sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(worker_frames.lock().len(), 1);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
        RPC_TIMEOUT_ERROR,
    );
}

#[test]
pub(super) fn should_forward_timeout_error_given_expired_queued_request() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/queued-timeout");
    let caller_inbox_addr = session_inbox_address(family, 7);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    router.register(
        caller_inbox_addr.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: reply_frames.clone(),
        }) as Arc<dyn MailboxSink>,
    );
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
            caller_inbox_addr,
            Instant::now() + Duration::from_millis(5),
        ),
    );

    // Act
    sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[0]);
    assert_eq!(error_response.correlation_id, correlation_id);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
        RPC_TIMEOUT_ERROR,
    );
}

#[test]
pub(super) fn should_drop_timeout_error_given_requester_cleanup_before_expiration() {
    // Arrange
    let router = Arc::new(Router::new());
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(
        RpcDomainSink::new(router.clone(), admin_read_model)
            .with_request_timeout(Duration::from_millis(10))
            .with_metrics(metrics.clone()),
    );
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/timeout");
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
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"ping");
    let (request_msg_type, request_payload) =
        crate::benchkit::extract_single_tlv_field(&request_frame);
    let request_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(request_msg_type),
        request_payload,
        family,
    );

    sink.deliver(Envelope::from_route(
        request_source.clone(),
        request_addr,
        request_ctx,
    ))
    .expect("deliver request");
    sink.deliver(Envelope::new(
        RouteAddress::new(family, Route::new("rpc://cleanup")),
        crate::runtime::SessionCleanup { session_id: 1 },
    ))
    .expect("cleanup requester session");

    // Act
    sink.expire_timed_out_requests_at(Instant::now() + Duration::from_millis(25));

    // Assert
    assert_eq!(sink.pending_request_count(), 0);
    assert_eq!(worker_frames.lock().len(), 1);
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 0);
    assert_eq!(metrics.counter_get("rpc_timeout_errors_dropped_total"), 1);
    assert_eq!(
        metrics.counter_get("rpc_responses_dropped_closed_caller_total"),
        1
    );
}

#[test]
pub(super) fn should_reject_rpc_request_when_pending_capacity_reached() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model));
    let family = RouteFamily::new(1);
    let request_route = Route::new("rpc://bench/system/resource/operation");
    let source_addr = session_inbox_address(family, 1);
    let worker_inbox_addr = session_inbox_address(family, 42);
    let reply_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new()));
    let reply_sink = Arc::new(CaptureRpcFrameSink {
        frames: reply_frames.clone(),
    });
    let worker_sink = Arc::new(CaptureRpcFrameSink {
        frames: worker_frames.clone(),
    });
    router.register(source_addr.clone(), reply_sink as Arc<dyn MailboxSink>);
    router.register(worker_inbox_addr, worker_sink as Arc<dyn MailboxSink>);
    sink.register_registration_for_tests(test_rpc_worker(family, &request_route, 42));
    for _ in 0..RPC_MAX_PENDING_REQUESTS {
        sink.track_pending_request_for_tests(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &request_route,
                7,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
    }
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"payload");
    let (msg_type, payload) = crate::benchkit::extract_single_tlv_field(&request_frame);
    let correlation_id =
        crate::dispatch::protocol::rpc_codec::extract_request_correlation_id(&payload)
            .expect("request correlation id");
    let frame_ctx = FrameContext::new(
        1,
        crate::dispatch::protocol::frame::ChannelId::Rpc,
        crate::dispatch::protocol::tlv::MessageType::new(msg_type),
        payload,
        family,
    );
    let request_addr = RouteAddress::new(family, request_route);
    let envelope = Envelope::from_route(source_addr, request_addr, frame_ctx);

    // Act
    let result = sink.deliver(envelope);

    // Assert
    assert!(result.is_ok());
    assert_eq!(sink.pending_request_count(), RPC_MAX_PENDING_REQUESTS);
    assert!(worker_frames.lock().is_empty());
    let reply_frames = reply_frames.lock();
    assert_eq!(reply_frames.len(), 1);
    assert_rpc_terminal_code_error(
        &reply_frames[0],
        correlation_id,
        crate::dispatch::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        RPC_BACKPRESSURE_ERROR,
    );
}
