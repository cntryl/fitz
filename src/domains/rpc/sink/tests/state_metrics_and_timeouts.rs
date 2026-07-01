use super::*;

pub(super) fn assert_rpc_code_error(payload: &[u8], expected_code: u16, expected_message: &str) {
    let (code, message) =
        crate::protocol::rpc_codec::decode_error_body(payload).expect("rpc code error");
    assert_eq!(code, expected_code);
    assert_eq!(message, expected_message);
}

pub(super) fn parse_forwarded_rpc_response(
    frame: &FrameContext,
) -> crate::domains::rpc::protocol::RpcResponse {
    match crate::protocol::rpc_codec::parse_request(frame, &frame.payload, frame.route_family)
        .expect("parse forwarded rpc response")
    {
        crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
        other => panic!("expected rpc response, found {other:?}"),
    }
}

pub(super) struct CaptureRpcFrameSink {
    pub(super) frames: Arc<parking_lot::Mutex<Vec<FrameContext>>>,
}

pub(super) fn test_rpc_worker(family: RouteFamily, route: &Route, session_id: u64) -> RpcWorker {
    RpcWorker::with_stats(
        RouteAddress::new(family, route.clone()),
        session_inbox_address(family, session_id),
        session_id,
        "2026-03-14T12:00:00Z",
        0,
        0,
    )
}

pub(super) fn test_pending_request(
    family: RouteFamily,
    route: &Route,
    caller_session_id: u64,
    worker_session_id: u64,
    expires_at: Instant,
) -> RpcPendingRequest {
    let submitted_at_instant = Instant::now();
    RpcPendingRequest::new(RpcPendingRequestInit {
        route: route.clone(),
        caller_session_id,
        caller_inbox_addr: session_inbox_address(family, caller_session_id),
        worker_addr: RouteAddress::new(family, route.clone()),
        worker_session_id,
        submitted_at: "2026-03-14T12:00:00Z".to_string(),
        submitted_at_instant,
        expires_at,
    })
}

impl MailboxSink for CaptureRpcFrameSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(ctx) = envelope.payload::<FrameContext>() {
            self.frames.lock().push(ctx.clone());
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[test]
pub(super) fn should_create_rpc_domain_sink() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();

    // Act
    let sink = RpcDomainSink::new(router, admin_read_model);

    // Assert
    assert!(sink.active.load(Ordering::Relaxed));
    assert_eq!(sink.core.state.lock().live_request_count(), 0);
}

#[test]
pub(super) fn should_claim_workers_in_registration_order_given_route_local_rpc_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut route_state = RpcRouteState::new();
    route_state.register_worker(test_rpc_worker(family, &route, 10));
    route_state.register_worker(test_rpc_worker(family, &route, 11));

    // Act
    let first = route_state.claim_worker().map(|worker| worker.session_id);
    let second = route_state.claim_worker().map(|worker| worker.session_id);

    // Assert
    assert_eq!(first, Some(10));
    assert_eq!(second, Some(11));
}

#[test]
pub(super) fn should_reuse_route_state_given_equivalent_rpc_route_keys() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let duplicate_route = Route::new("rpc://bench/system/resource/operation");
    let mut state = RpcState::new();
    state
        .ensure_route_state(&route)
        .register_worker(test_rpc_worker(family, &route, 10));

    // Act
    let worker_count = state.ensure_route_state(&duplicate_route).worker_count();

    // Assert
    assert_eq!(worker_count, 1);
    assert_eq!(state.route_count(), 1);
}

#[test]
pub(super) fn should_ignore_duplicate_worker_registration_given_same_session_and_route() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut route_state = RpcRouteState::new();
    let worker = test_rpc_worker(family, &route, 10);

    // Act
    route_state.register_worker(worker.clone());
    route_state.register_worker(worker);

    // Assert
    assert_eq!(route_state.worker_count(), 1);
}

#[test]
pub(super) fn should_schedule_rpc_admin_snapshot_when_interval_elapsed_given_dirty_state() {
    // Arrange
    let now_elapsed_us = RPC_ADMIN_SNAPSHOT_INTERVAL_US + 1;

    // Act
    let due = rpc_admin_snapshot_due(true, false, now_elapsed_us, 0);

    // Assert
    assert!(due);
}

#[test]
pub(super) fn should_skip_rpc_admin_snapshot_when_interval_not_elapsed_and_not_forced() {
    // Arrange
    let now_elapsed_us = RPC_ADMIN_SNAPSHOT_INTERVAL_US - 1;

    // Act
    let due = rpc_admin_snapshot_due(true, false, now_elapsed_us, 0);

    // Assert
    assert!(!due);
}

#[test]
pub(super) fn should_snapshot_live_pending_request_details_given_rpc_admin_snapshot() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model.clone());
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://prod/api/users/get");
    let correlation_id = uuid::Uuid::new_v4();

    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&route)
            .register_worker(test_rpc_worker(family, &route, 42));
        state.pending.track_pending(
            correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: route.clone(),
                caller_session_id: 7,
                caller_inbox_addr: session_inbox_address(family, 7),
                worker_addr: RouteAddress::new(family, route.clone()),
                worker_session_id: 42,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now().checked_sub(Duration::from_secs(9)).unwrap(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );
    }

    // Act
    sink.sync_admin_snapshot();
    let pending = admin_read_model.rpc_pending(Some("prod"));

    // Assert
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].correlation_id, correlation_id.to_string());
    assert_eq!(pending[0].route, route.as_str());
    assert_eq!(pending[0].submitted_at, "2026-03-14T12:00:00Z");
    assert_eq!(pending[0].worker_session_id.as_deref(), Some("42"));
    assert!(pending[0].age_seconds >= 9);
}

#[test]
pub(super) fn should_snapshot_live_worker_metrics_after_terminal_response_given_rpc_admin_snapshot()
{
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone()));
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://prod/api/users/get");
    let request_addr = RouteAddress::new(family, route.clone());
    let caller_addr = session_inbox_address(family, 7);
    let worker_inbox_addr = session_inbox_address(family, 42);
    let response_payload = crate::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            uuid::Uuid::new_v4(),
            bytes::Bytes::from_static(b"ok"),
        ),
    );
    let response = match crate::protocol::rpc_codec::parse_request(
        &FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload.clone()),
            family,
        ),
        &response_payload,
        family,
    )
    .expect("parse rpc response")
    {
        crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
        other => panic!("expected rpc response, found {other:?}"),
    };

    router.register(
        caller_addr.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
        }) as Arc<dyn MailboxSink>,
    );
    router.register(
        worker_inbox_addr.clone(),
        Arc::new(CaptureRpcFrameSink {
            frames: Arc::new(parking_lot::Mutex::new(Vec::<FrameContext>::new())),
        }) as Arc<dyn MailboxSink>,
    );

    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&route)
            .register_worker(RpcWorker::with_stats(
                request_addr.clone(),
                worker_inbox_addr.clone(),
                42,
                "2026-03-14T11:59:00Z",
                0,
                0,
            ));
        let correlation_id = response.correlation_id;
        state.pending.track_pending(
            correlation_id,
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: route.clone(),
                caller_session_id: 7,
                caller_inbox_addr: caller_addr.clone(),
                worker_addr: request_addr.clone(),
                worker_session_id: 42,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now()
                    .checked_sub(Duration::from_millis(50))
                    .unwrap(),
                expires_at: Instant::now() + Duration::from_secs(30),
            }),
        );
    }

    // Act
    sink.deliver(Envelope::from_route(
        worker_inbox_addr,
        request_addr,
        FrameContext::new(
            42,
            crate::protocol::frame::ChannelId::Rpc,
            crate::protocol::tlv::MessageType::new(303),
            bytes::Bytes::from(response_payload),
            family,
        ),
    ))
    .expect("deliver terminal response");
    sink.sync_admin_snapshot();
    let workers = admin_read_model.rpc_workers(Some("prod"));
    let pending = admin_read_model.rpc_pending(Some("prod"));

    // Assert
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].session_id, "42");
    assert_eq!(workers[0].route, route.as_str());
    assert_eq!(workers[0].registered_at, "2026-03-14T11:59:00Z");
    assert_eq!(workers[0].requests_handled, 1);
    assert!(workers[0].average_latency_ms >= 50.0);
    assert!(pending.is_empty());
}

#[test]
pub(super) fn should_accumulate_cleanup_counters_given_rpc_session_cleanup() {
    // Arrange
    let family = RouteFamily::new(1);
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
    let worker_route_a = Route::new("rpc://bench/system/resource/operation-a");
    let worker_route_b = Route::new("rpc://bench/system/resource/operation-b");
    let external_route_a = Route::new("rpc://bench/external/resource/operation-a");
    let external_route_b = Route::new("rpc://bench/external/resource/operation-b");

    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&worker_route_a)
            .register_worker(test_rpc_worker(family, &worker_route_a, 42));
        state
            .ensure_route_state(&worker_route_b)
            .register_worker(test_rpc_worker(family, &worker_route_b, 42));
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &worker_route_a,
                90,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &worker_route_b,
                91,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &external_route_a,
                42,
                7,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &external_route_b,
                42,
                8,
                Instant::now() + Duration::from_secs(30),
            ),
        );
    }

    // Act
    let cleanup = sink.apply_session_cleanup(42);

    // Assert
    assert_eq!(cleanup.removed_workers, 2);
    assert_eq!(cleanup.detached_callers, 2);
    assert_eq!(cleanup.removed_pending, 2);
    assert_eq!(cleanup.pending_len, 2);
    assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 2);
    assert_eq!(metrics.counter_get("rpc_cleanup_callers_detached_total"), 2);
    assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
    assert_eq!(metrics.gauge_get("rpc_pending_requests"), 2);
}

#[test]
pub(super) fn should_accumulate_pending_removed_counter_given_rpc_worker_unsubscribe() {
    // Arrange
    let family = RouteFamily::new(1);
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
    let removed_route = Route::new("rpc://bench/system/resource/operation");
    let retained_route = Route::new("rpc://bench/system/resource/other");
    let removed_addr = RouteAddress::new(family, removed_route.clone());

    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&removed_route)
            .register_worker(test_rpc_worker(family, &removed_route, 42));
        state
            .ensure_route_state(&retained_route)
            .register_worker(test_rpc_worker(family, &retained_route, 42));
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &removed_route,
                90,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &removed_route,
                91,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            test_pending_request(
                family,
                &retained_route,
                92,
                42,
                Instant::now() + Duration::from_secs(30),
            ),
        );
    }

    // Act
    let cleanup = sink.apply_worker_unsubscribe(&removed_addr, 42);

    // Assert
    assert_eq!(cleanup.removed_workers, 1);
    assert_eq!(cleanup.removed_pending, 2);
    assert_eq!(cleanup.pending_len, 1);
    assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 1);
    assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 2);
    assert_eq!(metrics.gauge_get("rpc_pending_requests"), 1);
}

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
    {
        let mut state = sink.state.lock();
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: Route::new("rpc://bench/system/resource/a"),
                caller_session_id: 1,
                caller_inbox_addr: caller_one,
                worker_addr: RouteAddress::new(family, Route::new("rpc://bench/system/resource/a")),
                worker_session_id: 42,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_millis(5),
            }),
        );
        state.pending.track_pending(
            uuid::Uuid::new_v4(),
            RpcPendingRequest::new(RpcPendingRequestInit {
                route: Route::new("rpc://bench/system/resource/b"),
                caller_session_id: 2,
                caller_inbox_addr: caller_two,
                worker_addr: RouteAddress::new(family, Route::new("rpc://bench/system/resource/b")),
                worker_session_id: 43,
                submitted_at: "2026-03-14T12:00:00Z".to_string(),
                submitted_at_instant: Instant::now(),
                expires_at: Instant::now() + Duration::from_millis(5),
            }),
        );
    }

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
    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&request_route)
            .register_worker(test_rpc_worker(family, &request_route, 42));
    }
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
    assert_eq!(reply_frames.len(), 2);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_eq!(reply_frames[0].payload[0], 0);
    assert_eq!(reply_frames[1].msg_type.as_u16(), 303);
    let error_response = parse_forwarded_rpc_response(&reply_frames[1]);
    assert!(error_response.stream_end);
    assert_rpc_code_error(
        error_response.body.as_ref(),
        crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
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
    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&route)
            .register_worker(test_rpc_worker(family, &route, 42));
        state.queue_request(
            correlation_id,
            RpcQueuedRequest::from_request(
                crate::domains::rpc::protocol::RpcRequest::new(
                    family,
                    correlation_id,
                    route.clone(),
                    Route::new("inbox://session/7/custom"),
                    bytes::Bytes::from_static(b"queued"),
                ),
                7,
                caller_inbox_addr,
                Instant::now() + Duration::from_millis(5),
            ),
        );
    }

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
        crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
        RPC_TIMEOUT_ERROR,
    );
}

#[test]
pub(super) fn should_drop_timeout_error_given_requester_cleanup_before_expiration() {
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
    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&request_route)
            .register_worker(test_rpc_worker(family, &request_route, 42));
    }
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
    assert_eq!(reply_frames.len(), 1);
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_eq!(reply_frames[0].payload[0], 0);
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
    {
        let mut state = sink.state.lock();
        state
            .ensure_route_state(&request_route)
            .register_worker(test_rpc_worker(family, &request_route, 42));
        for _ in 0..RPC_MAX_PENDING_REQUESTS {
            state.pending.track_pending(
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
    }
    let request_frame = crate::benchkit::build_rpc_request(request_route.as_str(), b"payload");
    let (msg_type, payload) = crate::benchkit::extract_single_tlv_field(&request_frame);
    let frame_ctx = FrameContext::new(
        1,
        crate::protocol::frame::ChannelId::Rpc,
        crate::protocol::tlv::MessageType::new(msg_type),
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
    assert_eq!(reply_frames[0].msg_type.as_u16(), 302);
    assert_rpc_code_error(
        &reply_frames[0].payload,
        crate::protocol::error_codes::rpc::ERR_RPC_BACKPRESSURE,
        RPC_BACKPRESSURE_ERROR,
    );
}
