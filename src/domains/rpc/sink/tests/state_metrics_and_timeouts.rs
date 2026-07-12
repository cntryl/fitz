use super::*;

pub(super) fn assert_rpc_code_error(payload: &[u8], expected_code: u16, expected_message: &str) {
    let (code, message) =
        crate::dispatch::protocol::rpc_codec::decode_error_body(payload).expect("rpc code error");
    assert_eq!(code, expected_code);
    assert_eq!(message, expected_message);
}

#[test]
fn should_keep_rpc_worker_state_isolated_by_route_family() {
    // Arrange
    let route = Route::new("rpc://bench/system/resource/operation");
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);
    let mut state = RpcState::new();
    state
        .ensure_route_state_for_family(family_one, &route)
        .register_worker(test_rpc_worker(family_one, &route, 11));
    state
        .ensure_route_state_for_family(family_two, &route)
        .register_worker(test_rpc_worker(family_two, &route, 22));

    // Act
    let family_one_workers = state
        .routes
        .get(&(family_one, route.clone()))
        .expect("family one route")
        .worker_count();
    let family_two_workers = state
        .routes
        .get(&(family_two, route))
        .expect("family two route")
        .worker_count();

    // Assert
    assert_eq!(family_one_workers, 1);
    assert_eq!(family_two_workers, 1);
    assert_eq!(state.route_count(), 2);
}

#[test]
fn should_allow_same_rpc_correlation_in_separate_route_families() {
    // Arrange
    let route = Route::new("rpc://bench/system/resource/operation");
    let correlation_id = uuid::Uuid::new_v4();
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);
    let mut state = RpcState::new();
    state
        .ensure_route_state_for_family(family_one, &route)
        .register_worker(test_rpc_worker(family_one, &route, 11));
    state
        .ensure_route_state_for_family(family_two, &route)
        .register_worker(test_rpc_worker(family_two, &route, 22));

    // Act
    let first = state.dispatch_or_queue_request(
        crate::domains::rpc::protocol::RpcRequest::new(
            family_one,
            correlation_id,
            route.clone(),
            bytes::Bytes::from_static(b"one"),
        ),
        101,
        session_inbox_address(family_one, 101),
        Duration::from_secs(30),
        8,
        32,
    );
    let second = state.dispatch_or_queue_request(
        crate::domains::rpc::protocol::RpcRequest::new(
            family_two,
            correlation_id,
            route,
            bytes::Bytes::from_static(b"two"),
        ),
        202,
        session_inbox_address(family_two, 202),
        Duration::from_secs(30),
        8,
        32,
    );

    // Assert
    assert!(matches!(first, RpcRequestDispatch::Immediate { .. }));
    assert!(matches!(second, RpcRequestDispatch::Immediate { .. }));
    assert_eq!(state.live_request_count(), 2);
}

#[test]
fn should_release_worker_credit_after_caller_disconnects() {
    // Arrange
    let route = Route::new("rpc://bench/system/resource/operation");
    let family = RouteFamily::new(1);
    let correlation_id = uuid::Uuid::new_v4();
    let mut state = RpcState::new();
    state
        .ensure_route_state_for_family(family, &route)
        .register_worker(test_rpc_worker(family, &route, 11));
    let dispatch = state.dispatch_or_queue_request(
        crate::domains::rpc::protocol::RpcRequest::new(
            family,
            correlation_id,
            route.clone(),
            bytes::Bytes::from_static(b"payload"),
        ),
        101,
        session_inbox_address(family, 101),
        Duration::from_secs(30),
        8,
        32,
    );
    assert!(matches!(dispatch, RpcRequestDispatch::Immediate { .. }));
    state.pending.cleanup_session(101);

    // Act
    let response =
        state
            .pending
            .pending_for_response_in_family(family, &correlation_id, 11, 0, true);
    if let RpcPendingResponseDisposition::Forward { pending, .. } = response {
        state.release_worker_for_dispatch_info(&pending, None);
    } else {
        panic!("caller cleanup must retain the worker-owned pending request");
    }

    // Assert
    assert!(state
        .routes
        .get(&(family, route))
        .expect("family route")
        .has_available_worker());
}

pub(super) fn assert_rpc_terminal_code_error(
    frame: &FrameContext,
    expected_correlation_id: uuid::Uuid,
    expected_code: u16,
    expected_message: &str,
) {
    assert_eq!(frame.msg_type.as_u16(), 303);
    let response = parse_forwarded_rpc_response(frame);
    assert_eq!(response.correlation_id, expected_correlation_id);
    assert_eq!(response.seq, 0);
    assert!(response.stream_end);
    assert_rpc_code_error(&response.body, expected_code, expected_message);
}

pub(super) fn parse_forwarded_rpc_response(
    frame: &FrameContext,
) -> crate::domains::rpc::protocol::RpcResponse {
    match crate::dispatch::protocol::rpc_codec::parse_request(
        frame,
        &frame.payload,
        frame.route_family,
    )
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

pub(super) fn test_rpc_timestamp() -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::parse_from_rfc3339("2026-03-14T12:00:00Z")
        .expect("test timestamp")
        .with_timezone(&chrono::Utc)
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
        worker_slot: 0,
        submitted_at: test_rpc_timestamp(),
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
    assert!(sink.is_active());
    assert_eq!(sink.live_request_count_for_tests(), 0);
}

#[test]
pub(super) fn should_reject_rpc_delivery_when_managed_actor_is_stopped() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model);
    let destination = RouteAddress::new(
        RouteFamily::new(1),
        Route::new("rpc://prod/system/resource/op"),
    );
    let envelope = Envelope::new(destination, 42_u64);

    // Act
    sink.stop_actor_for_tests();
    let result = sink.deliver(envelope);

    // Assert
    assert!(!sink.is_actor_running());
    assert!(matches!(result, Err(DeliveryError::ActorStopped)));
}

#[test]
pub(super) fn should_route_rpc_live_count_queries_through_managed_actor() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model);
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://prod/system/resource/op");
    let correlation_id = uuid::Uuid::new_v4();
    sink.register_worker_for_tests(test_rpc_worker(family, &route, 42));
    sink.track_pending_request_for_tests(
        correlation_id,
        test_pending_request(
            family,
            &route,
            7,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    sink.stop_actor_for_tests();
    let worker_count = sink.worker_count();
    let pending_request_count = sink.pending_request_count();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(worker_count, 0);
    assert_eq!(pending_request_count, 0);
}

#[test]
pub(super) fn should_route_rpc_session_cleanup_helper_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
    let route = Route::new("rpc://prod/system/resource/cleanup");
    sink.register_worker_for_tests(test_rpc_worker(family, &route, 42));
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &route,
            7,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    sink.stop_actor_for_tests();
    let cleanup = sink.apply_session_cleanup(42);
    let pending_count = sink.live_request_count_for_tests();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(cleanup.removed_workers, 0);
    assert_eq!(cleanup.detached_callers, 0);
    assert_eq!(cleanup.removed_pending, 0);
    assert_eq!(cleanup.pending_len, 0);
    assert_eq!(pending_count, 1);
    assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 0);
    assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 0);
}

#[test]
pub(super) fn should_route_rpc_worker_unsubscribe_helper_through_managed_actor() {
    // Arrange
    let family = RouteFamily::new(1);
    let metrics = crate::observability::metrics::MetricsCollector::new();
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin_read_model).with_metrics(metrics.clone());
    let route = Route::new("rpc://prod/system/resource/unsubscribe");
    let worker_addr = RouteAddress::new(family, route.clone());
    sink.register_worker_for_tests(test_rpc_worker(family, &route, 42));
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &route,
            7,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

    // Act
    sink.stop_actor_for_tests();
    let cleanup = sink.apply_worker_unsubscribe(&worker_addr, 42);
    let pending_count = sink.live_request_count_for_tests();

    // Assert
    assert!(!sink.is_actor_running());
    assert_eq!(cleanup.removed_workers, 0);
    assert_eq!(cleanup.removed_pending, 0);
    assert_eq!(cleanup.pending_len, 0);
    assert_eq!(pending_count, 1);
    assert_eq!(metrics.counter_get("rpc_cleanup_workers_removed_total"), 0);
    assert_eq!(metrics.counter_get("rpc_cleanup_pending_removed_total"), 0);
}

#[test]
pub(super) fn should_keep_rpc_mailbox_sink_impl_below_file_size_limit() {
    // Arrange
    let line_count = include_str!("../mailbox_sink_impl.rs").lines().count();

    // Act
    let within_limit = line_count < 1_000;

    // Assert
    assert!(
        within_limit,
        "rpc mailbox sink impl has {line_count} lines; split before adding behavior"
    );
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
pub(super) fn should_consume_worker_credit_before_fanning_out_given_route_local_rpc_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut route_state = RpcRouteState::new();
    route_state.register_worker(RpcWorker::new(
        RouteAddress::new(family, route.clone()),
        session_inbox_address(family, 10),
        10,
        2,
    ));
    route_state.register_worker(RpcWorker::new(
        RouteAddress::new(family, route.clone()),
        session_inbox_address(family, 11),
        11,
        1,
    ));

    // Act
    let first = route_state.claim_worker().map(|worker| worker.session_id);
    let second = route_state.claim_worker().map(|worker| worker.session_id);
    let third = route_state.claim_worker().map(|worker| worker.session_id);

    // Assert
    assert_eq!(first, Some(10));
    assert_eq!(second, Some(10));
    assert_eq!(third, Some(11));
}

#[test]
pub(super) fn should_rotate_one_credit_workers_after_release_given_route_local_rpc_state() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut route_state = RpcRouteState::new();
    route_state.register_worker(test_rpc_worker(family, &route, 10));
    route_state.register_worker(test_rpc_worker(family, &route, 11));

    // Act
    let first = route_state.claim_worker().expect("first worker");
    route_state.release_worker_slot(first.slot, None);
    let second = route_state.claim_worker().expect("second worker");

    // Assert
    assert_eq!(first.session_id, 10);
    assert_eq!(second.session_id, 11);
}

#[test]
pub(super) fn should_restore_multi_credit_worker_priority_after_release_given_route_local_rpc_state(
) {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://bench/system/resource/operation");
    let mut route_state = RpcRouteState::new();
    route_state.register_worker(RpcWorker::new(
        RouteAddress::new(family, route.clone()),
        session_inbox_address(family, 10),
        10,
        2,
    ));
    route_state.register_worker(RpcWorker::new(
        RouteAddress::new(family, route.clone()),
        session_inbox_address(family, 11),
        11,
        1,
    ));

    // Act
    let first = route_state.claim_worker().expect("first credit");
    let second = route_state.claim_worker().expect("second credit");
    route_state.release_worker_slot(second.slot, None);
    let third = route_state.claim_worker().expect("restored credit");

    // Assert
    assert_eq!(first.session_id, 10);
    assert_eq!(second.session_id, 10);
    assert_eq!(third.session_id, 10);
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

    sink.register_worker_for_tests(test_rpc_worker(family, &route, 42));
    sink.track_pending_request_for_tests(
        correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: route.clone(),
            caller_session_id: 7,
            caller_inbox_addr: session_inbox_address(family, 7),
            worker_addr: RouteAddress::new(family, route.clone()),
            worker_session_id: 42,
            worker_slot: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now().checked_sub(Duration::from_secs(9)).unwrap(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

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
    let response_payload = crate::dispatch::protocol::rpc_codec::encode_response_message(
        &crate::domains::rpc::protocol::RpcResponse::single(
            uuid::Uuid::new_v4(),
            bytes::Bytes::from_static(b"ok"),
        ),
    );
    let response = match crate::dispatch::protocol::rpc_codec::parse_request(
        &FrameContext::new(
            42,
            crate::dispatch::protocol::frame::ChannelId::Rpc,
            crate::dispatch::protocol::tlv::MessageType::new(303),
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

    sink.register_worker_for_tests(RpcWorker::with_stats(
        request_addr.clone(),
        worker_inbox_addr.clone(),
        42,
        "2026-03-14T11:59:00Z",
        0,
        0,
    ));
    sink.track_pending_request_for_tests(
        response.correlation_id,
        RpcPendingRequest::new(RpcPendingRequestInit {
            route: route.clone(),
            caller_session_id: 7,
            caller_inbox_addr: caller_addr.clone(),
            worker_addr: request_addr.clone(),
            worker_session_id: 42,
            worker_slot: 0,
            submitted_at: test_rpc_timestamp(),
            submitted_at_instant: Instant::now()
                .checked_sub(Duration::from_millis(50))
                .unwrap(),
            expires_at: Instant::now() + Duration::from_secs(30),
        }),
    );

    // Act
    sink.deliver(Envelope::from_route(
        worker_inbox_addr,
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

    sink.register_worker_for_tests(test_rpc_worker(family, &worker_route_a, 42));
    sink.register_worker_for_tests(test_rpc_worker(family, &worker_route_b, 42));
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &worker_route_a,
            90,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &worker_route_b,
            91,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &external_route_a,
            42,
            7,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &external_route_b,
            42,
            8,
            Instant::now() + Duration::from_secs(30),
        ),
    );

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

    sink.register_worker_for_tests(test_rpc_worker(family, &removed_route, 42));
    sink.register_worker_for_tests(test_rpc_worker(family, &retained_route, 42));
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &removed_route,
            90,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &removed_route,
            91,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );
    sink.track_pending_request_for_tests(
        uuid::Uuid::new_v4(),
        test_pending_request(
            family,
            &retained_route,
            92,
            42,
            Instant::now() + Duration::from_secs(30),
        ),
    );

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
