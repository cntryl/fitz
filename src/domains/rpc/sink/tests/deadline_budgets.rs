use super::*;
use crate::domains::rpc::RpcRequest;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

mod ingress;

fn request(id: uuid::Uuid) -> RpcRequest {
    RpcRequest::new(
        RouteFamily::new(1),
        id,
        Route::new("rpc://acme/jobs/orders/process"),
        bytes::Bytes::from_static(b"request"),
    )
}

fn admit(
    state: &mut RpcState,
    request: RpcRequest,
    timeout: Duration,
    global: Option<&AtomicUsize>,
) -> RpcRequestDispatch {
    admit_with_deadline(state, request, None, timeout, global)
}

fn admit_with_deadline(
    state: &mut RpcState,
    request: RpcRequest,
    deadline: Option<Instant>,
    timeout: Duration,
    global: Option<&AtomicUsize>,
) -> RpcRequestDispatch {
    state.dispatch_or_queue_request_with_deadline(
        request,
        deadline,
        7,
        session_inbox_address(RouteFamily::new(1), 7),
        timeout,
        32,
        256,
        global,
    )
}

#[test]
fn should_deduct_elapsed_time_from_local_request_budget() {
    // Arrange
    let now = Instant::now();
    let deadline = now + Duration::from_secs(5);

    // Act
    let remaining =
        super::super::deadlines::remaining_budget_at(deadline, now + Duration::from_secs(3));

    // Assert
    assert_eq!(remaining, Duration::from_secs(2));
}

#[test]
fn should_report_zero_remaining_budget_after_local_deadline() {
    // Arrange
    let now = Instant::now();

    // Act
    let remaining = super::super::deadlines::remaining_budget_at(now, now + Duration::from_secs(1));

    // Assert
    assert_eq!(remaining, Duration::ZERO);
}

#[test]
fn should_preserve_legacy_worker_request_bytes_with_local_deadline() {
    // Arrange
    let request = request(uuid::Uuid::new_v4());
    let mut state = RpcState::new();
    state.register_registration(test_rpc_worker(request.family_id, &request.route, 42));
    let expected = crate::protocol::rpc_codec::encode_worker_request_tlv_frame(&request);

    // Act
    let result = admit_with_deadline(
        &mut state,
        request,
        Some(Instant::now() + Duration::from_secs(5)),
        Duration::from_secs(30),
        None,
    );
    let RpcRequestDispatch::Immediate {
        request: dispatched_request,
        ..
    } = result
    else {
        panic!("request with worker credit should dispatch immediately");
    };
    let actual = crate::protocol::rpc_codec::encode_worker_request_tlv_frame(&dispatched_request);

    // Assert
    assert_eq!(actual, expected);
}

#[test]
fn should_reject_expired_budget_without_claiming_worker_or_global_capacity() {
    // Arrange
    let mut state = RpcState::new();
    let request = request(uuid::Uuid::new_v4());
    let route = request.route.clone();
    state.register_registration(test_rpc_worker(request.family_id, &route, 42));
    let global = AtomicUsize::new(0);

    // Act
    let result = admit_with_deadline(
        &mut state,
        request,
        Some(Instant::now()),
        Duration::from_secs(30),
        Some(&global),
    );
    let route_count = state.route_count();
    let available = state.claim_registration_for_tests(RouteFamily::new(1), &route);

    // Assert
    assert!(matches!(
        result,
        RpcRequestDispatch::Rejected {
            reason: RpcRequestRejection::Expired,
            ..
        }
    ));
    assert!(available.is_some());
    assert_eq!(global.load(Ordering::Acquire), 0);
    assert_eq!(state.live_request_count(), 0);
    assert_eq!(route_count, 0);
}

#[test]
fn should_cap_local_request_deadline_to_broker_timeout() {
    // Arrange
    let mut state = RpcState::new();
    let request = request(uuid::Uuid::new_v4());
    let id = request.correlation_id;
    state.register_registration(test_rpc_worker(request.family_id, &request.route, 42));
    let before = Instant::now();
    let timeout = Duration::from_secs(30);

    // Act
    let result = admit_with_deadline(
        &mut state,
        request,
        Some(Instant::now() + Duration::from_secs(300)),
        timeout,
        None,
    );
    let after = Instant::now();
    let (pending, _) = state
        .remove_pending_request_for_family(RouteFamily::new(1), &id)
        .unwrap();

    // Assert
    assert!(matches!(result, RpcRequestDispatch::Immediate { .. }));
    assert!(pending.expires_at >= before + timeout);
    assert!(pending.expires_at <= after + timeout);
}

#[test]
fn should_preserve_original_local_deadline_after_rpc_queueing() {
    // Arrange
    let mut state = RpcState::new();
    let first = request(uuid::Uuid::new_v4());
    let first_id = first.correlation_id;
    state.register_registration(test_rpc_worker(first.family_id, &first.route, 42));
    assert!(matches!(
        admit(&mut state, first, Duration::from_secs(30), None),
        RpcRequestDispatch::Immediate { .. }
    ));
    let deadline = Instant::now() + Duration::from_secs(5);
    let queued = request(uuid::Uuid::new_v4());
    let queued_id = queued.correlation_id;
    assert!(matches!(
        admit_with_deadline(
            &mut state,
            queued,
            Some(deadline),
            Duration::from_secs(30),
            None,
        ),
        RpcRequestDispatch::Queued { .. }
    ));

    // Act
    state
        .remove_pending_request_for_family(RouteFamily::new(1), &first_id)
        .unwrap();
    let dispatched = state
        .next_ready_dispatch_for_family(RouteFamily::new(1))
        .unwrap();
    let (pending, _) = state
        .remove_pending_request_for_family(RouteFamily::new(1), &queued_id)
        .unwrap();

    // Assert
    assert_eq!(dispatched.expires_at, deadline);
    assert_eq!(pending.expires_at, deadline);
}

#[test]
fn should_preserve_live_request_when_expired_duplicate_arrives() {
    // Arrange
    let mut state = RpcState::new();
    let original = request(uuid::Uuid::new_v4());
    let id = original.correlation_id;
    let route = original.route.clone();
    state.register_registration(test_rpc_worker(original.family_id, &route, 42));
    assert!(matches!(
        admit(&mut state, original.clone(), Duration::from_secs(30), None),
        RpcRequestDispatch::Immediate { .. }
    ));

    // Act
    let result = admit_with_deadline(
        &mut state,
        original,
        Some(Instant::now()),
        Duration::from_secs(30),
        None,
    );
    let available = state.claim_registration_for_tests(RouteFamily::new(1), &route);

    // Assert
    assert!(matches!(
        result,
        RpcRequestDispatch::Rejected {
            reason: RpcRequestRejection::Duplicate,
            ..
        }
    ));
    assert!(state.contains_correlation(&id));
    assert!(available.is_none());
    assert_eq!(state.live_request_count(), 1);
}

#[test]
fn should_expire_queued_dispatch_before_worker_delivery_and_release_exact_credit() {
    // Arrange
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let caller_frames = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let worker_frames = Arc::new(parking_lot::Mutex::new(Vec::new()));
    router.register(
        session_inbox_address(family, 7),
        Arc::new(CaptureRpcFrameSink {
            frames: caller_frames.clone(),
        }),
    );
    router.register(
        session_inbox_address(family, 42),
        Arc::new(CaptureRpcFrameSink {
            frames: worker_frames.clone(),
        }),
    );
    let config = RpcDomainConfig {
        router,
        admin_read_model: crate::control::admin::read_model::AdminReadModel::new(),
        request_timeout: Duration::from_secs(30),
        cancellation_grace_period: Duration::from_secs(5),
        route_pending_capacity: 32,
        global_pending_count: Arc::new(AtomicUsize::new(1)),
        snapshot_epoch: Instant::now(),
        metrics: None,
    };
    let mut core = RpcDomain::new_family_state(&config, family);
    let request = request(uuid::Uuid::new_v4());
    let deadline = Instant::now();
    let route = request.route.clone();
    core.state
        .register_registration(test_rpc_worker(family, &route, 42));
    let registration = core
        .state
        .claim_registration_for_tests(family, &route)
        .unwrap();
    let pending = RpcPendingRequest::from_dispatch(
        &request,
        7,
        session_inbox_address(family, 7),
        &registration,
        deadline,
    );
    core.state
        .pending
        .track_pending_for_family(family, request.correlation_id, pending);
    let active = AtomicBool::new(true);
    let mut runtime = RpcFamilyRuntime {
        core: &mut core,
        active: &active,
    };

    // Act
    runtime.forward_queued_dispatch(&RpcQueuedDispatch {
        request,
        registration,
        live_request_count: 1,
        expires_at: deadline,
    });
    let available = runtime
        .core
        .state
        .claim_registration_for_tests(family, &route);
    let frames = caller_frames.lock();

    // Assert
    assert!(worker_frames.lock().is_empty());
    assert_eq!(frames.len(), 1);
    let response = parse_forwarded_rpc_response(&frames[0]);
    let (error_code, _) = crate::protocol::rpc_codec::decode_error_body(&response.body).unwrap();
    assert_eq!(
        error_code,
        crate::protocol::error_codes::rpc::ERR_RPC_TIMEOUT
    );
    assert!(available.is_some());
    assert_eq!(runtime.core.state.registration_count(), 1);
    assert_eq!(runtime.core.state.live_request_count(), 0);
    assert_eq!(config.global_pending_count.load(Ordering::Acquire), 0);
}

#[test]
fn should_keep_worker_credit_until_deadline_cancellation_grace_expires() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = Route::new("rpc://acme/jobs/orders/process");
    let mut state = RpcState::new();
    state
        .register_registration(test_rpc_worker(family, &route, 42).with_cancellation_support(true));
    let correlation_id = uuid::Uuid::new_v4();
    let deadline = Instant::now() + Duration::from_millis(10);
    let grace = Duration::from_secs(5);
    let request = request(correlation_id);
    let dispatch = state.dispatch_or_queue_request_with_deadline(
        request,
        Some(deadline),
        7,
        session_inbox_address(family, 7),
        Duration::from_secs(30),
        32,
        256,
        None,
    );
    let RpcRequestDispatch::Immediate { .. } = dispatch else {
        panic!("live worker should receive the request immediately");
    };

    // Act
    let timeout_at = deadline + Duration::from_millis(1);
    let timeout = state.expire_timed_out_with_grace(timeout_at, grace, |_, _| None);
    let available_during_grace = state.claim_registration_for_tests(family, &route);
    let grace_expiry = timeout_at + grace + Duration::from_millis(1);
    let expired = state.expire_timed_out_with_grace(grace_expiry, grace, |_, _| None);
    let duplicate_expiry = state.expire_timed_out_with_grace(grace_expiry, grace, |_, _| None);
    let cleanup = state.cleanup_session_with_grace(42, grace_expiry, grace);

    // Assert
    assert_eq!(timeout.timed_out_requests, 1);
    assert_eq!(timeout.removed_pending, 0);
    assert_eq!(timeout.pending_len, 1);
    assert_eq!(timeout.cancellations.len(), 1);
    assert_eq!(
        timeout.cancellations[0].reason,
        crate::protocol::rpc_codec::RpcCancellationReason::BrokerDeadline
    );
    assert!(available_during_grace.is_none());
    assert_eq!(expired.close_worker_sessions, [(family, 42)]);
    assert_eq!(expired.removed_pending, 0);
    assert_eq!(expired.pending_len, 1);
    assert_eq!(
        duplicate_expiry.close_worker_sessions,
        Vec::<(RouteFamily, u64)>::new()
    );
    assert_eq!(cleanup.removed_pending, 1);
    assert_eq!(state.live_request_count(), 0);
}
