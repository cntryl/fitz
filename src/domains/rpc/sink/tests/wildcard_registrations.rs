use super::*;

fn register(
    state: &mut RpcState,
    family: RouteFamily,
    pattern: &str,
    session_id: u64,
    max_concurrent: usize,
) -> RouteAddress {
    let addr = RouteAddress::new(family, Route::new(pattern));
    state.register_registration(RpcWorker::new(
        addr.clone(),
        session_inbox_address(family, session_id),
        session_id,
        max_concurrent,
    ));
    addr
}

fn request(
    state: &mut RpcState,
    family: RouteFamily,
    route: &str,
    caller_session_id: u64,
    correlation_id: uuid::Uuid,
    timeout: Duration,
) -> RpcRequestDispatch {
    state.dispatch_or_queue_request_with_global_count(
        crate::domains::rpc::RpcRequest::new(
            family,
            correlation_id,
            Route::new(route),
            bytes::Bytes::from_static(b"request"),
        ),
        caller_session_id,
        session_inbox_address(family, caller_session_id),
        timeout,
        32,
        256,
        None,
    )
}

fn remove_pending(state: &mut RpcState, family: RouteFamily, correlation_id: uuid::Uuid) {
    state
        .remove_pending_request_for_family(family, &correlation_id)
        .expect("pending RPC request");
}

#[test]
fn should_select_registration_through_dispatch_state_seam() {
    // Arrange
    let family = RouteFamily::new(7);
    let route = Route::new("rpc://bench/system/orders/create");
    let mut state = RpcState::new();
    register(&mut state, family, route.as_str(), 41, 1);
    let dispatch_state: &mut dyn RpcDispatchState = &mut state;

    // Act
    let selected = dispatch_state.claim_registration_for_route(family, &route);

    // Assert
    assert_eq!(selected.map(|worker| worker.session_id), Some(41));
}

#[test]
fn should_evict_idle_routes_behind_wildcard_registration() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/**", 10, 1);

    // Act
    for index in 0..64 {
        let correlation_id = uuid::Uuid::new_v4();
        let dispatch = request(
            &mut state,
            family,
            &format!("rpc://bench/jobs/task/{index}"),
            1,
            correlation_id,
            Duration::from_secs(30),
        );
        assert!(matches!(dispatch, RpcRequestDispatch::Immediate { .. }));
        remove_pending(&mut state, family, correlation_id);
    }

    // Assert
    assert_eq!(state.route_count(), 0);
    assert_eq!(state.registration_count(), 1);
}

#[test]
fn should_retain_route_state_until_last_live_request_finishes() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "rpc://bench/jobs/task/run";
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/**", 10, 2);
    let first_id = uuid::Uuid::new_v4();
    let second_id = uuid::Uuid::new_v4();
    assert!(matches!(
        request(
            &mut state,
            family,
            route,
            1,
            first_id,
            Duration::from_secs(30),
        ),
        RpcRequestDispatch::Immediate { .. }
    ));
    assert!(matches!(
        request(
            &mut state,
            family,
            route,
            2,
            second_id,
            Duration::from_secs(30),
        ),
        RpcRequestDispatch::Immediate { .. }
    ));

    // Act
    remove_pending(&mut state, family, first_id);
    let route_count_with_live_request = state.route_count();
    remove_pending(&mut state, family, second_id);

    // Assert
    assert_eq!(route_count_with_live_request, 1);
    assert_eq!(state.route_count(), 0);
}

#[test]
fn should_not_retain_idle_route_when_global_request_capacity_is_full() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/**", 10, 1);
    let request = crate::domains::rpc::RpcRequest::new(
        family,
        uuid::Uuid::new_v4(),
        Route::new("rpc://bench/jobs/task/rejected"),
        bytes::Bytes::from_static(b"request"),
    );

    // Act
    let dispatch = state.dispatch_or_queue_request_with_global_count(
        request,
        1,
        session_inbox_address(family, 1),
        Duration::from_secs(30),
        32,
        0,
        None,
    );

    // Assert
    assert!(matches!(
        dispatch,
        RpcRequestDispatch::Rejected {
            reason: super::RpcRequestRejection::GlobalCapacityFull,
            ..
        }
    ));
    assert_eq!(state.route_count(), 0);
}

#[test]
fn should_queue_cross_route_call_when_wildcard_credit_is_exhausted() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/system/*/*", 10, 1);
    let first_id = uuid::Uuid::new_v4();
    let second_id = uuid::Uuid::new_v4();
    let first = request(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        1,
        first_id,
        Duration::from_secs(30),
    );

    // Act
    let second = request(
        &mut state,
        family,
        "rpc://bench/system/invoices/send",
        2,
        second_id,
        Duration::from_secs(30),
    );

    // Assert
    assert!(matches!(first, RpcRequestDispatch::Immediate { .. }));
    assert!(matches!(second, RpcRequestDispatch::Queued { .. }));
    assert!(state.contains_correlation(&first_id));
    assert!(state.contains_correlation(&second_id));
}

#[test]
fn should_preserve_route_local_fifo_for_wildcard_registration() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/system/*/*", 10, 1);
    let blocker_id = uuid::Uuid::new_v4();
    let first_id = uuid::Uuid::new_v4();
    let second_id = uuid::Uuid::new_v4();
    request(
        &mut state,
        family,
        "rpc://bench/system/control/block",
        1,
        blocker_id,
        Duration::from_secs(30),
    );
    request(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        2,
        first_id,
        Duration::from_secs(30),
    );
    request(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        3,
        second_id,
        Duration::from_secs(30),
    );

    // Act
    remove_pending(&mut state, family, blocker_id);
    let first = state
        .next_ready_dispatch_for_family(family)
        .expect("first queued dispatch");
    remove_pending(&mut state, family, first.request.correlation_id);
    let second = state
        .next_ready_dispatch_for_family(family)
        .expect("second queued dispatch");

    // Assert
    assert_eq!(first.request.correlation_id, first_id);
    assert_eq!(second.request.correlation_id, second_id);
}

#[test]
fn should_rotate_ready_routes_under_sustained_wildcard_traffic() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/system/*/*", 10, 1);
    let blocker_id = uuid::Uuid::new_v4();
    request(
        &mut state,
        family,
        "rpc://bench/system/control/block",
        1,
        blocker_id,
        Duration::from_secs(30),
    );
    let route_a = "rpc://bench/system/orders/create";
    let route_b = "rpc://bench/system/invoices/send";
    for (caller, route) in [(2, route_a), (3, route_a), (4, route_b), (5, route_b)] {
        request(
            &mut state,
            family,
            route,
            caller,
            uuid::Uuid::new_v4(),
            Duration::from_secs(30),
        );
    }

    // Act
    remove_pending(&mut state, family, blocker_id);
    let mut routes = Vec::new();
    for _ in 0..4 {
        let dispatch = state
            .next_ready_dispatch_for_family(family)
            .expect("rotating queued dispatch");
        routes.push(dispatch.request.route.as_str().to_string());
        remove_pending(&mut state, family, dispatch.request.correlation_id);
    }

    // Assert
    assert_eq!(routes, [route_a, route_b, route_a, route_b]);
}

#[test]
fn should_wake_queued_route_when_new_registration_adds_credit() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/system/*/*", 10, 1);
    request(
        &mut state,
        family,
        "rpc://bench/system/control/block",
        1,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    let queued_id = uuid::Uuid::new_v4();
    request(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        2,
        queued_id,
        Duration::from_secs(30),
    );

    // Act
    register(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        11,
        1,
    );
    let dispatch = state
        .next_ready_dispatch_for_family(family)
        .expect("dispatch awakened by registration");

    // Assert
    assert_eq!(dispatch.request.correlation_id, queued_id);
    assert_eq!(dispatch.registration.session_id, 11);
}

#[test]
fn should_keep_exact_and_wildcard_registration_credit_independent() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "rpc://bench/system/orders/create";
    let mut state = RpcState::new();
    register(&mut state, family, route, 10, 1);
    register(&mut state, family, "rpc://bench/system/*/*", 11, 1);

    // Act
    let first = request(
        &mut state,
        family,
        route,
        1,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    let second = request(
        &mut state,
        family,
        route,
        2,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    let third = request(
        &mut state,
        family,
        route,
        3,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );

    // Assert
    assert!(
        matches!(first, RpcRequestDispatch::Immediate { registration, .. } if registration.session_id == 10)
    );
    assert!(
        matches!(second, RpcRequestDispatch::Immediate { registration, .. } if registration.session_id == 11)
    );
    assert!(matches!(third, RpcRequestDispatch::Queued { .. }));
}

#[test]
fn should_retain_original_credit_on_duplicate_registration() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "rpc://bench/system/orders/create";
    let mut state = RpcState::new();
    register(&mut state, family, route, 10, 1);

    // Act
    register(&mut state, family, route, 10, 8);
    let first = request(
        &mut state,
        family,
        route,
        1,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    let second = request(
        &mut state,
        family,
        route,
        2,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );

    // Assert
    assert_eq!(state.registration_count(), 1);
    assert!(matches!(first, RpcRequestDispatch::Immediate { .. }));
    assert!(matches!(second, RpcRequestDispatch::Queued { .. }));
}

#[test]
fn should_remove_only_owning_overlap_registration_and_pending_call() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "rpc://bench/system/orders/create";
    let mut state = RpcState::new();
    let exact = register(&mut state, family, route, 10, 1);
    register(&mut state, family, "rpc://bench/system/*/*", 11, 1);
    request(
        &mut state,
        family,
        route,
        1,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    request(
        &mut state,
        family,
        route,
        2,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );

    // Act
    let cleanup = state.unregister_registration(&exact, 10);

    // Assert
    assert_eq!(cleanup.removed_registrations, 1);
    assert_eq!(cleanup.removed_pending, 1);
    assert_eq!(state.registration_count(), 1);
    assert_eq!(state.pending.len(), 1);
}

#[test]
fn should_remove_only_disconnected_sessions_overlap_registration() {
    // Arrange
    let family = RouteFamily::new(1);
    let route = "rpc://bench/system/orders/create";
    let mut state = RpcState::new();
    register(&mut state, family, route, 10, 1);
    register(&mut state, family, "rpc://bench/system/*/*", 11, 1);
    request(
        &mut state,
        family,
        route,
        1,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );
    request(
        &mut state,
        family,
        route,
        2,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );

    // Act
    let cleanup = state.cleanup_session(10);

    // Assert
    assert_eq!(cleanup.removed_registrations, 1);
    assert_eq!(cleanup.removed_pending, 1);
    assert_eq!(state.registration_count(), 1);
    assert_eq!(state.pending.len(), 1);
}

#[test]
fn should_release_wildcard_credit_after_terminal_timeout() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    register(&mut state, family, "rpc://bench/system/*/*", 10, 1);
    request(
        &mut state,
        family,
        "rpc://bench/system/orders/create",
        1,
        uuid::Uuid::new_v4(),
        Duration::from_millis(1),
    );
    let queued_id = uuid::Uuid::new_v4();
    request(
        &mut state,
        family,
        "rpc://bench/system/invoices/send",
        2,
        queued_id,
        Duration::from_secs(30),
    );

    // Act
    let timeout = state.expire_timed_out(Instant::now() + Duration::from_millis(5));
    let dispatch = state
        .next_ready_dispatch_for_family(family)
        .expect("dispatch after timeout credit release");

    // Assert
    assert_eq!(timeout.removed_pending, 1);
    assert_eq!(dispatch.request.correlation_id, queued_id);
}

#[test]
fn should_isolate_wildcard_registration_by_route_family() {
    // Arrange
    let family_one = RouteFamily::new(1);
    let family_two = RouteFamily::new(2);
    let mut state = RpcState::new();
    register(&mut state, family_one, "rpc://bench/system/*/*", 10, 1);

    // Act
    let dispatch = request(
        &mut state,
        family_two,
        "rpc://bench/system/orders/create",
        2,
        uuid::Uuid::new_v4(),
        Duration::from_secs(30),
    );

    // Assert
    assert!(matches!(
        dispatch,
        RpcRequestDispatch::Rejected {
            reason: super::RpcRequestRejection::NoWorkers,
            ..
        }
    ));
}

#[test]
fn should_cap_wildcard_registrations_per_session() {
    // Arrange
    let family = RouteFamily::new(1);
    let mut state = RpcState::new();
    for index in 0..crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION {
        register(
            &mut state,
            family,
            &format!("rpc://bench/area{index}/*/run"),
            10,
            1,
        );
    }
    let overflow = RpcWorker::new(
        RouteAddress::new(family, Route::new("rpc://bench/overflow/*/run")),
        session_inbox_address(family, 10),
        10,
        1,
    );

    // Act
    let result = state.register_registration(overflow);

    // Assert
    assert!(matches!(result, RpcWorkerRegistration::WildcardLimit));
    assert_eq!(
        state.registration_count(),
        crate::domains::subscription_state::MAX_WILDCARD_REGISTRATIONS_PER_SESSION
    );
}

#[test]
fn should_snapshot_unused_wildcard_registration_once() {
    // Arrange
    let router = Arc::new(Router::new());
    let admin = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin.clone());
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(RouteFamily::new(1), Route::new("rpc://bench/system/*/*")),
        session_inbox_address(RouteFamily::new(1), 10),
        10,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));

    // Act
    sink.sync_admin_snapshot();
    let workers = admin.rpc_workers(None);

    // Assert
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].route, "rpc://bench/system/*/*");
    assert_eq!(sink.worker_count(), 1);
}

#[test]
fn should_aggregate_wildcard_completion_statistics_across_routes() {
    // Arrange
    let family = RouteFamily::new(1);
    let router = Arc::new(Router::new());
    let admin = crate::control::admin::read_model::AdminReadModel::new();
    let sink = RpcDomainSink::new(router, admin.clone());
    sink.register_registration_for_tests(RpcWorker::with_stats(
        RouteAddress::new(family, Route::new("rpc://bench/system/*/*")),
        session_inbox_address(family, 10),
        10,
        "2026-03-14T12:00:00Z",
        0,
        0,
    ));

    // Act
    {
        let mut state = sink.core.state.lock();
        let first = state
            .claim_registration_for_tests(family, &Route::new("rpc://bench/system/orders/create"))
            .expect("first wildcard dispatch");
        state.release_registration_with_latency_for_tests(first.registration_id, 1_000);
        let second = state
            .claim_registration_for_tests(family, &Route::new("rpc://bench/system/invoices/send"))
            .expect("second wildcard dispatch");
        state.release_registration_with_latency_for_tests(second.registration_id, 3_000);
    }
    sink.sync_admin_snapshot();
    let workers = admin.rpc_workers(None);

    // Assert
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].requests_handled, 2);
    assert!((workers[0].average_latency_ms - 2.0).abs() < f64::EPSILON);
    assert_eq!(sink.worker_count(), 1);
}
