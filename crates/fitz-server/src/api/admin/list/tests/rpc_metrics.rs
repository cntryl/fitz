use super::*;

#[test]
fn should_aggregate_rpc_operations_with_more_than_one_hundred_pending_calls() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime.admin_read_model().replace_rpc_workers(vec![
        RpcWorker {
            route_family: 1,
            session_id: "one".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 5,
            average_latency_ms: 3.0,
        },
        RpcWorker {
            route_family: 1,
            session_id: "two".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 7,
            average_latency_ms: 8.0,
        },
    ]);
    let mut pending = (0..150)
        .map(|index| RpcPendingRequest {
            route_family: 1,
            correlation_id: format!("request-{index}"),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            submitted_at: "2026-07-31T12:00:00Z".to_string(),
            age_seconds: 1,
            worker_session_id: None,
        })
        .collect::<Vec<_>>();
    pending.push(RpcPendingRequest {
        route_family: 1,
        correlation_id: "pending-only".to_string(),
        route: "rpc://prod/jobs/reconcile/wait".to_string(),
        submitted_at: "2026-07-31T12:00:00Z".to_string(),
        age_seconds: 1,
        worker_session_id: None,
    });
    runtime.admin_read_model().replace_rpc_pending(pending);
    let path = ResourcePath {
        realm: "prod",
        area: "jobs",
        resource: "reconcile",
    };

    // Act
    let collection = rpc_operations(&runtime, &path, Some(1));

    // Assert
    assert_eq!(collection.workers_registered, 2);
    assert_eq!(collection.requests_pending, 151);
    assert_eq!(collection.operations.len(), 2);
    let run = collection
        .operations
        .iter()
        .find(|entry| entry.operation == "run")
        .expect("run operation");
    assert_eq!(run.workers_registered, 2);
    assert_eq!(run.requests_pending, 150);
    assert_eq!(run.requests_handled_by_live_workers, Some(12));
    assert_eq!(run.slowest_worker_average_latency_ms, Some(8.0));
    let wait = collection
        .operations
        .iter()
        .find(|entry| entry.operation == "wait")
        .expect("pending-only operation");
    assert_eq!(wait.workers_registered, 0);
    assert_eq!(wait.requests_pending, 1);
}

#[test]
fn should_not_double_count_wildcard_rpc_workers_across_operations() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_rpc_workers(vec![RpcWorker {
            route_family: 1,
            session_id: "wildcard".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/*".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 9,
            average_latency_ms: 8.0,
        }]);
    runtime.admin_read_model().replace_rpc_pending(vec![
        RpcPendingRequest {
            route_family: 1,
            correlation_id: "one".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            submitted_at: "2026-07-31T12:00:00Z".to_string(),
            age_seconds: 1,
            worker_session_id: None,
        },
        RpcPendingRequest {
            route_family: 1,
            correlation_id: "two".to_string(),
            route: "rpc://prod/jobs/reconcile/wait".to_string(),
            submitted_at: "2026-07-31T12:00:00Z".to_string(),
            age_seconds: 1,
            worker_session_id: None,
        },
    ]);
    let path = ResourcePath {
        realm: "prod",
        area: "jobs",
        resource: "reconcile",
    };

    // Act
    let collection = rpc_operations(&runtime, &path, Some(1));

    // Assert
    assert_eq!(collection.workers_registered, 1);
    assert_eq!(collection.requests_pending, 2);
    for operation in &collection.operations {
        assert_eq!(operation.workers_registered, 1);
        assert_eq!(operation.requests_handled_by_live_workers, None);
        assert_eq!(operation.slowest_worker_average_latency_ms, None);
    }
}

#[test]
fn should_leave_rpc_operation_handled_count_unknown_for_wildcard_worker() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_rpc_workers(vec![RpcWorker {
            route_family: 1,
            session_id: "wildcard".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/*".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 9,
            average_latency_ms: 8.0,
        }]);
    let path = RpcOperationPath {
        realm: "prod",
        area: "jobs",
        resource: "reconcile",
        operation: "run",
    };

    // Act
    let detail = rpc_operation_detail(&runtime, &path, Some(1));

    // Assert
    assert_eq!(detail.requests_handled_by_live_workers, None);
}

#[test]
fn should_preserve_rpc_operation_handled_count_for_exact_workers() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_rpc_workers(vec![RpcWorker {
            route_family: 1,
            session_id: "exact".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 9,
            average_latency_ms: 8.0,
        }]);
    let path = RpcOperationPath {
        realm: "prod",
        area: "jobs",
        resource: "reconcile",
        operation: "run",
    };

    // Act
    let detail = rpc_operation_detail(&runtime, &path, Some(1));

    // Assert
    assert_eq!(detail.requests_handled_by_live_workers, Some(9));
}
