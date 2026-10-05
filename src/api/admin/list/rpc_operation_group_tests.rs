use super::rpc_operations_from_rows;
use crate::api::admin::list::{matches_operation_route, ResourcePath, RpcOperationPath};
use crate::api::admin::troubleshooting::{rpc_resource_timeline, ResourceTimelineKind};
use crate::control::admin::RpcWorker;

fn worker(route: &str) -> RpcWorker {
    RpcWorker::snapshot(1, 1, "acme", route, "2026-10-04T00:00:00Z", 0, 0.0)
}

fn path() -> ResourcePath<'static> {
    ResourcePath {
        realm: "acme",
        area: "jobs",
        resource: "alpha",
    }
}

#[test]
fn should_group_flexible_suffixes_under_the_first_four_operation_components() {
    // Arrange
    let operation = RpcOperationPath {
        realm: "acme",
        area: "jobs",
        resource: "alpha",
        operation: "run",
    };
    let routes = [
        "rpc://acme/jobs/alpha/run/detail",
        "rpc://acme/jobs/alpha/run/*",
        "rpc://acme/jobs/alpha/run/**/done",
    ];

    // Act
    let matched = routes.map(|route| matches_operation_route(route, &operation));

    // Assert
    assert_eq!(matched, [true; 3]);
}

#[test]
fn should_count_flexible_suffix_workers_under_the_known_operation_name() {
    // Arrange
    let workers = [worker("rpc://acme/jobs/alpha/run/*")];

    // Act
    let result = rpc_operations_from_rows(&path(), &workers, &[]);

    // Assert
    assert_eq!(result.workers_registered, 1);
    assert_eq!(result.operations.len(), 1);
    assert_eq!(result.operations[0].operation, "run");
    assert_eq!(result.operations[0].workers_registered, 1);
    assert_eq!(result.operations[0].requests_handled_by_live_workers, None);
}

#[test]
fn should_omit_ambiguous_operation_names_after_a_multi_segment_wildcard() {
    // Arrange
    let workers = [worker("rpc://acme/**/run/done")];

    // Act
    let result = rpc_operations_from_rows(&path(), &workers, &[]);

    // Assert
    assert_eq!(result.workers_registered, 1);
    assert!(result.operations.is_empty());
}

#[test]
fn should_preserve_known_operation_names_after_single_segment_wildcards() {
    // Arrange
    let workers = [worker("rpc://acme/jobs/*/run/*")];

    // Act
    let result = rpc_operations_from_rows(&path(), &workers, &[]);

    // Assert
    assert_eq!(result.operations.len(), 1);
    assert_eq!(result.operations[0].operation, "run");
    assert_eq!(result.operations[0].workers_registered, 1);
}

#[test]
fn should_attribute_exact_flexible_suffix_counters_to_the_known_operation() {
    // Arrange
    let workers = [RpcWorker::snapshot(
        1,
        1,
        "acme",
        "rpc://acme/jobs/alpha/run/detail",
        "2026-10-04T00:00:00Z",
        7,
        12.0,
    )];

    // Act
    let result = rpc_operations_from_rows(&path(), &workers, &[]);

    // Assert
    assert_eq!(
        result.operations[0].requests_handled_by_live_workers,
        Some(7)
    );
    assert_eq!(
        result.operations[0].slowest_worker_average_latency_ms,
        Some(12.0)
    );
}

#[test]
fn should_omit_ambiguous_operation_labels_from_worker_timeline_events() {
    // Arrange
    let workers = [worker("rpc://acme/**/run/done")];

    // Act
    let timeline = rpc_resource_timeline(&workers, &[], &path(), 10);

    // Assert
    let registration = timeline
        .events
        .iter()
        .find(|event| event.kind == ResourceTimelineKind::Registration)
        .expect("matching worker registration");
    assert_eq!(registration.operation, None);
}

#[test]
fn should_preserve_concrete_flexible_operation_labels_in_worker_timeline_events() {
    // Arrange
    let workers = [worker("rpc://acme/jobs/alpha/run/detail")];

    // Act
    let timeline = rpc_resource_timeline(&workers, &[], &path(), 10);

    // Assert
    let registration = timeline
        .events
        .iter()
        .find(|event| event.kind == ResourceTimelineKind::Registration)
        .expect("matching worker registration");
    assert_eq!(registration.operation.as_deref(), Some("run"));
}
