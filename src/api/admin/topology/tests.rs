use super::*;
use crate::control::admin::{read_model::AdminReadModel, RpcWorker};
use crate::runtime::Router;
use std::sync::Arc;

#[test]
fn should_collect_family_topology_before_applying_connection_limit() {
    // Arrange
    let model = AdminReadModel::new();
    let mut workers = (0..300)
        .map(|id| {
            RpcWorker::snapshot(
                2,
                id,
                "other",
                "rpc://other/app/work/run",
                "2026-10-04T00:00:00Z",
                0,
                0.0,
            )
        })
        .collect::<Vec<_>>();
    workers.push(RpcWorker::snapshot(
        1,
        301,
        "acme",
        "rpc://acme/app/work/run",
        "2026-10-04T00:00:00Z",
        0,
        0.0,
    ));
    model.replace_rpc_workers(workers);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);

    // Act
    let topology = build_family_topology(&runtime, 1);

    // Assert
    assert!(topology
        .connections
        .items
        .iter()
        .any(|item| item.scope.route_family == Some(1)));
    assert!(topology
        .connections
        .items
        .iter()
        .all(|item| item.scope.route_family == Some(1)));
}
