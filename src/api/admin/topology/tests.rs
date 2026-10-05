use super::*;
use crate::control::admin::{
    read_model::AdminReadModel, NoticeSubscription, RpcPendingRequest, RpcWorker,
};
use crate::runtime::Router;
use std::sync::Arc;

fn runtime_with_identical_routes_in_two_families() -> Runtime {
    let model = AdminReadModel::new();
    model.replace_notice_subscriptions(
        (1..=2)
            .map(|family| {
                NoticeSubscription::snapshot(
                    family,
                    family,
                    family,
                    "acme",
                    "notice://acme/jobs/work".into(),
                    "2026-10-04T00:00:00Z",
                )
            })
            .collect(),
    );
    model.replace_rpc_workers(
        (1..=2)
            .map(|family| {
                RpcWorker::snapshot(
                    family,
                    family,
                    "acme",
                    "rpc://acme/jobs/work/run",
                    "2026-10-04T00:00:00Z",
                    0,
                    0.0,
                )
            })
            .collect(),
    );
    model.replace_rpc_pending(
        (1..=2)
            .map(|family| {
                RpcPendingRequest::snapshot(
                    family,
                    &format!("pending-{family}"),
                    "rpc://acme/jobs/work/run",
                    "2026-10-04T00:00:00Z",
                    0,
                    Some(family.to_string()),
                )
            })
            .collect(),
    );
    Runtime::with_admin_read_model(Arc::new(Router::new()), model)
}

#[test]
fn should_preserve_unscoped_route_scopes_and_rollups_in_global_rest_topology() {
    // Arrange
    let runtime = runtime_with_identical_routes_in_two_families();

    // Act
    let topology = build_messaging_topology(&runtime);

    // Assert
    for connection in topology.connections.items.iter().filter(|item| {
        matches!(
            item.kind,
            types::TopologyConnectionKind::NoticeSubscription
                | types::TopologyConnectionKind::RpcWorker
                | types::TopologyConnectionKind::RpcPendingAssignment
        )
    }) {
        assert_eq!(connection.scope.route_family, None);
        assert_eq!(connection.scope.realm.as_deref(), Some("acme"));
    }
    for lane in topology
        .lanes
        .iter()
        .filter(|lane| matches!(lane.id.as_str(), "notice" | "rpc"))
    {
        assert_eq!(lane.top_scoped_resources.len(), 1);
        assert_eq!(lane.top_scoped_resources[0].scope.route_family, None);
    }
}

#[test]
fn should_keep_route_families_distinct_in_all_family_mcp_topology() {
    // Arrange
    let runtime = runtime_with_identical_routes_in_two_families();

    // Act
    let topology = mcp_topology_value(&runtime, None);

    // Assert
    for lane in topology["lanes"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|lane| matches!(lane["id"].as_str(), Some("notice" | "rpc")))
    {
        let resources = lane["top_scoped_resources"].as_array().unwrap();
        assert_eq!(resources.len(), 2);
        assert_eq!(resources[0]["scope"]["route_family"], 1);
        assert_eq!(resources[1]["scope"]["route_family"], 2);
        assert_ne!(resources[0]["id"], resources[1]["id"]);
    }
}

#[test]
fn should_keep_selected_family_notice_and_rpc_topology_scopes() {
    // Arrange
    let runtime = runtime_with_identical_routes_in_two_families();

    // Act
    let topology = build_family_topology(&runtime, 1);

    // Assert
    assert_eq!(topology.connections.items.len(), 3);
    assert!(topology
        .connections
        .items
        .iter()
        .all(|connection| connection.scope.route_family == Some(1)));
    for lane in topology
        .lanes
        .iter()
        .filter(|lane| matches!(lane.id.as_str(), "notice" | "rpc"))
    {
        assert_eq!(lane.top_scoped_resources.len(), 1);
        assert_eq!(lane.top_scoped_resources[0].scope.route_family, Some(1));
    }
}

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
