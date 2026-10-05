use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::auth::Permission;
use crate::control::admin::{
    read_model::AdminReadModel, NoticeSubscription, RpcWorker, ScheduleInfo,
};
use crate::runtime::Router;
use crate::session::SessionPermissions;
use std::sync::Arc;

fn context(permission: &str) -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "one-resource".into(),
            route_family_access: AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
        },
        SessionPermissions::from_permissions(vec![Permission::parse(permission).unwrap()]),
    )
}

fn assert_no_unattributed_counter(result: &Value) {
    let mut facts = result.clone();
    facts.as_object_mut().unwrap().remove("_meta");
    assert!(!facts.to_string().contains("9999"));
}

#[test]
fn should_identify_unknown_global_stat_fields_and_conservative_health() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let context = McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "admin".into(),
            route_family_access: AdminRouteFamilyAccess::wildcard(),
        },
        crate::auth::default_anonymous_permissions(),
    );
    let registry = McpToolRegistry::summary_only();
    let policy = McpCapabilityPolicy::summary_only();

    // Act
    let stats = registry
        .execute("get_global_stats", &runtime, &context, &policy, None)
        .unwrap();
    let diagnosis = registry
        .execute(
            "get_global_troubleshooting",
            &runtime,
            &context,
            &policy,
            None,
        )
        .unwrap();

    // Assert
    let pointers = stats["_meta"]["unavailable_fields"].as_array().unwrap();
    assert_eq!(pointers.len(), 10);
    assert!(pointers
        .iter()
        .all(|pointer| stats.pointer(pointer.as_str().unwrap()).is_some()));
    assert_eq!(diagnosis["incident_summary"]["status"], "unknown");
}

#[test]
fn should_include_wildcard_notice_registrations_in_resource_details() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_notice_subscriptions(vec![NoticeSubscription {
        route_family: 1,
        subscription_id: 1,
        session_id: "worker".into(),
        realm: "acme".into(),
        pattern: "notice://acme/jobs/*".into(),
        created_at: "2026-10-04T00:00:00Z".into(),
        notifications_received: 0,
    }]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"notice","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context("notice://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_eq!(result["subscriptions_active"], 1);
}

#[test]
fn should_not_attribute_wildcard_notice_delivery_counts_to_one_resource() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_notice_subscriptions(vec![NoticeSubscription {
        route_family: 1,
        subscription_id: 1,
        session_id: "worker".into(),
        realm: "acme".into(),
        pattern: "notice://acme/jobs/*".into(),
        created_at: "2026-10-04T00:00:00Z".into(),
        notifications_received: 9999,
    }]);
    model.replace_notice_routes(vec![crate::control::admin::NoticeRouteInfo {
        route_family: 1,
        route: "notice://acme/jobs/*".into(),
        subscribers: 1,
        publishes_total: 9999,
        publishes_per_minute: 9999.0,
    }]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"notice","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &context("notice://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_ne!(result["events"].as_array().unwrap().len(), 0);
    assert_no_unattributed_counter(&result);
}

#[test]
fn should_count_wildcard_rpc_workers_without_inventing_operation_names() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_rpc_workers(vec![
        RpcWorker::snapshot(
            1,
            1,
            "acme",
            "rpc://acme/jobs/*/run",
            "2026-10-04T00:00:00Z",
            0,
            0.0,
        ),
        RpcWorker::snapshot(
            1,
            2,
            "acme",
            "rpc://acme/**",
            "2026-10-04T00:00:00Z",
            0,
            0.0,
        ),
    ]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"rpc","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context("rpc://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_eq!(result["workers_registered"], 2);
    assert_eq!(result["operations"].as_array().unwrap().len(), 1);
    assert_eq!(result["operations"][0]["operation"], "run");
    assert_eq!(result["operations"][0]["workers_registered"], 2);
    assert_eq!(
        result["operations"][0]["requests_handled_by_live_workers"],
        Value::Null
    );
}

#[test]
fn should_include_wildcard_rpc_timeline_without_sibling_aggregate_counts() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_rpc_workers(vec![RpcWorker::snapshot(
        1,
        1,
        "acme",
        "rpc://acme/jobs/*/run",
        "2026-10-04T00:00:00Z",
        9999,
        9999.0,
    )]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"rpc","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &context("rpc://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_ne!(result["events"].as_array().unwrap().len(), 0);
    assert_no_unattributed_counter(&result);
}

#[test]
fn should_match_rpc_operation_after_nonterminal_multi_segment_wildcard() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_rpc_workers(vec![RpcWorker::snapshot(
        1,
        1,
        "acme",
        "rpc://acme/**/run",
        "2026-10-04T00:00:00Z",
        0,
        0.0,
    )]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"rpc","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context("rpc://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_eq!(result["workers_registered"], 1);
}

#[test]
fn should_select_resource_before_copying_sibling_domains_and_resource_rows() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_notice_subscriptions(
        (0..700)
            .map(|id| NoticeSubscription {
                route_family: 1,
                subscription_id: id,
                session_id: "private".into(),
                realm: "private".into(),
                pattern: format!("notice://private/jobs/topic-{id}"),
                created_at: "2026-10-04T00:00:00Z".into(),
                notifications_received: 0,
            })
            .collect(),
    );
    model.replace_rpc_workers(
        (0..700)
            .map(|id| {
                RpcWorker::snapshot(
                    1,
                    id,
                    "acme",
                    &format!("rpc://acme/jobs/private-{id}/run"),
                    "2026-10-04T00:00:00Z",
                    0,
                    0.0,
                )
            })
            .chain(std::iter::once(RpcWorker::snapshot(
                1,
                9999,
                "acme",
                "rpc://acme/jobs/alpha/run",
                "2026-10-04T00:00:00Z",
                0,
                0.0,
            )))
            .collect(),
    );
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"rpc","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context("rpc://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_eq!(result["workers_registered"], 1);
    assert_eq!(result["_meta"]["partial"], false);
    assert!(!result.to_string().contains("private"));
}

#[test]
fn should_not_attribute_family_schedule_pressure_to_one_authorized_resource() {
    // Arrange
    let model = AdminReadModel::new();
    model.replace_schedules(vec![ScheduleInfo::enabled_snapshot(
        1,
        "acme".into(),
        "jobs".into(),
        "alpha".into(),
        "run".into(),
        "* * * * *".into(),
        "2099-01-01T00:00:00Z",
    )]);
    model.set_schedule_family_pending_fire_count(1, 9999);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    let arguments = serde_json::json!({"scheme":"schedule","route_family":1,"realm":"acme","area":"jobs","resource":"alpha"});

    // Act
    let result = McpToolRegistry::read_only()
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &context("schedule://acme/jobs/alpha#read"),
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
        .unwrap();

    // Assert
    assert_no_unattributed_counter(&result);
    assert!(result["_meta"]["unavailable"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item
            .as_str()
            .is_some_and(|item| item.contains("Schedule pressure"))));
}

#[test]
fn should_select_authorized_provisioned_families_before_discovery_limit() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    runtime.configure_route_families(&(1..=10_000).collect::<Vec<_>>());
    let access = AdminRouteFamilyAccess::Explicit(vec!["9999".into()]);

    // Act
    let (selected, truncated) = runtime
        .admin_auth()
        .bounded_provisioned_route_families(256, Some(&access));
    let (global, global_truncated) = runtime
        .admin_auth()
        .bounded_provisioned_route_families(64, None);
    let discovery = McpToolRegistry::read_only()
        .execute(
            "get_mcp_discovery",
            &runtime,
            &McpExecutionContext::authenticated(
                AdminPrincipal {
                    username: "one-family".into(),
                    route_family_access: access,
                },
                crate::auth::default_anonymous_permissions(),
            ),
            &McpCapabilityPolicy::read_only(),
            None,
        )
        .unwrap();

    // Assert
    assert_eq!(selected, vec![9999]);
    assert!(!truncated);
    assert_eq!(global.len(), 64);
    assert!(global_truncated);
    assert_eq!(discovery["route_families"], serde_json::json!(["9999"]));
    assert!(discovery["broker"]["ready"].is_boolean());
    assert!(discovery["_meta"]["unavailable"]
        .to_string()
        .contains("readiness"));
}
