use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::auth::Permission;
use crate::control::admin::{read_model::AdminReadModel, StreamInfo};
use crate::runtime::Router;
use crate::session::SessionPermissions;
use serde_json::json;
use std::sync::Arc;

fn runtime() -> Runtime {
    let model = AdminReadModel::new();
    model.replace_streams(
        [1, 2]
            .map(|family| StreamInfo {
                route_family: family,
                realm: "acme".into(),
                area: "jobs".into(),
                resource: "orders".into(),
                committed_event_count: family * 10,
                offset: family * 100,
                watermark: family * 90,
                size_bytes: family * 1_000,
                sessions_active: 0,
                subscriptions_active: 0,
            })
            .to_vec(),
    );
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), model);
    runtime.admin_auth().set_provisioned_route_families(&[1, 2]);
    runtime
}

fn context(access: AdminRouteFamilyAccess) -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "stream-observer".into(),
            route_family_access: access,
        },
        SessionPermissions::from_permissions(vec![Permission::parse(
            "stream://acme/jobs/orders#read",
        )
        .unwrap()]),
    )
}

fn inspect(
    tool: &str,
    family: Option<u64>,
    access: AdminRouteFamilyAccess,
) -> McpToolResult<Value> {
    McpToolRegistry::read_only().execute(
        tool,
        &runtime(),
        &context(access),
        &McpCapabilityPolicy::read_only(),
        Some(&json!({
            "scheme": "stream", "realm": "acme", "area": "jobs",
            "resource": "orders", "route_family": family
        })),
    )
}

#[test]
fn should_reject_ambiguous_all_family_stream_detail() {
    // Arrange
    let access = AdminRouteFamilyAccess::wildcard();

    // Act
    let result = inspect("inspect_resource_detail", None, access);

    // Assert
    assert!(
        matches!(result, Err(McpToolError::InvalidArguments { reason, .. })
        if reason.contains("explicit route_family"))
    );
}

#[test]
fn should_reject_ambiguous_all_family_stream_timeline() {
    // Arrange
    let access = AdminRouteFamilyAccess::wildcard();

    // Act
    let result = inspect("inspect_resource_timeline", None, access);

    // Assert
    assert!(
        matches!(result, Err(McpToolError::InvalidArguments { reason, .. })
        if reason.contains("explicit route_family"))
    );
}

#[test]
fn should_return_only_the_authorized_family_stream_detail() {
    // Arrange
    let access = AdminRouteFamilyAccess::Explicit(vec!["2".into()]);

    // Act
    let result = inspect("inspect_resource_detail", Some(2), access).unwrap();

    // Assert
    assert_eq!(result["offset"], 200);
    assert_eq!(result["watermark"], 180);
}

#[test]
fn should_return_only_the_authorized_family_stream_timeline() {
    // Arrange
    let access = AdminRouteFamilyAccess::Explicit(vec!["2".into()]);

    // Act
    let result = inspect("inspect_resource_timeline", Some(2), access).unwrap();

    // Assert
    assert_eq!(result["family"], 2);
    assert!(result["events"].as_array().unwrap().iter().all(|event| {
        event["family"] == 2 && event["summary"].as_str().unwrap().contains("Offset 200")
    }));
}
