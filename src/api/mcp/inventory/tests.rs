use super::*;
use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::auth::Permission;
use crate::control::admin::{read_model::AdminReadModel, RpcWorker};
use crate::runtime::Router;
use crate::session::SessionPermissions;
use serde_json::json;
use std::sync::Arc;

fn runtime() -> Runtime {
    let model = AdminReadModel::new();
    model.replace_rpc_workers(
        ["alpha", "beta", "gamma"]
            .into_iter()
            .enumerate()
            .map(|(index, resource)| {
                RpcWorker::snapshot(
                    1,
                    index as u64,
                    "acme",
                    &format!("rpc://acme/jobs/{resource}/run"),
                    "2026-10-04T00:00:00Z",
                    0,
                    0.0,
                )
            })
            .collect(),
    );
    Runtime::with_admin_read_model(Arc::new(Router::new()), model)
}

fn context(username: &str, permission: &str) -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: username.into(),
            route_family_access: AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
        },
        SessionPermissions::from_permissions(vec![Permission::parse(permission).unwrap()]),
    )
}

fn page(
    runtime: &Runtime,
    context: &McpExecutionContext,
    arguments: &Value,
) -> McpToolResult<Value> {
    McpToolRegistry::read_only().execute(
        "list_resource_inventory",
        runtime,
        context,
        &super::super::McpCapabilityPolicy::read_only(),
        Some(arguments),
    )
}

fn query() -> Value {
    json!({"route_family":1,"scheme":"rpc","realm":"acme","area":"jobs","limit":1})
}

#[test]
fn should_bind_cursor_to_principal_and_scope() {
    // Arrange
    let runtime = runtime();
    let owner = context("owner", "rpc://acme/jobs/**#read");
    let foreign = context("foreign", "rpc://acme/jobs/**#read");
    let first = page(&runtime, &owner, &query()).unwrap();
    let cursor = first["next_cursor"].as_str().unwrap();
    let mut same_query = query();
    same_query["cursor"] = json!(cursor);
    let mut changed_query = same_query.clone();
    changed_query["limit"] = json!(2);

    // Act
    let next = page(&runtime, &owner, &same_query).unwrap();
    let foreign_result = page(&runtime, &foreign, &same_query);
    let changed_result = page(&runtime, &owner, &changed_query);

    // Assert
    assert_eq!(cursor.len(), 32);
    assert!(cursor.bytes().all(|byte| byte.is_ascii_hexdigit()));
    assert_eq!(first["items"][0]["resource"], "alpha");
    assert_eq!(next["items"][0]["resource"], "beta");
    assert!(matches!(
        foreign_result,
        Err(McpToolError::InvalidArguments { .. })
    ));
    assert!(matches!(
        changed_result,
        Err(McpToolError::InvalidArguments { .. })
    ));
}

#[test]
fn should_revalidate_current_authority_before_continuing_inventory() {
    // Arrange
    let runtime = runtime();
    let original = context("owner", "rpc://acme/jobs/**#read");
    let first = page(&runtime, &original, &query()).unwrap();
    let revoked = context("owner", "rpc://other/jobs/**#read");
    let mut next_query = query();
    next_query["cursor"] = first["next_cursor"].clone();

    // Act
    let result = page(&runtime, &revoked, &next_query);

    // Assert
    assert!(matches!(result, Err(McpToolError::ScopeDenied { .. })));
}

#[test]
fn should_deny_broad_inventory_before_collecting_protected_names() {
    // Arrange
    let runtime = runtime();
    let context = context("one-resource", "rpc://acme/jobs/alpha/*#read");
    let mut registry = McpToolRegistry::read_only();
    for tool in &mut registry.tools {
        tool.handler = |_, _, _| panic!("unauthorized inventory collection executed");
    }

    // Act
    let result = registry.execute(
        "list_resource_inventory",
        &runtime,
        &context,
        &super::super::McpCapabilityPolicy::read_only(),
        Some(&query()),
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::ScopeDenied { .. })));
}

#[test]
fn should_report_unknown_projection_age_and_partial_evidence() {
    // Arrange
    let runtime = runtime();
    let context = context("reader", "rpc://acme/jobs/**#read");

    // Act
    let result = page(&runtime, &context, &query()).unwrap();

    // Assert
    assert_eq!(result["_meta"]["cached_projection"], true);
    assert!(result["_meta"]["source_updated_at"].is_null());
    assert!(result["_meta"]["observed_at"].is_string());
    assert_eq!(result["_meta"]["partial"], true);
    assert!(result["_meta"]["freshness"]
        .as_str()
        .unwrap()
        .contains("cannot prove absence"));
    assert!(result["_meta"]["unavailable"]
        .as_array()
        .unwrap()
        .contains(&json!("source publication timestamp and age")));
}

#[test]
fn should_allow_exact_resource_inventory_without_revealing_siblings() {
    // Arrange
    let runtime = runtime();
    let context = context("one-resource", "rpc://acme/jobs/alpha/*#read");
    let mut exact = query();
    exact["resource"] = json!("alpha");

    // Act
    let result = page(&runtime, &context, &exact).unwrap();

    // Assert
    assert_eq!(result["items"].as_array().unwrap().len(), 1);
    assert_eq!(result["items"][0]["resource"], "alpha");
    assert_eq!(result["has_more"], false);
    assert!(result["next_cursor"].is_null());
    assert!(!result.to_string().contains("beta"));
}
