use super::*;
use crate::api::admin::auth::AdminPrincipal;
use crate::api::admin::{kv_detail, ResourcePath};
use crate::auth::default_anonymous_permissions;
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::runtime::Router;
use crate::session::permissions::SessionPermissions;
use std::sync::Arc;

fn authenticated_context(permissions: SessionPermissions) -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "admin".to_string(),
            route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::wildcard(),
        },
        permissions,
    )
}

fn anonymous_context(permissions: SessionPermissions) -> McpExecutionContext {
    McpExecutionContext::anonymous(permissions)
}

fn assert_troubleshooting_output(output: &serde_json::Value) {
    let summary = &output["incident_summary"];
    assert!(summary.is_object());
    assert!(matches!(
        summary["status"].as_str(),
        Some("healthy" | "degraded" | "stalled" | "recovering" | "unknown")
    ));
    assert!(matches!(
        summary["severity"].as_str(),
        Some("informational" | "low" | "medium" | "high" | "critical")
    ));
    assert!(summary["title"].is_string());
    assert!(summary["explanation"].is_string());
    assert!(summary["confidence"]
        .as_f64()
        .is_some_and(|confidence| (0.0..=1.0).contains(&confidence)));
    assert!(output["hotspots"].is_array());
    assert!(output["top_bottleneck"].is_null() || output["top_bottleneck"].is_object());
    assert!(
        output["last_significant_transition_at"].is_null()
            || output["last_significant_transition_at"].is_string()
    );
}

#[test]
fn should_register_summary_tools_given_default_registry() {
    // Arrange
    let registry = McpToolRegistry::summary_only();

    // Act
    let descriptors = registry.tool_descriptors();

    // Assert
    assert_eq!(descriptors.len(), 2);
    assert_eq!(descriptors[0].name, "get_global_stats");
    assert_eq!(descriptors[0].capability, McpCapabilityClass::Summary);
    assert_eq!(descriptors[0].rest_path, "/api/v1/stats");
    assert_eq!(descriptors[1].name, "get_global_troubleshooting");
    assert_eq!(descriptors[1].capability, McpCapabilityClass::Summary);
    assert_eq!(descriptors[1].rest_path, "/api/v1/troubleshooting");
}

#[test]
fn should_register_inspect_tools_given_read_only_registry() {
    // Arrange
    let registry = McpToolRegistry::read_only();

    // Act
    let descriptors = registry.tool_descriptors();

    // Assert
    assert_eq!(descriptors.len(), 10);
    let by_name = descriptors
        .into_iter()
        .map(|descriptor| (descriptor.name.clone(), descriptor))
        .collect::<std::collections::HashMap<_, _>>();
    assert_eq!(
        by_name["inspect_resource_detail"].capability,
        McpCapabilityClass::Inspect
    );
    assert_eq!(
        by_name["inspect_resource_detail"].budget,
        McpCostBudget::inspect()
    );
    assert_eq!(
        by_name["inspect_resource_timeline"].capability,
        McpCapabilityClass::Inspect
    );
    assert_eq!(
        by_name["inspect_resource_timeline"].budget,
        McpCostBudget::timeline()
    );
    assert_eq!(
        by_name["explain_global_troubleshooting"].capability,
        McpCapabilityClass::Explain
    );
    assert_eq!(
        by_name["explain_global_troubleshooting"].budget,
        McpCostBudget::summary()
    );
}

#[test]
fn should_allow_inspect_tools_given_read_only_policy() {
    // Arrange
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let summary_allowed = policy.allows(McpCapabilityClass::Summary);
    let inspect_allowed = policy.allows(McpCapabilityClass::Inspect);
    let explain_allowed = policy.allows(McpCapabilityClass::Explain);
    let mutate_allowed = policy.allows(McpCapabilityClass::Mutate);

    // Assert
    assert!(summary_allowed);
    assert!(inspect_allowed);
    assert!(explain_allowed);
    assert!(!mutate_allowed);
}

#[test]
fn should_execute_summary_tools_given_empty_runtime() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::summary_only();
    let context = authenticated_context(default_anonymous_permissions());
    let policy = McpCapabilityPolicy::summary_only();

    // Act
    let stats_output = registry
        .execute("get_global_stats", &runtime, &context, &policy, None)
        .expect("global stats output");
    let troubleshooting_output = registry
        .execute(
            "get_global_troubleshooting",
            &runtime,
            &context,
            &policy,
            None,
        )
        .expect("global troubleshooting output");

    // Assert
    assert_eq!(stats_output["broker"]["connections"], 0);
    assert!(stats_output["domains"].is_object());
    assert!(stats_output["diagnostics"].is_object());
    assert_troubleshooting_output(&troubleshooting_output);
}

#[test]
fn should_execute_resource_detail_given_kv_scope() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::read_only();
    let context = authenticated_context(default_anonymous_permissions());
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "kv",
        "realm": "acme",
        "area": "app",
        "resource": "users"
    });
    let expected = serde_json::to_value(kv_detail(
        &runtime,
        &ResourcePath {
            realm: "acme",
            area: "app",
            resource: "users",
        },
        None,
    ))
    .expect("expected kv detail");

    // Act
    let output = registry
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context,
            &policy,
            Some(&arguments),
        )
        .expect("resource detail output");

    // Assert
    assert!(output["_meta"]["evidence_id"].is_string());
    let mut facts = output.clone();
    facts.as_object_mut().unwrap().remove("_meta");
    assert_eq!(facts, expected);
}

#[test]
fn should_execute_resource_timeline_given_kv_scope() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::read_only();
    let context = authenticated_context(default_anonymous_permissions());
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "kv",
        "realm": "acme",
        "area": "app",
        "resource": "users",
        "limit": 3
    });
    let read_model = runtime.admin_read_model();
    let expected = serde_json::to_value(kv_resource_timeline(
        &read_model.kv_transactions(None),
        &ResourcePath {
            realm: "acme",
            area: "app",
            resource: "users",
        },
        3,
    ))
    .expect("expected kv timeline");

    // Act
    let output = registry
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &context,
            &policy,
            Some(&arguments),
        )
        .expect("resource timeline output");

    // Assert
    assert!(output["_meta"]["evidence_id"].is_string());
    let mut facts = output.clone();
    facts.as_object_mut().unwrap().remove("_meta");
    assert_eq!(facts, expected);
    let audit_records = context.audit_records();
    assert_eq!(audit_records.len(), 1);
    assert_eq!(audit_records[0].decision, McpAuditDecision::Allowed);
    assert_eq!(audit_records[0].tool_name, "inspect_resource_timeline");
    assert!(audit_records[0].scope_route.is_none());
    assert_eq!(audit_records[0].result_summary, "scope_denied");
}

#[test]
fn should_require_authentication_given_summary_tool() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::summary_only();
    let context = anonymous_context(default_anonymous_permissions());
    let policy = McpCapabilityPolicy::summary_only();

    // Act
    let result = registry.execute("get_global_stats", &runtime, &context, &policy, None);

    // Assert
    assert!(matches!(
        result,
        Err(McpToolError::AuthenticationRequired { .. })
    ));
    let audit_records = context.audit_records();
    assert_eq!(audit_records.len(), 1);
    assert_eq!(audit_records[0].decision, McpAuditDecision::Denied);
}

#[test]
fn should_deny_resource_detail_given_missing_scope() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::read_only();
    let context = authenticated_context(SessionPermissions::empty());
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "kv",
        "realm": "acme",
        "area": "app",
        "resource": "users"
    });

    // Act
    let result = registry.execute(
        "inspect_resource_detail",
        &runtime,
        &context,
        &policy,
        Some(&arguments),
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::ScopeDenied { .. })));
    let audit_records = context.audit_records();
    assert_eq!(audit_records.len(), 1);
    assert_eq!(audit_records[0].decision, McpAuditDecision::Denied);
    assert_eq!(
        audit_records[0].scope_route.as_deref(),
        Some("kv://acme/app/users")
    );
}

#[test]
fn should_exclude_unauthorized_family_from_resource_detail() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.upsert_lease(crate::control::admin::LeaseInfo::snapshot(
        2,
        "acme",
        "locks",
        "billing",
        "family-2-owner",
        "2026-10-01T12:00:00Z",
        "2026-10-01T12:05:00Z".to_string(),
        0,
        7,
    ));
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let registry = McpToolRegistry::read_only();
    let context = McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "family-1-admin".to_string(),
            route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::Explicit(vec![
                "1".to_string(),
            ]),
        },
        SessionPermissions::from_permissions(vec![crate::auth::Permission {
            raw: "lease://acme/locks/billing".to_string(),
            access: Access::Read,
        }]),
    );
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({
        "scheme": "lease",
        "realm": "acme",
        "area": "locks",
        "resource": "billing"
    });

    // Act
    let result = registry.execute(
        "inspect_resource_detail",
        &runtime,
        &context,
        &policy,
        Some(&arguments),
    );

    // Assert
    if let Ok(output) = result {
        assert_eq!(
            output["active_leases"], 0,
            "a family-1 principal must not observe a lease owned by family 2"
        );
    }
}

#[test]
fn should_execute_explain_tools_given_empty_runtime() {
    // Arrange
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let registry = McpToolRegistry::read_only();
    let context = authenticated_context(default_anonymous_permissions());
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let output = registry
        .execute(
            "explain_global_troubleshooting",
            &runtime,
            &context,
            &policy,
            None,
        )
        .expect("explanation output");

    // Assert
    assert_troubleshooting_output(&output);
}
