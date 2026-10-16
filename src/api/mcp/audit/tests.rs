use super::*;
use crate::api::mcp::{McpCapabilityPolicy, McpToolError, McpToolRegistry};
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::runtime::Router;

fn context() -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "operator".to_string(),
            route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::wildcard(),
        },
        SessionPermissions::all(),
    )
}

fn record(name: String) -> McpAuditRecord {
    McpAuditRecord {
        correlation_id: None,
        principal: Some("operator".to_string()),
        tool_name: name,
        capability: McpCapabilityClass::Summary,
        scope_route: None,
        argument_summary: "absent".to_string(),
        decision: McpAuditDecision::Denied,
        result_summary: "denied".to_string(),
        duration_ms: 0,
    }
}

#[test]
fn should_bound_audit_retention_across_cloned_contexts() {
    // Arrange
    let context = context();
    let clone = context.clone();
    for index in 0..AUDIT_RECORD_LIMIT {
        context.record_audit(record(index.to_string()));
    }

    // Act
    clone.record_audit(record("newest".to_string()));

    // Assert
    let records = context.audit_records();
    assert_eq!(records.len(), AUDIT_RECORD_LIMIT);
    assert_eq!(records.first().unwrap().tool_name, "1");
    assert_eq!(records.last().unwrap().tool_name, "newest");
    assert_eq!(context.dropped_audit_records(), 1);
    assert_eq!(clone.dropped_audit_records(), 1);
}

#[test]
fn should_bound_multibyte_audit_fields_without_log_control_characters() {
    // Arrange
    let context = context();
    let name = format!("\n{}", "é".repeat(AUDIT_FIELD_BYTES));

    // Act
    context.record_audit(record(name));

    // Assert
    let records = context.audit_records();
    let name = &records[0].tool_name;
    assert!(name.len() <= AUDIT_FIELD_BYTES);
    assert!(!name.chars().any(char::is_control));
    assert!(name.ends_with('é'));
}

#[test]
fn should_audit_unknown_tools_without_retaining_untrusted_names_or_arguments() {
    // Arrange
    let context = context();
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let arguments = serde_json::json!({ "token": "private-secret" });

    // Act
    let result = McpToolRegistry::read_only().execute(
        "private-tool-secret",
        &runtime,
        &context,
        &McpCapabilityPolicy::read_only(),
        Some(&arguments),
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::UnknownTool { .. })));
    let records = context.audit_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].result_summary, "unknown_tool");
    assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
}

#[test]
fn should_audit_invalid_arguments_without_retaining_credentials() {
    // Arrange
    let context = context();
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let arguments = serde_json::json!({ "token": "private-secret" });

    // Act
    let result = McpToolRegistry::read_only().execute(
        "inspect_resource_detail",
        &runtime,
        &context,
        &McpCapabilityPolicy::read_only(),
        Some(&arguments),
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::InvalidArguments { .. })));
    let records = context.audit_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].result_summary, "invalid_arguments");
    assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
}

#[test]
fn should_redact_unused_arguments_on_successful_summary_reads() {
    // Arrange
    let context = context();
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let arguments = serde_json::json!({ "token": "private-secret" });

    // Act
    let result = McpToolRegistry::read_only().execute(
        "get_global_stats",
        &runtime,
        &context,
        &McpCapabilityPolicy::read_only(),
        Some(&arguments),
    );

    // Assert
    assert!(result.is_ok());
    let records = context.audit_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].argument_summary, "provided");
    assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
}

#[test]
fn should_audit_handler_failure_without_retaining_error_details() {
    // Arrange
    let context = context();
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let descriptor = McpToolRegistry::summary_only().tool_descriptors().remove(0);
    let registry = McpToolRegistry {
        tools: vec![crate::api::mcp::McpToolDefinition::new(
            descriptor,
            |_, _, _| {
                Err(McpToolError::Serialization {
                    tool_name: "get_global_stats".to_string(),
                    reason: "private-secret".to_string(),
                })
            },
        )],
    };

    // Act
    let result = registry.execute(
        "get_global_stats",
        &runtime,
        &context,
        &McpCapabilityPolicy::read_only(),
        None,
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::Serialization { .. })));
    let records = context.audit_records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].result_summary, "handler_error");
    assert!(!serde_json::to_string(&records).unwrap().contains("secret"));
}

#[test]
fn should_bound_untrusted_request_identifiers_in_retained_audit() {
    // Arrange
    let context = context().with_correlation_id(format!("\n{}", "é".repeat(32_000)));

    // Act
    context.record_audit(record("test".into()));

    // Assert
    let records = context.audit_records();
    let identifier = records[0].correlation_id.as_ref().unwrap();
    assert!(identifier.len() <= AUDIT_FIELD_BYTES);
    assert!(!identifier.chars().any(char::is_control));
}

#[test]
fn should_not_retain_unpermitted_scope_arguments_in_denial_audit() {
    // Arrange
    let context = McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "reader".into(),
            route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::Explicit(vec![
                "1".into(),
            ]),
        },
        SessionPermissions::empty(),
    );
    let runtime = Runtime::new(Arc::new(Router::new()));
    let arguments = serde_json::json!({"scheme":"rpc","realm":"private-secret","area":"jobs","resource":"orders","route_family":1});

    // Act
    let result = McpToolRegistry::read_only().execute(
        "inspect_resource_detail",
        &runtime,
        &context,
        &McpCapabilityPolicy::read_only(),
        Some(&arguments),
    );

    // Assert
    assert!(matches!(result, Err(McpToolError::ScopeDenied { .. })));
    assert!(!serde_json::to_string(&context.audit_records())
        .unwrap()
        .contains("private-secret"));
    assert!(context.audit_records()[0].scope_route.is_none());
}
