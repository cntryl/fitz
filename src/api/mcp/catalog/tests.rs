use super::*;
use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::api::mcp::{McpCapabilityPolicy, McpExecutionContext};
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::runtime::Router;
use crate::session::SessionPermissions;
use std::sync::Arc;

fn context() -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "catalog-test".to_string(),
            route_family_access: AdminRouteFamilyAccess::wildcard(),
        },
        SessionPermissions::all(),
    )
}

#[test]
fn should_pin_distinct_sdk_lifecycles_for_supported_revisions() {
    // Arrange
    let primary = primary_revision();
    let compatibility = compatibility_revision();
    let other: ProtocolVersion = serde_json::from_value(json!("2025-06-18")).unwrap();

    // Act
    let supported = [primary.clone(), compatibility.clone(), other]
        .map(|revision| supports_revision(&revision));

    // Assert
    assert_eq!(supported, [true, true, false]);
    assert!(!primary.has_initialize());
    assert!(compatibility.has_initialize());
}

#[test]
fn should_preserve_read_tool_names_and_paginated_inventory_in_stable_protocol_catalog_order() {
    // Arrange
    let registry = McpToolRegistry::read_only();

    // Act
    let tools = registry.protocol_tools();
    let names: Vec<_> = tools.iter().map(|tool| tool.name.as_ref()).collect();

    // Assert
    assert_eq!(
        names,
        [
            "explain_global_troubleshooting",
            "get_global_stats",
            "get_global_troubleshooting",
            "get_mcp_discovery",
            "get_sessions",
            "get_structured_metrics",
            "get_topology",
            "inspect_resource_detail",
            "inspect_resource_timeline",
            "list_resource_inventory",
        ]
    );
    assert!(tools
        .iter()
        .all(|tool| tool.annotations.as_ref().unwrap().read_only_hint == Some(true)));
    assert!(tools
        .iter()
        .all(
            |tool| tool.meta.as_ref().unwrap().0["fitz.enforcement"]["hardRuntimeDeadline"]
                == false
        ));
}

#[test]
fn should_validate_registry_outputs_against_catalog_schemas_for_all_seven_domains() {
    // Arrange
    let registry = McpToolRegistry::read_only();
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new());
    let context = context();
    let policy = McpCapabilityPolicy::read_only();
    let tools = registry.protocol_tools();

    // Act
    for tool in tools {
        let schema = serde_json::to_value(tool.output_schema.as_ref().unwrap()).unwrap();
        let validator = jsonschema::validator_for(&schema).unwrap();
        let schemes = if tool.name.starts_with("inspect_resource_") {
            vec![
                "kv", "queue", "stream", "lease", "schedule", "notice", "rpc",
            ]
        } else {
            vec!["global"]
        };
        for scheme in schemes {
            let arguments = match scheme {
                "global" => None,
                _ => Some(json!({
                    "scheme":scheme,
                    "realm":"app-namespace",
                    "area":"jobs",
                    "resource":"run",
                })),
            };
            let result = registry
                .execute(&tool.name, &runtime, &context, &policy, arguments.as_ref())
                .unwrap();

            // Assert
            assert!(
                validator.is_valid(&result),
                "{} ({scheme}): {:?}",
                tool.name,
                validator.iter_errors(&result).collect::<Vec<_>>()
            );
            assert!(
                !validator.is_valid(&json!({})),
                "empty output must violate required fields"
            );
        }
    }
}
