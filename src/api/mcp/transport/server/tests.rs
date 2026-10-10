use super::*;

#[test]
fn should_preserve_independent_family_and_realm_in_domain_prompt_scope() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "41",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch",
        "limit": "12"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let scope = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"))
        .expect("valid Queue diagnostic arguments");

    // Assert
    assert_eq!(scope.route_family, Some(41));
    assert_eq!(scope.resource.scheme, "queue");
    assert_eq!(scope.resource.realm, "operations");
    assert_eq!(scope.resource.limit, Some(12));
}

#[test]
fn should_reject_prompt_scope_with_route_family_outside_supported_range() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "4294967296",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let result = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"));

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_unknown_arguments_in_domain_prompt_scope() {
    // Arrange
    let arguments = serde_json::json!({
        "route_family": "41",
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch",
        "scheme": "queue"
    })
    .as_object()
    .unwrap()
    .clone();

    // Act
    let result = parse_resource_prompt_arguments(&arguments, PromptKind::Domain("queue"));

    // Assert
    assert!(result.is_err());
}

fn action_server(audit_directory: &std::path::Path, policy: McpCapabilityPolicy) -> McpServer {
    McpServer {
        runtime: Arc::new(Runtime::with_admin_read_model(
            Arc::new(crate::runtime::Router::new()),
            crate::control::admin::read_model::AdminReadModel::new(),
        )),
        registry: McpToolRegistry::read_only(),
        context: McpExecutionContext::authenticated(
            crate::api::admin::auth::AdminPrincipal {
                username: "operator-a".to_string(),
                route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::wildcard(),
            },
            crate::session::permissions::SessionPermissions::all(),
        ),
        policy,
        token_fingerprint: [9; 32],
        actions: Some(Arc::new(
            McpActionState::open(
                "fitz-production-east",
                audit_directory.join("actions.jsonl"),
            )
            .expect("open action audit sink"),
        )),
        execution_slots: Arc::new(Semaphore::new(MAX_EXECUTIONS)),
    }
}

#[tokio::test]
async fn should_not_durably_audit_action_calls_from_principal_without_action_capability() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let server = action_server(directory.path(), McpCapabilityPolicy::read_only());
    let oversized = serde_json::json!({ "padding": "x".repeat(MAX_ARGUMENT_BYTES) });
    let calls = [
        (PREVIEW_QUEUE_TOOL, None),
        (CONFIRM_QUEUE_TOOL, None),
        (PREVIEW_DRAIN_TOOL, None),
        (CONFIRM_DRAIN_TOOL, None),
        (PREVIEW_QUEUE_TOOL, Some(oversized)),
    ];

    // Act
    let mut errors = Vec::new();
    for (tool, arguments) in calls {
        let result = server
            .execute_action_tool(
                server.context.clone(),
                CancellationToken::new(),
                tool,
                arguments,
            )
            .await;
        errors.push(result.expect_err("inspect-only action call must be denied"));
    }

    // Assert
    let audit_bytes = std::fs::metadata(directory.path().join("actions.jsonl"))
        .expect("audit file")
        .len();
    assert_eq!(audit_bytes, 0);
    assert!(errors
        .iter()
        .all(|error| error == "MCP action request was denied"));
    let counted_denials = server
        .context
        .audit_records()
        .into_iter()
        .filter(|record| record.decision == crate::api::mcp::McpAuditDecision::Denied)
        .count();
    assert_eq!(counted_denials, 5);
}

#[tokio::test]
async fn should_durably_audit_denied_action_from_principal_with_action_capability() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let server = action_server(directory.path(), policy);

    // Act
    let result = server
        .execute_action_tool(
            server.context.clone(),
            CancellationToken::new(),
            PREVIEW_QUEUE_TOOL,
            None,
        )
        .await;

    // Assert
    assert_eq!(result.unwrap_err(), "MCP action request was denied");
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl"))
        .expect("durable action audit");
    let records = audit
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["principal"], "operator-a");
    assert_eq!(records[0]["phase"], "missing_arguments");
}
