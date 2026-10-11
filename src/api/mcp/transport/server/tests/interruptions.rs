use super::*;

#[tokio::test]
async fn should_keep_unauthorized_queue_targets_in_memory_before_admission() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let mut server = action_server(directory.path(), policy);
    server
        .runtime
        .admin_auth()
        .set_provisioned_route_families(&[1]);
    server.execution_slots = Arc::new(Semaphore::new(0));
    let cases = [
        ("purge", 1, "operations"),
        ("replay", 2, "operations"),
        ("replay", 1, "\0"),
    ];

    // Act
    for (operation, family, realm) in cases {
        for cancelled in [false, true] {
            let cancellation = CancellationToken::new();
            if cancelled {
                cancellation.cancel();
            }
            let result = server
                .execute_action_tool(
                    server.context.clone(),
                    cancellation,
                    PREVIEW_QUEUE_TOOL,
                    Some(serde_json::json!({
                        "operation": operation, "route_family": family, "realm": realm,
                        "area": "jobs", "resource": "dispatch", "message_id": 7
                    })),
                )
                .await;
            assert!(result.is_err());
        }
    }

    // Assert
    assert_eq!(
        std::fs::metadata(directory.path().join("actions.jsonl"))
            .unwrap()
            .len(),
        0
    );
}

fn queue_preview(server: &McpServer) -> serde_json::Value {
    server
        .runtime
        .admin_auth()
        .set_provisioned_route_families(&[1]);
    server
        .runtime
        .admin_read_model()
        .replace_queue_dead_letters(vec![crate::control::admin::QueueDeadLetter {
            message_id: 7,
            family: 1,
            realm: "operations".into(),
            area: "jobs".into(),
            resource: "dispatch".into(),
            dead_lettered_at: "2026-10-10T23:00:00Z".into(),
            attempts: 3,
            reason: "attempts_exhausted".into(),
        }]);
    server
        .actions
        .as_ref()
        .unwrap()
        .preview_queue_dead_letter(
            Some(serde_json::json!({
                "operation": "replay", "route_family": 1, "realm": "operations",
                "area": "jobs", "resource": "dispatch", "message_id": 7
            })),
            &server.context,
            &server.policy,
            server.token_fingerprint,
            server.runtime.as_ref(),
        )
        .unwrap()
}

#[tokio::test]
async fn should_keep_oversized_queue_denials_in_memory_until_target_is_authorized() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let server = action_server(directory.path(), policy);
    let arguments = serde_json::json!({"padding": "x".repeat(MAX_ARGUMENT_BYTES)});

    // Act
    for _ in 0..25 {
        let error = server
            .execute_action_tool(
                server.context.clone(),
                CancellationToken::new(),
                PREVIEW_QUEUE_TOOL,
                Some(arguments.clone()),
            )
            .await
            .unwrap_err();
        assert_eq!(error, "arguments exceed the MCP request limit");
    }

    // Assert
    assert_eq!(
        std::fs::metadata(directory.path().join("actions.jsonl"))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(server.context.audit_records().len(), 25);
}

#[tokio::test]
async fn should_keep_unbound_queue_confirmations_in_memory_when_execution_is_full() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let mut server = action_server(directory.path(), policy);
    server.execution_slots = Arc::new(Semaphore::new(0));
    let arguments = serde_json::json!({
        "challenge_id": uuid::Uuid::new_v4().to_string(),
        "target": "untrusted target",
        "confirmation": "REPLAY"
    });

    // Act
    let error = server
        .execute_action_tool(
            server.context.clone(),
            CancellationToken::new(),
            CONFIRM_QUEUE_TOOL,
            Some(arguments),
        )
        .await
        .unwrap_err();

    // Assert
    assert_eq!(error, "MCP execution capacity is full; retry later");
    assert_eq!(
        std::fs::metadata(directory.path().join("actions.jsonl"))
            .unwrap()
            .len(),
        0
    );
}

#[tokio::test]
async fn should_durably_record_oversized_drain_denial_for_authorized_operator() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy =
        McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate, McpCapabilityClass::Admin]);
    let server = action_server(directory.path(), policy);
    let arguments = serde_json::json!({"padding": "x".repeat(MAX_ARGUMENT_BYTES)});

    // Act
    let error = server
        .execute_action_tool(
            server.context.clone(),
            CancellationToken::new(),
            PREVIEW_DRAIN_TOOL,
            Some(arguments),
        )
        .await
        .unwrap_err();

    // Assert
    assert_eq!(error, "arguments exceed the MCP request limit");
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&audit).unwrap()["phase"],
        "arguments_over_limit"
    );
}

#[tokio::test]
async fn should_durably_record_queue_admission_denial_after_target_authorization() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let mut server = action_server(directory.path(), policy);
    server
        .runtime
        .admin_auth()
        .set_provisioned_route_families(&[1]);
    server.execution_slots = Arc::new(Semaphore::new(0));
    let arguments = serde_json::json!({
        "operation": "replay", "route_family": 1, "realm": "operations",
        "area": "jobs", "resource": "dispatch", "message_id": 7
    });

    // Act
    let error = server
        .execute_action_tool(
            server.context.clone(),
            CancellationToken::new(),
            PREVIEW_QUEUE_TOOL,
            Some(arguments),
        )
        .await
        .unwrap_err();

    // Assert
    assert_eq!(error, "MCP execution capacity is full; retry later");
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&audit).unwrap()["phase"],
        "request_not_admitted"
    );
}

#[test]
fn should_preserve_queue_interruption_audit_authority_after_consuming_preview() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let server = action_server(directory.path(), policy);
    let preview = queue_preview(&server);
    let arguments = serde_json::json!({
        "challenge_id": preview["challenge_id"], "target": preview["target"],
        "confirmation": preview["confirmation"]
    });
    let actions = server.actions.as_ref().unwrap();
    let authorized = actions.interruption_authorized(
        CONFIRM_QUEUE_TOOL,
        Some(&arguments),
        &server.context,
        &server.policy,
        server.token_fingerprint,
        server.runtime.as_ref(),
    );

    // Act
    let _ticket = actions
        .consume_confirmation(
            CONFIRM_QUEUE_TOOL,
            Some(arguments.clone()),
            &server.context,
            &server.policy,
            server.token_fingerprint,
            server.runtime.as_ref(),
        )
        .unwrap();
    let still_bound = actions.interruption_authorized(
        CONFIRM_QUEUE_TOOL,
        Some(&arguments),
        &server.context,
        &server.policy,
        server.token_fingerprint,
        server.runtime.as_ref(),
    );
    actions
        .record_request_interruption(
            CONFIRM_QUEUE_TOOL,
            preview["challenge_id"].as_str(),
            &server.context,
            server.token_fingerprint,
            "indeterminate_do_not_retry",
            authorized,
        )
        .unwrap();

    // Assert
    assert!(authorized);
    assert!(!still_bound);
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    let last: serde_json::Value = serde_json::from_str(audit.lines().last().unwrap()).unwrap();
    assert_eq!(last["phase"], "indeterminate_do_not_retry");
    assert_eq!(last["operation_id"], preview["challenge_id"]);
}

#[test]
fn should_keep_foreign_queue_preview_interruptions_in_memory() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let server = action_server(directory.path(), policy);
    let preview = queue_preview(&server);
    let arguments = serde_json::json!({
        "challenge_id": preview["challenge_id"], "target": preview["target"],
        "confirmation": preview["confirmation"]
    });
    let actions = server.actions.as_ref().unwrap();
    let before = std::fs::metadata(directory.path().join("actions.jsonl"))
        .unwrap()
        .len();

    // Act
    let authorized = actions.interruption_authorized(
        CONFIRM_QUEUE_TOOL,
        Some(&arguments),
        &server.context,
        &server.policy,
        [8; 32],
        server.runtime.as_ref(),
    );
    actions
        .record_request_interruption(
            CONFIRM_QUEUE_TOOL,
            preview["challenge_id"].as_str(),
            &server.context,
            [8; 32],
            "request_not_admitted",
            authorized,
        )
        .unwrap();

    // Assert
    assert!(!authorized);
    assert_eq!(
        std::fs::metadata(directory.path().join("actions.jsonl"))
            .unwrap()
            .len(),
        before
    );
}
