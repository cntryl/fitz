use super::*;

#[test]
fn should_durably_record_interrupted_request_separately_from_later_command_completion() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let ticket = actions
        .consume_confirmation(
            CONFIRM_DRAIN_TOOL,
            Some(confirmation_arguments(&preview)),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();

    // Act
    actions
        .record_request_interruption(
            CONFIRM_DRAIN_TOOL,
            preview["challenge_id"].as_str(),
            &context,
            [9; 32],
            "request_cancelled_indeterminate",
        )
        .unwrap();
    let result = actions
        .execute(ticket, &context, &policy, [9; 32], &runtime)
        .unwrap();

    // Assert
    assert_eq!(result["outcome"], "completed");
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    let records = audit
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records[1]["phase"], "request_cancelled_indeterminate");
    assert_eq!(records[1]["target"], "redacted");
    assert_eq!(records[1]["operation_id"], preview["challenge_id"]);
    assert_eq!(records[3]["phase"], "completed");
}

#[test]
fn should_redact_foreign_pending_challenge_in_request_interruption_audit() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let owner = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let other = context("operator-b", AdminRouteFamilyAccess::wildcard());
    let preview = actions
        .preview_runtime_drain(&owner, &privileged_policy(), [9; 32], &runtime)
        .unwrap();

    // Act
    actions
        .record_request_interruption(
            CONFIRM_DRAIN_TOOL,
            preview["challenge_id"].as_str(),
            &other,
            [8; 32],
            "request_cancelled_indeterminate",
        )
        .unwrap();

    // Assert
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    let latest = serde_json::from_str::<serde_json::Value>(audit.lines().last().unwrap()).unwrap();
    assert_ne!(latest["operation_id"], preview["challenge_id"]);
    assert_eq!(latest["principal"], "operator-b");
    assert_eq!(latest["target"], "redacted");
}

#[test]
fn should_fail_request_interruption_audit_when_mandatory_sink_is_full() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    actions
        .audit
        .file
        .lock()
        .set_len(MAX_AUDIT_FILE_BYTES)
        .unwrap();

    // Act
    let result = actions.record_request_interruption(
        CONFIRM_DRAIN_TOOL,
        Some("untrusted arbitrary argument"),
        &context,
        [9; 32],
        "request_cancelled_indeterminate",
    );

    // Assert
    assert!(result.unwrap_err().contains("could not be durably audited"));
}

#[test]
fn should_fail_closed_when_mandatory_intent_audit_cannot_append() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let arguments = confirmation_arguments(&preview);
    let ticket = actions
        .consume_confirmation(
            CONFIRM_DRAIN_TOOL,
            Some(arguments.clone()),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    actions
        .audit
        .file
        .lock()
        .set_len(MAX_AUDIT_FILE_BYTES)
        .unwrap();

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);
    let retry = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.unwrap_err().contains("command was not started"));
    assert!(retry.is_err());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_report_indeterminate_without_retry_when_outcome_audit_fails() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let arguments = confirmation_arguments(&preview);
    let ticket = actions
        .consume_confirmation(
            CONFIRM_DRAIN_TOOL,
            Some(arguments.clone()),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();
    let intent_bytes = serde_json::to_vec(&serde_json::json!({
        "timestamp_epoch_ms": now,
        "operation_id": ticket.operation_id,
        "principal": ticket.username,
        "action": ticket.action.audit_name(),
        "target": ticket.target,
        "phase": "intent",
    }))
    .unwrap()
    .len()
        + 1;
    actions
        .audit
        .file
        .lock()
        .set_len(MAX_AUDIT_FILE_BYTES - u64::try_from(intent_bytes).unwrap())
        .unwrap();

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);
    let retry = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    let error = result.unwrap_err();
    assert!(error.contains("indeterminate; do not retry"));
    assert!(error.contains(preview["challenge_id"].as_str().unwrap()));
    assert_eq!(runtime.lifecycle_state().as_str(), "draining");
    assert!(retry.is_err());
}

#[test]
fn should_reject_symlink_as_mandatory_action_audit_file() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let target = directory.path().join("target.jsonl");
    std::fs::write(&target, "unchanged").unwrap();
    let link = directory.path().join("linked.jsonl");
    std::os::unix::fs::symlink(&target, &link).unwrap();

    // Act
    let result = McpActionAuditSink::open(link);

    // Assert
    assert!(result.is_err());
    assert_eq!(std::fs::read_to_string(target).unwrap(), "unchanged");
}
