use super::*;

fn dead_letter(family: u64, message_id: u64) -> crate::control::admin::QueueDeadLetter {
    crate::control::admin::QueueDeadLetter {
        message_id,
        family,
        realm: "operations".into(),
        area: "jobs".into(),
        resource: "dispatch".into(),
        dead_lettered_at: "2026-10-04T23:00:00Z".into(),
        attempts: 3,
        reason: "attempts_exhausted".into(),
    }
}

fn queue_arguments(family: u64, operation: &str) -> serde_json::Value {
    serde_json::json!({
        "operation": operation,
        "route_family": family,
        "realm": "operations",
        "area": "jobs",
        "resource": "dispatch",
        "message_id": 7,
    })
}

#[test]
fn should_preview_only_exact_authorized_queue_dead_letter() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let read_model = AdminReadModel::new();
    read_model.replace_queue_dead_letters(vec![dead_letter(42, 7), dead_letter(41, 7)]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["41".into()]),
    );
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);

    // Act
    let result = actions
        .preview_queue_dead_letter(
            Some(queue_arguments(41, "replay")),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();

    // Assert
    assert_eq!(result["action"], "queue.dead-letter.replay");
    assert!(result["target"]
        .as_str()
        .unwrap()
        .starts_with("route_family=41;queue://operations/jobs/dispatch;message_id=7;"));
    assert_eq!(result["observed_state_sha256"].as_str().unwrap().len(), 64);
}

#[test]
fn should_deny_queue_dead_letter_preview_from_another_family() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let read_model = AdminReadModel::new();
    read_model.replace_queue_dead_letters(vec![dead_letter(42, 7)]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["41".into()]),
    );

    // Act
    let result = actions.preview_queue_dead_letter(
        Some(queue_arguments(42, "replay")),
        &context,
        &privileged_policy(),
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.is_err());
    assert!(actions.previews.lock().is_empty());
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert!(audit.contains("authority_denied"));
    assert!(!audit.contains("queue://operations/jobs/dispatch"));
}

#[test]
fn should_reject_execution_when_authoritative_queue_state_is_unavailable() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let read_model = AdminReadModel::new();
    read_model.replace_queue_dead_letters(vec![dead_letter(41, 7)]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model.clone());
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["41".into()]),
    );
    let policy = privileged_policy();
    let preview = actions
        .preview_queue_dead_letter(
            Some(queue_arguments(41, "purge")),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    let ticket = actions
        .consume_confirmation(
            CONFIRM_QUEUE_TOOL,
            Some(confirmation_arguments(&preview)),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    let mut changed = dead_letter(41, 7);
    changed.attempts += 1;
    read_model.replace_queue_dead_letters(vec![changed]);

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);

    // Assert
    assert!(result.unwrap_err().contains("no command was started"));
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert!(audit.contains("state_revalidation_failed"));
    assert!(!audit.contains("indeterminate"));
}

#[test]
fn should_not_reuse_confirmation_when_queue_state_cannot_be_revalidated() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let read_model = AdminReadModel::new();
    read_model.replace_queue_dead_letters(vec![dead_letter(41, 7)]);
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["41".into()]),
    );
    let policy = privileged_policy();
    let preview = actions
        .preview_queue_dead_letter(
            Some(queue_arguments(41, "replay")),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    let arguments = confirmation_arguments(&preview);
    let ticket = actions
        .consume_confirmation(
            CONFIRM_QUEUE_TOOL,
            Some(arguments.clone()),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);
    let retry = actions.consume_confirmation(
        CONFIRM_QUEUE_TOOL,
        Some(arguments),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.unwrap_err().contains("no command was started"));
    assert!(retry.is_err());
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert!(audit.contains("\"phase\":\"state_revalidation_failed\""));
}
