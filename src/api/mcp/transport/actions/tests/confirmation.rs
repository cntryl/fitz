use super::*;

#[test]
fn should_bound_confirmation_state_before_issuing_another_preview() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let template = actions.previews.lock()[preview["challenge_id"].as_str().unwrap()].clone();
    for _ in 1..MAX_PREVIEWS {
        actions
            .previews
            .lock()
            .insert(Uuid::new_v4().to_string(), template.clone());
    }

    // Act
    let result = actions.preview_runtime_drain(&context, &policy, [9; 32], &runtime);

    // Assert
    assert!(result.is_err());
    assert_eq!(actions.previews.lock().len(), MAX_PREVIEWS);
}

#[test]
fn should_discard_expired_challenges_before_issuing_another_preview() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    actions
        .previews
        .lock()
        .values_mut()
        .for_each(|pending| pending.expires_at = Instant::now());

    // Act
    let fresh = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();

    // Assert
    assert_ne!(fresh["challenge_id"], preview["challenge_id"]);
    assert_eq!(actions.previews.lock().len(), 1);
}

#[test]
fn should_reject_changed_confirmation_target() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let mut arguments = confirmation_arguments(&preview);
    arguments["target"] = serde_json::json!("other-broker");

    // Act
    let result = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.is_err());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_reject_changed_confirmation_phrase() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let mut arguments = confirmation_arguments(&preview);
    arguments["confirmation"] = serde_json::json!("DRAIN");

    // Act
    let result = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.is_err());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_reject_expired_confirmation_before_dispatch() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    actions
        .previews
        .lock()
        .get_mut(preview["challenge_id"].as_str().unwrap())
        .unwrap()
        .expires_at = Instant::now();

    // Act
    let result = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(confirmation_arguments(&preview)),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.is_err());
    assert!(actions.previews.lock().is_empty());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_revalidate_action_capabilities_immediately_before_dispatch() {
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
    let revoked = McpCapabilityPolicy::from_classes([McpCapabilityClass::Inspect]);

    // Act
    let result = actions.execute(ticket, &context, &revoked, [9; 32], &runtime);

    // Assert
    assert!(result.unwrap_err().contains("no command was started"));
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_require_fresh_preview_after_observed_runtime_state_changes() {
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
    runtime.begin_drain();

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);

    // Assert
    assert!(result.unwrap_err().contains("fresh preview"));
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    assert!(audit.contains("state_changed"));
    assert!(!audit.contains("\"phase\":\"completed\""));
}

#[test]
fn should_consume_confirmation_once_given_concurrent_requests() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let preview = actions
        .preview_runtime_drain(&context, &policy, [9; 32], &runtime)
        .unwrap();
    let barrier = std::sync::Barrier::new(2);

    // Act
    let accepted = std::thread::scope(|scope| {
        let consume = || {
            barrier.wait();
            actions
                .consume_confirmation(
                    CONFIRM_DRAIN_TOOL,
                    Some(confirmation_arguments(&preview)),
                    &context,
                    &policy,
                    [9; 32],
                    &runtime,
                )
                .is_ok()
        };
        let first = scope.spawn(consume);
        let second = scope.spawn(consume);
        usize::from(first.join().unwrap()) + usize::from(second.join().unwrap())
    });

    // Assert
    assert_eq!(accepted, 1);
    assert!(actions.previews.lock().is_empty());
}
