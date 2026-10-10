use super::*;
use crate::domains::queue::{MessageId, QueueActor, QueueKey};
use crate::runtime::routing::RouteFamily;
use chrono::TimeZone as _;

fn fixture() -> (crate::testkit::DomainRuntimeFixture, u64) {
    let fixture = crate::testkit::create_domain_runtime_fixture();
    let id = fixture.seed_queue_dead_letter("operations", "jobs", "dispatch");
    let actor = recovered_actor(&fixture);
    let snapshot = actor.admin_dead_letter(id).unwrap().unwrap();
    let timestamp = chrono::Utc
        .timestamp_millis_opt(snapshot.dead_lettered_at_epoch_ms.cast_signed())
        .single()
        .unwrap()
        .to_rfc3339();
    fixture
        .runtime()
        .admin_read_model()
        .replace_queue_dead_letters(vec![crate::control::admin::QueueDeadLetter::snapshot(
            &crate::control::admin::QueueDeadLetterSnapshot {
                message_id: id.as_u64(),
                family: 1,
                realm: "operations",
                area: "jobs",
                resource: "dispatch",
                dead_lettered_at: &timestamp,
                attempts: snapshot.attempts,
                reason: snapshot.reason,
            },
        )]);
    (fixture, id.as_u64())
}

fn arguments(message_id: u64, operation: &str) -> serde_json::Value {
    serde_json::json!({"operation": operation, "route_family": 1, "realm": "operations", "area": "jobs", "resource": "dispatch", "message_id": message_id})
}

fn recovered_actor(fixture: &crate::testkit::DomainRuntimeFixture) -> QueueActor {
    QueueActor::new(
        RouteFamily::new(1),
        QueueKey {
            family: RouteFamily::new(1),
            realm: "operations".into(),
            area: "jobs".into(),
            resource: "dispatch".into(),
        },
        fixture.store(),
        Some(2),
        crate::utils::idempotency::default_dedup_store(),
    )
}

#[test]
fn should_replay_real_queue_dead_letter_through_confirmed_shared_admin_command() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let (fixture, message_id) = fixture();
    let runtime = fixture.runtime();
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
    );
    let policy = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let preview = actions
        .preview_queue_dead_letter(
            Some(arguments(message_id, "replay")),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();
    let confirmation = confirmation_arguments(&preview);
    let ticket = actions
        .consume_confirmation(
            CONFIRM_QUEUE_TOOL,
            Some(confirmation.clone()),
            &context,
            &policy,
            [9; 32],
            &runtime,
        )
        .unwrap();

    // Act
    let result = actions
        .execute(ticket, &context, &policy, [9; 32], &runtime)
        .unwrap();
    let retry = actions.consume_confirmation(
        CONFIRM_QUEUE_TOOL,
        Some(confirmation),
        &context,
        &policy,
        [9; 32],
        &runtime,
    );
    let recovered = recovered_actor(&fixture);

    // Assert
    assert_eq!(result["outcome"], "completed");
    assert!(retry.is_err());
    assert_eq!(recovered.admin_dead_letters().len(), 0);
    assert!(recovered.ready_contains(MessageId::new(message_id)));
}

#[test]
fn should_purge_real_queue_dead_letter_through_confirmed_shared_admin_command() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let (fixture, message_id) = fixture();
    let runtime = fixture.runtime();
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
    );
    let policy = privileged_policy();
    let preview = actions
        .preview_queue_dead_letter(
            Some(arguments(message_id, "purge")),
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

    // Act
    let result = actions
        .execute(ticket, &context, &policy, [9; 32], &runtime)
        .unwrap();
    let recovered = recovered_actor(&fixture);

    // Assert
    assert_eq!(result["outcome"], "completed");
    assert_eq!(recovered.admin_dead_letters().len(), 0);
    assert!(!recovered.ready_contains(MessageId::new(message_id)));
}

#[test]
fn should_recheck_actor_dead_letter_state_at_dispatch_after_purge() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let (fixture, message_id) = fixture();
    let runtime = fixture.runtime();
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
    );
    let policy = privileged_policy();
    let preview = actions
        .preview_queue_dead_letter(
            Some(arguments(message_id, "replay")),
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
    assert!(runtime
        .queue_purge_dead_letter(
            RouteFamily::new(1),
            "operations",
            "jobs",
            "dispatch",
            message_id
        )
        .unwrap());

    // Act
    let result = actions.execute(ticket, &context, &policy, [9; 32], &runtime);

    // Assert
    assert!(result.unwrap_err().contains("fresh preview"));
    assert_eq!(recovered_actor(&fixture).admin_dead_letters().len(), 0);
}

#[test]
fn should_not_preview_purged_dead_letter_without_an_intervening_list_call() {
    // Arrange
    let directory = tempfile::tempdir().unwrap();
    let actions = action_state(directory.path());
    let (fixture, message_id) = fixture();
    let runtime = fixture.runtime();
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["1".into()]),
    );
    assert!(runtime
        .queue_purge_dead_letter(
            RouteFamily::new(1),
            "operations",
            "jobs",
            "dispatch",
            message_id
        )
        .unwrap());

    // Act
    let result = actions.preview_queue_dead_letter(
        Some(arguments(message_id, "replay")),
        &context,
        &privileged_policy(),
        [9; 32],
        &runtime,
    );

    // Assert
    assert!(result.is_err());
    let audit = std::fs::read_to_string(directory.path().join("actions.jsonl")).unwrap();
    let latest = serde_json::from_str::<serde_json::Value>(audit.lines().last().unwrap()).unwrap();
    assert_eq!(latest["phase"], "dead_letter_not_found");
}
