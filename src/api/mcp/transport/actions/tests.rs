use super::audit::MAX_AUDIT_FILE_BYTES;
use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::auth::Access;
use crate::control::admin::read_model::AdminReadModel;
use crate::runtime::Router;
use crate::session::permissions::SessionPermissions;
use std::path::Path;

fn runtime() -> Runtime {
    Runtime::with_admin_read_model(Arc::new(Router::new()), AdminReadModel::new())
}

fn context(username: &str, family_access: AdminRouteFamilyAccess) -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: username.to_string(),
            route_family_access: family_access,
        },
        SessionPermissions::all(),
    )
}

fn action_state(directory: &Path) -> McpActionState {
    McpActionState {
        runtime_target: "fitz-production-east".into(),
        audit: Arc::new(
            McpActionAuditSink::open(directory.join("actions.jsonl"))
                .expect("open mandatory action audit sink"),
        ),
        previews: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    }
}

fn privileged_policy() -> McpCapabilityPolicy {
    McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate, McpCapabilityClass::Admin])
}

fn confirmation_arguments(preview: &serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "challenge_id": preview["challenge_id"].as_str().unwrap(),
        "target": preview["target"].as_str().unwrap(),
        "confirmation": preview["confirmation"].as_str().unwrap(),
    })
}

#[test]
fn should_require_exact_one_use_confirmation_before_runtime_drain() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let actions = action_state(directory.path());
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let policy = privileged_policy();
    let fingerprint = [9; 32];
    let preview = actions
        .preview_runtime_drain(&context, &policy, fingerprint, &runtime)
        .expect("drain preview");
    let arguments = confirmation_arguments(&preview);

    // Act
    let ticket = actions
        .consume_confirmation(
            CONFIRM_DRAIN_TOOL,
            Some(arguments.clone()),
            &context,
            &policy,
            fingerprint,
            &runtime,
        )
        .expect("exact confirmation");
    let output = actions
        .execute(ticket, &context, &policy, fingerprint, &runtime)
        .expect("drain command");
    let retry = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &context,
        &policy,
        fingerprint,
        &runtime,
    );

    // Assert
    assert_eq!(runtime.lifecycle_state().as_str(), "draining");
    assert_eq!(output["outcome"], "completed");
    assert!(retry.is_err());
    let records = std::fs::read_to_string(directory.path().join("actions.jsonl"))
        .expect("durable action records");
    let phases = records
        .lines()
        .map(|line| {
            serde_json::from_str::<serde_json::Value>(line).unwrap()["phase"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        phases,
        [
            "preview",
            "intent",
            "completed",
            "challenge_missing_or_used"
        ]
    );
}

#[test]
fn should_reject_confirmation_from_another_principal_or_token() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let actions = action_state(directory.path());
    let runtime = runtime();
    let policy = privileged_policy();
    let owner = context("operator-a", AdminRouteFamilyAccess::wildcard());
    let preview = actions
        .preview_runtime_drain(&owner, &policy, [9; 32], &runtime)
        .expect("drain preview");

    // Act
    let other_principal = context("operator-b", AdminRouteFamilyAccess::wildcard());
    let arguments = confirmation_arguments(&preview);
    let principal_result = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments.clone()),
        &other_principal,
        &policy,
        [9; 32],
        &runtime,
    );
    let token_result = actions.consume_confirmation(
        CONFIRM_DRAIN_TOOL,
        Some(arguments),
        &owner,
        &policy,
        [8; 32],
        &runtime,
    );

    // Assert
    assert!(principal_result.is_err());
    assert!(token_result.is_err());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
}

#[test]
fn should_fail_closed_when_mandatory_preview_audit_cannot_append() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let audit_path = directory.path().join("full.jsonl");
    let file = std::fs::File::create(&audit_path).expect("create audit file");
    file.set_len(MAX_AUDIT_FILE_BYTES)
        .expect("fill audit capacity");
    let actions = McpActionState {
        runtime_target: "fitz-production-east".into(),
        audit: Arc::new(McpActionAuditSink::open(audit_path).expect("open full audit sink")),
        previews: Arc::new(parking_lot::Mutex::new(HashMap::new())),
    };
    let runtime = runtime();
    let context = context("operator-a", AdminRouteFamilyAccess::wildcard());

    // Act
    let result = actions.preview_runtime_drain(&context, &privileged_policy(), [9; 32], &runtime);

    // Assert
    assert!(result.is_err());
    assert_eq!(runtime.lifecycle_state().as_str(), "running");
    assert!(actions.previews.lock().is_empty());
}

#[test]
fn should_restrict_action_audit_file_permissions_on_unix() {
    // Arrange
    let directory = tempfile::tempdir().expect("temporary audit directory");
    let sink = McpActionAuditSink::open(directory.path().join("actions.jsonl"))
        .expect("open action audit sink");

    // Act
    let mode = {
        use std::os::unix::fs::PermissionsExt;
        sink.file.lock().metadata().unwrap().permissions().mode() & 0o777
    };

    // Assert
    assert_eq!(mode, 0o600);
}

#[test]
fn should_require_admin_capability_for_queue_dead_letter_purge() {
    // Arrange
    let runtime = runtime();
    let context = context(
        "queue-operator",
        AdminRouteFamilyAccess::Explicit(vec!["41".to_string()]),
    );
    let target = QueueTarget {
        route_family: 41,
        realm: "operations".into(),
        area: "jobs".into(),
        resource: "dispatch".into(),
        message_id: 7,
    };
    let mutate = McpCapabilityPolicy::from_classes([McpCapabilityClass::Mutate]);
    let replay = QueueOperationInput::Replay;
    let purge = QueueOperationInput::Purge;

    // Act
    let can_replay =
        McpActionState::authorized_queue(&context, &mutate, &runtime, &target, &replay);
    let can_purge = McpActionState::authorized_queue(&context, &mutate, &runtime, &target, &purge);
    let can_purge_with_admin =
        McpActionState::authorized_queue(&context, &privileged_policy(), &runtime, &target, &purge);

    // Assert
    assert!(can_replay);
    assert!(!can_purge);
    assert!(can_purge_with_admin);
    assert!(context
        .permissions
        .allows_route("queue://operations/jobs/dispatch", Access::Write));
}
