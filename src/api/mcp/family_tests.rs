use super::*;
use crate::api::admin::auth::AdminRouteFamilyAccess;
use crate::auth::default_anonymous_permissions;
use crate::runtime::Router;
use std::sync::Arc;

pub(super) fn context() -> McpExecutionContext {
    McpExecutionContext::authenticated(
        AdminPrincipal {
            username: "scoped-admin".to_string(),
            route_family_access: AdminRouteFamilyAccess::Explicit(vec!["1".to_string()]),
        },
        default_anonymous_permissions(),
    )
}

#[test]
fn should_deny_all_family_tools_given_restricted_principal() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();
    let tools = [
        "get_global_stats",
        "get_global_troubleshooting",
        "explain_global_troubleshooting",
    ];

    // Act
    let results = tools.map(|tool| registry.execute(tool, &runtime, &context, &policy, None));

    // Assert
    assert!(results.iter().all(Result::is_err));
    assert_eq!(context.audit_records().len(), tools.len());
    assert!(context
        .audit_records()
        .iter()
        .all(|record| record.decision == McpAuditDecision::Denied));
}

#[test]
fn should_deny_all_family_resource_tools_even_with_queue_family_on_other_domains() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();
    let domains = ["kv", "stream", "lease", "schedule", "notice", "rpc"];

    // Act
    let results: Vec<_> = domains.into_iter().flat_map(|scheme| {
        ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
            let arguments = serde_json::json!({"scheme":scheme,"realm":"acme","area":"jobs","resource":"orders","queue_family":1});
            registry.execute(tool, &runtime, &context, &policy, Some(&arguments))
        })
    }).collect();

    // Assert
    assert!(results.iter().all(Result::is_err));
    assert_eq!(context.audit_records().len(), 12);
}

#[test]
fn should_allow_queue_tools_only_for_authorized_explicit_family() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({"scheme":"queue","realm":"acme","area":"jobs","resource":"orders","queue_family":1});

    // Act
    let results = ["inspect_resource_detail", "inspect_resource_timeline"]
        .map(|tool| registry.execute(tool, &runtime, &context, &policy, Some(&arguments)));

    // Assert
    assert!(results.iter().all(Result::is_ok), "{results:?}");
}

#[test]
fn should_deny_queue_tools_for_missing_or_unauthorized_family() {
    // Arrange
    let runtime = Runtime::new(Arc::new(Router::new()));
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let results: Vec<_> = [None, Some(2)].into_iter().flat_map(|family| {
        ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
            let arguments = serde_json::json!({"scheme":"queue","realm":"acme","area":"jobs","resource":"orders","queue_family":family});
            registry.execute(tool, &runtime, &context, &policy, Some(&arguments))
        })
    }).collect();

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_exclude_sibling_queue_facts_given_authorized_family() {
    // Arrange
    use crate::control::admin::{
        QueueAgeBuckets, QueueDeadLetter, QueueInflight, QueueInfo, QueueInfoSnapshot,
    };
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    read_model.replace_queues(
        [1, 2]
            .map(|family| {
                QueueInfo::snapshot(&QueueInfoSnapshot {
                    family,
                    realm: "acme",
                    area: "jobs",
                    resource: "orders",
                    subscriptions_active: 0,
                    messages_ready: if family == 1 { 1 } else { 99 },
                    messages_delayed: 0,
                    messages_inflight: 1,
                    messages_dead_lettered: 1,
                    messages_total: if family == 1 { 3 } else { 101 },
                    oldest_message_age_seconds: 0,
                    oldest_backlog_age_seconds: 0,
                    backlog_age_buckets: QueueAgeBuckets::default(),
                    delay_age_buckets: QueueAgeBuckets::default(),
                    enqueue_success_total: 0,
                    complete_success_total: 0,
                    in_rate_per_second: 0.0,
                    out_rate_per_second: 0.0,
                })
            })
            .to_vec(),
    );
    read_model.replace_queue_inflight(
        [1, 2]
            .map(|family| QueueInflight {
                message_id: family,
                family,
                realm: "acme".into(),
                area: "jobs".into(),
                resource: "orders".into(),
                inflight_token: family.to_string(),
                session_id: format!("family-{family}-owner"),
                expires_at: "2099-01-01T00:00:00Z".into(),
                attempts: 1,
            })
            .to_vec(),
    );
    read_model.replace_queue_dead_letters(
        [1, 2]
            .map(|family| QueueDeadLetter {
                message_id: family,
                family,
                realm: "acme".into(),
                area: "jobs".into(),
                resource: "orders".into(),
                dead_lettered_at: "2026-10-01T00:00:00Z".into(),
                attempts: 1,
                reason: format!("family-{family}-reason"),
            })
            .to_vec(),
    );
    let runtime = Runtime::with_admin_read_model(Arc::new(Router::new()), read_model);
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();
    let arguments = serde_json::json!({"scheme":"queue","realm":"acme","area":"jobs","resource":"orders","queue_family":1});

    // Act
    let detail = registry
        .execute(
            "inspect_resource_detail",
            &runtime,
            &context,
            &policy,
            Some(&arguments),
        )
        .expect("authorized detail");
    let timeline = registry
        .execute(
            "inspect_resource_timeline",
            &runtime,
            &context,
            &policy,
            Some(&arguments),
        )
        .expect("authorized timeline");

    // Assert
    assert_eq!(detail["messages_ready"], 1);
    assert_eq!(detail["messages_total"], 3);
    let encoded = timeline.to_string();
    assert!(encoded.contains("family-1-owner"));
    assert!(!encoded.contains("family-2-owner"));
    assert!(!encoded.contains("99 ready"));
    assert!(encoded.contains("1 dead-lettered"));
}
