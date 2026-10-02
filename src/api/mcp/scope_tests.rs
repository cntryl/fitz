use super::family_tests::context;
use super::*;
use serde_json::json;
use std::sync::Arc;

#[test]
fn should_allow_explicit_authorized_family_for_all_resource_domains() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let results: Vec<_> = [
        "kv", "queue", "stream", "lease", "schedule", "notice", "rpc",
    ]
    .into_iter()
    .flat_map(|scheme| {
        ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
            registry.execute(
                tool,
                &runtime,
                &context,
                &policy,
                Some(&json!({
                    "scheme": scheme, "realm": "application-namespace", "area": "jobs",
                    "resource": "orders", "route_family": 1
                })),
            )
        })
    })
    .collect();

    // Assert
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(context.audit_records().len(), 14);
}

#[test]
fn should_authorize_family_before_invoking_resource_collector() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    let mut registry = McpToolRegistry::read_only();
    for tool in &mut registry.tools {
        tool.handler = |_, _| panic!("unauthorized collection executed");
    }
    let context = context();
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let results: Vec<_> = [
        "kv", "queue", "stream", "lease", "schedule", "notice", "rpc",
    ]
    .into_iter()
    .flat_map(|scheme| {
        ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
            registry.execute(
                tool,
                &runtime,
                &context,
                &policy,
                Some(&json!({
                    "scheme": scheme, "realm": "1", "area": "jobs", "resource": "orders",
                    "route_family": 2
                })),
            )
        })
    })
    .collect();

    // Assert
    assert!(results
        .iter()
        .all(|result| matches!(result, Err(McpToolError::ScopeDenied { .. }))));
}

#[test]
fn should_reject_conflicting_queue_scope_before_collection() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    let mut registry = McpToolRegistry::read_only();
    for tool in &mut registry.tools {
        tool.handler = |_, _| panic!("ambiguous collection executed");
    }
    let context = context();
    let arguments = json!({"scheme":"queue", "realm":"acme", "area":"jobs",
        "resource":"orders", "route_family":1, "queue_family":2});

    // Act
    let results = ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
        registry.execute(
            tool,
            &runtime,
            &context,
            &McpCapabilityPolicy::read_only(),
            Some(&arguments),
        )
    });

    // Assert
    assert!(results
        .iter()
        .all(|result| matches!(result, Err(McpToolError::InvalidArguments { .. }))));
}

fn scoped_runtime(families: &[u64]) -> Runtime {
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    read_model.replace_kv_transactions(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(json!({"route_family":family,"tx_id":family,"realm":"acme",
            "area":"jobs","resource":"orders","mode":format!("session:family-{family}"),
            "started_at":"2099-01-01T00:00:00Z","operations_count":family,"idle_seconds":family}))
                .unwrap()
            })
            .collect(),
    );
    read_model.replace_streams(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(json!({"route_family":family,"realm":"acme","area":"jobs",
            "resource":"orders","committed_event_count":family,"offset":family,
            "watermark":family,"size_bytes":family,"sessions_active":family,
            "subscriptions_active":family}))
                .unwrap()
            })
            .collect(),
    );
    read_model.replace_leases(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(json!({"route_family":family,"realm":"acme","area":"jobs",
            "resource":"orders","owner_session_id":format!("family-{family}"),
            "acquired_at":"2099-01-01T00:00:00Z","expires_at":"2099-02-01T00:00:00Z",
            "renewals":family,"fencing_token":family}))
                .unwrap()
            })
            .collect(),
    );
    add_additional_facts(&read_model, families);
    Runtime::with_admin_read_model(Arc::new(crate::runtime::Router::new()), read_model)
}

fn normalize_observation_time(value: &mut Value) {
    match value {
        Value::Object(fields) => {
            fields.remove("observed_at");
            for field in fields.values_mut() {
                normalize_observation_time(field);
            }
        }
        Value::Array(items) => {
            for item in items {
                normalize_observation_time(item);
            }
        }
        _ => {}
    }
}

#[test]
fn should_keep_scoped_resource_facts_unchanged_when_sibling_family_is_added() {
    // Arrange
    let single = scoped_runtime(&[1]);
    let combined = scoped_runtime(&[1, 99]);
    let registry = McpToolRegistry::read_only();
    let context = context();
    let policy = McpCapabilityPolicy::read_only();

    // Act
    let comparisons: Vec<_> = [
        "kv", "queue", "stream", "lease", "schedule", "notice", "rpc",
    ]
    .into_iter()
    .flat_map(|scheme| {
        ["inspect_resource_detail", "inspect_resource_timeline"].map(|tool| {
            let arguments = json!({"scheme":scheme,"realm":"acme","area":"jobs",
                    "resource":"orders","route_family":1});
            let mut expected = registry
                .execute(tool, &single, &context, &policy, Some(&arguments))
                .unwrap();
            let mut actual = registry
                .execute(tool, &combined, &context, &policy, Some(&arguments))
                .unwrap();
            normalize_observation_time(&mut expected);
            normalize_observation_time(&mut actual);
            (scheme, tool, expected, actual)
        })
    })
    .collect();

    // Assert
    for (scheme, tool, expected, actual) in comparisons {
        assert_eq!(actual, expected, "sibling changed {scheme} {tool}");
        assert!(!actual.to_string().contains("family-99"));
    }
}

#[test]
fn should_require_global_read_authority_before_summary_collection() {
    // Arrange
    let runtime = Runtime::new(Arc::new(crate::runtime::Router::new()));
    let mut registry = McpToolRegistry::read_only();
    for tool in &mut registry.tools {
        tool.handler = |_, _| panic!("unauthorized global collection executed");
    }
    let principal = crate::api::admin::auth::AdminPrincipal {
        username: "scoped-route-reader".into(),
        route_family_access: crate::api::admin::auth::AdminRouteFamilyAccess::wildcard(),
    };
    let context = McpExecutionContext::authenticated(
        principal,
        crate::session::SessionPermissions::from_permissions(vec![crate::auth::Permission {
            raw: "kv://acme/jobs/orders#read".into(),
            access: Access::Read,
        }]),
    );

    // Act
    let results = [
        "get_global_stats",
        "get_global_troubleshooting",
        "explain_global_troubleshooting",
    ]
    .map(|tool| {
        registry.execute(
            tool,
            &runtime,
            &context,
            &McpCapabilityPolicy::read_only(),
            None,
        )
    });

    // Assert
    assert!(results
        .iter()
        .all(|result| matches!(result, Err(McpToolError::ScopeDenied { .. }))));
}

fn add_additional_facts(
    read_model: &crate::control::admin::read_model::AdminReadModel,
    families: &[u64],
) {
    use crate::control::admin::{QueueAgeBuckets, QueueInfo, QueueInfoSnapshot, ScheduleInfo};
    read_model.replace_notice_subscriptions(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(json!({"route_family":family,"subscription_id":family,
            "session_id":format!("family-{family}"),"realm":"acme",
            "pattern":"notice://acme/jobs/orders/*","created_at":"2099-01-01T00:00:00Z",
            "notifications_received":family}))
                .unwrap()
            })
            .collect(),
    );
    read_model.replace_notice_routes(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(
                    json!({"route_family":family,"route":"notice://acme/jobs/orders/process",
            "subscribers":family,"publishes_total":family,"publishes_per_minute":family}),
                )
                .unwrap()
            })
            .collect(),
    );
    read_model.replace_rpc_workers(families.iter().map(|family| {
        serde_json::from_value(json!({"route_family":family,"session_id":format!("family-{family}"),
            "realm":"acme","route":"rpc://acme/jobs/orders/process",
            "registered_at":"2099-01-01T00:00:00Z","requests_handled":family,"average_latency_ms":family})).unwrap()
    }).collect());
    read_model.replace_rpc_pending(
        families
            .iter()
            .map(|family| {
                serde_json::from_value(
                    json!({"route_family":family,"correlation_id":format!("family-{family}"),
            "route":"rpc://acme/jobs/orders/process","submitted_at":"2099-01-01T00:00:00Z",
            "age_seconds":family,"worker_session_id":format!("family-{family}")}),
                )
                .unwrap()
            })
            .collect(),
    );
    read_model.replace_schedules(
        families
            .iter()
            .map(|family| {
                ScheduleInfo::enabled_snapshot(
                    *family,
                    "acme".into(),
                    "jobs".into(),
                    "orders".into(),
                    format!("family-{family}"),
                    "* * * * *".into(),
                    "2099-01-01T00:00:00Z",
                )
            })
            .collect(),
    );
    read_model.replace_queues(
        families
            .iter()
            .map(|family| {
                QueueInfo::snapshot(&QueueInfoSnapshot {
                    family: *family,
                    realm: "acme",
                    area: "jobs",
                    resource: "orders",
                    subscriptions_active: 1,
                    messages_ready: usize::try_from(*family).unwrap(),
                    messages_delayed: 0,
                    messages_inflight: 0,
                    messages_dead_lettered: 0,
                    messages_total: usize::try_from(*family).unwrap(),
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
            .collect(),
    );
}
