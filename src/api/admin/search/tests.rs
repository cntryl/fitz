use super::*;
use crate::api::admin::auth::{AdminPrincipal, AdminRouteFamilyAccess};
use crate::api::admin::{
    KvTransaction, QueueDeadLetter, QueueInfo, RpcPendingRequest, ScheduleInfo,
};
use crate::boot::Runtime;
use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::{QueueAgeBuckets, QueueDeadLetterSnapshot, QueueInfoSnapshot};
use crate::runtime::Router;
use hyper::StatusCode;
use std::sync::Arc;

fn runtime_with_read_model(read_model: Arc<AdminReadModel>) -> Arc<Runtime> {
    Arc::new(Runtime::with_admin_read_model(
        Arc::new(Router::new()),
        read_model,
    ))
}

fn wildcard_access() -> AdminRouteFamilyAccess {
    AdminRouteFamilyAccess::wildcard()
}

fn explicit_access(route_families: &[&str]) -> AdminRouteFamilyAccess {
    AdminRouteFamilyAccess::Explicit(
        route_families
            .iter()
            .map(std::string::ToString::to_string)
            .collect(),
    )
}

fn queue_snapshot(family: u64, realm: &str, resource: &str) -> QueueInfo {
    QueueInfo::snapshot(&QueueInfoSnapshot {
        family,
        realm,
        area: "payments",
        resource,
        subscriptions_active: 0,
        messages_ready: 4,
        messages_delayed: 0,
        messages_inflight: 0,
        messages_dead_lettered: 0,
        messages_total: 4,
        oldest_message_age_seconds: 5,
        oldest_backlog_age_seconds: 5,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
        enqueue_success_total: 4,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
    })
}

#[test]
fn should_search_queue_dead_letters_by_message_id() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_queue_dead_letters(vec![QueueDeadLetter::snapshot(
        &QueueDeadLetterSnapshot {
            message_id: 42,
            family: 2,
            realm: "billing",
            area: "payments",
            resource: "settlement",
            dead_lettered_at: "2026-06-23T00:00:00Z",
            attempts: 3,
            reason: "timeout",
        },
    )]);
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: "42".to_string(),
        route_family: Some("2".to_string()),
        domain: Some("queue".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].kind, "dead_letter");
    assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
    assert!(response.results[0]
        .matched_fields
        .contains(&"message_id".to_string()));
}

#[test]
fn should_filter_search_given_domain_realm_filters() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
    read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
        1,
        "ops".to_string(),
        "jobs".to_string(),
        "cleanup".to_string(),
        "run".to_string(),
        "0 * * * *".to_string(),
        "2026-06-23T00:00:00Z",
    ));
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: None,
        domain: Some("queue".to_string()),
        realm: Some("billing".to_string()),
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].domain, "queue");
    assert_eq!(response.results[0].realm.as_deref(), Some("billing"));
}

#[test]
fn should_search_rpc_pending_by_correlation_id() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_rpc_pending(vec![RpcPendingRequest::snapshot(
        1,
        &"corr-123",
        "rpc://billing/payments/settlement/run",
        "2026-06-23T00:00:00Z",
        7,
        None,
    )]);
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: "corr-123".to_string(),
        route_family: None,
        domain: Some("rpc".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].operation.as_deref(), Some("run"));
}

#[test]
fn should_limit_unfiltered_search_to_explicit_route_family_access() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_queues(vec![
        queue_snapshot(1, "billing", "settlement"),
        queue_snapshot(2, "billing", "settlement"),
    ]);
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: None,
        domain: Some("queue".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].route_family.as_deref(), Some("1"));
}

#[test]
fn should_hide_unknown_route_family_candidates_from_explicit_access() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
    read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
        2,
        "ops".to_string(),
        "jobs".to_string(),
        "cleanup".to_string(),
        "run".to_string(),
        "0 * * * *".to_string(),
        "2026-06-23T00:00:00Z",
    ));
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: None,
        domain: None,
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

    // Assert
    assert_eq!(response.total, 1);
    assert!(response
        .results
        .iter()
        .all(|result| result.route_family.as_deref() == Some("1")));
}

#[test]
fn should_hide_unknown_route_family_candidates_from_explicit_route_filter() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_queues(vec![queue_snapshot(1, "billing", "settlement")]);
    read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
        2,
        "billing".to_string(),
        "payments".to_string(),
        "settlement".to_string(),
        "run".to_string(),
        "0 * * * *".to_string(),
        "2026-06-23T00:00:00Z",
    ));
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: Some("1".to_string()),
        domain: None,
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["1"]));

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].domain, "queue");
    assert_eq!(response.results[0].route_family.as_deref(), Some("1"));
}

#[test]
fn should_keep_unknown_route_family_candidates_visible_to_wildcard_access() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.upsert_schedule(ScheduleInfo::enabled_snapshot(
        2,
        "ops".to_string(),
        "jobs".to_string(),
        "cleanup".to_string(),
        "run".to_string(),
        "0 * * * *".to_string(),
        "2026-06-23T00:00:00Z",
    ));
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: None,
        domain: Some("schedule".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
}

#[test]
fn should_filter_kv_transaction_search_by_route_family() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_kv_transactions(vec![
        KvTransaction::snapshot(
            1,
            41,
            10,
            "billing",
            "payments",
            "settlement",
            "2026-06-23T00:00:00Z",
        ),
        KvTransaction::snapshot(
            2,
            41,
            11,
            "billing",
            "payments",
            "settlement",
            "2026-06-23T00:00:00Z",
        ),
    ]);
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: Some("2".to_string()),
        domain: Some("kv".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &wildcard_access());

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
    assert_eq!(
        response.results[0].id,
        "kv:transaction:2:billing:payments:settlement:41"
    );
}

#[test]
fn should_hide_kv_transaction_search_results_outside_explicit_route_family_access() {
    // Arrange
    let read_model = AdminReadModel::new();
    read_model.replace_kv_transactions(vec![
        KvTransaction::snapshot(
            1,
            41,
            10,
            "billing",
            "payments",
            "settlement",
            "2026-06-23T00:00:00Z",
        ),
        KvTransaction::snapshot(
            2,
            42,
            11,
            "billing",
            "payments",
            "settlement",
            "2026-06-23T00:00:00Z",
        ),
    ]);
    let runtime = runtime_with_read_model(read_model);
    let options = SearchOptions {
        query: String::new(),
        route_family: None,
        domain: Some("kv".to_string()),
        realm: None,
        area: None,
        resource: None,
        operation: None,
        limit: 50,
    };

    // Act
    let response = search_runtime(runtime.as_ref(), &options, &explicit_access(&["2"]));

    // Assert
    assert_eq!(response.total, 1);
    assert_eq!(response.results[0].route_family.as_deref(), Some("2"));
    assert_eq!(response.results[0].metadata.get("tx_id").unwrap(), "42");
}

#[tokio::test]
async fn should_reject_disallowed_route_family_filter() {
    // Arrange
    let runtime = runtime_with_read_model(AdminReadModel::new());
    let uri = "/api/v1/search?route_family=2".parse().unwrap();
    let principal = AdminPrincipal {
        username: "admin".to_string(),
        route_family_access: explicit_access(&["1"]),
    };

    // Act
    let response = handle_search(&uri, &runtime, &principal);

    // Assert
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
}
