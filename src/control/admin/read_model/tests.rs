//! Admin read-model regression tests.

use super::*;

#[test]
fn should_insert_enabled_schedule_snapshot_given_upsert_schedule_fields() {
    // Arrange
    let read_model = AdminReadModel::default();

    // Act
    read_model.upsert_schedule_fields(
        1,
        "acme".to_string(),
        "billing".to_string(),
        "invoices".to_string(),
        "send".to_string(),
        "0 * * * *".to_string(),
    );
    let schedules = read_model.schedules(None);

    // Assert
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].realm, "acme");
    assert_eq!(schedules[0].area, "billing");
    assert_eq!(schedules[0].resource, "invoices");
    assert_eq!(schedules[0].operation, "send");
    assert_eq!(schedules[0].cron, "0 * * * *");
    assert!(schedules[0].enabled);
    assert!(schedules[0].last_run.is_none());
    assert_eq!(schedules[0].executions_total, 0);
    assert!(!schedules[0].next_run.is_empty());
}

#[test]
fn should_refresh_schedule_metadata_given_repeated_upsert_schedule_fields() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_schedule_fields(
        1,
        "acme".to_string(),
        "billing".to_string(),
        "invoices".to_string(),
        "send".to_string(),
        "0 * * * *".to_string(),
    );
    let first_schedule = read_model.schedules(None).into_iter().next().unwrap();

    // Act
    read_model.upsert_schedule_fields(
        1,
        "acme".to_string(),
        "billing".to_string(),
        "invoices".to_string(),
        "send".to_string(),
        "0 * * * *".to_string(),
    );
    let schedules = read_model.schedules(None);

    // Assert
    assert_eq!(schedules.len(), 1);
    assert_ne!(schedules[0].next_run, first_schedule.next_run);
}

#[test]
fn should_reset_schedule_state_given_changed_cron_on_upsert_schedule_fields() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_schedule(ScheduleInfo {
        route_family: 1,
        realm: "acme".to_string(),
        area: "billing".to_string(),
        resource: "invoices".to_string(),
        operation: "send".to_string(),
        cron: "0 * * * *".to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        next_run: "2026-03-31T00:00:00Z".to_string(),
        last_run: Some("2026-03-30T23:00:00Z".to_string()),
        executions_total: 42,
        enabled: false,
    });

    // Act
    read_model.upsert_schedule_fields(
        1,
        "acme".to_string(),
        "billing".to_string(),
        "invoices".to_string(),
        "send".to_string(),
        "*/5 * * * *".to_string(),
    );
    let schedules = read_model.schedules(None);

    // Assert
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].cron, "*/5 * * * *");
    assert!(schedules[0].enabled);
    assert!(schedules[0].last_run.is_none());
    assert_eq!(schedules[0].executions_total, 0);
}

#[test]
fn should_apply_schedule_metadata_given_same_definition() {
    // Arrange
    let read_model = AdminReadModel::default();
    let mut schedule = ScheduleInfo::enabled_snapshot(
        1,
        "acme".to_string(),
        "billing".to_string(),
        "invoices".to_string(),
        "send".to_string(),
        "0 * * * *".to_string(),
        "2026-03-31T00:00:00Z",
    );
    read_model.upsert_schedule(schedule.clone());
    schedule.delivery_mode = crate::domains::schedule::ScheduleDeliveryMode::Single;
    schedule.next_run = "2026-04-01T00:00:00Z".to_string();
    schedule.last_run = Some("2026-03-30T23:00:00Z".to_string());
    schedule.executions_total = 3;

    // Act
    read_model.upsert_schedule(schedule);
    let schedules = read_model.schedules(None);

    // Assert
    assert_eq!(schedules.len(), 1);
    assert_eq!(
        schedules[0].delivery_mode,
        crate::domains::schedule::ScheduleDeliveryMode::Single
    );
    assert_eq!(schedules[0].next_run, "2026-04-01T00:00:00Z");
    assert_eq!(
        schedules[0].last_run.as_deref(),
        Some("2026-03-30T23:00:00Z")
    );
    assert_eq!(schedules[0].executions_total, 3);
}

#[test]
fn should_filter_notice_routes_given_realm() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.replace_notice_routes(vec![
        NoticeRouteInfo::snapshot(1, "notice://acme/app/orders".to_string(), 1),
        NoticeRouteInfo::snapshot(1, "notice://globex/app/orders".to_string(), 2),
    ]);

    // Act
    let routes = read_model.notice_routes(Some("acme"));

    // Assert
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].route, "notice://acme/app/orders");
}

#[test]
fn should_filter_notice_routes_by_exact_route_realm() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.replace_notice_routes(vec![
        NoticeRouteInfo::snapshot(1, "notice://acme/app/orders".to_string(), 1),
        NoticeRouteInfo::snapshot(1, "notice://globex/acme/orders".to_string(), 2),
        NoticeRouteInfo::snapshot(1, "notice://globex/app/acme/events".to_string(), 3),
    ]);

    // Act
    let routes = read_model.notice_routes(Some("acme"));

    // Assert
    assert_eq!(routes.len(), 1);
    assert_eq!(routes[0].route, "notice://acme/app/orders");
}

#[test]
fn should_include_wildcard_rpc_worker_given_matching_realm_filter() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.replace_rpc_workers(vec![RpcWorker::snapshot(
        1,
        7,
        "*",
        "rpc://*/billing/**",
        "2026-08-01T00:00:00Z",
        0,
        0.0,
    )]);

    // Act
    let workers = read_model.rpc_workers(Some("acme"));

    // Assert
    assert_eq!(workers.len(), 1);
    assert_eq!(workers[0].route, "rpc://*/billing/**");
}

#[test]
fn should_filter_rpc_pending_by_exact_route_realm() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.replace_rpc_pending(vec![
        RpcPendingRequest::snapshot(
            1,
            &"corr-acme",
            "rpc://acme/payments/settlement/run",
            "2026-03-31T00:00:00Z",
            1,
            None,
        ),
        RpcPendingRequest::snapshot(
            1,
            &"corr-area",
            "rpc://globex/acme/settlement/run",
            "2026-03-31T00:00:00Z",
            1,
            None,
        ),
        RpcPendingRequest::snapshot(
            1,
            &"corr-resource",
            "rpc://globex/payments/acme/run",
            "2026-03-31T00:00:00Z",
            1,
            None,
        ),
    ]);

    // Act
    let requests = read_model.rpc_pending(Some("acme"));

    // Assert
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].correlation_id, "corr-acme");
}

#[test]
fn should_filter_notice_subscriptions_given_route_pattern() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.replace_notice_subscriptions(vec![
        NoticeSubscription::snapshot(
            1,
            1,
            10,
            "acme",
            "notice://acme/app/orders".to_string(),
            "2026-03-31T00:00:00Z",
        ),
        NoticeSubscription::snapshot(
            1,
            2,
            11,
            "acme",
            "notice://acme/app/invoices".to_string(),
            "2026-03-31T00:00:00Z",
        ),
    ]);

    // Act
    let subscriptions = read_model.notice_subscriptions(Some("acme"), Some("orders"));

    // Assert
    assert_eq!(subscriptions.len(), 1);
    assert_eq!(subscriptions[0].pattern, "notice://acme/app/orders");
}

#[test]
fn should_update_kv_transactions_given_incremental_session_changes() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_kv_transaction(KvTransaction::snapshot(
        1,
        41,
        7,
        "acme",
        "app",
        "users",
        "2026-03-31T00:00:00Z",
    ));
    read_model.upsert_kv_transaction(KvTransaction::snapshot(
        1,
        41,
        8,
        "acme",
        "app",
        "orders",
        "2026-03-31T00:00:01Z",
    ));

    // Act
    read_model.remove_kv_transaction(7, 41);
    let after_transaction_remove = read_model.kv_transactions(None);
    read_model.remove_kv_transactions_for_session(8);
    let after_session_remove = read_model.kv_transactions(None);

    // Assert
    assert_eq!(after_transaction_remove.len(), 1);
    assert_eq!(after_transaction_remove[0].resource, "orders");
    assert!(after_session_remove.is_empty());
}

#[test]
fn should_replace_kv_transaction_given_matching_session_identity() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_kv_transaction(KvTransaction::snapshot(
        1,
        41,
        7,
        "acme",
        "app",
        "users",
        "2026-03-31T00:00:00Z",
    ));

    // Act
    read_model.upsert_kv_transaction(KvTransaction::snapshot(
        1,
        41,
        7,
        "acme",
        "app",
        "orders",
        "2026-03-31T00:00:01Z",
    ));
    let transactions = read_model.kv_transactions(None);

    // Assert
    assert_eq!(transactions.len(), 1);
    assert_eq!(transactions[0].resource, "orders");
}

#[test]
fn should_count_kv_transactions_without_materializing_snapshots() {
    // Arrange
    let read_model = AdminReadModel::default();
    for (tx_id, resource) in [(41, "users"), (42, "users"), (43, "orders")] {
        read_model.upsert_kv_transaction(KvTransaction::snapshot(
            1,
            tx_id,
            7,
            "acme",
            "app",
            resource,
            "2026-03-31T00:00:00Z",
        ));
    }

    // Act
    let total = read_model.kv_transaction_count();
    let users = read_model.kv_transaction_count_for_resource(1, "acme", "app", "users");

    // Assert
    assert_eq!(total, 3);
    assert_eq!(users, 2);
}

#[test]
fn should_upsert_lease_given_incremental_update() {
    // Arrange
    let read_model = AdminReadModel::default();

    // Act
    read_model.upsert_lease(LeaseInfo::snapshot(
        1,
        "acme",
        "locks",
        "billing",
        "session:10",
        "2026-03-31T00:00:00Z",
        "2026-03-31T00:00:30Z".to_string(),
        0,
        7,
    ));
    let leases = read_model.leases(Some("acme"));

    // Assert
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].resource, "billing");
    assert_eq!(leases[0].fencing_token, 7);
}

#[test]
fn should_remove_lease_given_incremental_update() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_lease(LeaseInfo::snapshot(
        1,
        "acme",
        "locks",
        "billing",
        "session:10",
        "2026-03-31T00:00:00Z",
        "2026-03-31T00:00:30Z".to_string(),
        0,
        7,
    ));

    // Act
    read_model.remove_lease(1, "acme", "locks", "billing");
    let leases = read_model.leases(Some("acme"));

    // Assert
    assert!(leases.is_empty());
}

#[test]
fn should_replace_existing_lease_given_matching_identity_on_upsert() {
    // Arrange
    let read_model = AdminReadModel::default();
    read_model.upsert_lease(LeaseInfo::snapshot(
        1,
        "acme",
        "locks",
        "billing",
        "session:10",
        "2026-03-31T00:00:00Z",
        "2026-03-31T00:00:30Z".to_string(),
        0,
        7,
    ));

    // Act
    read_model.upsert_lease(LeaseInfo::snapshot(
        1,
        "acme",
        "locks",
        "billing",
        "session:11",
        "2026-03-31T00:00:05Z",
        "2026-03-31T00:00:40Z".to_string(),
        0,
        8,
    ));
    let leases = read_model.leases(Some("acme"));

    // Assert
    assert_eq!(leases.len(), 1);
    assert_eq!(leases[0].owner_session_id, "session:11");
    assert_eq!(leases[0].fencing_token, 8);
}
