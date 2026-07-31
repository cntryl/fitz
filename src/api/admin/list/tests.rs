use super::*;
use crate::boot::domains::DomainHandles;
use crate::boot::Runtime;
use crate::control::admin::QueueInfoSnapshot;
use crate::domains::kv::sink::KvDomainSink;
use crate::domains::lease::sink::LeaseDomainSink;
use crate::domains::notice::sink::NoticeDomainSink;
use crate::domains::queue::sink::QueueDomainSink;
use crate::domains::rpc::sink::RpcDomainSink;
use crate::domains::schedule::sink::ScheduleDomainSink;
use crate::domains::schedule::store::{ScheduleInsert, ScheduleStore};
use crate::domains::stream::sink::StreamDomainSink;
use crate::runtime::Router;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn current_epoch_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

fn snapshot_runtime() -> Arc<Runtime> {
    let read_model = crate::control::admin::read_model::AdminReadModel::new();
    Arc::new(Runtime::with_admin_read_model(
        Arc::new(Router::new()),
        read_model,
    ))
}

fn runtime_with_preloaded_schedule() -> Arc<Runtime> {
    let router = Arc::new(Router::new());
    let admin_read_model = crate::control::admin::read_model::AdminReadModel::new();
    let runtime = Arc::new(Runtime::with_admin_read_model(
        router.clone(),
        admin_read_model.clone(),
    ));
    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let schedule_store = ScheduleStore::new(store.clone());
    let payload = Bytes::from_static(b"nightly");
    let now_ms = current_epoch_ms();

    schedule_store
        .insert(
            1,
            ScheduleInsert {
                route: "schedule://acme/jobs/invoices/send",
                cron: "0 * * * *",
                delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms: now_ms.saturating_add(60_000),
                previous_fire_ms: None,
                last_fire_ms: Some(now_ms.saturating_sub(1_000)),
                executions_total: 7,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert schedule");

    let domains = Arc::new(DomainHandles::new(
        Arc::new(KvDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            crate::utils::idempotency::default_dedup_store(),
        )),
        Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(StreamDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            crate::domains::stream::sink::StreamStorageWriteOptions::local(),
        )),
        Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone())),
        Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(ScheduleDomainSink::new(
            store,
            router,
            admin_read_model.clone(),
        )),
    ));

    domains
        .preload_schedule_families()
        .expect("preload schedules");
    runtime.attach_domains(domains);
    runtime
}

#[test]
fn should_match_resource_ref_given_matching_resource_path() {
    // Arrange
    let path = ResourcePath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
    };
    let resource = ResourceRef {
        realm: "acme".to_string(),
        area: "billing".to_string(),
        resource: "invoices".to_string(),
    };

    // Act
    let result = resource.matches_path(&path);

    // Assert
    assert!(result);
}

#[test]
fn should_match_resource_route_given_matching_path() {
    // Arrange
    let path = ResourcePath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
    };

    // Act
    let result = matches_resource_route("notice://acme/billing/invoices", &path);

    // Assert
    assert!(result);
}

#[test]
fn should_match_rpc_operation_given_matching_operation_path() {
    // Arrange
    let path = RpcOperationPath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
        operation: "send",
    };
    let operation = OwnedRpcOperation {
        realm: "acme".to_string(),
        area: "billing".to_string(),
        resource: "invoices".to_string(),
        operation: "send".to_string(),
    };

    // Act
    let result = operation.matches_operation_path(&path);

    // Assert
    assert!(result);
}

#[test]
fn should_match_operation_route_given_matching_path() {
    // Arrange
    let path = RpcOperationPath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
        operation: "send",
    };

    // Act
    let result = matches_operation_route("rpc://acme/billing/invoices/send", &path);

    // Assert
    assert!(result);
}

#[test]
fn should_collect_resource_refs_given_resource_items() {
    // Arrange
    let items = vec![QueueInfo::snapshot(&QueueInfoSnapshot {
        family: 1,
        realm: "acme",
        area: "billing",
        resource: "invoices",
        subscriptions_active: 0,
        messages_ready: 0,
        messages_delayed: 0,
        messages_inflight: 0,
        messages_dead_lettered: 0,
        messages_total: 0,
        oldest_message_age_seconds: 0,
        oldest_backlog_age_seconds: 0,
        backlog_age_buckets: QueueAgeBuckets::default(),
        delay_age_buckets: QueueAgeBuckets::default(),
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
    })];

    // Act
    let resources = collect_resource_refs(items);

    // Assert
    assert_eq!(resources.len(), 1);
    assert_eq!(resources[0].realm, "acme");
    assert_eq!(resources[0].area, "billing");
    assert_eq!(resources[0].resource, "invoices");
}

#[test]
fn should_collect_queue_rollups_given_queue_rows() {
    // Arrange
    let queues = vec![
        QueueInfo::snapshot(&QueueInfoSnapshot {
            family: 1,
            realm: "prod",
            area: "jobs",
            resource: "emails",
            subscriptions_active: 2,
            messages_ready: 5,
            messages_delayed: 0,
            messages_inflight: 1,
            messages_dead_lettered: 0,
            messages_total: 6,
            oldest_message_age_seconds: 30,
            oldest_backlog_age_seconds: 30,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
            enqueue_success_total: 12,
            complete_success_total: 3,
            in_rate_per_second: 2.0,
            out_rate_per_second: 1.0,
        }),
        QueueInfo::snapshot(&QueueInfoSnapshot {
            family: 2,
            realm: "prod",
            area: "jobs",
            resource: "emails",
            subscriptions_active: 1,
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 1,
            messages_dead_lettered: 0,
            messages_total: 1,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
            enqueue_success_total: 2,
            complete_success_total: 2,
            in_rate_per_second: 0.5,
            out_rate_per_second: 0.5,
        }),
    ];

    // Act
    let realms = collect_queue_realms(&queues);
    let areas = collect_queue_areas(&queues, "prod");
    let resources = collect_queue_resources(&queues, "prod", "jobs");

    // Assert
    assert_eq!(realms.realms[0].queue_count, 1);
    assert_eq!(realms.realms[0].subscriptions_active, 3);
    assert_eq!(realms.realms[0].messages_total, 7);
    assert_eq!(realms.realms[0].status, "falling_behind");
    assert_eq!(areas.areas[0].queue_count, 1);
    assert_eq!(resources.resources[0].family_count, 2);
    assert!((resources.resources[0].in_rate_per_second - 2.5).abs() < f64::EPSILON);
    assert!((resources.resources[0].out_rate_per_second - 1.5).abs() < f64::EPSILON);
}

#[test]
fn should_collect_realms_given_duplicate_resources() {
    // Arrange
    let resources = vec![
        ResourceRef::new(
            "prod".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
        ),
        ResourceRef::new(
            "prod".to_string(),
            "jobs".to_string(),
            "pending".to_string(),
        ),
        ResourceRef::new(
            "staging".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
        ),
    ];

    // Act
    let collection = collect_realms(&resources);

    // Assert
    assert_eq!(collection.realms.len(), 2);
    assert_eq!(collection.realms[0].realm, "prod");
    assert_eq!(collection.realms[1].realm, "staging");
}

#[test]
fn should_collect_areas_given_realm_filter() {
    // Arrange
    let resources = vec![
        ResourceRef::new(
            "prod".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
        ),
        ResourceRef::new(
            "prod".to_string(),
            "jobs".to_string(),
            "pending".to_string(),
        ),
        ResourceRef::new(
            "staging".to_string(),
            "support".to_string(),
            "tickets".to_string(),
        ),
    ];

    // Act
    let collection = collect_areas(&resources, "prod");

    // Assert
    assert_eq!(collection.realm, "prod");
    assert_eq!(collection.areas.len(), 2);
    assert_eq!(collection.areas[0].area, "billing");
    assert_eq!(collection.areas[1].area, "jobs");
}

#[test]
fn should_collect_resources_given_area_filter() {
    // Arrange
    let resources = vec![
        ResourceRef::new("prod".to_string(), "jobs".to_string(), "active".to_string()),
        ResourceRef::new(
            "prod".to_string(),
            "jobs".to_string(),
            "pending".to_string(),
        ),
        ResourceRef::new(
            "prod".to_string(),
            "billing".to_string(),
            "invoices".to_string(),
        ),
    ];

    // Act
    let collection = collect_resources(&resources, "prod", "jobs");

    // Assert
    assert_eq!(collection.realm, "prod");
    assert_eq!(collection.area, "jobs");
    assert_eq!(collection.resources.len(), 2);
    assert_eq!(collection.resources[0].resource, "active");
    assert_eq!(collection.resources[1].resource, "pending");
}

#[test]
fn should_aggregate_stream_resource_rollups_across_families() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime.admin_read_model().replace_streams(vec![
        StreamInfo {
            route_family: 1,
            realm: "prod".to_string(),
            area: "events".to_string(),
            resource: "orders".to_string(),
            offset: 2,
            watermark: 2,
            size_bytes: 100,
            sessions_active: 1,
        },
        StreamInfo {
            route_family: 2,
            realm: "prod".to_string(),
            area: "events".to_string(),
            resource: "orders".to_string(),
            offset: 4,
            watermark: 4,
            size_bytes: 250,
            sessions_active: 2,
        },
    ]);

    // Act
    let collection = collect_stream_resources(&runtime, "prod", "events", None);

    // Assert
    assert_eq!(collection.resources.len(), 1);
    assert_eq!(collection.resources[0].committed_event_count, 8);
    assert_eq!(collection.resources[0].size_bytes, 350);
    assert_eq!(collection.resources[0].sessions_active, 3);
}

#[test]
fn should_isolate_stream_resource_rollups_by_family() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime.admin_read_model().replace_streams(vec![
        StreamInfo {
            route_family: 1,
            realm: "prod".to_string(),
            area: "events".to_string(),
            resource: "orders".to_string(),
            offset: 2,
            watermark: 2,
            size_bytes: 100,
            sessions_active: 1,
        },
        StreamInfo {
            route_family: 2,
            realm: "prod".to_string(),
            area: "events".to_string(),
            resource: "orders".to_string(),
            offset: 4,
            watermark: 4,
            size_bytes: 250,
            sessions_active: 2,
        },
    ]);

    // Act
    let collection = collect_stream_resources(&runtime, "prod", "events", Some(1));

    // Assert
    assert_eq!(collection.resources[0].committed_event_count, 3);
    assert_eq!(collection.resources[0].size_bytes, 100);
    assert_eq!(collection.resources[0].sessions_active, 1);
}

#[test]
fn should_aggregate_notice_resource_counters_and_exclude_invalid_routes() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_notice_subscriptions(vec![NoticeSubscription {
            route_family: 1,
            subscription_id: 7,
            session_id: "session".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders".to_string(),
            created_at: "2026-07-31T12:00:00Z".to_string(),
            notifications_received: 12,
        }]);
    runtime.admin_read_model().replace_notice_routes(vec![
        NoticeRouteInfo {
            route_family: 1,
            route: "notice://prod/events/orders".to_string(),
            subscribers: 1,
            publishes_total: 20,
            publishes_per_minute: 2.5,
        },
        NoticeRouteInfo {
            route_family: 1,
            route: "invalid".to_string(),
            subscribers: 1,
            publishes_total: 20,
            publishes_per_minute: 9.0,
        },
    ]);

    // Act
    let collection = collect_notice_resources(&runtime, "prod", "events", Some(1));

    // Assert
    assert_eq!(collection.resources.len(), 1);
    assert_eq!(collection.resources[0].subscriptions_active, 1);
    assert_eq!(collection.resources[0].notifications_received, 12);
    assert!((collection.resources[0].publishes_per_minute - 2.5).abs() < f64::EPSILON);
}

#[test]
fn should_include_pending_only_rpc_resource_with_missing_latency() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_rpc_pending(vec![RpcPendingRequest {
            route_family: 1,
            correlation_id: "request".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            submitted_at: "2026-07-31T12:00:00Z".to_string(),
            age_seconds: 1,
            worker_session_id: None,
        }]);

    // Act
    let collection = collect_rpc_resources(&runtime, "prod", "jobs", Some(1));

    // Assert
    assert_eq!(collection.resources.len(), 1);
    assert_eq!(collection.resources[0].workers_registered, 0);
    assert_eq!(collection.resources[0].requests_pending, 1);
    assert_eq!(
        collection.resources[0].slowest_worker_average_latency_ms,
        None
    );
}

#[test]
fn should_select_slowest_rpc_worker_latency_across_duplicate_paths() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime.admin_read_model().replace_rpc_workers(vec![
        RpcWorker {
            route_family: 1,
            session_id: "one".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/run".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 2,
            average_latency_ms: 3.0,
        },
        RpcWorker {
            route_family: 2,
            session_id: "two".to_string(),
            realm: "prod".to_string(),
            route: "rpc://prod/jobs/reconcile/execute".to_string(),
            registered_at: "2026-07-31T12:00:00Z".to_string(),
            requests_handled: 1,
            average_latency_ms: 8.0,
        },
    ]);

    // Act
    let collection = collect_rpc_resources(&runtime, "prod", "jobs", None);

    // Assert
    assert_eq!(collection.resources[0].workers_registered, 2);
    assert_eq!(
        collection.resources[0].slowest_worker_average_latency_ms,
        Some(8.0)
    );
}

#[test]
fn should_leave_schedule_next_run_missing_given_disabled_definitions() {
    // Arrange
    let runtime = snapshot_runtime();
    runtime
        .admin_read_model()
        .replace_schedules(vec![ScheduleInfo {
            route_family: 1,
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "cleanup".to_string(),
            operation: "run".to_string(),
            cron: "0 * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            next_run: "2026-08-01T12:00:00Z".to_string(),
            last_run: None,
            executions_total: 0,
            enabled: false,
        }]);

    // Act
    let collection = collect_schedule_resources(&runtime, "prod", "jobs", Some(1));

    // Assert
    assert_eq!(collection.resources[0].schedules_active, 0);
    assert_eq!(collection.resources[0].pending_claims, 0);
    assert_eq!(collection.resources[0].next_run, None);
}

#[test]
fn should_aggregate_schedule_detail_given_multiple_schedules() {
    // Arrange
    let path = ResourcePath {
        realm: "acme",
        area: "billing",
        resource: "invoices",
    };
    let schedules = vec![
        ScheduleInfo {
            route_family: 1,
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "send".to_string(),
            cron: "0 * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            next_run: "2026-03-31T02:00:00Z".to_string(),
            last_run: None,
            executions_total: 2,
            enabled: false,
        },
        ScheduleInfo {
            route_family: 1,
            realm: "acme".to_string(),
            area: "billing".to_string(),
            resource: "invoices".to_string(),
            operation: "retry".to_string(),
            cron: "*/5 * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            next_run: "2026-03-31T01:00:00Z".to_string(),
            last_run: None,
            executions_total: 3,
            enabled: true,
        },
    ];

    // Act
    let detail = ScheduleResourceDetail::aggregate(&path, &schedules);

    // Assert
    assert!(detail.enabled);
    assert_eq!(detail.cron, None);
    assert_eq!(detail.next_run.as_deref(), Some("2026-03-31T01:00:00Z"));
    assert_eq!(detail.executions_total, 5);
}

#[test]
fn should_expose_persisted_schedule_execution_state_given_preloaded_runtime() {
    // Arrange
    let runtime = runtime_with_preloaded_schedule();
    let path = ResourcePath {
        realm: "acme",
        area: "jobs",
        resource: "invoices",
    };

    // Act
    let schedules = runtime.schedule_list_schedules(Some("acme"));
    let detail = schedule_detail(runtime.as_ref(), &path, None);

    // Assert
    assert_eq!(schedules.len(), 1);
    assert_eq!(schedules[0].operation, "send");
    assert!(schedules[0].last_run.is_some());
    assert_eq!(schedules[0].executions_total, 7);
    assert!(detail.enabled);
    assert_eq!(detail.cron.as_deref(), Some("0 * * * *"));
    assert_eq!(detail.executions_total, 7);
    assert!(detail.next_run.is_some());
}
