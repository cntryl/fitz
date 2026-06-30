use super::*;
use crate::boot::domains::{
    DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink,
    ScheduleDomainSink, StreamDomainSink,
};
use crate::boot::Runtime;
use crate::control::admin::QueueInfoSnapshot;
use crate::domains::schedule::store::{ScheduleInsert, ScheduleStore};
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
                payload: &payload,
                next_fire_ms: now_ms.saturating_add(60_000),
                previous_fire_ms: None,
                last_fire_ms: Some(now_ms.saturating_sub(1_000)),
                executions_total: 7,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert schedule");

    let domains = Arc::new(DomainHandles {
        kv: Arc::new(KvDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        queue: Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            crate::utils::idempotency::default_dedup_store(),
        )),
        notice: Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        stream: Arc::new(StreamDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        rpc: Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone())),
        lease: Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        schedule: Arc::new(ScheduleDomainSink::new(
            store,
            router,
            admin_read_model.clone(),
        )),
    });

    domains
        .schedule
        .preload_persisted_families()
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
