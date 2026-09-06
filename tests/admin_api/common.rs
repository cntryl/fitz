//! Integration test for admin REST API

pub(crate) use bytes::Bytes;
pub(crate) use fitz::api::admin::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueAgeBuckets,
    QueueDeadLetter, QueueInflight, QueueInfo, RpcPendingRequest, RpcWorker, ScheduleInfo,
    StreamAreaWatermark, StreamAreaWatermarkDetail, StreamInfo, StreamRealmWatermark,
    StreamRealmWatermarkDetail,
};
pub(crate) use fitz::api::http::Body;
pub(crate) use fitz::api::runtime_ingress::{Ingress, RuntimeIngress};
pub(crate) use fitz::boot::domains::DomainHandles;
pub(crate) use fitz::boot::{BootConfig, Runtime};
pub(crate) use fitz::domains::kv::sink::KvDomainSink;
pub(crate) use fitz::domains::kv::{KvActor, KvMessage, KvResourceScope, KvResponse, TxMode};
pub(crate) use fitz::domains::lease::sink::LeaseDomainSink;
pub(crate) use fitz::domains::notice::sink::NoticeDomainSink;
pub(crate) use fitz::domains::queue::sink::QueueDomainSink;
pub(crate) use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
pub(crate) use fitz::domains::rpc::sink::RpcDomainSink;
pub(crate) use fitz::domains::schedule::protocol::parse_concrete_schedule_route;
pub(crate) use fitz::domains::schedule::sink::ScheduleDomainSink;
pub(crate) use fitz::domains::schedule::store::{ScheduleFireClaim, ScheduleInsert, ScheduleStore};
pub(crate) use fitz::domains::stream::protocol::StreamWriteMode;
pub(crate) use fitz::domains::stream::sink::StreamDomainSink;
pub(crate) use fitz::domains::stream::store::{CommitRecordsParams, EventPayload, StreamStore};
pub(crate) use fitz::runtime::routing::RouteFamily;
pub(crate) use fitz::runtime::Router;
pub(crate) use fitz::session::{
    SessionInfo as RuntimeSessionInfo, SessionMetadata, SessionPermissions, TransportKind,
};
pub(crate) use fitz::testkit::body;
pub(crate) use hyper::header::{COOKIE, SET_COOKIE};
pub(crate) use hyper::{Method, StatusCode};
pub(crate) use serial_test::serial;
use std::fmt::Write as _;
pub(crate) use std::sync::Arc;
pub(crate) use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn configure_admin_auth() {
    std::env::set_var("FITZ_ROOT_PASSWORD", "pwd123");
    std::env::set_var("FITZ_ADMIN_SESSION_TTL_SECS", "3600");
}

pub(crate) fn test_runtime() -> Arc<Runtime> {
    fitz::boot::observability::metrics().clear();
    configure_admin_auth();
    let router = Arc::new(Router::new());
    let runtime = Runtime::new(router);
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
    Arc::new(runtime)
}

pub(crate) fn test_runtime_not_ready() -> Arc<Runtime> {
    fitz::boot::observability::metrics().clear();
    configure_admin_auth();
    let router = Arc::new(Router::new());
    Arc::new(Runtime::new(router))
}

pub(crate) fn test_runtime_from_boot_config(assume_external_tls: bool) -> Arc<Runtime> {
    fitz::boot::observability::metrics().clear();
    configure_admin_auth();
    let config = BootConfig::new()
        .with_auth_config(fitz::auth::AuthConfig::Disabled)
        .with_bind_addr("127.0.0.1".to_string())
        .with_assume_external_tls(assume_external_tls);
    let (_, _, _, runtime) = fitz::boot::runtime::init(&config).expect("initialize runtime");
    Arc::new(runtime)
}

pub(crate) struct EnvGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvGuard {
    pub(crate) fn unset(key: &'static str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::remove_var(key);
        Self { key, previous }
    }

    pub(crate) fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        match &self.previous {
            Some(value) => std::env::set_var(self.key, value),
            None => std::env::remove_var(self.key),
        }
    }
}

pub(crate) fn assert_browser_security_headers(headers: &hyper::HeaderMap) {
    assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(headers.get("referrer-policy").unwrap(), "no-referrer");
    assert_eq!(
        headers.get("content-security-policy").unwrap(),
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; font-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
    );
}

pub(crate) fn assert_prometheus_counter(metrics: &str, name: &str, value: u64) {
    assert!(
        metrics
            .lines()
            .any(|line| line.starts_with(&format!("# HELP {name} "))),
        "missing HELP line for {name}"
    );
    assert!(
        metrics.contains(&format!("# TYPE {name} counter")),
        "missing TYPE line for {name}"
    );
    assert!(
        metrics.contains(&format!("{name} {value}")),
        "missing sample line for {name}={value}"
    );
}

/// Convert the structured metrics response into a test-only text view so the
/// historical metric assertions continue to verify names, kinds, labels, and
/// values without making the production API parse Prometheus text.
pub(crate) fn structured_metrics_text(body: &[u8]) -> String {
    let payload: serde_json::Value = serde_json::from_slice(body).expect("structured metrics JSON");
    let mut output = String::new();
    for sample in payload["samples"].as_array().expect("metrics samples") {
        let name = sample["name"].as_str().expect("metric name");
        let help = sample["help"].as_str().expect("metric help");
        let kind = sample["kind"].as_str().expect("metric kind");
        writeln!(&mut output, "# HELP {name} {help}").expect("write metric help");
        writeln!(&mut output, "# TYPE {name} {kind}").expect("write metric type");
        let labels = sample["labels"].as_object().expect("metric labels");
        let labels = labels
            .iter()
            .map(|(key, value)| {
                format!("{key}=\"{}\"", value.as_str().expect("metric label value"))
            })
            .collect::<Vec<_>>()
            .join(",");
        let sample_name = if labels.is_empty() {
            name.to_string()
        } else {
            format!("{name}{{{labels}}}")
        };
        let value = sample["value"].as_f64().expect("metric value");
        let value = if value.is_finite() && value.fract() == 0.0 {
            format!("{value:.0}")
        } else {
            value.to_string()
        };
        writeln!(&mut output, "{sample_name} {value}").expect("write metric sample");
    }
    output
}

pub(crate) fn mark_runtime_ready(runtime: &Runtime) {
    runtime.mark_storage_ready();
    runtime.mark_domains_ready();
    runtime.mark_auth_config_ready();
    runtime.mark_startup_complete();
}

pub(crate) fn queue_runtime_with_domains() -> (Arc<Runtime>, Arc<cntryl_midge::Engine>) {
    configure_admin_auth();
    let router = Arc::new(Router::new());
    let runtime = Arc::new(Runtime::new(router.clone()));
    let admin_read_model = runtime.admin_read_model();
    let store = fitz::testkit::create_test_engine_with_cfs(vec![1]);

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
            fitz::utils::idempotency::default_dedup_store(),
        )),
        Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(StreamDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            fitz::domains::stream::sink::StreamStorageWriteOptions::local(),
        )),
        Arc::new(RpcDomainSink::new(router.clone(), admin_read_model.clone())),
        Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(ScheduleDomainSink::new(
            store.clone(),
            router,
            admin_read_model.clone(),
        )),
    ));

    runtime.attach_domains(domains);
    mark_runtime_ready(runtime.as_ref());
    (runtime, store)
}

pub(crate) fn schedule_runtime_with_domains() -> (
    Arc<Runtime>,
    Arc<cntryl_midge::Engine>,
    Arc<ScheduleDomainSink>,
) {
    configure_admin_auth();
    let router = Arc::new(Router::new());
    let runtime = Arc::new(Runtime::new(router.clone()));
    let admin_read_model = runtime.admin_read_model();
    let store = fitz::testkit::create_test_engine_with_cfs(vec![1]);
    let schedule = Arc::new(ScheduleDomainSink::new(
        store.clone(),
        router,
        admin_read_model.clone(),
    ));

    let domains = Arc::new(DomainHandles::new(
        Arc::new(KvDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
        )),
        Arc::new(QueueDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            fitz::utils::idempotency::default_dedup_store(),
        )),
        Arc::new(NoticeDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        Arc::new(StreamDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
            fitz::domains::stream::sink::StreamStorageWriteOptions::local(),
        )),
        Arc::new(RpcDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        Arc::new(LeaseDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        schedule.clone(),
    ));

    runtime.attach_domains(domains);
    mark_runtime_ready(runtime.as_ref());
    (runtime, store, schedule)
}

pub(crate) fn current_epoch_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}

pub(crate) fn seed_dead_lettered_queue_message(store: Arc<cntryl_midge::Engine>) -> u64 {
    const ADMIN_QUEUE_SEED_SESSION_ID: u64 = 1;

    let family = RouteFamily::new(1);
    let queue_key = QueueKey {
        family,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
    };
    let mut actor = QueueActor::new(
        family,
        queue_key,
        store,
        Some(2),
        fitz::utils::idempotency::default_dedup_store(),
    );
    let message_id = match actor.handle_send(Bytes::from_static(b"email"), None) {
        QueueResponse::Sent { id } => id,
        other => panic!("Expected Sent response, found {other:?}"),
    };

    for _ in 0..2 {
        match actor.handle_receive_for_session(ADMIN_QUEUE_SEED_SESSION_ID, 0, Some(1)) {
            QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
            other => panic!("Expected Received response, found {other:?}"),
        }
        actor.process_expired_timers();
    }

    message_id.as_u64()
}

pub(crate) fn seed_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_kv_transactions(vec![KvTransaction {
        route_family: 1,
        tx_id: 41,
        realm: "prod".to_string(),
        area: "app".to_string(),
        resource: "users".to_string(),
        mode: "readwrite".to_string(),
        started_at: "2026-03-14T12:00:00Z".to_string(),
        operations_count: 3,
        idle_seconds: 1,
    }]);
    read_model.replace_notice_subscriptions(vec![
        NoticeSubscription {
            route_family: 1,
            subscription_id: 7,
            session_id: "123".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: "2026-03-14T12:00:00Z".to_string(),
            notifications_received: 5,
        },
        NoticeSubscription {
            route_family: 1,
            subscription_id: 8,
            session_id: "124".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: "2026-03-14T12:00:05Z".to_string(),
            notifications_received: 2,
        },
        NoticeSubscription {
            route_family: 1,
            subscription_id: 9,
            session_id: "125".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/updated".to_string(),
            created_at: "2026-03-14T12:00:10Z".to_string(),
            notifications_received: 1,
        },
    ]);
    read_model.replace_notice_routes(vec![
        NoticeRouteInfo {
            route_family: 1,
            route: "notice://prod/events/orders/created".to_string(),
            subscribers: 2,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
        NoticeRouteInfo {
            route_family: 1,
            route: "notice://prod/events/orders/updated".to_string(),
            subscribers: 1,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
    ]);
    read_model.replace_rpc_workers(vec![RpcWorker {
        route_family: 1,
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![RpcPendingRequest {
        route_family: 1,
        correlation_id: "corr-abc-123".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        submitted_at: "2026-03-14T12:00:07Z".to_string(),
        age_seconds: 7,
        worker_session_id: Some("9001".to_string()),
    }]);
}

pub(crate) fn seed_queue_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        subscriptions_active: 0,
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 4,
        messages_total: 10,
        oldest_message_age_seconds: 9,
        oldest_backlog_age_seconds: 600,
        backlog_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 1,
            under_15m: 1,
            over_15m: 0,
        },
        delay_age_buckets: QueueAgeBuckets {
            under_1m: 1,
            under_5m: 0,
            under_15m: 0,
            over_15m: 1,
        },
        enqueue_success_total: 0,
        complete_success_total: 0,
        in_rate_per_second: 0.0,
        out_rate_per_second: 0.0,
        status: "backlogged".to_string(),
    }]);
    read_model.replace_queue_dead_letters(vec![QueueDeadLetter {
        message_id: 42,
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        dead_lettered_at: "2099-01-01T00:00:00Z".to_string(),
        attempts: 3,
        reason: "max_attempts_exceeded".to_string(),
    }]);
}

pub(crate) fn seed_queue_compare_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![
        QueueInfo {
            family: 1,
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "worker".to_string(),
            subscriptions_active: 0,
            messages_ready: 1,
            messages_delayed: 2,
            messages_inflight: 3,
            messages_dead_lettered: 4,
            messages_total: 10,
            oldest_message_age_seconds: 9,
            oldest_backlog_age_seconds: 600,
            backlog_age_buckets: QueueAgeBuckets {
                under_1m: 1,
                under_5m: 1,
                under_15m: 1,
                over_15m: 0,
            },
            delay_age_buckets: QueueAgeBuckets {
                under_1m: 1,
                under_5m: 0,
                under_15m: 0,
                over_15m: 1,
            },
            enqueue_success_total: 0,
            complete_success_total: 0,
            in_rate_per_second: 0.0,
            out_rate_per_second: 0.0,
            status: "backlogged".to_string(),
        },
        QueueInfo {
            family: 2,
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "backup".to_string(),
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
            status: "idle".to_string(),
        },
    ]);
    read_model.replace_queue_dead_letters(vec![QueueDeadLetter {
        message_id: 42,
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        dead_lettered_at: "2099-01-01T00:00:00Z".to_string(),
        attempts: 3,
        reason: "max_attempts_exceeded".to_string(),
    }]);
}

pub(crate) fn seed_pending_schedule_claim(store: Arc<cntryl_midge::Engine>) {
    let schedule_store = ScheduleStore::new(store);
    let route = "schedule://prod/jobs/billing/send";
    let payload = Bytes::from_static(b"billing");
    let now_ms = current_epoch_ms();
    let claimed_fire_ms = now_ms.saturating_sub(30_000);
    let next_fire_ms = now_ms.saturating_add(30_000);
    let last_fire_ms = Some(now_ms.saturating_sub(1_000));
    let route_parts = parse_concrete_schedule_route(route).expect("valid schedule route");

    schedule_store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms: claimed_fire_ms,
                previous_fire_ms: None,
                last_fire_ms,
                executions_total: 4,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert schedule");
    schedule_store
        .claim_due_batch(
            1,
            &[ScheduleFireClaim {
                route,
                route_parts: &route_parts,
                cron: "* * * * *",
                delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                claimed_at_ms: claimed_fire_ms,
                next_fire_ms,
                previous_fire_ms: claimed_fire_ms,
                last_fire_ms,
                executions_total: 4,
            }],
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("claim schedule fire");
}

pub(crate) fn seed_active_schedule_definition(store: Arc<cntryl_midge::Engine>) {
    let schedule_store = ScheduleStore::new(store);
    let route = "schedule://prod/jobs/billing/send";
    let payload = Bytes::from_static(b"billing");
    let now_ms = current_epoch_ms();
    let next_fire_ms = now_ms.saturating_add(30_000);
    let last_fire_ms = Some(now_ms.saturating_sub(1_000));

    schedule_store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms,
                previous_fire_ms: None,
                last_fire_ms,
                executions_total: 7,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert active schedule");
}

pub(crate) fn seed_stream_snapshot_data(store: Arc<cntryl_midge::Engine>) {
    let stream_store = StreamStore::new(store);

    let logs_application = [EventPayload {
        body: Bytes::from_static(b"app-log"),
        metadata: None,
        discriminator: None,
    }];
    stream_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "prod",
            area: "logs",
            resource: "application",
            expected_resource_next_offset: 0,
            events: &logs_application,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit application stream record");

    let logs_system = [EventPayload {
        body: Bytes::from_static(b"system-log"),
        metadata: None,
        discriminator: None,
    }];
    stream_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "prod",
            area: "logs",
            resource: "system",
            expected_resource_next_offset: 0,
            events: &logs_system,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit system stream record");

    let audit_events = [EventPayload {
        body: Bytes::from_static(b"audit-log"),
        metadata: None,
        discriminator: None,
    }];
    stream_store
        .commit_records(CommitRecordsParams {
            family: 1,
            realm: "prod",
            area: "audit",
            resource: "events",
            expected_resource_next_offset: 0,
            events: &audit_events,
            ingest_metadata: None,
            mode: StreamWriteMode::Sync,
        })
        .expect("commit audit stream record");
}

pub(crate) fn seed_stream_watermark_lag_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_stream_area_watermarks(vec![
        StreamAreaWatermarkDetail {
            realm: "prod".to_string(),
            area: "logs".to_string(),
            resource_count: 3,
            family_watermarks: vec![
                StreamAreaWatermark {
                    family: 1,
                    watermark: 100,
                },
                StreamAreaWatermark {
                    family: 2,
                    watermark: 92,
                },
                StreamAreaWatermark {
                    family: 3,
                    watermark: 50,
                },
            ],
        },
        StreamAreaWatermarkDetail {
            realm: "prod".to_string(),
            area: "audit".to_string(),
            resource_count: 2,
            family_watermarks: vec![
                StreamAreaWatermark {
                    family: 1,
                    watermark: 20,
                },
                StreamAreaWatermark {
                    family: 2,
                    watermark: 0,
                },
            ],
        },
        StreamAreaWatermarkDetail {
            realm: "prod".to_string(),
            area: "infra".to_string(),
            resource_count: 2,
            family_watermarks: vec![
                StreamAreaWatermark {
                    family: 1,
                    watermark: 300,
                },
                StreamAreaWatermark {
                    family: 2,
                    watermark: 150,
                },
            ],
        },
    ]);
}

pub(crate) fn seed_committed_kv_values(
    store: Arc<cntryl_midge::Engine>,
    family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    entries: &[(&[u8], &[u8])],
) {
    let route_family = RouteFamily::try_from(family).expect("test family must fit in u32");
    let scope = KvResourceScope::new(route_family, realm, area, resource);
    let mut actor = KvActor::new(store);
    let tx_id = match actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    }) {
        KvResponse::BeginOk { tx_id } => tx_id,
        other => panic!("Expected BeginOk response, found {other:?}"),
    };

    for (key, value) in entries {
        match actor.handle(KvMessage::Put {
            tx_id,
            scope: scope.clone(),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        }) {
            KvResponse::PutOk => {}
            other => panic!("Expected PutOk response, found {other:?}"),
        }
    }

    match actor.handle(KvMessage::Commit { tx_id, scope }) {
        KvResponse::CommitOk => {}
        other => panic!("Expected CommitOk response, found {other:?}"),
    }
}

pub(crate) fn delete_committed_kv_range(
    store: Arc<cntryl_midge::Engine>,
    family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    start: &[u8],
    end: &[u8],
) {
    let route_family = RouteFamily::try_from(family).expect("test family must fit in u32");
    let scope = KvResourceScope::new(route_family, realm, area, resource);
    let mut actor = KvActor::new(store);
    let tx_id = match actor.handle(KvMessage::Begin {
        scope: scope.clone(),
        mode: TxMode::ReadWrite,
        write_options: cntryl_midge::WriteOptions::buffered().into(),
    }) {
        KvResponse::BeginOk { tx_id } => tx_id,
        other => panic!("Expected BeginOk response, found {other:?}"),
    };

    match actor.handle(KvMessage::DeleteRange {
        tx_id,
        scope: scope.clone(),
        start: Bytes::copy_from_slice(start),
        end: Bytes::copy_from_slice(end),
    }) {
        KvResponse::DeleteRangeOk => {}
        other => panic!("Expected DeleteRangeOk response, found {other:?}"),
    }

    match actor.handle(KvMessage::Commit { tx_id, scope }) {
        KvResponse::CommitOk => {}
        other => panic!("Expected CommitOk response, found {other:?}"),
    }
}

pub(crate) fn seed_stream_latency_pressure_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_streams(vec![StreamInfo {
        route_family: 1,
        realm: "prod".to_string(),
        area: "logs".to_string(),
        resource: "application".to_string(),
        offset: 0,
        watermark: 0,
        size_bytes: 0,
        sessions_active: 0,
    }]);
}

pub(crate) fn seed_cross_family_stream_watermark_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_streams(vec![
        StreamInfo {
            route_family: 1,
            realm: "prod".to_string(),
            area: "logs".to_string(),
            resource: "application".to_string(),
            offset: 4,
            watermark: 3,
            size_bytes: 32,
            sessions_active: 0,
        },
        StreamInfo {
            route_family: 2,
            realm: "prod".to_string(),
            area: "logs".to_string(),
            resource: "security".to_string(),
            offset: 9,
            watermark: 8,
            size_bytes: 64,
            sessions_active: 0,
        },
    ]);
    read_model.replace_stream_realm_watermarks(vec![StreamRealmWatermarkDetail {
        realm: "prod".to_string(),
        area_count: 1,
        resource_count: 2,
        family_watermarks: vec![
            StreamRealmWatermark {
                family: 1,
                watermark: 3,
            },
            StreamRealmWatermark {
                family: 2,
                watermark: 8,
            },
        ],
    }]);
    read_model.replace_stream_area_watermarks(vec![StreamAreaWatermarkDetail {
        realm: "prod".to_string(),
        area: "logs".to_string(),
        resource_count: 2,
        family_watermarks: vec![
            StreamAreaWatermark {
                family: 1,
                watermark: 3,
            },
            StreamAreaWatermark {
                family: 2,
                watermark: 8,
            },
        ],
    }]);
}

pub(crate) async fn login_cookie(runtime: Arc<Runtime>) -> String {
    let req = hyper::http::Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .header("host", "localhost")
        .header("origin", "http://localhost")
        .body(Body::from(r#"{"username":"root","password":"pwd123"}"#))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_string()
}

pub(crate) fn expired_admin_cookie() -> String {
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap_or(i64::MAX);
    let claims = serde_json::json!({
        "sub": "root",
        "role": "admin",
        "iat": now - 7_200,
        "exp": now - 3_600,
    });
    let token = jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        &claims,
        &jsonwebtoken::EncodingKey::from_secret(b"jwt-secret"),
    )
    .unwrap();
    format!("fitz_admin_session={token}")
}

pub(crate) fn assert_clear_admin_cookie(response: &fitz::api::http::Response) {
    let set_cookie = response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(set_cookie.contains("fitz_admin_session=;"));
    assert!(set_cookie.contains("; Max-Age=0"));
    assert!(set_cookie.contains("; HttpOnly"));
    assert!(set_cookie.contains("; Secure"));
    assert!(set_cookie.contains("; SameSite=Strict"));
}
