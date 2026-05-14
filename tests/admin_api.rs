//! Integration test for admin REST API

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use bytes::Bytes;
use fitz::api::admin::{
    KvTransaction, LeaseInfo, NoticeRouteInfo, NoticeSubscription, QueueAgeBuckets,
    QueueDeadLetter, QueueInfo, RpcPendingRequest, RpcWorker, StreamAreaWatermark,
    StreamAreaWatermarkDetail, StreamInfo,
};
use fitz::boot::domains::{
    DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink,
    ScheduleDomainSink, StreamDomainSink,
};
use fitz::boot::Runtime;
use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
use fitz::domains::schedule::store::{ScheduleFireClaim, ScheduleInsert, ScheduleStore};
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::store::{CommitRecordsParams, EventPayload, StreamStore};
use fitz::runtime::routing::RouteFamily;
use fitz::runtime::Router;
use hyper::header::{COOKIE, SET_COOKIE};
use hyper::{body, Body, Method, Request, StatusCode};
use serial_test::serial;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn password_hash_for(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn configure_admin_auth() {
    std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
    std::env::set_var("FITZ_ADMIN_PASSWORD_HASH", password_hash_for("pwd123"));
    std::env::set_var("FITZ_ADMIN_JWT_SECRET", "jwt-secret");
    std::env::set_var("FITZ_ADMIN_SESSION_TTL_SECS", "3600");
}

fn test_runtime() -> Arc<Runtime> {
    configure_admin_auth();
    let router = Arc::new(Router::new());
    Arc::new(Runtime::new(router))
}

fn queue_runtime_with_domains() -> (Arc<Runtime>, Arc<cntryl_midge::Engine>) {
    configure_admin_auth();
    let router = Arc::new(Router::new());
    let runtime = Arc::new(Runtime::new(router.clone()));
    let admin_read_model = runtime.admin_read_model();
    let store = fitz::testkit::create_test_engine_with_cfs(vec![1]);

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
            fitz::utils::idempotency::default_dedup_store(),
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
            store.clone(),
            router,
            admin_read_model.clone(),
        )),
    });

    runtime.attach_domains(domains);
    (runtime, store)
}

fn schedule_runtime_with_domains() -> (
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

    let domains = Arc::new(DomainHandles {
        kv: Arc::new(KvDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
        )),
        queue: Arc::new(QueueDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            fitz::utils::idempotency::default_dedup_store(),
        )),
        notice: Arc::new(NoticeDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        stream: Arc::new(StreamDomainSink::new(
            store.clone(),
            runtime.router(),
            admin_read_model.clone(),
        )),
        rpc: Arc::new(RpcDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        lease: Arc::new(LeaseDomainSink::new(
            runtime.router(),
            admin_read_model.clone(),
        )),
        schedule: schedule.clone(),
    });

    runtime.attach_domains(domains);
    (runtime, store, schedule)
}

fn current_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn seed_dead_lettered_queue_message(store: Arc<cntryl_midge::Engine>) -> u64 {
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
        match actor.handle_receive(0, Some(1)) {
            QueueResponse::Received { messages } => assert_eq!(messages.len(), 1),
            other => panic!("Expected Received response, found {other:?}"),
        }
        actor.process_expired_timers();
    }

    message_id.as_u64()
}

fn seed_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_kv_transactions(vec![KvTransaction {
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
            subscription_id: 7,
            session_id: "123".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: "2026-03-14T12:00:00Z".to_string(),
            notifications_received: 5,
        },
        NoticeSubscription {
            subscription_id: 8,
            session_id: "124".to_string(),
            realm: "prod".to_string(),
            pattern: "notice://prod/events/orders/created".to_string(),
            created_at: "2026-03-14T12:00:05Z".to_string(),
            notifications_received: 2,
        },
        NoticeSubscription {
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
            route: "notice://prod/events/orders/created".to_string(),
            subscribers: 2,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
        NoticeRouteInfo {
            route: "notice://prod/events/orders/updated".to_string(),
            subscribers: 1,
            publishes_total: 0,
            publishes_per_minute: 0.0,
        },
    ]);
    read_model.replace_rpc_workers(vec![RpcWorker {
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![RpcPendingRequest {
        correlation_id: "corr-abc-123".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        submitted_at: "2026-03-14T12:00:07Z".to_string(),
        age_seconds: 7,
        worker_session_id: Some("9001".to_string()),
    }]);
}

fn seed_queue_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
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

fn seed_queue_compare_snapshot_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![
        QueueInfo {
            family: 1,
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "worker".to_string(),
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
        },
        QueueInfo {
            family: 2,
            realm: "prod".to_string(),
            area: "jobs".to_string(),
            resource: "backup".to_string(),
            messages_ready: 0,
            messages_delayed: 0,
            messages_inflight: 0,
            messages_dead_lettered: 0,
            messages_total: 0,
            oldest_message_age_seconds: 0,
            oldest_backlog_age_seconds: 0,
            backlog_age_buckets: QueueAgeBuckets::default(),
            delay_age_buckets: QueueAgeBuckets::default(),
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

fn seed_pending_schedule_claim(store: Arc<cntryl_midge::Engine>) {
    let schedule_store = ScheduleStore::new(store);
    let route = "schedule://prod/jobs/billing/send";
    let payload = Bytes::from_static(b"billing");
    let now_ms = current_epoch_ms();
    let claimed_fire_ms = now_ms.saturating_sub(30_000);
    let next_fire_ms = now_ms.saturating_add(30_000);
    let last_fire_ms = Some(now_ms.saturating_sub(1_000));

    schedule_store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
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
                cron: "* * * * *",
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

fn seed_active_schedule_definition(store: Arc<cntryl_midge::Engine>) {
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

fn seed_stream_snapshot_data(store: Arc<cntryl_midge::Engine>) {
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

fn seed_stream_watermark_lag_data(runtime: &Arc<Runtime>) {
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

fn seed_stream_latency_pressure_data(runtime: &Arc<Runtime>) {
    let read_model = runtime.admin_read_model();
    read_model.replace_streams(vec![StreamInfo {
        realm: "prod".to_string(),
        area: "logs".to_string(),
        resource: "application".to_string(),
        offset: 0,
        watermark: 0,
        size_bytes: 0,
        sessions_active: 0,
    }]);
}

async fn login_cookie(runtime: Arc<Runtime>) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"username":"admin","password":"pwd123"}"#))
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

#[tokio::test]
#[serial]
async fn should_create_admin_session_and_set_cookie() {
    let runtime = test_runtime();
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .body(Body::from(r#"{"username":"admin","password":"pwd123"}"#))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(response
        .headers()
        .get(SET_COOKIE)
        .unwrap()
        .to_str()
        .unwrap()
        .contains("fitz_admin_session="));
}

#[tokio::test]
#[serial]
async fn should_require_auth_for_hierarchical_route() {
    let runtime = test_runtime();
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms")
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
#[serial]
async fn should_list_kv_realms_with_valid_cookie() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""realm":"prod""#));
}

#[tokio::test]
#[serial]
async fn should_return_area_collection_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_resource_collection_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_leaf_resource_detail() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/areas/logs/resources/application")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "logs");
    assert_eq!(payload["resource"], "application");
    assert_eq!(payload["diagnostics"]["current_stage"], "healthy");
    assert_eq!(payload["diagnostics"]["severity"], "informational");
}

#[tokio::test]
#[serial]
async fn should_return_stream_realm_watermarks_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/watermarks")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area_count"], 2);
    assert_eq!(payload["resource_count"], 3);
    assert_eq!(payload["family_watermarks"][0]["family"], 1);
    assert_eq!(payload["family_watermarks"][0]["watermark"], 2);
}

#[tokio::test]
#[serial]
async fn should_return_stream_area_watermarks_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/realms/prod/areas/logs/watermarks")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "logs");
    assert_eq!(payload["resource_count"], 2);
    assert_eq!(payload["family_watermarks"][0]["family"], 1);
    assert_eq!(payload["family_watermarks"][0]["watermark"], 1);
}

#[tokio::test]
#[serial]
async fn should_return_kv_transactions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/kv/realms/prod/areas/app/resources/users/transactions")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""tx_id":41"#));
}

#[tokio::test]
#[serial]
async fn should_return_queue_inflight_under_resource() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/inflight")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_return_queue_detail_with_delayed_and_dead_letter_counts() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["messages_ready"], 1);
    assert_eq!(payload["messages_delayed"], 2);
    assert_eq!(payload["messages_inflight"], 3);
    assert_eq!(payload["messages_dead_lettered"], 4);
    assert_eq!(payload["messages_total"], 10);
    assert_eq!(payload["delay_age_buckets"]["under_1m"], 1);
    assert_eq!(payload["delay_age_buckets"]["over_15m"], 1);
    assert_eq!(
        payload["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "dead-letter pressure"
    );
    assert_eq!(payload["diagnostics"]["age_seconds"], 600);
}

#[tokio::test]
#[serial]
async fn should_return_lease_detail_with_age_and_diagnostics() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_leases(vec![LeaseInfo {
        realm: "prod".to_string(),
        area: "locks".to_string(),
        resource: "cache".to_string(),
        owner_session_id: "session-1".to_string(),
        acquired_at: "2026-03-14T12:00:00Z".to_string(),
        expires_at: "2026-03-14T12:05:00Z".to_string(),
        renewals: 2,
        fencing_token: 17,
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/lease/realms/prod/areas/locks/resources/cache")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["realm"], "prod");
    assert_eq!(payload["area"], "locks");
    assert_eq!(payload["resource"], "cache");
    assert_eq!(payload["active_leases"], 1);
    assert!(payload["oldest_lease_age_seconds"].as_u64().unwrap_or(0) > 0);
    assert_eq!(payload["diagnostics"]["current_stage"], "contention");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "lease ownership churn"
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint.as_str().unwrap_or("").contains("renewals recorded")));
    assert_eq!(
        payload["diagnostics"]["age_seconds"],
        payload["oldest_lease_age_seconds"]
    );
}

#[tokio::test]
#[serial]
async fn should_return_schedule_stats_with_latency_pressure() {
    // Arrange
    let (runtime, store, schedule) = schedule_runtime_with_domains();
    seed_active_schedule_definition(store);
    schedule
        .preload_persisted_families()
        .expect("preload schedules");
    let metrics = fitz::boot::observability::metrics();
    let schedule_latency_before = metrics
        .histogram_get_buckets("fitz_schedule_latency_ms")
        .unwrap_or([0; 9]);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);
    metrics.histogram_observe_ms("fitz_schedule_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/schedule/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["schedules_active"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        schedule_latency_before[0] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        schedule_latency_before[5] + 2
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "schedule latency"
    );
    assert_eq!(payload["diagnostics"]["severity"], "high");
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("schedule request latency tail")));
}

#[tokio::test]
#[serial]
async fn should_return_schedule_stats_with_pending_claim_age() {
    // Arrange
    let (runtime, store, schedule) = schedule_runtime_with_domains();
    seed_pending_schedule_claim(store);
    schedule
        .preload_persisted_families()
        .expect("preload schedules");
    let metrics = fitz::boot::observability::metrics();
    let expired_before = metrics.counter_get("fitz_schedule_pending_claims_expired_total");
    let cleanup_failures_before =
        metrics.counter_get("fitz_schedule_pending_claim_cleanup_failure_total");
    metrics.counter_add("fitz_schedule_pending_claims_expired_total", 2);
    metrics.counter_add("fitz_schedule_pending_claim_cleanup_failure_total", 1);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/schedule/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["schedules_active"], 1);
    assert_eq!(payload["pending_fire_claims"], 1);
    assert_eq!(payload["pending_ack_retries"], 0);
    assert!(
        payload["oldest_pending_claim_age_seconds"]
            .as_u64()
            .unwrap_or(0)
            >= 30
    );
    assert_eq!(payload["pending_claims_expired_total"], expired_before + 2);
    assert_eq!(
        payload["pending_claim_cleanup_failures_total"],
        cleanup_failures_before + 1
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "stale_handoff");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "durable handoff"
    );
    assert_eq!(
        payload["diagnostics"]["age_seconds"],
        payload["oldest_pending_claim_age_seconds"]
    );
}

#[tokio::test]
#[serial]
async fn should_return_queue_events_with_bounded_timeline() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/events?family=1&limit=3")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["domain"], "queue");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["limit"], 3);
    assert_eq!(
        payload["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    let events = payload["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().any(|event| event["kind"] == "failure"));
    assert!(events
        .iter()
        .any(|event| event["kind"] == "ownership_change"));
    assert!(events
        .iter()
        .any(|event| event["message_id"] == 42 && event["attempts"] == 3));
}

#[tokio::test]
#[serial]
async fn should_return_queue_comparison_between_two_resource_snapshots() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_compare_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/compare?family=1&against_realm=prod&against_area=jobs&against_resource=backup&against_family=2")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["domain"], "queue");
    assert_eq!(payload["comparison_mode"], "snapshot_vs_snapshot");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["left"]["scope"]["family"], 1);
    assert_eq!(payload["right"]["scope"]["family"], 2);
    assert_eq!(
        payload["left"]["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    assert_eq!(payload["right"]["diagnostics"]["current_stage"], "healthy");
    assert_eq!(payload["delta"]["backlog"], 3);
    assert_eq!(payload["delta"]["dead_letters"], 4);
    assert!(payload["summary"].as_str().unwrap().contains("left side"));
}

#[tokio::test]
#[serial]
async fn should_return_queue_dead_letters_under_resource() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""message_id":42"#));
    assert!(payload.contains(r#""reason":"max_attempts_exceeded""#));
}

#[tokio::test]
#[serial]
async fn should_reject_dead_letter_replay_given_missing_family_query_param() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{}/replay",
            message_id
        ))
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
#[serial]
async fn should_replay_dead_letter_given_family_targeted_admin_request() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let replay_req = Request::builder()
        .method(Method::POST)
        .uri(format!(
            "/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{}/replay?family=1",
            message_id
        ))
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();

    // Act
    let replay_response = fitz::api::admin::handlers::handle_request(replay_req, runtime.clone())
        .await
        .unwrap();
    let detail_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker?family=1")
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();
    let detail_response = fitz::api::admin::handlers::handle_request(detail_req, runtime.clone())
        .await
        .unwrap();
    let dead_letters_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters?family=1")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();
    let dead_letters_response =
        fitz::api::admin::handlers::handle_request(dead_letters_req, runtime)
            .await
            .unwrap();

    // Assert
    assert_eq!(replay_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_body = body::to_bytes(detail_response.into_body()).await.unwrap();
    let detail_payload = String::from_utf8(detail_body.to_vec()).unwrap();
    assert!(detail_payload.contains(r#""messages_ready":1"#));
    assert!(detail_payload.contains(r#""messages_dead_lettered":0"#));
    assert!(detail_payload.contains(r#""messages_total":1"#));
    assert_eq!(dead_letters_response.status(), StatusCode::OK);
    let dead_letters_body = body::to_bytes(dead_letters_response.into_body())
        .await
        .unwrap();
    let dead_letters_payload = String::from_utf8(dead_letters_body.to_vec()).unwrap();
    assert!(dead_letters_payload.contains(r#""messages":[]"#));
}

#[tokio::test]
#[serial]
async fn should_purge_dead_letter_given_family_targeted_admin_request() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    let message_id = seed_dead_lettered_queue_message(store);
    let cookie = login_cookie(runtime.clone()).await;

    let purge_req = Request::builder()
        .method(Method::DELETE)
        .uri(format!(
            "/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters/{}?family=1",
            message_id
        ))
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();

    // Act
    let purge_response = fitz::api::admin::handlers::handle_request(purge_req, runtime.clone())
        .await
        .unwrap();
    let detail_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker?family=1")
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();
    let detail_response = fitz::api::admin::handlers::handle_request(detail_req, runtime.clone())
        .await
        .unwrap();
    let dead_letters_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/dead-letters?family=1")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();
    let dead_letters_response =
        fitz::api::admin::handlers::handle_request(dead_letters_req, runtime)
            .await
            .unwrap();

    // Assert
    assert_eq!(purge_response.status(), StatusCode::NO_CONTENT);
    assert_eq!(detail_response.status(), StatusCode::OK);
    let detail_body = body::to_bytes(detail_response.into_body()).await.unwrap();
    let detail_payload = String::from_utf8(detail_body.to_vec()).unwrap();
    assert!(detail_payload.contains(r#""messages_ready":0"#));
    assert!(detail_payload.contains(r#""messages_dead_lettered":0"#));
    assert!(detail_payload.contains(r#""messages_total":0"#));
    assert_eq!(dead_letters_response.status(), StatusCode::OK);
    let dead_letters_body = body::to_bytes(dead_letters_response.into_body())
        .await
        .unwrap();
    let dead_letters_payload = String::from_utf8(dead_letters_body.to_vec()).unwrap();
    assert!(dead_letters_payload.contains(r#""messages":[]"#));
}

#[tokio::test]
#[serial]
async fn should_return_notice_subscriptions_under_resource() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/notice/realms/prod/areas/events/resources/orders/subscriptions")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""subscription_id":7"#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_workers_under_operation() {
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/realms/prod/areas/api/resources/users/operations/get/workers")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""session_id":"9001""#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_pending_requests() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/pending?realm=prod")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""correlation_id":"corr-abc-123""#));
    assert!(payload.contains(r#""route":"rpc://prod/api/users/get""#));
    assert!(payload.contains(r#""worker_session_id":"9001""#));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_events_with_worker_registration_and_pending_transition() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/realms/prod/areas/api/resources/users/events?limit=3")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["domain"], "rpc");
    assert_eq!(payload["derived"], true);
    assert_eq!(payload["limit"], 3);
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    let events = payload["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events
        .iter()
        .any(|event| event["kind"] == "registration" && event["worker_session"] == "9001"));
    assert!(events.iter().any(|event| {
        event["kind"] == "transition" && event["correlation_id"] == "corr-abc-123"
    }));
}

#[tokio::test]
#[serial]
async fn should_return_queue_domain_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let notify_before = metrics.counter_get("fitz_queue_notify_drops_total");
    metrics.counter_add("fitz_queue_notify_drops_total", 6);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""messages_ready":1"#));
    assert!(payload.contains(r#""messages_delayed":2"#));
    assert!(payload.contains(r#""messages_pending":3"#));
    assert!(payload.contains(r#""messages_dead_lettered":4"#));
    assert!(payload.contains(r#""oldest_message_age_seconds":9"#));
    assert!(payload.contains(r#""oldest_backlog_age_seconds":600"#));
    assert!(payload.contains(r#""inflight_active":0"#));
    let payload_json: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload_json["backlog_age_buckets"]["under_1m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["under_5m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["under_15m"], 1);
    assert_eq!(payload_json["backlog_age_buckets"]["over_15m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["under_1m"], 1);
    assert_eq!(payload_json["delay_age_buckets"]["under_5m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["under_15m"], 0);
    assert_eq!(payload_json["delay_age_buckets"]["over_15m"], 1);
    assert_eq!(payload_json["notify_drops_total"], notify_before + 6);
}

#[tokio::test]
#[serial]
async fn should_return_queue_operation_counters_given_recorded_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let requests_before = metrics.counter_get("fitz_queue_requests_total");
    let success_before = metrics.counter_get("fitz_queue_success_total");
    let failure_before = metrics.counter_get("fitz_queue_failure_total");
    let enqueues_before = metrics.counter_get("fitz_queue_enqueue_total");
    let reserves_before = metrics.counter_get("fitz_queue_reserve_total");
    let completes_before = metrics.counter_get("fitz_queue_complete_total");
    let releases_before = metrics.counter_get("fitz_queue_release_total");
    let extends_before = metrics.counter_get("fitz_queue_extend_total");
    let notify_before = metrics.counter_get("fitz_queue_notify_drops_total");
    let redeliveries_before = metrics.counter_get("fitz_queue_redeliveries_total");
    let dead_letter_transitions_before = metrics.counter_get("fitz_queue_dlq_transitions_total");
    let complete_rejected_before = metrics.counter_get("fitz_queue_complete_rejected_total");
    metrics.counter_add("fitz_queue_requests_total", 5);
    metrics.counter_add("fitz_queue_success_total", 4);
    metrics.counter_add("fitz_queue_failure_total", 2);
    metrics.counter_add("fitz_queue_enqueue_total", 3);
    metrics.counter_add("fitz_queue_reserve_total", 7);
    metrics.counter_add("fitz_queue_complete_total", 11);
    metrics.counter_add("fitz_queue_release_total", 13);
    metrics.counter_add("fitz_queue_extend_total", 17);
    metrics.counter_add("fitz_queue_notify_drops_total", 19);
    metrics.counter_add("fitz_queue_redeliveries_total", 23);
    metrics.counter_add("fitz_queue_dlq_transitions_total", 29);
    metrics.counter_add("fitz_queue_complete_rejected_total", 31);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["requests_total"], requests_before + 5);
    assert_eq!(payload["success_total"], success_before + 4);
    assert_eq!(payload["failure_total"], failure_before + 2);
    assert_eq!(payload["enqueues_total"], enqueues_before + 3);
    assert_eq!(payload["reserves_total"], reserves_before + 7);
    assert_eq!(payload["completes_total"], completes_before + 11);
    assert_eq!(payload["releases_total"], releases_before + 13);
    assert_eq!(payload["extends_total"], extends_before + 17);
    assert_eq!(payload["notify_drops_total"], notify_before + 19);
    assert_eq!(payload["redeliveries_total"], redeliveries_before + 23);
    assert_eq!(
        payload["dead_letter_transitions_total"],
        dead_letter_transitions_before + 29
    );
    assert_eq!(
        payload["complete_rejected_total"],
        complete_rejected_before + 31
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("dead-letter transition")));
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("queue complete rejection")));
    assert!(payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);

    let metrics_req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, login_cookie(runtime.clone()).await)
        .body(Body::empty())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = String::from_utf8(metrics_body.to_vec()).unwrap();
    assert!(metrics_payload.contains("fitz_queue_complete_total"));
    assert!(metrics_payload.contains("fitz_queue_release_total"));
    assert!(metrics_payload.contains("fitz_queue_oldest_message_age_seconds 9"));
    assert!(metrics_payload.contains("fitz_queue_oldest_backlog_age_seconds 600"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_1m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_5m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_under_15m 1"));
    assert!(metrics_payload.contains("fitz_queue_backlog_age_bucket_over_15m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_1m 1"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_5m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_under_15m 0"));
    assert!(metrics_payload.contains("fitz_queue_delay_age_bucket_over_15m 1"));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_redeliveries_total {}",
        redeliveries_before + 23
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_dlq_transitions_total {}",
        dead_letter_transitions_before + 29
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_complete_rejected_total {}",
        complete_rejected_before + 31
    )));
    assert!(metrics_payload.contains(&format!(
        "fitz_queue_notify_drops_total {}",
        notify_before + 19
    )));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_and_lease_domain_stats_given_recorded_metrics() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_rpc_workers(vec![RpcWorker {
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![
        RpcPendingRequest {
            correlation_id: "corr-abc-123".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            submitted_at: "2026-03-14T12:00:07Z".to_string(),
            age_seconds: 7,
            worker_session_id: Some("9001".to_string()),
        },
        RpcPendingRequest {
            correlation_id: "corr-xyz-789".to_string(),
            route: "rpc://prod/api/orders/create".to_string(),
            submitted_at: "2026-03-14T12:00:13Z".to_string(),
            age_seconds: 13,
            worker_session_id: None,
        },
    ]);
    read_model.replace_leases(vec![LeaseInfo {
        realm: "prod".to_string(),
        area: "locks".to_string(),
        resource: "cache".to_string(),
        owner_session_id: "session-1".to_string(),
        acquired_at: "2026-03-14T12:00:00Z".to_string(),
        expires_at: "2026-03-14T12:05:00Z".to_string(),
        renewals: 2,
        fencing_token: 17,
    }]);

    let metrics = fitz::boot::observability::metrics();
    let rpc_requests_before = metrics.counter_get("fitz_rpc_requests_total");
    let rpc_success_before = metrics.counter_get("fitz_rpc_success_total");
    let rpc_failure_before = metrics.counter_get("fitz_rpc_failure_total");
    let rpc_timeouts_before = metrics.counter_get("rpc_request_timeouts_total");
    let rpc_backpressure_before = metrics.counter_get("rpc_backpressure_rejects_total");
    let rpc_duplicate_before =
        metrics.counter_get("rpc_requests_rejected_duplicate_correlation_total");
    let rpc_wrong_worker_before = metrics.counter_get("rpc_responses_rejected_wrong_worker_total");
    let rpc_closed_caller_before = metrics.counter_get("rpc_responses_dropped_closed_caller_total");
    let rpc_missing_pending_before = metrics.counter_get("rpc_responses_missing_pending_total");
    let rpc_ack_wrong_worker_before = metrics.counter_get("rpc_acks_rejected_wrong_worker_total");
    let lease_requests_before = metrics.counter_get("fitz_lease_requests_total");
    let lease_success_before = metrics.counter_get("fitz_lease_success_total");
    let lease_failure_before = metrics.counter_get("fitz_lease_failure_total");
    let lease_timeouts_before = metrics.counter_get("fitz_lease_acquire_timeouts_total");
    let lease_forced_before = metrics.counter_get("fitz_lease_forced_releases_total");
    let lease_invalid_before = metrics.counter_get("fitz_lease_invalid_token_rejects_total");
    let lease_churn_before = metrics.counter_get("fitz_lease_ownership_churn_total");

    metrics.counter_add("fitz_rpc_requests_total", 8);
    metrics.counter_add("fitz_rpc_success_total", 5);
    metrics.counter_add("fitz_rpc_failure_total", 3);
    metrics.counter_add("rpc_request_timeouts_total", 2);
    metrics.counter_add("rpc_backpressure_rejects_total", 4);
    metrics.counter_add("rpc_requests_rejected_duplicate_correlation_total", 6);
    metrics.counter_add("rpc_responses_rejected_wrong_worker_total", 7);
    metrics.counter_add("rpc_responses_dropped_closed_caller_total", 9);
    metrics.counter_add("rpc_responses_missing_pending_total", 11);
    metrics.counter_add("rpc_acks_rejected_wrong_worker_total", 13);
    metrics.counter_add("fitz_lease_requests_total", 4);
    metrics.counter_add("fitz_lease_success_total", 2);
    metrics.counter_add("fitz_lease_failure_total", 1);
    metrics.counter_add("fitz_lease_acquire_timeouts_total", 3);
    metrics.counter_add("fitz_lease_forced_releases_total", 5);
    metrics.counter_add("fitz_lease_invalid_token_rejects_total", 7);
    metrics.counter_add("fitz_lease_ownership_churn_total", 11);
    metrics.gauge_set("fitz_lease_waiters_gauge", 4);
    metrics.gauge_set("fitz_lease_waiter_depth", 4);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let rpc_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();
    let lease_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/lease/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let rpc_response = fitz::api::admin::handlers::handle_request(rpc_req, runtime.clone())
        .await
        .unwrap();
    let lease_response = fitz::api::admin::handlers::handle_request(lease_req, runtime.clone())
        .await
        .unwrap();

    // Assert
    assert_eq!(rpc_response.status(), StatusCode::OK);
    let rpc_body = body::to_bytes(rpc_response.into_body()).await.unwrap();
    let rpc_payload: serde_json::Value = serde_json::from_slice(&rpc_body).unwrap();
    assert_eq!(rpc_payload["workers_registered"], 1);
    assert_eq!(rpc_payload["requests_pending"], 2);
    assert_eq!(rpc_payload["oldest_pending_request_age_seconds"], 13);
    assert_eq!(rpc_payload["pending_routes_active"], 2);
    assert_eq!(rpc_payload["slowest_worker_average_latency_ms"], 4.5);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_5ms"], 1);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_25ms"], 0);
    assert_eq!(rpc_payload["worker_latency_buckets"]["under_100ms"], 0);
    assert_eq!(rpc_payload["worker_latency_buckets"]["over_100ms"], 0);
    assert_eq!(rpc_payload["requests_total"], rpc_requests_before + 8);
    assert_eq!(rpc_payload["success_total"], rpc_success_before + 5);
    assert_eq!(rpc_payload["failure_total"], rpc_failure_before + 3);
    assert_eq!(
        rpc_payload["request_timeouts_total"],
        rpc_timeouts_before + 2
    );
    assert_eq!(
        rpc_payload["backpressure_rejects_total"],
        rpc_backpressure_before + 4
    );
    assert_eq!(
        rpc_payload["duplicate_correlation_rejects_total"],
        rpc_duplicate_before + 6
    );
    assert_eq!(
        rpc_payload["wrong_worker_rejects_total"],
        rpc_wrong_worker_before + 7
    );
    assert_eq!(
        rpc_payload["responses_dropped_closed_caller_total"],
        rpc_closed_caller_before + 9
    );
    assert_eq!(
        rpc_payload["responses_missing_pending_total"],
        rpc_missing_pending_before + 11
    );
    assert_eq!(
        rpc_payload["acks_rejected_wrong_worker_total"],
        rpc_ack_wrong_worker_before + 13
    );
    assert!(rpc_payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);

    assert_eq!(lease_response.status(), StatusCode::OK);
    let lease_body = body::to_bytes(lease_response.into_body()).await.unwrap();
    let lease_payload: serde_json::Value = serde_json::from_slice(&lease_body).unwrap();
    assert_eq!(lease_payload["leases_active"], 1);
    assert_eq!(lease_payload["waiter_depth"], 4);
    assert!(
        lease_payload["oldest_lease_age_seconds"]
            .as_u64()
            .unwrap_or(0)
            > 0
    );
    assert_eq!(lease_payload["requests_total"], lease_requests_before + 4);
    assert_eq!(lease_payload["success_total"], lease_success_before + 2);
    assert_eq!(lease_payload["failure_total"], lease_failure_before + 1);
    assert_eq!(
        lease_payload["acquire_timeouts_total"],
        lease_timeouts_before + 3
    );
    assert_eq!(
        lease_payload["forced_releases_total"],
        lease_forced_before + 5
    );
    assert_eq!(
        lease_payload["invalid_token_rejects_total"],
        lease_invalid_before + 7
    );
    assert_eq!(
        lease_payload["ownership_churn_total"],
        lease_churn_before + 11
    );
    assert!(
        lease_payload["operations_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );

    let metrics_req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, login_cookie(runtime.clone()).await)
        .body(Body::empty())
        .unwrap();
    let metrics_response = fitz::api::admin::handlers::handle_request(metrics_req, runtime)
        .await
        .unwrap();
    assert_eq!(metrics_response.status(), StatusCode::OK);
    let metrics_body = body::to_bytes(metrics_response.into_body()).await.unwrap();
    let metrics_payload = String::from_utf8(metrics_body.to_vec()).unwrap();
    assert!(metrics_payload.contains("fitz_rpc_requests_pending 2"));
    assert!(metrics_payload.contains("fitz_rpc_oldest_pending_request_age_seconds 13"));
    assert!(metrics_payload.contains("fitz_rpc_pending_routes_active 2"));
    assert!(metrics_payload.contains("fitz_lease_oldest_lease_age_seconds"));
    assert!(metrics_payload.contains(&format!(
        "fitz_lease_ownership_churn_total {}",
        lease_churn_before + 11
    )));
}

#[tokio::test]
#[serial]
async fn should_return_rpc_data_loss_risk_given_late_response_drops() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    let late_drops_before = metrics.counter_get("rpc_responses_dropped_closed_caller_total");
    let missing_before = metrics.counter_get("rpc_responses_missing_pending_total");
    metrics.counter_add("rpc_responses_dropped_closed_caller_total", 4);
    metrics.counter_add("rpc_responses_missing_pending_total", 2);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        payload["responses_dropped_closed_caller_total"],
        late_drops_before + 4
    );
    assert_eq!(
        payload["responses_missing_pending_total"],
        missing_before + 2
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "data_loss_risk");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "late response drop"
    );
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value.as_str().unwrap_or("").contains("late response drop")));
}

#[tokio::test]
#[serial]
async fn should_return_stream_domain_stats_given_recorded_operations() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    seed_stream_watermark_lag_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    metrics.counter_add("fitz_stream_operations_total", 5);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 8);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 60);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["events_total"], 3);
    assert_eq!(payload["watermark_lag_buckets"]["caught_up"], 3);
    assert_eq!(payload["watermark_lag_buckets"]["under_10"], 1);
    assert_eq!(payload["watermark_lag_buckets"]["under_100"], 2);
    assert_eq!(payload["watermark_lag_buckets"]["over_100"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_10ms"],
        stream_latency_before[2] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_100ms"],
        stream_latency_before[4] + 1
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 1
    );
    assert!(payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);
}

#[tokio::test]
#[serial]
async fn should_classify_stream_latency_pressure_given_recorded_latency_tail() {
    // Arrange
    let runtime = test_runtime();
    seed_stream_latency_pressure_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    metrics.counter_add("fitz_stream_operations_total", 5);
    for _ in 0..10 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    }
    for _ in 0..40 {
        metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    }
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["streams_active"], 1);
    assert_eq!(
        payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 10
    );
    assert_eq!(
        payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 40
    );
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(
        payload["diagnostics"]["likely_bottleneck"],
        "stream latency"
    );
    assert_eq!(payload["diagnostics"]["severity"], "high");
    assert!(payload["diagnostics"]["explanation_hints"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hint| hint
            .as_str()
            .unwrap_or("")
            .contains("stream request latency tail")));
}

#[tokio::test]
#[serial]
async fn should_return_stream_and_notice_domain_stats_given_recorded_metrics() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store.clone());
    seed_stream_watermark_lag_data(&runtime);
    seed_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let stream_latency_before = metrics
        .histogram_get_buckets("fitz_stream_latency_ms")
        .unwrap_or([0; 9]);
    let stream_requests_before = metrics.counter_get("fitz_stream_requests_total");
    let stream_success_before = metrics.counter_get("fitz_stream_success_total");
    let stream_failure_before = metrics.counter_get("fitz_stream_failure_total");
    let stream_started_before = metrics.counter_get("fitz_stream_append_sessions_started_total");
    let stream_ended_before = metrics.counter_get("fitz_stream_append_sessions_ended_total");
    let stream_conflicts_before = metrics.counter_get("fitz_stream_append_conflicts_total");
    let stream_notify_drops_before = metrics.counter_get("fitz_stream_notify_drops_total");
    let notice_requests_before = metrics.counter_get("fitz_notice_requests_total");
    let notice_success_before = metrics.counter_get("fitz_notice_success_total");
    let notice_failure_before = metrics.counter_get("fitz_notice_failure_total");
    let notice_drops_before = metrics.counter_get("fitz_notice_delivery_drops_total");
    let notice_unsubscribes_before = metrics.counter_get("fitz_notice_unsubscribes_total");
    let notice_wildcard_before = metrics.counter_get("fitz_notice_wildcard_limit_rejects_total");

    metrics.counter_add("fitz_stream_requests_total", 4);
    metrics.counter_add("fitz_stream_success_total", 3);
    metrics.counter_add("fitz_stream_failure_total", 1);
    metrics.counter_add("fitz_stream_operations_total", 6);
    metrics.counter_add("fitz_stream_append_sessions_started_total", 5);
    metrics.counter_add("fitz_stream_append_sessions_ended_total", 3);
    metrics.counter_add("fitz_stream_append_conflicts_total", 2);
    metrics.counter_add("fitz_stream_notify_drops_total", 5);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 8);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 60);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    metrics.counter_add("fitz_notice_requests_total", 7);
    metrics.counter_add("fitz_notice_success_total", 5);
    metrics.counter_add("fitz_notice_failure_total", 2);
    metrics.counter_add("fitz_notice_delivery_drops_total", 3);
    metrics.counter_add("fitz_notice_unsubscribes_total", 6);
    metrics.counter_add("fitz_notice_wildcard_limit_rejects_total", 4);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let stream_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stream/stats")
        .header(COOKIE, cookie.clone())
        .body(Body::empty())
        .unwrap();
    let notice_req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/notice/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let stream_response = fitz::api::admin::handlers::handle_request(stream_req, runtime.clone())
        .await
        .unwrap();
    let notice_response = fitz::api::admin::handlers::handle_request(notice_req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(stream_response.status(), StatusCode::OK);
    let stream_body = body::to_bytes(stream_response.into_body()).await.unwrap();
    let stream_payload: serde_json::Value = serde_json::from_slice(&stream_body).unwrap();
    assert_eq!(stream_payload["streams_active"], 0);
    assert_eq!(stream_payload["append_sessions_active"], 0);
    assert_eq!(stream_payload["events_total"], 3);
    assert_eq!(stream_payload["requests_total"], stream_requests_before + 4);
    assert_eq!(stream_payload["success_total"], stream_success_before + 3);
    assert_eq!(stream_payload["failure_total"], stream_failure_before + 1);
    assert_eq!(
        stream_payload["append_sessions_started_total"],
        stream_started_before + 5
    );
    assert_eq!(
        stream_payload["append_sessions_ended_total"],
        stream_ended_before + 3
    );
    assert_eq!(
        stream_payload["append_conflicts_total"],
        stream_conflicts_before + 2
    );
    assert_eq!(
        stream_payload["notify_drops_total"],
        stream_notify_drops_before + 5
    );
    assert_eq!(stream_payload["watermark_lag_buckets"]["caught_up"], 3);
    assert_eq!(stream_payload["watermark_lag_buckets"]["under_10"], 1);
    assert_eq!(stream_payload["watermark_lag_buckets"]["under_100"], 2);
    assert_eq!(stream_payload["watermark_lag_buckets"]["over_100"], 1);
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_1ms"],
        stream_latency_before[0] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_10ms"],
        stream_latency_before[2] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_100ms"],
        stream_latency_before[4] + 1
    );
    assert_eq!(
        stream_payload["request_latency_buckets"]["under_500ms"],
        stream_latency_before[5] + 1
    );
    assert!(
        stream_payload["operations_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );

    assert_eq!(notice_response.status(), StatusCode::OK);
    let notice_body = body::to_bytes(notice_response.into_body()).await.unwrap();
    let notice_payload: serde_json::Value = serde_json::from_slice(&notice_body).unwrap();
    assert_eq!(notice_payload["subscriptions_active"], 3);
    assert_eq!(notice_payload["routes_active"], 2);
    assert_eq!(notice_payload["max_route_subscribers"], 2);
    assert_eq!(notice_payload["requests_total"], notice_requests_before + 7);
    assert_eq!(notice_payload["success_total"], notice_success_before + 5);
    assert_eq!(notice_payload["failure_total"], notice_failure_before + 2);
    assert_eq!(
        notice_payload["delivery_drops_total"],
        notice_drops_before + 3
    );
    assert_eq!(
        notice_payload["unsubscribes_total"],
        notice_unsubscribes_before + 6
    );
    assert_eq!(
        notice_payload["wildcard_limit_rejects_total"],
        notice_wildcard_before + 4
    );
    assert_eq!(notice_payload["diagnostics"]["current_stage"], "throughput");
    assert_eq!(
        notice_payload["diagnostics"]["likely_bottleneck"],
        "route concentration"
    );
    assert!(
        notice_payload["publishes_per_second"]
            .as_f64()
            .unwrap_or(0.0)
            > 0.0
    );
}

#[tokio::test]
#[serial]
async fn should_export_notice_churn_and_concentration_metrics_given_recorded_notice_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_snapshot_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let unsubscribes_before = metrics.counter_get("fitz_notice_unsubscribes_total");
    metrics.counter_add("fitz_notice_unsubscribes_total", 5);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_notice_subscriptions_active"));
    assert!(payload.contains("fitz_notice_routes_active"));
    assert!(payload.contains("fitz_notice_max_route_subscribers"));
    assert!(payload.contains("fitz_notice_unsubscribes_total"));
    assert!(payload.contains("fitz_notice_subscriptions_active 3"));
    assert!(payload.contains(&format!(
        "fitz_notice_unsubscribes_total {}",
        unsubscribes_before + 5
    )));
    assert!(payload.contains("fitz_notice_routes_active 2"));
    assert!(payload.contains("fitz_notice_max_route_subscribers 2"));
}

#[tokio::test]
#[serial]
async fn should_export_stream_counters_and_rates_given_recorded_stream_metrics() {
    // Arrange
    let runtime = test_runtime();
    seed_stream_watermark_lag_data(&runtime);
    let metrics = fitz::boot::observability::metrics();
    let operations_before = metrics.counter_get("fitz_stream_operations_total");
    let started_before = metrics.counter_get("fitz_stream_append_sessions_started_total");
    let ended_before = metrics.counter_get("fitz_stream_append_sessions_ended_total");
    let conflicts_before = metrics.counter_get("fitz_stream_append_conflicts_total");
    let drops_before = metrics.counter_get("fitz_stream_notify_drops_total");
    metrics.counter_add("fitz_stream_operations_total", 3);
    metrics.counter_add("fitz_stream_append_sessions_started_total", 2);
    metrics.counter_add("fitz_stream_append_sessions_ended_total", 4);
    metrics.counter_add("fitz_stream_append_conflicts_total", 2);
    metrics.counter_add("fitz_stream_notify_drops_total", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 1);
    metrics.histogram_observe_ms("fitz_stream_latency_ms", 250);
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_stream_events_total"));
    assert!(payload.contains("fitz_stream_append_sessions_active"));
    assert!(payload.contains("fitz_stream_append_sessions_started_total"));
    assert!(payload.contains("fitz_stream_append_sessions_ended_total"));
    assert!(payload.contains("fitz_stream_operations_per_second"));
    assert!(payload.contains("fitz_stream_subscriptions_active"));
    assert!(payload.contains("fitz_stream_latency_ms{le=\"100ms\"}"));
    assert!(payload.contains("fitz_stream_latency_ms_count"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_caught_up"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_10"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_100"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_over_100"));
    assert!(payload.contains(&format!(
        "fitz_stream_operations_total {}",
        operations_before + 3
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_sessions_started_total {}",
        started_before + 2
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_sessions_ended_total {}",
        ended_before + 4
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_conflicts_total {}",
        conflicts_before + 2
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_notify_drops_total {}",
        drops_before + 1
    )));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_caught_up 3"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_10 1"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_under_100 2"));
    assert!(payload.contains("fitz_stream_watermark_lag_bucket_over_100 1"));
}

#[tokio::test]
#[serial]
async fn should_export_stream_watermark_series_given_committed_stream_history() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/metrics")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains("fitz_stream_realm_watermark{realm=\"prod\",family=\"1\"} 2"));
    assert!(payload
        .contains("fitz_stream_area_watermark{realm=\"prod\",area=\"audit\",family=\"1\"} 0"));
    assert!(
        payload.contains("fitz_stream_area_watermark{realm=\"prod\",area=\"logs\",family=\"1\"} 1")
    );
}

#[tokio::test]
#[serial]
async fn should_return_global_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 100,
        messages_total: 110,
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
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/stats")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["domains"]["queue"]["messages_dead_lettered"], 100);
    assert_eq!(payload["domains"]["queue"]["oldest_message_age_seconds"], 9);
    assert_eq!(
        payload["domains"]["queue"]["oldest_backlog_age_seconds"],
        600
    );
    assert_eq!(
        payload["domains"]["queue"]["backlog_age_buckets"]["under_1m"],
        1
    );
    assert_eq!(
        payload["domains"]["queue"]["delay_age_buckets"]["under_1m"],
        1
    );
    assert_eq!(
        payload["domains"]["queue"]["delay_age_buckets"]["over_15m"],
        1
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["status"],
        "stalled"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["severity"],
        "high"
    );
    assert_eq!(payload["diagnostics"]["top_bottleneck"]["domain"], "queue");
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["title"],
        "Inspect recent transitions"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["priority"],
        1
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["recommended_next_query"],
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["endpoint"]
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][0]["remediation"],
        "Use the transition history to isolate the failure reason or retry pattern before taking any follow-up action."
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][1]["title"],
        "Inspect current resource snapshot"
    );
    assert_eq!(
        payload["diagnostics"]["incident_summary"]["suggested_next_queries"][1]["priority"],
        2
    );
    assert_eq!(
        payload["domains"]["queue"]["diagnostics"]["current_stage"],
        "dead_letter_pressure"
    );
    let signals_matched = payload["domains"]["queue"]["diagnostics"]
        ["confidence_justification"]["signals_matched"]
        .as_array()
        .expect("queue confidence signals_matched");
    assert!(signals_matched.iter().any(|signal| signal == "failure_signal_present"));
    assert!(payload["domains"]["queue"]["diagnostics"]["confidence_justification"]
        ["rationale"]
        .as_str()
        .expect("queue confidence rationale")
        .contains("telemetry freshness"));
}

#[tokio::test]
#[serial]
async fn should_return_global_troubleshooting_guidance() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let read_model = runtime.admin_read_model();
    read_model.replace_queues(vec![QueueInfo {
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        messages_ready: 1,
        messages_delayed: 2,
        messages_inflight: 3,
        messages_dead_lettered: 100,
        messages_total: 110,
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
    }]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/troubleshooting")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["incident_summary"]["status"], "stalled");
    assert_eq!(payload["incident_summary"]["severity"], "high");
    assert_eq!(payload["top_bottleneck"]["domain"], "queue");
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["title"],
        "Inspect recent transitions"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["priority"],
        1
    );
    assert_eq!(
        payload["incident_summary"]["recommended_next_query"],
        payload["incident_summary"]["suggested_next_queries"][0]["endpoint"]
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][0]["remediation"],
        "Use the transition history to isolate the failure reason or retry pattern before taking any follow-up action."
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][1]["title"],
        "Inspect current resource snapshot"
    );
    assert_eq!(
        payload["incident_summary"]["suggested_next_queries"][1]["priority"],
        2
    );
}

#[tokio::test]
#[serial]
async fn should_return_exact_rpc_operation_detail_counts() {
    // Arrange
    let runtime = test_runtime();
    let read_model = runtime.admin_read_model();
    read_model.replace_rpc_workers(vec![RpcWorker {
        session_id: "9001".to_string(),
        realm: "prod".to_string(),
        route: "rpc://prod/api/users/get".to_string(),
        registered_at: "2026-03-14T12:00:00Z".to_string(),
        requests_handled: 12,
        average_latency_ms: 4.5,
    }]);
    read_model.replace_rpc_pending(vec![
        RpcPendingRequest {
            correlation_id: "corr-get".to_string(),
            route: "rpc://prod/api/users/get".to_string(),
            submitted_at: "2026-03-14T12:00:07Z".to_string(),
            age_seconds: 7,
            worker_session_id: Some("9001".to_string()),
        },
        RpcPendingRequest {
            correlation_id: "corr-get-details".to_string(),
            route: "rpc://prod/api/users/get-details".to_string(),
            submitted_at: "2026-03-14T12:00:08Z".to_string(),
            age_seconds: 8,
            worker_session_id: Some("9002".to_string()),
        },
    ]);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/rpc/realms/prod/areas/api/resources/users/operations/get")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    // Act
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    // Assert
    assert_eq!(response.status(), StatusCode::OK);
    let body = body::to_bytes(response.into_body()).await.unwrap();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(payload["workers_registered"], 1);
    assert_eq!(payload["requests_pending"], 1);
    assert_eq!(payload["slowest_worker_average_latency_ms"], 4.5);
    assert_eq!(payload["worker_latency_buckets"]["under_5ms"], 1);
    assert_eq!(payload["worker_latency_buckets"]["under_25ms"], 0);
    assert_eq!(payload["diagnostics"]["current_stage"], "throughput");
}

#[tokio::test]
#[serial]
async fn should_return_sessions_collection_only() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
#[serial]
async fn should_reject_removed_session_detail_route() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/sessions/123")
        .header(COOKIE, cookie)
        .body(Body::empty())
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
