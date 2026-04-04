//! Integration test for admin REST API

use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHasher,
};
use bytes::Bytes;
use fitz::api::admin::{
    KvTransaction, NoticeSubscription, QueueDeadLetter, QueueInfo, RpcPendingRequest, RpcWorker,
};
use fitz::boot::domains::{
    DomainHandles, KvDomainSink, LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink,
    ScheduleDomainSink, StreamDomainSink,
};
use fitz::boot::Runtime;
use fitz::domains::stream::protocol::StreamWriteMode;
use fitz::domains::stream::store::{CommitRecordsParams, EventPayload, StreamStore};
use fitz::domains::queue::{QueueActor, QueueKey, QueueResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::runtime::Router;
use hyper::header::{COOKIE, SET_COOKIE};
use hyper::{body, Body, Method, Request, StatusCode};
use serial_test::serial;
use std::sync::Arc;

fn password_hash_for(password: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .unwrap()
        .to_string()
}

fn configure_admin_auth() {
    std::env::set_var("FITZ_ADMIN_USERNAME", "admin");
    std::env::set_var(
        "FITZ_ADMIN_PASSWORD_HASH",
        password_hash_for("secret-password"),
    );
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
        fitz::utils::idempotency::global_dedup_store(),
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
    read_model.replace_notice_subscriptions(vec![NoticeSubscription {
        subscription_id: 7,
        session_id: "123".to_string(),
        realm: "prod".to_string(),
        pattern: "notice://prod/events/orders/created".to_string(),
        created_at: "2026-03-14T12:00:00Z".to_string(),
        notifications_received: 5,
    }]);
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
        messages_leased: 3,
        messages_dead_lettered: 4,
        messages_total: 10,
        oldest_message_age_seconds: 9,
    }]);
    read_model.replace_queue_dead_letters(vec![QueueDeadLetter {
        message_id: 42,
        family: 1,
        realm: "prod".to_string(),
        area: "jobs".to_string(),
        resource: "worker".to_string(),
        dead_lettered_at: "2026-03-14T12:01:00Z".to_string(),
        attempts: 3,
        reason: "max_attempts_exceeded".to_string(),
    }]);
}

fn seed_stream_snapshot_data(store: Arc<cntryl_midge::Engine>) {
    let stream_store = StreamStore::new(store);

    let logs_application = [EventPayload {
        body: Bytes::from_static(b"app-log"),
        metadata: None,
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

async fn login_cookie(runtime: Arc<Runtime>) -> String {
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/v1/session")
        .header("Content-Type", "application/json")
        .body(Body::from(
            r#"{"username":"admin","password":"secret-password"}"#,
        ))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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
        .body(Body::from(
            r#"{"username":"admin","password":"secret-password"}"#,
        ))
        .unwrap();

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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

    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""realm":"prod""#));
    assert!(payload.contains(r#""area":"logs""#));
    assert!(payload.contains(r#""resource":"application""#));
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
    let response = fitz::api::admin::handlers::handle_request(req, runtime)
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
async fn should_return_queue_leases_under_resource() {
    let runtime = test_runtime();
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/realms/prod/areas/jobs/resources/worker/leases")
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
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""messages_ready":1"#));
    assert!(payload.contains(r#""messages_delayed":2"#));
    assert!(payload.contains(r#""messages_leased":3"#));
    assert!(payload.contains(r#""messages_dead_lettered":4"#));
    assert!(payload.contains(r#""messages_total":10"#));
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
async fn should_return_queue_domain_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
    let cookie = login_cookie(runtime.clone()).await;

    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/v1/queue/stats")
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
    assert!(payload.contains(r#""messages_ready":1"#));
    assert!(payload.contains(r#""messages_delayed":2"#));
    assert!(payload.contains(r#""messages_pending":3"#));
    assert!(payload.contains(r#""messages_dead_lettered":4"#));
    assert!(payload.contains(r#""leases_active":0"#));
}

#[tokio::test]
#[serial]
async fn should_return_stream_domain_stats_given_recorded_operations() {
    // Arrange
    let (runtime, store) = queue_runtime_with_domains();
    seed_stream_snapshot_data(store);
    let metrics = fitz::boot::observability::metrics();
    metrics.counter_add("fitz_stream_operations_total", 5);
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
    assert!(payload["operations_per_second"].as_f64().unwrap_or(0.0) > 0.0);
}

#[tokio::test]
#[serial]
async fn should_export_stream_counters_and_rates_given_recorded_stream_metrics() {
    // Arrange
    let runtime = test_runtime();
    let metrics = fitz::boot::observability::metrics();
    let operations_before = metrics.counter_get("fitz_stream_operations_total");
    let conflicts_before = metrics.counter_get("fitz_stream_append_conflicts_total");
    let drops_before = metrics.counter_get("fitz_stream_notify_drops_total");
    metrics.counter_add("fitz_stream_operations_total", 3);
    metrics.counter_add("fitz_stream_append_conflicts_total", 2);
    metrics.counter_add("fitz_stream_notify_drops_total", 1);
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
    assert!(payload.contains("fitz_stream_operations_per_second"));
    assert!(payload.contains("fitz_stream_subscriptions_active"));
    assert!(payload.contains(&format!(
        "fitz_stream_operations_total {}",
        operations_before + 3
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_append_conflicts_total {}",
        conflicts_before + 2
    )));
    assert!(payload.contains(&format!(
        "fitz_stream_notify_drops_total {}",
        drops_before + 1
    )));
}

#[tokio::test]
#[serial]
async fn should_return_global_stats() {
    // Arrange
    let runtime = test_runtime();
    seed_queue_snapshot_data(&runtime);
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
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""queue":{"#));
    assert!(payload.contains(r#""messages_dead_lettered":4"#));
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
    let payload = String::from_utf8(body.to_vec()).unwrap();
    assert!(payload.contains(r#""workers_registered":1"#));
    assert!(payload.contains(r#""requests_pending":1"#));
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
