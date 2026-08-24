use super::*;
pub(super) use crate::auth::Access;
pub(super) use crate::control::admin::read_model::AdminReadModel;
pub(super) use crate::domains::kv::sink::KvDomainSink;
pub(super) use crate::domains::lease::sink::LeaseDomainSink;
pub(super) use crate::domains::notice::sink::NoticeDomainSink;
pub(super) use crate::domains::queue::sink::QueueDomainSink;
pub(super) use crate::domains::rpc::sink::RpcDomainSink;
pub(super) use crate::domains::schedule::sink::ScheduleDomainSink;
pub(super) use crate::domains::stream::sink::StreamDomainSink;
pub(super) use crate::protocol::frame::ChannelId;
pub(super) use crate::protocol::payload_codec::PayloadEncoder;
pub(super) use crate::protocol::tlv::MessageType;
pub(super) use crate::protocol::FrameContext;
pub(super) use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
pub(super) use crate::runtime::{DeliveryError, Envelope, Mailbox, MailboxSink};
pub(super) use crate::session::{SessionInfo, SessionMetadata, SessionPermissions, TransportKind};
pub(super) use bytes::Bytes;
pub(super) use std::sync::atomic::{AtomicUsize, Ordering};
pub(super) use std::sync::{Arc, Mutex, Once};
pub(super) use std::time::Duration;

pub(super) const TEST_AUTH_ISSUER: &str = "https://idp.example";
pub(super) const TEST_AUTH_AUDIENCE: &str = "fitz-broker";
pub(super) const TEST_AUTH_SECRET: &str = "test-secret-key";
pub(super) const TEST_AUTH_JWKS_URL: &str = "https://idp.example/.well-known/jwks.json";

pub(super) static TEST_AUTH_JWKS_CACHE: Once = Once::new();

#[derive(Default)]
pub(super) struct CleanupTrackingSink {
    cleanup_sessions: Mutex<Vec<u64>>,
}

impl CleanupTrackingSink {
    fn recorded_sessions(&self) -> Vec<u64> {
        self.cleanup_sessions.lock().unwrap().clone()
    }
}

impl MailboxSink for CleanupTrackingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        self.cleanup_sessions
            .lock()
            .unwrap()
            .push(cleanup.session_id);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

pub(super) fn make_session_info(id: u64, kind: TransportKind) -> SessionInfo {
    SessionInfo {
        session_id: id,
        transport_kind: kind,
        peer_addr: None,
        metadata: Arc::new(SessionMetadata::new()),
        permissions_snapshot: SessionPermissions::empty(),
        claims: None,
        authenticated: false,
        route_family: crate::runtime::routing::RouteFamily::new(0), // Test mode = family 0
    }
}

pub(super) fn permissions_from_strings(raw_permissions: &[&str]) -> SessionPermissions {
    let permissions = raw_permissions
        .iter()
        .map(|permission| crate::auth::Permission::parse(permission).unwrap())
        .collect();
    SessionPermissions::from_permissions(permissions)
}

pub(super) fn make_authenticated_session_info(
    id: u64,
    kind: TransportKind,
    route_family: RouteFamily,
    raw_permissions: &[&str],
) -> SessionInfo {
    let mut session = make_session_info(id, kind);
    session.authenticated = true;
    session.route_family = route_family;
    session.permissions_snapshot = permissions_from_strings(raw_permissions);
    session
}

pub(super) fn install_expired_session_actor(
    ingress: &RuntimeIngress,
    session_id: u64,
    raw_permissions: &[&str],
) {
    let permissions = raw_permissions
        .iter()
        .map(|permission| crate::auth::Permission::parse(permission).unwrap())
        .collect::<Vec<_>>();
    let snapshot = SessionPermissions::from_permissions(permissions.clone());
    let claims = crate::auth::Claims {
        sub: format!("test-session-{session_id}"),
        identity_claim: Some("test".to_string()),
        identity_value: Some("test".to_string()),
        permissions,
        exp: 0,
    };
    let mut actor = crate::session::actor::SessionActor::new(
        crate::session::session::SessionId(session_id),
        snapshot.clone(),
    );
    actor.authenticate(claims, snapshot);
    ingress.session_actors.insert(session_id, actor);
}

pub(super) fn auth_spec(msg_type: u16) -> DomainAuthorizationSpec {
    RuntimeIngress::domain_dispatch_for_msg_type(MessageType::new(msg_type))
        .unwrap()
        .unwrap()
}

pub(super) fn receive_frame(mailbox: &Mailbox, label: &str) -> FrameContext {
    let envelope = mailbox
        .receiver()
        .try_recv()
        .unwrap_or_else(|_| panic!("expected {label}"));

    if let Some(frame) = envelope.payload::<FrameContext>() {
        return frame.clone();
    }

    if let Some(request) = envelope.payload::<crate::domains::kv::KvClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::queue::QueueClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) =
        envelope.payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
    {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::lease::LeaseClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::notice::NoticeClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::schedule::ScheduleClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::stream::StreamClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    if let Some(request) = envelope.payload::<crate::domains::rpc::RpcClientRequest>() {
        return frame_context_from_client_meta(request.meta);
    }

    panic!("expected {label} frame context")
}

fn frame_context_from_client_meta(meta: crate::runtime::ClientFrameMeta) -> FrameContext {
    FrameContext::new(
        meta.session_id,
        protocol_channel_from_client(meta.channel),
        MessageType::new(meta.message_type),
        bytes::Bytes::new(),
        meta.route_family,
    )
}

fn protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}

pub(super) fn decode_domain_error_code(payload: &[u8]) -> u16 {
    let (code, _) =
        crate::protocol::error_codes::decode_error_body(payload).expect("decode domain error");
    code
}

pub(super) fn runtime_auth_config() -> crate::auth::AuthConfig {
    crate::auth::AuthConfig::jwks(
        vec![TEST_AUTH_AUDIENCE.to_string()],
        vec![crate::auth::JwksIssuerConfig {
            issuer: TEST_AUTH_ISSUER.to_string(),
            jwks_url: TEST_AUTH_JWKS_URL.to_string(),
        }],
    )
}

pub(super) fn runtime_ingress_with_jwks_auth() -> RuntimeIngress {
    RuntimeIngress::new(true).with_auth_config(runtime_auth_config())
}

pub(super) fn seed_runtime_jwks_cache() {
    TEST_AUTH_JWKS_CACHE.call_once(|| {
        use base64::Engine;

        let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(TEST_AUTH_SECRET);
        let jwks = serde_json::json!({
            "keys": [
                {
                    "kty": "oct",
                    "kid": "",
                    "k": key,
                }
            ]
        })
        .to_string();

        crate::auth::cache_jwks_from_json(TEST_AUTH_JWKS_URL, &jwks).unwrap();
    });
}

pub(super) fn signed_jwks_jwt(mut payload: serde_json::Value) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header};

    seed_runtime_jwks_cache();
    if let Some(map) = payload.as_object_mut() {
        match map.get("iss") {
            Some(serde_json::Value::String(value)) if !value.is_empty() => {}
            _ => {
                map.insert(
                    "iss".to_string(),
                    serde_json::Value::String(TEST_AUTH_ISSUER.to_string()),
                );
            }
        }

        map.entry("aud".to_string())
            .or_insert_with(|| serde_json::Value::String(TEST_AUTH_AUDIENCE.to_string()));
    }

    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &payload,
        &EncodingKey::from_secret(TEST_AUTH_SECRET.as_bytes()),
    )
    .unwrap()
}

pub(super) fn encode_notice_subscribe(pattern: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(pattern);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_bytes(payload);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_lease_acquire(route: &str, owner_id: &str, ttl_secs: u64) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(owner_id);
    encoder.put_u64(ttl_secs);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_lease_subscribe(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    encoder.put_string(cron);
    encoder.put_u8(crate::domains::schedule::ScheduleDeliveryMode::Broadcast as u8);
    encoder.put_bytes(payload);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_schedule_subscribe(route: &str) -> Bytes {
    let mut encoder = PayloadEncoder::new();
    encoder.put_string(route);
    Bytes::from(encoder.finish())
}

pub(super) fn encode_queue_send(route: &str, body: &[u8]) -> Bytes {
    let mut payload = Vec::new();
    bytes::BufMut::put_u32(
        &mut payload,
        u32::try_from(route.len()).expect("queue route length fits in u32"),
    );
    bytes::BufMut::put_slice(&mut payload, route.as_bytes());
    bytes::BufMut::put_u32(
        &mut payload,
        u32::try_from(body.len()).expect("queue body length fits in u32"),
    );
    bytes::BufMut::put_slice(&mut payload, body);
    Bytes::from(payload)
}

pub(super) fn encode_queue_reserve(route: &str, inflight_seconds: u64, batch_size: u32) -> Bytes {
    let mut payload = Vec::new();
    bytes::BufMut::put_u32(
        &mut payload,
        u32::try_from(route.len()).expect("queue route length fits in u32"),
    );
    bytes::BufMut::put_slice(&mut payload, route.as_bytes());
    bytes::BufMut::put_u64(&mut payload, inflight_seconds);
    bytes::BufMut::put_u8(&mut payload, 1);
    bytes::BufMut::put_u32(&mut payload, batch_size);
    bytes::BufMut::put_u8(&mut payload, 0);
    Bytes::from(payload)
}

pub(super) fn queue_receive_response_message_count(frame: &FrameContext) -> u32 {
    assert_eq!(frame.payload[0], 0, "expected success status");
    u32::from_be_bytes(
        frame.payload[1..5]
            .try_into()
            .expect("receive payload should include count"),
    )
}

pub(super) fn parse_rpc_response_frame(
    frame: &FrameContext,
) -> crate::domains::rpc::protocol::RpcResponse {
    match crate::protocol::rpc_codec::parse_request(frame, &frame.payload, frame.route_family)
        .expect("parse rpc response frame")
    {
        crate::domains::rpc::protocol::RpcMessage::Response(response) => response,
        other => panic!("expected rpc response, found {other:?}"),
    }
}

pub(super) fn drain_mailbox(mailbox: &Mailbox) {
    while mailbox.receiver().try_recv().is_ok() {}
}

pub(super) fn register_fallback_cleanup_domains(
    router: &Arc<crate::runtime::Router>,
    real_domain: DispatchDomain,
) {
    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == real_domain {
            continue;
        }

        router.register_domain_pattern(domain.as_str(), Arc::new(CleanupTrackingSink::default()));
    }
}

pub(super) fn make_cleanup_ingress(
    router: Arc<crate::runtime::Router>,
    admin_read_model: Arc<AdminReadModel>,
) -> RuntimeIngress {
    runtime_ingress_with_jwks_auth()
        .with_router(router)
        .with_admin_read_model(admin_read_model)
}

fn assert_queue_cleanup_admin_state(
    admin_read_model: &AdminReadModel,
    next_worker_session_id: u64,
) {
    let queues = admin_read_model.queues(None);
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].messages_ready, 0);
    assert_eq!(queues[0].messages_inflight, 1);
    assert_eq!(admin_read_model.queue_inflight(None).len(), 1);
    assert_eq!(
        admin_read_model.queue_inflight(None)[0].session_id,
        next_worker_session_id.to_string()
    );
}

#[tokio::test]
async fn should_open_session() {
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(1, TransportKind::WebSocket);

    let result = ingress.on_open(session).await;

    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
    assert_eq!(ingress.session_count(), 1);
}

#[test]
pub(super) fn should_process_frame() {
    // Arrange
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);
    let session = make_session_info(2, TransportKind::WebSocket);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();

        // First, perform a connect to authenticate the session
        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:2",
            "exp": 9_999_999_999_u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let jwt = signed_jwks_jwt(payload);

        let decision = ingress
            .on_frame(
                2,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;

        // Assert
        assert_eq!(decision, IngressDecision::Accept);
    });
}

#[tokio::test]
async fn should_reject_unknown_session() {
    let ingress = runtime_ingress_with_jwks_auth().with_route_family_map(&[("acme-prod", 1)]);

    let decision = ingress
        .on_frame(
            999,
            ChannelId::Control,
            crate::protocol::tlv::MessageType::new(42),
            Bytes::from("test"),
        )
        .await;

    assert!(matches!(decision, IngressDecision::Close(_)));
}

#[test]
pub(super) fn should_call_event_handler() {
    // Arrange
    let event_count = Arc::new(AtomicUsize::new(0));
    let count_clone = event_count.clone();
    let ingress = runtime_ingress_with_jwks_auth()
        .with_route_family_map(&[("acme-prod", 1)])
        .with_event_handler(move |_event| {
            count_clone.fetch_add(1, Ordering::SeqCst);
        });
    let session = make_session_info(3, TransportKind::WebSocket);

    // Act
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        ingress.on_open(session).await.unwrap();
        // Authenticate session with a connect
        let payload = serde_json::json!({
            "iss": "",
            "aud": "fitz-broker",
            "sub": "user:3",
            "exp": 9_999_999_999_u64,
            "tid": "acme-prod",
            "permissions": ["notice://prod/orders/**#read"]
        });
        let jwt = signed_jwks_jwt(payload);

        ingress
            .on_frame(
                3,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                Bytes::from(jwt),
            )
            .await;
        ingress.on_close(3, CloseReason::ClientClose).await;
    });

    // Assert
    assert_eq!(event_count.load(Ordering::SeqCst), 3);
}

#[test]
pub(super) fn should_dispatch_session_cleanup_to_all_registered_domains() {
    // Arrange
    let router = crate::runtime::Router::new();
    let route_family = crate::runtime::routing::RouteFamily::new(7);
    let session_id = 42;
    let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .copied()
        .map(|domain| {
            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink.clone());
            (domain, sink)
        })
        .collect::<Vec<_>>();

    // Act
    let failed_domains = dispatch_session_cleanup(&router, route_family, session_id);

    // Assert
    assert!(failed_domains.is_empty());
    for (_, sink) in sinks {
        assert_eq!(sink.recorded_sessions(), vec![session_id]);
    }
}

#[test]
pub(super) fn should_report_missing_cleanup_domain_registration() {
    // Arrange
    let router = crate::runtime::Router::new();
    let route_family = crate::runtime::routing::RouteFamily::new(9);
    let session_id = 55;
    let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .copied()
        .filter(|domain| *domain != DispatchDomain::Queue)
        .map(|domain| {
            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink.clone());
            sink
        })
        .collect::<Vec<_>>();

    // Act
    let failed_domains = dispatch_session_cleanup(&router, route_family, session_id);

    // Assert
    assert_eq!(failed_domains, vec![DispatchDomain::Queue]);
    for sink in sinks {
        assert_eq!(sink.recorded_sessions(), vec![session_id]);
    }
}

#[test]
pub(super) fn should_keep_session_cleanup_order_aligned_with_domain_manifest() {
    // Arrange
    let mut cleanup_domains = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .map(|domain| domain.as_str())
        .collect::<Vec<_>>();
    let mut registered_domains = DispatchDomain::ALL
        .iter()
        .map(|domain| domain.as_str())
        .collect::<Vec<_>>();

    // Act
    cleanup_domains.sort_unstable();
    registered_domains.sort_unstable();

    // Assert
    assert_eq!(cleanup_domains, registered_domains);
}

#[tokio::test]
async fn should_cleanup_registered_domains_and_remove_session_state_on_close() {
    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
    let session_id = 77;
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = RouteFamily::new(77);

    let sinks = DispatchDomain::SESSION_CLEANUP_ORDER
        .iter()
        .copied()
        .map(|domain| {
            let sink = Arc::new(CleanupTrackingSink::default());
            router.register_domain_pattern(domain.as_str(), sink.clone());
            sink
        })
        .collect::<Vec<_>>();

    ingress.on_open(session).await.unwrap();
    assert_eq!(admin_read_model.sessions().len(), 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    assert_eq!(ingress.session_count(), 0);
    assert!(ingress.get_session(session_id).is_none());
    assert!(ingress.get_session_actor(session_id).is_none());
    assert!(admin_read_model.sessions().is_empty());
    for sink in sinks {
        assert_eq!(sink.recorded_sessions(), vec![session_id]);
    }
}

#[tokio::test]
async fn should_record_cleanup_failures_when_on_close_cannot_reach_all_domains() {
    // Arrange
    let collector = crate::observability::metrics();

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model.clone());
    let session_id = 88;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(88);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }

        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    let failures_before = collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Assert
    assert!(
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_FAILURES) > failures_before,
        "expected cleanup failure metric to increase"
    );
    assert_eq!(ingress.session_count(), 0);
    assert!(ingress.get_session(session_id).is_none());
    assert!(ingress.get_session_actor(session_id).is_none());
    assert!(admin_read_model.sessions().is_empty());
    assert!(ingress.pending_session_cleanups.contains_key(&session_id));
}

#[tokio::test]
async fn should_retry_pending_session_cleanup_without_later_traffic() {
    // Arrange
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
    let session_id = 89;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(89);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }

        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    assert!(ingress.pending_session_cleanups.contains_key(&session_id));

    let queue_sink = Arc::new(CleanupTrackingSink::default());
    router.register_domain_pattern(DispatchDomain::Queue.as_str(), queue_sink.clone());

    // Act
    tokio::time::timeout(Duration::from_secs(1), async {
        while ingress.pending_session_cleanups.contains_key(&session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup worker should retry without later traffic");

    // Assert
    assert!(!ingress.pending_session_cleanups.contains_key(&session_id));
    assert_eq!(queue_sink.recorded_sessions(), vec![session_id]);
}

#[tokio::test]
async fn should_give_up_and_stop_retrying_session_cleanup_that_can_never_succeed() {
    // Arrange: never register Queue's sink, so its cleanup can never
    // succeed. Without a give-up threshold, the retry worker would keep
    // this ticket pending forever (capped-but-endless exponential backoff),
    // so the pending gauge would never return to zero for a genuinely dead
    // domain actor.
    let collector = crate::observability::metrics();
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let ingress = make_cleanup_ingress(router.clone(), admin_read_model);
    let session_id = 90;
    let mut session = make_session_info(session_id, TransportKind::Tcp);
    session.route_family = RouteFamily::new(90);

    for domain in DispatchDomain::SESSION_CLEANUP_ORDER {
        if domain == DispatchDomain::Queue {
            continue;
        }
        let sink = Arc::new(CleanupTrackingSink::default());
        router.register_domain_pattern(domain.as_str(), sink);
    }

    ingress.on_open(session).await.unwrap();
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    assert!(ingress.pending_session_cleanups.contains_key(&session_id));
    let permanent_failures_before =
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_PERMANENT_FAILURES);

    // Act: Queue's sink is intentionally never registered, so this ticket
    // can never succeed - the worker must eventually give up.
    tokio::time::timeout(Duration::from_secs(10), async {
        while ingress.pending_session_cleanups.contains_key(&session_id) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("cleanup worker should give up instead of retrying forever");

    // Assert
    assert!(!ingress.pending_session_cleanups.contains_key(&session_id));
    assert!(
        collector.counter_get(obs::METRIC_SESSION_CLEANUP_PERMANENT_FAILURES)
            > permanent_failures_before,
        "expected a permanent-failure metric increment"
    );
    assert_eq!(
        collector.gauge_get(obs::METRIC_SESSION_CLEANUP_PENDING),
        0,
        "pending gauge should return to zero after giving up"
    );
}

#[tokio::test]
async fn should_cleanup_real_notice_domain_subscription_on_close() {
    // Arrange
    let family = RouteFamily::new(91);
    let session_id = 91;
    let notice_route = "notice://acme/app/events";
    let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/91"));
    let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
    let notice_address = RouteAddress::new(family, Route::new(notice_route));

    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let notice_sink = Arc::new(NoticeDomainSink::new(
        router.clone(),
        admin_read_model.clone(),
    ));
    let subscriber_mailbox = Arc::new(Mailbox::new(8));

    router.register(subscriber_address.clone(), subscriber_mailbox.clone());
    router.register_domain_pattern("notice", notice_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Notice);

    let ingress = make_cleanup_ingress(router, admin_read_model.clone());
    let mut session = make_session_info(session_id, TransportKind::WebSocket);
    session.route_family = family;
    ingress.on_open(session).await.unwrap();

    notice_sink
        .deliver(Envelope::from_route(
            subscriber_address,
            notice_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(notice_route),
                family,
            ),
        ))
        .expect("subscribe notice route");

    let _subscribe_response = subscriber_mailbox
        .receiver()
        .try_recv()
        .expect("notice subscribe response");
    notice_sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(notice_sink.subscription_count(), Ok(1));
    assert_eq!(admin_read_model.notice_subscriptions(None, None).len(), 1);

    // Act
    ingress.on_close(session_id, CloseReason::ClientClose).await;
    notice_sink.refresh_admin_snapshot_if_dirty();
    notice_sink
        .deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                11,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish after cleanup");

    // Assert
    assert_eq!(notice_sink.subscription_count(), Ok(0));
    assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
    assert!(subscriber_mailbox.receiver().try_recv().is_err());
}

#[tokio::test]
async fn should_cleanup_real_queue_inflight_on_close() {
    // Arrange
    let family = RouteFamily::new(1);
    let sender_session_id = 7;
    let worker_session_id = 92;
    let next_worker_session_id = 12;
    let queue_route = "queue://acme/jobs/emails";
    let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
    let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
    let worker_address = RouteAddress::new(family, Route::new("inbox://session/92"));
    let next_worker_address = RouteAddress::new(family, Route::new("inbox://session/12"));

    let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(crate::runtime::Router::new());
    let admin_read_model = AdminReadModel::new();
    let queue_sink = Arc::new(QueueDomainSink::new(
        store,
        router.clone(),
        admin_read_model.clone(),
        cntryl_midge::WriteOptions::buffered(),
        crate::utils::idempotency::default_dedup_store(),
    ));

    let sender_mailbox = Arc::new(Mailbox::new(8));
    let worker_mailbox = Arc::new(Mailbox::new(8));
    let next_worker_mailbox = Arc::new(Mailbox::new(8));

    router.register(sender_address.clone(), sender_mailbox.clone());
    router.register(worker_address.clone(), worker_mailbox.clone());
    router.register(next_worker_address.clone(), next_worker_mailbox.clone());
    router.register_domain_pattern("queue", queue_sink.clone());
    register_fallback_cleanup_domains(&router, DispatchDomain::Queue);

    let ingress = make_cleanup_ingress(router, admin_read_model.clone());
    let mut worker_session = make_session_info(worker_session_id, TransportKind::WebSocket);
    worker_session.route_family = family;
    ingress.on_open(worker_session).await.unwrap();

    queue_sink
        .deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");
    let _send_ack = sender_mailbox
        .receiver()
        .try_recv()
        .expect("enqueue response");

    queue_sink
        .deliver(Envelope::from_route(
            worker_address,
            queue_address.clone(),
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message");
    let _reserve_ack = worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response");

    queue_sink.refresh_admin_snapshot_if_dirty();
    assert_eq!(admin_read_model.queue_inflight(None).len(), 1);

    // Act
    ingress
        .on_close(worker_session_id, CloseReason::ClientClose)
        .await;
    queue_sink.refresh_admin_snapshot_if_dirty();
    queue_sink
        .deliver(Envelope::from_route(
            next_worker_address,
            queue_address,
            FrameContext::new(
                next_worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message after cleanup");
    queue_sink.refresh_admin_snapshot_if_dirty();

    // Assert
    let reserve_after_cleanup = next_worker_mailbox
        .receiver()
        .try_recv()
        .expect("reserve response after cleanup")
        .into_payload::<FrameContext>()
        .expect("reserve response frame after cleanup");
    assert_eq!(
        queue_receive_response_message_count(&reserve_after_cleanup),
        1
    );

    assert_queue_cleanup_admin_state(&admin_read_model, next_worker_session_id);
}
