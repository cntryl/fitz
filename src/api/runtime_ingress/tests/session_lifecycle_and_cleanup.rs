use super::*;
pub(super) use crate::auth::Access;
pub(super) use crate::control::admin::read_model::AdminReadModel;
pub(super) use crate::domains::kv::sink::KvDomain;
pub(super) use crate::domains::lease::sink::LeaseDomain;
pub(super) use crate::domains::notice::sink::NoticeDomain;
pub(super) use crate::domains::queue::sink::QueueDomain;
pub(super) use crate::domains::rpc::sink::RpcDomain;
pub(super) use crate::domains::schedule::sink::ScheduleDomain;
pub(super) use crate::domains::stream::sink::StreamDomain;
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

struct BlockingSessionCleanupSink {
    entered: crossbeam_channel::Sender<u64>,
    release: crossbeam_channel::Receiver<()>,
    blocked_sessions: Mutex<std::collections::HashSet<u64>>,
}

impl MailboxSink for BlockingSessionCleanupSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        let should_block = self
            .blocked_sessions
            .lock()
            .expect("lock blocked sessions")
            .insert(cleanup.session_id);
        if should_block {
            self.entered
                .send(cleanup.session_id)
                .expect("record entered cleanup");
            self.release.recv().expect("release session cleanup");
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
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
    ingress.registry.session_actors.insert(session_id, actor);
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

        crate::api::jwks::cache_jwks_from_json(TEST_AUTH_JWKS_URL, &jwks).unwrap();
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

/// Distinguishes which mailbox lane a delivery arrived on.
#[derive(Default)]
pub(super) struct LaneTrackingSink {
    normal_lane_sessions: Mutex<Vec<u64>>,
    high_priority_sessions: Mutex<Vec<u64>>,
}

impl MailboxSink for LaneTrackingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        self.normal_lane_sessions
            .lock()
            .unwrap()
            .push(cleanup.session_id);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let cleanup = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .expect("cleanup payload");
        self.high_priority_sessions
            .lock()
            .unwrap()
            .push(cleanup.session_id);
        Ok(())
    }
}

#[test]
pub(super) fn should_dispatch_session_cleanup_on_the_control_lane() {
    // Arrange
    // Cleanup is control-plane work (architecture.md: "A separate bounded
    // control lane prevents control work from being hidden behind normal-lane
    // pressure"). A busy Queue actor can hold 16,384 client messages ahead of
    // anything on the normal lane; a cleanup command enqueued there can sit
    // long past the coordinator's 2.3s give-up window, leaving a disconnected
    // session's watches and reservations alive and inviting a duplicate
    // cleanup ticket for the same session.
    let router = crate::runtime::Router::new();
    let route_family = crate::runtime::routing::RouteFamily::new(11);
    let session_id = 77;
    let sink = Arc::new(LaneTrackingSink::default());
    router.register_domain_pattern(DispatchDomain::Queue.as_str(), sink.clone());

    // Act
    let _ = dispatch_session_cleanup_for_domains(
        &router,
        route_family,
        session_id,
        &[DispatchDomain::Queue],
    );

    // Assert
    assert_eq!(
        sink.high_priority_sessions.lock().unwrap().as_slice(),
        &[session_id],
        "session cleanup must be delivered on the control lane, not the normal one"
    );
    assert!(
        sink.normal_lane_sessions.lock().unwrap().is_empty(),
        "session cleanup must not compete with client traffic on the normal lane"
    );
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

include!("session_lifecycle_and_cleanup_more.rs");
