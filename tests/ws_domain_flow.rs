use bytes::Bytes;
use fitz::boot::domains;
use fitz::protocol::tlv::TlvEncoder;
use fitz::runtime::Router;
use fitz::session::{
    Ingress, NewSessionConfig, RuntimeIngress, Session, SessionMetadata, SessionOutboundSink,
    SessionPermissions, TransportKind,
};
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::Arc;

#[tokio::test]
async fn should_route_kv_get_through_ingress_to_kv_and_reply_to_inbox() {
    // Arrange
    let store = create_test_engine_with_cfs(vec![1]);
    let router = Arc::new(Router::new());
    domains::setup(&router, &store).unwrap();

    // Create ingress with router attached
    let ingress = Arc::new(RuntimeIngress::new(false).with_router(router.clone()));

    // Register a session outbound inbox sink
    let session_id = 1u64;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(10);
    let sink = std::sync::Arc::new(SessionOutboundSink::new(tx));
    let inbox_addr = fitz::runtime::routing::RouteAddress::new(
        fitz::runtime::routing::RouteFamily::new(0),
        fitz::runtime::routing::Route::new(format!("inbox://session/{}", session_id)),
    );
    router.register(
        inbox_addr.clone(),
        sink as std::sync::Arc<dyn fitz::runtime::router::MailboxSink>,
    );

    // Create session and open
    let session_config = NewSessionConfig::unauthenticated(
        TransportKind::Tcp,
        None,
        SessionPermissions::empty(),
        SessionMetadata::new(),
        10,
        None,
        fitz::runtime::routing::RouteFamily::new(0),
    );
    let mut session = Session::new(session_id, session_config);
    ingress.on_open(session.info()).await.unwrap();

    // Build a KV GET TLV message (msg_type 103)
    // Per CLIENT_SPEC: resource is implicit from transaction context (established at BEGIN)
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u64.to_be_bytes()); // tx_id
    let route = b"kv://realm/area/resource";
    payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
    payload.extend_from_slice(route);
    let key = b"nonexistent";
    payload.extend_from_slice(&(key.len() as u32).to_be_bytes());
    payload.extend_from_slice(key);

    let mut enc = TlvEncoder::new();
    enc.encode(fitz::protocol::tlv::MessageType::new(103), &payload);
    let frame = enc.finish();

    // Act
    let ingress_ref: &dyn fitz::session::manager::Ingress = ingress.as_ref();
    session.on_frame(frame, ingress_ref).await.unwrap();

    // Assert
    let resp = rx.recv().await.expect("expected response");
    // Decode TLV header to check msg_type and that the payload is a GET result
    let dec = fitz::protocol::tlv::TlvDecoder::new();
    let (record, _) = dec.decode_one(&Bytes::from(resp)).unwrap();
    assert_eq!(record.msg_type().as_u16(), 103);
    // Response body begins with a found byte (0/1)
    let body = record.value();
    assert!(!body.is_empty());
}
