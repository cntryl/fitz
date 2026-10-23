//! GOAWAY reaches every live session when the broker drains.

use super::*;

struct CapturingSink(Arc<std::sync::Mutex<Vec<crate::runtime::EncodedClientFrame>>>);

impl MailboxSink for CapturingSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(frame) = envelope.payload::<crate::runtime::EncodedClientFrame>() {
            self.0.lock().unwrap().push(frame.clone());
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

type Captured = Arc<std::sync::Mutex<Vec<crate::runtime::EncodedClientFrame>>>;

fn open_connected_session(
    rt: &tokio::runtime::Runtime,
    router: &Arc<crate::runtime::Router>,
    ingress: &RuntimeIngress,
    session_id: u64,
) -> Captured {
    let frames: Captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    router.register(
        crate::runtime::routing::RouteAddress::new(
            crate::runtime::routing::RouteFamily::new(1),
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        ),
        Arc::new(CapturingSink(frames.clone())) as Arc<dyn MailboxSink>,
    );
    rt.block_on(async {
        ingress
            .on_open(make_session_info(session_id, TransportKind::Tcp))
            .await
            .unwrap();
        ingress
            .on_frame(
                session_id,
                ChannelId::Control,
                crate::protocol::tlv::MessageType::CONNECT,
                connect_payload(""),
                None,
            )
            .await
    });
    frames
}

fn decoded_goaway(frames: &Captured) -> crate::protocol::wire2::handshake::GoAway {
    let frames = frames.lock().unwrap();
    assert_eq!(frames.len(), 2, "HELLO then GOAWAY, got {frames:?}");
    assert_eq!(
        frames[1].meta.message_type,
        crate::protocol::wire2::control::GOAWAY
    );
    assert!(frames[1].meta.correlation.is_none());
    crate::protocol::wire2::handshake::GoAway::decode(&frames[1].payload).expect("GOAWAY body")
}

#[test]
fn should_send_goaway_with_the_grace_to_every_live_session() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();
    let router = Arc::new(crate::runtime::Router::new());
    let ingress = RuntimeIngress::new(false).with_router(router.clone());
    let first = open_connected_session(&rt, &router, &ingress, 9_200);
    let second = open_connected_session(&rt, &router, &ingress, 9_201);

    // Act
    ingress.announce_goaway(
        crate::protocol::wire2::catalog::DRAINING,
        25_000,
        "redeploy",
    );

    // Assert
    for frames in [&first, &second] {
        let goaway = decoded_goaway(frames);
        assert_eq!(goaway.code, crate::protocol::wire2::catalog::DRAINING);
        assert_eq!(goaway.grace_ms, 25_000);
        assert_eq!(goaway.reason, "redeploy");
    }
}

#[test]
fn should_announce_goaway_when_the_runtime_begins_draining() {
    // Arrange
    let rt = tokio::runtime::Runtime::new().unwrap();
    let router = Arc::new(crate::runtime::Router::new());
    let ingress = Arc::new(RuntimeIngress::new(false).with_router(router.clone()));
    let frames = open_connected_session(&rt, &router, &ingress, 9_202);
    let runtime = crate::boot::Runtime::new(router.clone());
    runtime.attach_ingress(ingress.clone());

    // Act
    runtime.begin_drain();
    runtime.begin_drain();

    // Assert
    let goaway = decoded_goaway(&frames);
    assert_eq!(goaway.code, crate::protocol::wire2::catalog::DRAINING);
    assert_eq!(
        u64::from(goaway.grace_ms),
        runtime.drain_grace_seconds() * 1_000
    );
    assert_eq!(goaway.reason, runtime.drain_close_reason());
}
