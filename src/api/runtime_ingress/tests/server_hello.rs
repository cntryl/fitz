//! HELLO answers CONNECT on session establishment.

use super::*;

struct CapturingEncodedFrameSink(Arc<std::sync::Mutex<Vec<crate::runtime::EncodedClientFrame>>>);

impl MailboxSink for CapturingEncodedFrameSink {
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

#[test]
fn should_answer_connect_with_hello_as_the_first_frame() {
    // Arrange
    // HELLO must reach the client before any domain response, so the client
    // learns the version and limits in force before it reads an answer.
    let rt = tokio::runtime::Runtime::new().unwrap();
    let router = Arc::new(crate::runtime::Router::new());
    let session_id = 9_100;
    let frames = Arc::new(std::sync::Mutex::new(Vec::<
        crate::runtime::EncodedClientFrame,
    >::new()));
    let captured = frames.clone();
    router.register(
        crate::runtime::routing::RouteAddress::new(
            crate::runtime::routing::RouteFamily::new(1),
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        ),
        Arc::new(CapturingEncodedFrameSink(captured)) as Arc<dyn MailboxSink>,
    );
    let ingress = RuntimeIngress::new(false).with_router(router);

    // Act
    let decision = rt.block_on(async {
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

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    let frames = frames.lock().unwrap();
    assert_eq!(frames.len(), 1, "exactly one HELLO per session");
    assert_eq!(
        frames[0].meta.message_type,
        crate::protocol::wire2::control::HELLO
    );
    let hello =
        crate::protocol::wire2::handshake::Hello::decode(&frames[0].payload).expect("HELLO body");
    assert_eq!(hello.version, crate::protocol::wire2::WIRE_VERSION);
    assert_eq!(
        hello.broker_caps,
        crate::protocol::wire2::BROKER_CAPABILITIES
    );
    assert_eq!(
        hello.max_message_bytes,
        crate::protocol::wire2::DEFAULT_MAX_MESSAGE_BYTES
    );
    assert_eq!(hello.session_id, session_id);
    assert!(
        frames[0].meta.correlation.is_none(),
        "HELLO answers the connection, not a correlated request"
    );
}
