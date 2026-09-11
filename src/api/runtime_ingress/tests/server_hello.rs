//! Capability advertisement on session establishment.

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
fn should_announce_capabilities_as_the_first_frame_on_a_new_session() {
    // Arrange
    // The advertisement must reach the client before any domain response, or a
    // client cannot know whether the response it is about to read can carry a
    // correlation record.
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
                Bytes::new(),
                None,
            )
            .await
    });

    // Assert
    assert_eq!(decision, IngressDecision::Accept);
    let frames = frames.lock().unwrap();
    assert_eq!(frames.len(), 1, "exactly one advertisement per session");
    assert_eq!(
        frames[0].meta.message_type,
        crate::protocol::tlv::MessageType::SERVER_HELLO.as_u16()
    );
    assert_eq!(
        crate::protocol::correlation::decode_server_hello(&frames[0].payload),
        Some((
            crate::protocol::correlation::PROTOCOL_VERSION,
            crate::protocol::correlation::SUPPORTED_CAPABILITIES
        ))
    );
    assert!(
        frames[0].meta.correlation.is_none(),
        "an unsolicited advertisement answers no request"
    );
}
