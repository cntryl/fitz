use super::*;
use crate::protocol::tlv::{MessageType, TlvEncoder};
use crate::session::{
    CloseReason, NewSessionConfig, SessionInfo, SessionMetadata, SessionPermissions, TransportKind,
};
use std::sync::Mutex;
use std::time::Duration;

#[derive(Default)]
struct StampingIngress {
    received_at: Mutex<Vec<Instant>>,
    pause_first: bool,
    release_first: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl Ingress for StampingIngress {
    async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
        Ok(42)
    }

    async fn on_frame(
        &self,
        _session_id: u64,
        _channel_id: crate::protocol::frame::ChannelId,
        _msg_type: MessageType,
        _message_payload: Bytes,
        _correlation: Option<std::num::NonZeroU64>,
    ) -> IngressDecision {
        panic!("stamped session processing must use the timestamp hook")
    }

    async fn on_frame_received_at(
        &self,
        _session_id: u64,
        _channel_id: crate::protocol::frame::ChannelId,
        _msg_type: MessageType,
        _message_payload: Bytes,
        _correlation: Option<std::num::NonZeroU64>,
        received_at: Instant,
    ) -> IngressDecision {
        let first = {
            let mut stamps = self.received_at.lock().unwrap();
            stamps.push(received_at);
            stamps.len() == 1
        };
        if first && self.pause_first {
            self.release_first.notified().await;
        }
        IngressDecision::Accept
    }

    fn get_session_info(&self, _session_id: u64) -> Option<SessionInfo> {
        None
    }

    fn record_frame_received(&self, _session_id: u64) {}
    fn record_frame_sent(&self, _session_id: u64) {}
    async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}
}

fn test_session(transport: TransportKind) -> Session {
    Session::new(
        42,
        NewSessionConfig::unauthenticated(
            transport,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            10,
            None,
            crate::runtime::routing::RouteFamily::new(1),
        ),
    )
}

#[tokio::test]
async fn should_keep_transport_stamp_for_all_tlvs_while_first_handoff_waits() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        // Arrange
        let mut session = test_session(transport);
        let ingress = StampingIngress {
            pause_first: true,
            ..StampingIngress::default()
        };
        let received_at = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(200), b"first");
        encoder.encode(MessageType::new(200), b"second");

        // Act
        let mut processing = std::pin::pin!(process_session_frame_at(
            &mut session,
            encoder.finish(),
            &ingress,
            received_at,
        ));
        assert!(futures_util::poll!(processing.as_mut()).is_pending());
        assert_eq!(*ingress.received_at.lock().unwrap(), vec![received_at]);
        ingress.release_first.notify_one();
        let result = processing.await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(*ingress.received_at.lock().unwrap(), vec![received_at; 2]);
    }
}

#[tokio::test]
async fn should_stamp_fragmented_tlv_with_the_completing_transport_frame() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        // Arrange
        let mut session = test_session(transport);
        let ingress = StampingIngress::default();
        let completed_at = Instant::now();
        let started_at = completed_at.checked_sub(Duration::from_secs(2)).unwrap();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(200), b"fragmented");
        let frame = encoder.finish();
        let split = frame.len() - 2;

        // Act
        process_session_frame_at(&mut session, frame.slice(..split), &ingress, started_at)
            .await
            .unwrap();
        assert!(ingress.received_at.lock().unwrap().is_empty());
        let result =
            process_session_frame_at(&mut session, frame.slice(split..), &ingress, completed_at)
                .await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(*ingress.received_at.lock().unwrap(), vec![completed_at]);
    }
}
