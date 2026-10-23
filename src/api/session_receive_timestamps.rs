use super::*;
use crate::protocol::message::MessageType;
use crate::protocol::wire2::header::{encode_message, HeaderFlags};
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
        _labels: crate::protocol::wire2::header::HeaderLabels,
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
async fn should_stamp_the_message_with_its_transport_receive_time() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        // Arrange
        let mut session = test_session(transport);
        let ingress = StampingIngress::default();
        let received_at = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        let frame = Bytes::from(encode_message(200, HeaderFlags::none(), None, b"first"));

        // Act
        let result = process_session_frame_at(&mut session, frame, &ingress, received_at).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(*ingress.received_at.lock().unwrap(), vec![received_at]);
    }
}

#[tokio::test]
async fn should_keep_the_stamp_while_the_handoff_waits() {
    for transport in [TransportKind::Tcp, TransportKind::WebSocket] {
        // Arrange
        let mut session = test_session(transport);
        let ingress = StampingIngress {
            pause_first: true,
            ..StampingIngress::default()
        };
        let received_at = Instant::now().checked_sub(Duration::from_secs(2)).unwrap();
        let frame = Bytes::from(encode_message(200, HeaderFlags::none(), None, b"first"));

        // Act
        let mut processing = std::pin::pin!(process_session_frame_at(
            &mut session,
            frame,
            &ingress,
            received_at,
        ));
        let pending = futures_util::poll!(processing.as_mut()).is_pending();
        ingress.release_first.notify_one();
        let result = processing.await;

        // Assert
        assert!(pending);
        assert!(result.is_ok());
        assert_eq!(*ingress.received_at.lock().unwrap(), vec![received_at]);
    }
}
