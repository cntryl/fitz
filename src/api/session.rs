//! Async transport-edge handoff for synchronously decoded session messages.

use crate::api::runtime_ingress::{Ingress, IngressDecision};
use crate::session::{Session, SessionError};
use bytes::Bytes;
use std::time::Instant;
use tracing::{trace, warn};

#[cfg(test)]
#[path = "session_receive_timestamps.rs"]
mod receive_timestamps;

pub(super) fn is_stale_session_frame_error(error: &SessionError) -> bool {
    matches!(
        error,
        SessionError::IngressClose(reason) if reason.starts_with("unknown session:")
    )
}

/// Decode the one message in a transport frame and hand it to ingress.
///
/// # Errors
///
/// Returns `SessionError` when the frame is not exactly one message, ingress
/// signals backpressure, or ingress requests the session to close.
pub async fn process_session_frame(
    session: &mut Session,
    frame: Bytes,
    ingress: &dyn Ingress,
) -> Result<(), SessionError> {
    process_session_frame_at(session, frame, ingress, Instant::now()).await
}

/// Decode a transport frame with the time its complete bytes were received.
///
/// # Errors
///
/// Returns the same decoding, backpressure, and close errors as
/// [`process_session_frame`].
pub async fn process_session_frame_at(
    session: &mut Session,
    frame: Bytes,
    ingress: &dyn Ingress,
    received_at: Instant,
) -> Result<(), SessionError> {
    let message = session.decode_frame(&frame)?;
    let channel = message.channel;
    let ingress_start = Instant::now();
    let decision = ingress
        .on_frame_received_at(
            session.info().session_id,
            channel,
            message.msg_type,
            message.payload,
            crate::protocol::wire2::header::HeaderLabels {
                correlation: message.correlation,
                flags: message.flags,
            },
            received_at,
        )
        .await;
    crate::observability::hot_path_histogram_observe_us(
        crate::observability::METRIC_SESSION_INGRESS_HANDOFF_LATENCY,
        u64::try_from(
            ingress_start
                .elapsed()
                .as_micros()
                .min(u128::from(u64::MAX)),
        )
        .unwrap_or(u64::MAX),
    );
    session.release_channel(channel);

    match decision {
        IngressDecision::Accept => {
            trace!(
                session_id = session.info().session_id,
                "Ingress accepted frame"
            );
            Ok(())
        }
        IngressDecision::Backpressure => {
            warn!(session_id = session.info().session_id, channel = ?channel, "Ingress backpressure");
            Err(SessionError::Backpressure(channel))
        }
        IngressDecision::Close(reason) => {
            warn!(session_id = session.info().session_id, reason = %reason, "Ingress requested close");
            Err(SessionError::IngressClose(reason))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::runtime_ingress::IngressDecision;
    use crate::protocol::tlv::MessageType;
    use crate::protocol::wire2::header::{encode_message, HeaderFlags};
    use crate::session::{
        CloseReason, NewSessionConfig, SessionInfo, SessionMetadata, SessionPermissions,
        TransportKind,
    };

    struct BackpressureIngress;

    #[derive(Default)]
    struct RecordingIngress {
        message_types: std::sync::Mutex<Vec<MessageType>>,
        correlations: std::sync::Mutex<Vec<Option<std::num::NonZeroU64>>>,
    }

    #[async_trait::async_trait]
    impl Ingress for BackpressureIngress {
        async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(
            &self,
            _session_id: u64,
            _channel_id: crate::protocol::frame::ChannelId,
            _msg_type: MessageType,
            _message_payload: Bytes,
            _correlation: Option<std::num::NonZeroU64>,
        ) -> IngressDecision {
            IngressDecision::Backpressure
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}

        fn get_session_info(&self, _session_id: u64) -> Option<SessionInfo> {
            None
        }

        fn record_frame_received(&self, _session_id: u64) {}

        fn record_frame_sent(&self, _session_id: u64) {}
    }

    #[async_trait::async_trait]
    impl Ingress for RecordingIngress {
        async fn on_open(&self, _session: SessionInfo) -> Result<u64, String> {
            Ok(1)
        }

        async fn on_frame(
            &self,
            _session_id: u64,
            _channel_id: crate::protocol::frame::ChannelId,
            msg_type: MessageType,
            _message_payload: Bytes,
            correlation: Option<std::num::NonZeroU64>,
        ) -> IngressDecision {
            self.message_types.lock().unwrap().push(msg_type);
            self.correlations.lock().unwrap().push(correlation);
            IngressDecision::Accept
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}

        fn get_session_info(&self, _session_id: u64) -> Option<SessionInfo> {
            None
        }

        fn record_frame_received(&self, _session_id: u64) {}

        fn record_frame_sent(&self, _session_id: u64) {}
    }

    #[tokio::test]
    async fn should_return_backpressure_error_given_ingress_backpressure() {
        // Arrange
        let config = NewSessionConfig::unauthenticated(
            TransportKind::Tcp,
            None,
            SessionPermissions::empty(),
            SessionMetadata::new(),
            10,
            None,
            crate::runtime::routing::RouteFamily::new(1),
        );
        let mut session = Session::new(42, config);
        let frame = Bytes::from(encode_message(
            1,
            HeaderFlags::none(),
            None,
            b"backpressure",
        ));

        // Act
        let result = process_session_frame(&mut session, frame, &BackpressureIngress).await;

        // Assert
        assert!(matches!(result, Err(SessionError::Backpressure(_))));
    }

    fn test_session() -> Session {
        Session::new(
            42,
            NewSessionConfig::unauthenticated(
                TransportKind::Tcp,
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
    async fn should_reject_a_frame_carrying_more_than_one_message() {
        // Arrange
        let mut session = test_session();
        let mut frame = encode_message(1, HeaderFlags::none(), None, b"token");
        frame.extend_from_slice(&encode_message(200, HeaderFlags::none(), None, b"mutation"));
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, Bytes::from(frame), &ingress).await;

        // Assert
        assert!(matches!(result, Err(SessionError::Decode(_))));
        assert!(ingress.message_types.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn should_pass_the_header_correlation_to_ingress() {
        // Arrange
        let mut session = test_session();
        let correlation = std::num::NonZeroU64::new(77);
        let frame = Bytes::from(encode_message(
            200,
            HeaderFlags::none(),
            correlation,
            b"mutation",
        ));
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, frame, &ingress).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(
            *ingress.message_types.lock().unwrap(),
            vec![MessageType::new(200)]
        );
        assert_eq!(*ingress.correlations.lock().unwrap(), vec![correlation]);
    }

    #[tokio::test]
    async fn should_pass_no_correlation_for_a_zero_header() {
        // Arrange
        let mut session = test_session();
        let frame = Bytes::from(encode_message(202, HeaderFlags::none(), None, b"bare"));
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, frame, &ingress).await;

        // Assert
        assert!(result.is_ok());
        assert_eq!(*ingress.correlations.lock().unwrap(), vec![None]);
    }
}
