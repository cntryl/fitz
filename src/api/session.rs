//! Async transport-edge handoff for synchronously decoded session messages.

use crate::api::runtime_ingress::{Ingress, IngressDecision};
use crate::session::{Session, SessionError};
use bytes::Bytes;
use std::time::Instant;
use tracing::{trace, warn};

/// Decode all complete TLV messages in a transport frame and hand them to ingress.
///
/// # Errors
///
/// Returns `SessionError` when message decoding fails, ingress signals
/// backpressure, or ingress requests the session to close.
pub async fn process_session_frame(
    session: &mut Session,
    frame: Bytes,
    ingress: &dyn Ingress,
) -> Result<(), SessionError> {
    session.push_frame(frame);

    while let Some(message) = session.next_message()? {
        let channel = message.channel;
        let ingress_start = Instant::now();
        let decision = ingress
            .on_frame(
                session.info().session_id,
                channel,
                message.msg_type,
                message.payload,
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
            }
            IngressDecision::Backpressure => {
                warn!(session_id = session.info().session_id, channel = ?channel, "Ingress backpressure");
                return Err(SessionError::Backpressure(channel));
            }
            IngressDecision::Close(reason) => {
                warn!(session_id = session.info().session_id, reason = %reason, "Ingress requested close");
                return Err(SessionError::IngressClose(reason));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::runtime_ingress::IngressDecision;
    use crate::protocol::tlv::{MessageType, TlvEncoder};
    use crate::session::{
        CloseReason, NewSessionConfig, SessionInfo, SessionMetadata, SessionPermissions,
        TransportKind,
    };

    struct BackpressureIngress;

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
        ) -> IngressDecision {
            IngressDecision::Backpressure
        }

        async fn on_close(&self, _session_id: u64, _reason: CloseReason) {}
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
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::new(1), b"backpressure");

        // Act
        let result =
            process_session_frame(&mut session, encoder.finish(), &BackpressureIngress).await;

        // Assert
        assert!(matches!(result, Err(SessionError::Backpressure(_))));
    }
}
