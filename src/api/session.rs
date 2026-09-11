//! Async transport-edge handoff for synchronously decoded session messages.

use crate::api::runtime_ingress::{Ingress, IngressDecision};
use crate::session::{Session, SessionError};
use bytes::Bytes;
use std::time::Instant;
use tracing::{trace, warn};

pub(super) fn is_stale_session_frame_error(error: &SessionError) -> bool {
    matches!(
        error,
        SessionError::IngressClose(reason) if reason.starts_with("unknown session:")
    )
}

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
    session.push_frame(&frame);
    let mut connected_in_frame = false;

    while let Some(message) = session.next_message()? {
        let channel = message.channel;
        if connected_in_frame {
            session.release_channel(channel);
            return Err(SessionError::IngressClose(
                "CONNECT must be sent in a dedicated transport frame".to_string(),
            ));
        }
        let is_connect = message.msg_type == crate::protocol::tlv::MessageType::CONNECT;
        let ingress_start = Instant::now();
        let decision = ingress
            .on_frame(
                session.info().session_id,
                channel,
                message.msg_type,
                message.payload,
                message.correlation,
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
                connected_in_frame = is_connect;
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

    // A CORRELATE record labels the record that follows it, so one left over at
    // the end of a transport frame labels nothing. Only here is the frame
    // boundary visible - the TLV decoder sees a continuous byte stream - which
    // is why this check lives at the transport edge rather than in `Session`,
    // the same split the CONNECT-isolation rule above already uses.
    if session.has_dangling_correlation() {
        warn!(
            session_id = session.info().session_id,
            "CORRELATE record did not label a request"
        );
        return Err(SessionError::IngressClose(
            "CORRELATE must label a request in the same transport frame".to_string(),
        ));
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

    #[tokio::test]
    async fn should_reject_messages_pipelined_after_connect() {
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
        encoder.encode(MessageType::CONNECT, b"token");
        encoder.encode(MessageType::new(200), b"mutation");
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, encoder.finish(), &ingress).await;

        // Assert
        assert!(matches!(result, Err(SessionError::IngressClose(_))));
        assert_eq!(
            *ingress.message_types.lock().unwrap(),
            vec![MessageType::CONNECT]
        );
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

    fn correlate_bytes(value: u64) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[tokio::test]
    async fn should_attach_correlation_to_the_record_it_labels() {
        // Arrange
        let mut session = test_session();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &correlate_bytes(77));
        encoder.encode(MessageType::new(200), b"mutation");
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, encoder.finish(), &ingress).await;

        // Assert
        // CORRELATE is absorbed, so ingress sees only the request it labelled -
        // but it must see that request carrying the id, not merely see it.
        assert!(result.is_ok());
        assert_eq!(
            *ingress.message_types.lock().unwrap(),
            vec![MessageType::new(200)]
        );
        assert_eq!(
            *ingress.correlations.lock().unwrap(),
            vec![std::num::NonZeroU64::new(77)]
        );
        assert!(!session.has_dangling_correlation());
    }

    #[tokio::test]
    async fn should_not_leak_a_correlation_onto_the_next_unlabelled_request() {
        // Arrange
        // The stash is per-record, not sticky. If it leaked, the request after
        // a correlated one would answer the wrong caller - the precise failure
        // this feature exists to prevent.
        let mut session = test_session();
        let mut frame = Vec::new();
        let mut record = TlvEncoder::new();
        record.encode(MessageType::CORRELATE, &correlate_bytes(77));
        frame.extend_from_slice(&record.finish());
        let mut labelled = TlvEncoder::new();
        labelled.encode(MessageType::new(200), b"labelled");
        frame.extend_from_slice(&labelled.finish());
        let mut bare = TlvEncoder::new();
        bare.encode(MessageType::new(202), b"bare");
        frame.extend_from_slice(&bare.finish());
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, Bytes::from(frame), &ingress).await;

        // Assert
        assert!(result.is_ok(), "unexpected: {result:?}");
        assert_eq!(
            *ingress.correlations.lock().unwrap(),
            vec![std::num::NonZeroU64::new(77), None]
        );
    }

    #[tokio::test]
    async fn should_reject_a_correlate_that_labels_no_request() {
        // Arrange
        // A trailing CORRELATE labels nothing. Accepting it would let the next
        // transport frame's first request silently inherit a stale id, which is
        // exactly the misdelivery this feature exists to prevent.
        let mut session = test_session();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &correlate_bytes(77));
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, encoder.finish(), &ingress).await;

        // Assert
        assert!(matches!(
            result,
            Err(SessionError::IngressClose(reason))
                if reason.contains("CORRELATE must label a request")
        ));
        assert!(ingress.message_types.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn should_reject_a_correlate_that_labels_another_correlate() {
        // Arrange
        let mut session = test_session();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &correlate_bytes(77));
        let first = encoder.finish();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &correlate_bytes(78));
        let mut frame = first.to_vec();
        frame.extend_from_slice(&encoder.finish());
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, Bytes::from(frame), &ingress).await;

        // Assert
        assert!(matches!(
            result,
            Err(SessionError::IngressClose(reason))
                if reason.contains("not another CORRELATE")
        ));
    }

    #[tokio::test]
    async fn should_reject_the_reserved_zero_correlation() {
        // Arrange
        let mut session = test_session();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &correlate_bytes(0));
        encoder.encode(MessageType::new(200), b"mutation");
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, encoder.finish(), &ingress).await;

        // Assert
        assert!(matches!(
            result,
            Err(SessionError::IngressClose(reason)) if reason.contains("reserved")
        ));
    }

    #[tokio::test]
    async fn should_reject_a_malformed_correlate_width() {
        // Arrange
        let mut session = test_session();
        let mut encoder = TlvEncoder::new();
        encoder.encode(MessageType::CORRELATE, &[1, 2, 3]);
        encoder.encode(MessageType::new(200), b"mutation");
        let ingress = RecordingIngress::default();

        // Act
        let result = process_session_frame(&mut session, encoder.finish(), &ingress).await;

        // Assert
        assert!(matches!(
            result,
            Err(SessionError::IngressClose(reason)) if reason.contains("malformed CORRELATE")
        ));
    }

    #[tokio::test]
    async fn should_not_consume_a_channel_slot_for_correlate_records() {
        // Arrange
        // CORRELATE is absorbed before mux routing. If it were routed instead,
        // every correlated request would also take a Control-channel slot, and
        // a client pipelining more than the channel capacity would be closed
        // for backpressure it never caused. Capacity here is 10, so 30 labelled
        // requests prove the slot is never taken.
        let mut session = test_session();
        let ingress = RecordingIngress::default();

        // Act
        let mut frame = Vec::new();
        for id in 1..=30u64 {
            let mut record = TlvEncoder::new();
            record.encode(MessageType::CORRELATE, &correlate_bytes(id));
            frame.extend_from_slice(&record.finish());
            let mut request = TlvEncoder::new();
            request.encode(MessageType::new(200), b"mutation");
            frame.extend_from_slice(&request.finish());
        }
        let result = process_session_frame(&mut session, Bytes::from(frame), &ingress).await;

        // Assert
        assert!(result.is_ok(), "unexpected: {result:?}");
        assert_eq!(ingress.message_types.lock().unwrap().len(), 30);
    }
}
