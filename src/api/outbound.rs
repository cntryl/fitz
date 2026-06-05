use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::DeliveryError;
use crate::runtime::router::MailboxSink;

use bytes::{BufMut, Bytes, BytesMut};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// MailboxSink that forwards domain responses to a session's outbound channel
pub struct SessionOutboundSink {
    tx: mpsc::Sender<Bytes>,
}

impl SessionOutboundSink {
    pub fn new(tx: mpsc::Sender<Bytes>) -> Self {
        Self { tx }
    }
}

impl MailboxSink for SessionOutboundSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        const MAX_OUTBOUND_SEND_RETRIES: usize = 100;

        let _deliver_latency =
            crate::observability::ScopedHistogramUs::new(obs::METRIC_OUTBOUND_DELIVER_LATENCY);
        // Expect a FrameContext payload
        if let Some(ctx) = envelope.payload::<FrameContext>() {
            debug!(
                session_id = ctx.session_id,
                msg_type = ctx.msg_type.as_u16(),
                payload_len = ctx.payload.len(),
                "Outbound sink: encoding TLV response for session"
            );
            // TLV-encode a single record directly into an exact-sized buffer.
            let encode_start = Instant::now();
            let bytes = encode_single_tlv_frame(ctx.msg_type, &ctx.payload);
            crate::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            trace!(
                session_id = ctx.session_id,
                encoded_len = bytes.len(),
                "Outbound sink: sending TLV frame to transport channel"
            );

            let mut attempt = 0;
            loop {
                let send_start = Instant::now();
                let send_result = self.tx.try_send(bytes.clone());
                crate::observability::hot_path_histogram_observe_us(
                    obs::METRIC_OUTBOUND_SEND_LATENCY,
                    send_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                );

                match send_result {
                    Ok(()) => {
                        crate::observability::hot_path_counter_inc(obs::METRIC_FRAMES_SENT);
                        debug!(
                            session_id = ctx.session_id,
                            "Outbound sink: frame sent to transport successfully"
                        );
                        return Ok(());
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        let capacity = self.tx.max_capacity();
                        crate::observability::hot_path_counter_inc(
                            obs::METRIC_OUTBOUND_BACKPRESSURE,
                        );
                        attempt += 1;
                        if attempt >= MAX_OUTBOUND_SEND_RETRIES {
                            warn!(
                                session_id = ctx.session_id,
                                capacity = capacity,
                                "Outbound sink: transport channel full"
                            );
                            return Err(DeliveryError::MailboxFull {
                                capacity,
                                current_len: capacity,
                            });
                        }
                        std::thread::yield_now();
                        continue;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        warn!(
                            session_id = ctx.session_id,
                            "Outbound sink: transport channel closed"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            }
        } else {
            // Not a FrameContext - cannot deliver
            warn!(
                destination = ?envelope.destination(),
                "Outbound sink: envelope payload is not FrameContext, cannot deliver"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

fn encode_single_tlv_frame(msg_type: crate::protocol::tlv::MessageType, payload: &[u8]) -> Bytes {
    if payload.len() > u16::MAX as usize {
        panic!(
            "TLV value too large: {} bytes (max {})",
            payload.len(),
            u16::MAX
        );
    }

    let header_len = msg_type.encoded_type_len() + 2;
    let mut out = BytesMut::with_capacity(header_len + payload.len());

    if msg_type.is_single_byte() {
        out.put_u8(msg_type.as_u16() as u8);
    } else {
        out.put_u8(crate::protocol::tlv::MessageType::ESCAPE_MARKER);
        out.extend_from_slice(&msg_type.as_u16().to_be_bytes());
    }

    let payload_len = payload.len() as u16;
    out.extend_from_slice(&payload_len.to_be_bytes());
    out.extend_from_slice(payload);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};

    fn test_envelope(payload: Bytes) -> Envelope {
        Envelope::new(
            RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/1")),
            FrameContext::new(
                1,
                ChannelId::Control,
                MessageType::new(101),
                payload,
                RouteFamily::new(1),
            ),
        )
    }

    #[tokio::test]
    async fn should_deliver_frame_without_blocking_current_thread_runtime() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        let sink = SessionOutboundSink::new(tx);

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        let frame = rx.recv().await.expect("encoded frame");

        // Assert
        assert_eq!(result, Ok(()));
        assert_eq!(frame.as_ref(), &[101, 0, 2, b'o', b'k']);
    }

    #[tokio::test]
    async fn should_return_backpressure_given_full_outbound_channel() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"occupied")).unwrap();
        let sink = SessionOutboundSink::new(tx);

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        let occupied = rx.recv().await.expect("occupied frame");

        // Assert
        assert_eq!(
            result,
            Err(DeliveryError::MailboxFull {
                capacity: 1,
                current_len: 1,
            })
        );
        assert_eq!(occupied, Bytes::from_static(b"occupied"));
    }

    #[tokio::test]
    async fn should_retry_outbound_send_when_channel_is_briefly_full() {
        // Arrange
        let (tx, mut rx) = mpsc::channel(1);
        tx.try_send(Bytes::from_static(b"occupied")).unwrap();

        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("create runtime");
            let occupied = rt.block_on(async { rx.recv().await.expect("drained occupied frame") });
            ready_tx.send(()).expect("signal ready");
            release_rx.recv().expect("wait for release");
            occupied
        });

        let sink = SessionOutboundSink::new(tx);
        ready_rx
            .recv()
            .expect("wait until receiver drained occupied frame");

        // Act
        let result = sink.deliver(test_envelope(Bytes::from_static(b"ok")));
        release_tx.send(()).expect("release receiver thread");

        // Assert
        assert_eq!(result, Ok(()));
        let occupied = handle.join().expect("thread joined");
        assert_eq!(occupied, Bytes::from_static(b"occupied"));
    }
}
