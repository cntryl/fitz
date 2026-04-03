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
        let _deliver_latency = crate::boot::observability::ScopedHistogramUs::new(
            obs::METRIC_OUTBOUND_DELIVER_LATENCY,
        );
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
            crate::boot::observability::hot_path_histogram_observe_us(
                obs::METRIC_OUTBOUND_ENCODE_LATENCY,
                encode_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
            );

            trace!(
                session_id = ctx.session_id,
                encoded_len = bytes.len(),
                "Outbound sink: sending TLV frame to transport channel"
            );

            // Send TLV frame bytes to the outbound channel (sync try_send)
            let send_start = Instant::now();
            match self.tx.try_send(bytes) {
                Ok(()) => {
                    crate::boot::observability::hot_path_histogram_observe_us(
                        obs::METRIC_OUTBOUND_SEND_LATENCY,
                        send_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                    );
                    crate::boot::observability::hot_path_counter_inc(obs::METRIC_FRAMES_SENT);
                    debug!(
                        session_id = ctx.session_id,
                        "Outbound sink: frame sent to transport successfully"
                    );
                    Ok(())
                }
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(_) => {
                        crate::boot::observability::hot_path_histogram_observe_us(
                            obs::METRIC_OUTBOUND_SEND_LATENCY,
                            send_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                        );
                        crate::boot::observability::counter_inc(obs::METRIC_OUTBOUND_BACKPRESSURE);
                        warn!(
                            session_id = ctx.session_id,
                            "Outbound sink: transport channel full (backpressure)"
                        );
                        Err(DeliveryError::MailboxFull {
                            capacity: 0,
                            current_len: 0,
                        })
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        crate::boot::observability::hot_path_histogram_observe_us(
                            obs::METRIC_OUTBOUND_SEND_LATENCY,
                            send_start.elapsed().as_micros().min(u64::MAX as u128) as u64,
                        );
                        warn!(
                            session_id = ctx.session_id,
                            "Outbound sink: transport channel closed"
                        );
                        Err(DeliveryError::ActorStopped)
                    }
                },
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
