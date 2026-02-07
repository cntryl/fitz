use crate::protocol::frame_context::FrameContext;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::DeliveryError;
use crate::runtime::router::MailboxSink;

use tokio::sync::mpsc;
use tracing::{debug, trace, warn};

/// MailboxSink that forwards domain responses to a session's outbound channel
pub struct SessionOutboundSink {
    tx: mpsc::Sender<Vec<u8>>,
}

impl SessionOutboundSink {
    pub fn new(tx: mpsc::Sender<Vec<u8>>) -> Self {
        Self { tx }
    }
}

impl MailboxSink for SessionOutboundSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // Expect a FrameContext payload
        if let Some(ctx) = envelope.payload::<FrameContext>() {
            debug!(
                session_id = ctx.session_id,
                msg_type = ctx.msg_type.as_u16(),
                payload_len = ctx.payload.len(),
                "Outbound sink: encoding TLV response for session"
            );
            // TLV-encode a single record: [msg_type][len][value]
            let mut enc = crate::protocol::tlv::TlvEncoder::new();
            enc.encode(ctx.msg_type, &ctx.payload);
            let bytes = enc.finish().to_vec();

            trace!(
                session_id = ctx.session_id,
                encoded_len = bytes.len(),
                "Outbound sink: sending TLV frame to transport channel"
            );

            // Send TLV frame bytes to the outbound channel (sync try_send)
            match self.tx.try_send(bytes) {
                Ok(()) => {
                    debug!(
                        session_id = ctx.session_id,
                        "Outbound sink: frame sent to transport successfully"
                    );
                    Ok(())
                }
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(_) => {
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
