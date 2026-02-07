use crate::protocol::frame_context::FrameContext;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::DeliveryError;
use crate::runtime::router::MailboxSink;

use tokio::sync::mpsc;

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
            // TLV-encode a single record: [msg_type][len][value]
            let mut enc = crate::protocol::tlv::TlvEncoder::new();
            enc.encode(ctx.msg_type, &ctx.payload);
            let bytes = enc.finish().to_vec();

            // Send TLV frame bytes to the outbound channel (sync try_send)
            match self.tx.try_send(bytes) {
                Ok(()) => Ok(()),
                Err(e) => match e {
                    mpsc::error::TrySendError::Full(_) => Err(DeliveryError::MailboxFull {
                        capacity: 0,
                        current_len: 0,
                    }),
                    mpsc::error::TrySendError::Closed(_) => Err(DeliveryError::ActorStopped),
                },
            }
        } else {
            // Not a FrameContext - cannot deliver
            Err(DeliveryError::ActorStopped)
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}
