use super::{DeliveryError, Envelope, MailboxSink, NoticeDomainSink};

impl MailboxSink for NoticeDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, true)
    }
}
