use super::NoticeFamilyState;
use crate::runtime::{DeliveryError, Envelope};

pub(super) enum NoticeDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    DeliverAccepted(Envelope),
    #[cfg(test)]
    ReadSubscriptionCount(crossbeam_channel::Sender<usize>),
    #[cfg(test)]
    ReadStateCounts(crossbeam_channel::Sender<(usize, usize)>),
    #[cfg(test)]
    UnsubscribeAllForSession(u64, crossbeam_channel::Sender<usize>),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

pub(super) struct NoticeFamilyRuntime<'a> {
    pub(super) core: &'a mut NoticeFamilyState,
}

impl NoticeFamilyRuntime<'_> {
    pub(super) fn receive(&mut self, msg: NoticeDomainCommand) {
        match msg {
            NoticeDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(self.deliver_envelope(&envelope));
            }
            NoticeDomainCommand::DeliverAccepted(envelope) => {
                // The publish was already acknowledged, so there is nobody left
                // to report to - but a failure here must still be countable,
                // otherwise an accepted publish that never delivered looks
                // identical to one that did.
                if let Err(error) = self.deliver_accepted_envelope(&envelope) {
                    crate::observability::counter_inc(
                        crate::domains::notice::metrics::METRIC_ACCEPTED_DELIVERY_FAILURES_TOTAL,
                    );
                    tracing::warn!(
                        domain = "notice",
                        error = ?error,
                        "Accepted notice delivery failed after acknowledgement"
                    );
                }
            }
            #[cfg(test)]
            NoticeDomainCommand::ReadSubscriptionCount(reply) => {
                let _ = reply.send(self.subscription_count());
            }
            #[cfg(test)]
            NoticeDomainCommand::ReadStateCounts(reply) => {
                let _ = reply.send((self.core.families.len(), self.core.route_stats.len()));
            }
            #[cfg(test)]
            NoticeDomainCommand::UnsubscribeAllForSession(session_id, reply) => {
                let _ = reply.send(self.unsubscribe_all_for_session(session_id));
            }
            NoticeDomainCommand::PanicForFailpoint => {
                panic!("injected Notice domain actor panic");
            }
            #[cfg(test)]
            NoticeDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
    }

    fn deliver_envelope(&mut self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }

    fn deliver_accepted_envelope(&mut self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_accepted_envelope(envelope)
    }

    #[cfg(test)]
    fn subscription_count(&mut self) -> usize {
        self.core.subscription_count()
    }

    #[cfg(test)]
    fn unsubscribe_all_for_session(&mut self, session_id: u64) -> usize {
        self.core.unsubscribe_all_for_session(session_id)
    }
}
