use super::NoticeDomainCore;
use crate::runtime::{DeliveryError, Envelope};

pub(super) enum NoticeDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    DeliverAccepted(Envelope),
    ReadSubscriptionCount(crossbeam_channel::Sender<usize>),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
    UnsubscribeAllForSession(u64, crossbeam_channel::Sender<usize>),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

pub(super) struct NoticeDomainRuntime<'a> {
    pub(super) core: &'a NoticeDomainCore,
}

impl NoticeDomainRuntime<'_> {
    pub(super) fn receive(&self, msg: NoticeDomainCommand) {
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
            NoticeDomainCommand::ReadSubscriptionCount(reply) => {
                let _ = reply.send(self.subscription_count());
            }
            NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                self.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
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

    fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }

    fn deliver_accepted_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_accepted_envelope(envelope)
    }

    fn subscription_count(&self) -> usize {
        self.core.subscription_count()
    }

    fn refresh_admin_snapshot_if_dirty(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
    }

    fn unsubscribe_all_for_session(&self, session_id: u64) -> usize {
        self.core.unsubscribe_all_for_session(session_id)
    }
}
