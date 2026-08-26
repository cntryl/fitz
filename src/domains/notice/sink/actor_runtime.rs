use super::NoticeDomainCore;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{Actor, Context, DeliveryError, Envelope};
use std::sync::Arc;

pub(super) enum NoticeDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    DeliverAccepted(Envelope),
    ReadSubscriptionCount(crossbeam_channel::Sender<usize>),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
    UnsubscribeAllForSession(u64, crossbeam_channel::Sender<usize>),
    #[cfg(test)]
    PanicForTests,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
}

pub(super) struct NoticeDomainActor {
    core: Arc<NoticeDomainCore>,
}

struct NoticeDomainRuntime<'a> {
    core: &'a NoticeDomainCore,
}

impl NoticeDomainActor {
    pub(super) fn new(core: Arc<NoticeDomainCore>) -> Self {
        Self { core }
    }

    pub(super) fn route_address() -> RouteAddress {
        RouteAddress::new(RouteFamily::new(0), Route::new("internal://domain/notice"))
    }

    fn runtime(&self) -> NoticeDomainRuntime<'_> {
        NoticeDomainRuntime { core: &self.core }
    }
}

impl Actor for NoticeDomainActor {
    type Message = NoticeDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            NoticeDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(runtime.deliver_envelope(&envelope));
            }
            NoticeDomainCommand::DeliverAccepted(envelope) => {
                // The publish was already acknowledged, so there is nobody left
                // to report to - but a failure here must still be countable,
                // otherwise an accepted publish that never delivered looks
                // identical to one that did.
                if let Err(error) = runtime.deliver_accepted_envelope(&envelope) {
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
                let _ = reply.send(runtime.subscription_count());
            }
            NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            NoticeDomainCommand::UnsubscribeAllForSession(session_id, reply) => {
                let _ = reply.send(runtime.unsubscribe_all_for_session(session_id));
            }
            #[cfg(test)]
            NoticeDomainCommand::PanicForTests => {
                panic!("test Notice domain actor panic");
            }
            #[cfg(test)]
            NoticeDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
        }
    }
}

impl NoticeDomainRuntime<'_> {
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
