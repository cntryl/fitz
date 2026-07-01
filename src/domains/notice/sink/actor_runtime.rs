use super::NoticeDomainCore;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use crate::runtime::{Actor, Context, DeliveryError, Envelope};
use std::sync::Arc;

pub(super) enum NoticeDomainCommand {
    Deliver(
        Envelope,
        crossbeam_channel::Sender<Result<(), DeliveryError>>,
    ),
    ReadSubscriptionCount(crossbeam_channel::Sender<usize>),
    RefreshAdminSnapshotIfDirty(crossbeam_channel::Sender<()>),
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
            NoticeDomainCommand::ReadSubscriptionCount(reply) => {
                let _ = reply.send(runtime.subscription_count());
            }
            NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
        }
    }
}

impl NoticeDomainRuntime<'_> {
    fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }

    fn subscription_count(&self) -> usize {
        self.core.subscription_count()
    }

    fn refresh_admin_snapshot_if_dirty(&self) {
        self.core.refresh_admin_snapshot_if_dirty();
    }
}
