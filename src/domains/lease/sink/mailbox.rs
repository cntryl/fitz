//! Mailbox-lane routing and the family runtime's command loop.

use super::model::{LeaseDomain, LeaseDomainCommand, LeaseFamilyRuntime};
use crate::runtime::{DeliveryError, Envelope, MailboxSink};

impl MailboxSink for LeaseDomain {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self
                .cleanup_family_session(*envelope.destination().family(), cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.enqueue(
            family,
            crate::runtime::FamilyActorLane::Normal,
            LeaseDomainCommand::Deliver(envelope),
        )
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            return self
                .cleanup_family_session(*envelope.destination().family(), cleanup.session_id);
        }
        let family = *envelope.destination().family();
        self.enqueue(
            family,
            crate::runtime::FamilyActorLane::Control,
            LeaseDomainCommand::Deliver(envelope),
        )
    }
}

impl LeaseFamilyRuntime<'_> {
    pub(super) fn receive(&mut self, msg: LeaseDomainCommand) {
        let runtime = self;
        match msg {
            LeaseDomainCommand::Deliver(envelope) => {
                if let Err(error) = runtime.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "lease", error = %error, "Lease actor delivery failed");
                }
            }
            LeaseDomainCommand::CleanupSession(session_id, reply) => {
                runtime.cleanup_session(session_id);
                let _ = reply.send(());
            }
            LeaseDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            LeaseDomainCommand::ReadWaiters(reply) => {
                let _ = reply.send(runtime.admin_waiters());
            }
            LeaseDomainCommand::SweepExpiredState => {
                runtime.sweep_expired_state();
            }
            #[cfg(any(test, feature = "benchkit"))]
            LeaseDomainCommand::ApplyAcquireForBench(request, reply) => {
                let _ = reply.send(runtime.handle_acquire(request));
            }
            #[cfg(any(test, feature = "benchkit"))]
            LeaseDomainCommand::ApplyReleaseForBench(key, owner_id, fencing_token, reply) => {
                let _ = reply.send(runtime.handle_release(&key, owner_id.as_str(), fencing_token));
            }
            #[cfg(test)]
            LeaseDomainCommand::ApplyAcquireForTests(request, reply) => {
                let _ = reply.send(runtime.handle_acquire(request));
            }
            #[cfg(test)]
            LeaseDomainCommand::ApplyExtendForTests(
                key,
                owner_id,
                fencing_token,
                ttl_secs,
                reply,
            ) => {
                let _ = reply.send(runtime.handle_extend(
                    &key,
                    owner_id.as_str(),
                    fencing_token,
                    ttl_secs,
                ));
            }
            #[cfg(test)]
            LeaseDomainCommand::ExpireLeaseForTests(key, reply) => {
                let expired = if let Some(lease) = runtime.core.leases.get_mut(&key) {
                    lease.expiry = std::time::Instant::now()
                        .checked_sub(std::time::Duration::from_millis(1))
                        .expect("past instant");
                    true
                } else {
                    false
                };
                let _ = reply.send(expired);
            }
            #[cfg(test)]
            LeaseDomainCommand::ReadPendingWaiterCountForTests(key, reply) => {
                let _ = reply.send(runtime.pending_waiter_count(&key));
            }
            #[cfg(test)]
            LeaseDomainCommand::ApplyListForTests(
                family_id,
                pattern,
                cursor,
                limit,
                session_id,
                reply,
            ) => {
                let _ =
                    reply.send(runtime.handle_list(family_id, &pattern, cursor, limit, session_id));
            }
            LeaseDomainCommand::PanicForFailpoint => {
                panic!("injected Lease domain actor panic");
            }
            #[cfg(test)]
            LeaseDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
            }
            #[cfg(test)]
            LeaseDomainCommand::InspectForTests(inspect, reply) => {
                inspect(runtime.core);
                let _ = reply.send(());
            }
            #[cfg(test)]
            LeaseDomainCommand::SweepListSnapshotsForTests(reply) => {
                runtime.sweep_idle_list_snapshots();
                let _ = reply.send(());
            }
        }
    }
}
