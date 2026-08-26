//! Mailbox-lane routing and the domain actor's message loop.

use super::model::{LeaseDomainActor, LeaseDomainCommand, LeaseDomainSink, MailboxSink};
use crate::runtime::{Actor, Context};
use crate::runtime::{DeliveryError, Envelope};

impl MailboxSink for LeaseDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor.try_send(LeaseDomainCommand::Deliver(envelope))
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.actor
            .try_send_high_priority(LeaseDomainCommand::Deliver(envelope))
    }
}

impl Actor for LeaseDomainActor {
    type Message = LeaseDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.state.runtime();
        match msg {
            LeaseDomainCommand::Deliver(envelope) => {
                if let Err(error) = runtime.deliver_envelope(&envelope) {
                    tracing::warn!(domain = "lease", error = %error, "Lease actor delivery failed");
                }
            }
            LeaseDomainCommand::CleanupSession(session_id) => {
                runtime.cleanup_session(session_id);
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
                let expired = if let Some(lease) = runtime.core.leases.lock().get_mut(&key) {
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
            LeaseDomainCommand::PanicForTests => {
                panic!("test Lease domain actor panic");
            }
        }
    }
}
