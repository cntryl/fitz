//! Mailbox-lane routing and the domain actor's message loop.

use super::state_model::{
    DeliveryError, Envelope, MailboxSink, RpcDomainActor, RpcDomainCommand, RpcDomainSink,
};
use crate::runtime::{Actor, Context};

impl MailboxSink for RpcDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_with_priority(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_with_priority(envelope, true)
    }
}

impl RpcDomainSink {
    fn deliver_with_priority(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        // Family liveness is gated per-family inside `try_enqueue` below
        // (`FamilyActorPoolRuntime::is_family_running`) -- a panic scoped to
        // one route family must not reject delivery to every other family
        // sharing this pool.
        if !self.actor.is_running() {
            return Err(DeliveryError::ActorStopped);
        }
        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, high_priority)
        } else {
            self.deliver_to_actor(envelope, high_priority)
        }
    }
}

impl Actor for RpcDomainActor {
    type Message = RpcDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            RpcDomainCommand::Deliver(envelope, reply) => {
                let _ = reply.send(runtime.deliver_envelope(&envelope));
            }
            RpcDomainCommand::ExpireTimedOutRequestsAt(now, reply) => {
                runtime.expire_timed_out_requests_at(now);
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            RpcDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            #[cfg(test)]
            RpcDomainCommand::SyncAdminSnapshot(reply) => {
                runtime.sync_admin_snapshot();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            RpcDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            RpcDomainCommand::ApplySessionCleanupForTests(session_id, reply) => {
                let _ = reply.send(runtime.apply_session_cleanup(session_id));
            }
            #[cfg(test)]
            RpcDomainCommand::ApplyWorkerUnsubscribeForTests(worker_addr, session_id, reply) => {
                let _ = reply.send(runtime.apply_worker_unsubscribe(&worker_addr, session_id));
            }
            #[cfg(test)]
            RpcDomainCommand::PanicForTests => {
                panic!("test RPC domain actor panic");
            }
        }
    }
}

impl RpcDomainSink {
    fn deliver_to_family(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let Some(runtime) = self.family_runtime.as_ref() else {
            return Err(DeliveryError::ActorStopped);
        };
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = *envelope.destination().family();
        let command = RpcDomainCommand::Deliver(envelope, reply_tx);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)?;

        // Family delivery is called synchronously by the async transport edge.
        // Client responses are routed by the actor itself; waiting here would
        // block a Tokio worker while the synchronous domain actor runs.
        drop(reply_rx);
        Ok(())
    }

    fn family_enqueue_error(error: crate::runtime::FamilyActorEnqueueError) -> DeliveryError {
        match error {
            crate::runtime::FamilyActorEnqueueError::NormalLaneFull => DeliveryError::MailboxFull {
                capacity: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
                current_len: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            },
            crate::runtime::FamilyActorEnqueueError::ControlLaneFull => {
                DeliveryError::HighLaneFull {
                    capacity: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                    current_len: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                }
            }
            crate::runtime::FamilyActorEnqueueError::UnknownFamily
            | crate::runtime::FamilyActorEnqueueError::ActorStopped => DeliveryError::ActorStopped,
        }
    }

    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = RpcDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        // Reporting a busy actor as a stopped one costs the caller its session:
        // ingress treats `ActorStopped` as fatal but `Timeout` as retryable.
        reply_rx
            .recv_timeout(super::state_model::RPC_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}
