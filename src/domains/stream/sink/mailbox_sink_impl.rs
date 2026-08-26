use super::model::{
    admit_stream_client_delivery, record_stream_service_sample, Arc, DeliveryError, Envelope,
    Instant, MailboxSink, Mutex, Ordering, PayloadEncoder, Route, RouteFamily,
    RoutedSubscriptionSet, StreamActor, StreamClientFrame, StreamClientRequest,
    StreamClientResponseBody, StreamDomainActor, StreamDomainCommand, StreamDomainCore,
    StreamDomainRuntime, StreamDomainSink, StreamReadExecution, StreamSessionOwner,
    StreamSubscription, STREAM_ACTOR_REPLY_TIMEOUT, STREAM_OPERATIONS_TOTAL,
};
#[cfg(test)]
use crate::dispatch::protocol::FrameContext;
use crate::domains::stream::protocol::{IngestMetadata, StreamDiscriminator};
use crate::domains::stream::store::StreamStoreError;
#[cfg(test)]
use crate::runtime::routing::RouteAddress;
use crate::runtime::{Actor, Context};

mod envelope_dispatch;
mod session_operations;
mod subscription_frames;

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        // Family liveness is gated per-family inside `try_enqueue` below
        // (`FamilyActorPoolRuntime::is_family_running`) -- a panic scoped to
        // one route family must not reject delivery to every other family
        // sharing this pool.
        if !self.actor.is_running() {
            return Err(DeliveryError::ActorStopped);
        }

        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, false)
        } else {
            self.deliver_to_actor(envelope, false)
        }
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if self.family_runtime.is_some() {
            self.deliver_to_family(envelope, true)
        } else {
            self.deliver_to_actor(envelope, true)
        }
    }
}

impl Actor for StreamDomainActor {
    type Message = StreamDomainCommand;

    fn receive(&mut self, msg: Self::Message, _ctx: &mut Context<Self>) {
        let runtime = self.runtime();
        match msg {
            StreamDomainCommand::Deliver(envelope, reply, admission) => {
                let started_at = Instant::now();
                let outcome = runtime.deliver_envelope(&envelope);
                record_stream_service_sample(&self.core.delivery_service_us, started_at);
                let _ = reply.send(outcome);
                // Explicit: the slot is released here, once the work is
                // actually done, and not when the caller gave up waiting.
                drop(admission);
            }
            StreamDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(runtime.live_counts());
            }
            StreamDomainCommand::ReadResourceRecords(command) => {
                let request = command.request.as_borrowed();
                let _ = command
                    .reply
                    .send(runtime.admin_read_resource_records(request));
            }
            StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                runtime.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            StreamDomainCommand::RunMaintenance { family, reply } => {
                runtime.run_maintenance_slice(family);
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            StreamDomainCommand::SyncAdminSnapshot(reply) => {
                runtime.sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            StreamDomainCommand::PanicForTests => {
                panic!("test Stream domain actor panic");
            }
        }
    }
}

impl StreamDomainSink {
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
        // Never blocks its caller, so it never needs an admission slot.
        let command = StreamDomainCommand::Deliver(envelope, reply_tx, None);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)?;

        // Family delivery is called synchronously by the async transport edge.
        // The actor routes client responses through the router, so waiting for
        // the handler result here would block a Tokio worker. The receiver is
        // deliberately dropped after the bounded enqueue succeeds; the actor
        // still owns the command and treats the reply as best effort.
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
        // Refuse surplus client work before enqueue; never ration control-plane work.
        let is_control_plane = high_priority
            || envelope
                .payload::<crate::runtime::SessionCleanup>()
                .is_some();
        let admission = if is_control_plane {
            None
        } else {
            Some(admit_stream_client_delivery(
                &self.inflight_client_deliveries,
                &self.core.delivery_service_us,
                self.actor.is_running(),
            )?)
        };

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = StreamDomainCommand::Deliver(envelope, reply_tx, admission);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}

impl StreamDomainRuntime<'_> {
    pub(super) fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        self.core.deliver_envelope(envelope)
    }
}
