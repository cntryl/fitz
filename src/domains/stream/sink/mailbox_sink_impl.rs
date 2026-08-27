use super::model::{
    admit_stream_client_delivery, record_stream_service_sample, Arc, DeliveryError, Envelope,
    Instant, MailboxSink, Mutex, Ordering, PayloadEncoder, Route, RouteFamily,
    RoutedSubscriptionSet, StreamActor, StreamClientFrame, StreamClientRequest,
    StreamClientResponseBody, StreamDomainActor, StreamDomainCommand, StreamDomainCore,
    StreamDomainSink, StreamReadExecution, StreamSessionOwner, StreamSubscription, StreamWorkKey,
    STREAM_ACTOR_REPLY_TIMEOUT, STREAM_OPERATIONS_TOTAL,
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
        if self.family_runtime.is_none()
            && !self
                .actor
                .as_ref()
                .is_some_and(crate::runtime::ManagedActor::is_running)
        {
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
        match msg {
            StreamDomainCommand::Deliver(envelope, reply, admission) => {
                let started_at = Instant::now();
                let outcome = self.core.deliver_envelope(&envelope);
                record_stream_service_sample(&self.core.delivery_service_us, started_at);
                let _ = reply.send(outcome);
                // Explicit: the slot is released here, once the work is
                // actually done, and not when the caller gave up waiting.
                drop(admission);
            }
            StreamDomainCommand::ReadLiveCounts(reply) => {
                let _ = reply.send(self.core.live_counts());
            }
            StreamDomainCommand::ReadResourceRecords(command) => {
                let request = command.request.as_borrowed();
                let _ = command
                    .reply
                    .send(self.core.admin_read_resource_records(request));
            }
            StreamDomainCommand::RefreshAdminSnapshotIfDirty(reply) => {
                self.core.refresh_admin_snapshot_if_dirty();
                let _ = reply.send(());
            }
            StreamDomainCommand::RunMaintenance { family, reply } => {
                self.core.run_maintenance_slice(family);
                if let Some(reply) = reply {
                    let _ = reply.send(());
                }
            }
            #[cfg(test)]
            StreamDomainCommand::SyncAdminSnapshot(reply) => {
                self.core.sync_admin_snapshot();
                let _ = reply.send(());
            }
            #[cfg(test)]
            StreamDomainCommand::PanicForTests => {
                panic!("test Stream domain actor panic");
            }
            #[cfg(test)]
            StreamDomainCommand::BlockForTests(entered, release) => {
                let _ = entered.send(());
                let _ = release.recv();
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
        let confirm_execution = envelope
            .payload::<crate::runtime::SessionCleanup>()
            .is_some();
        if !runtime.is_family_running(family) {
            return Err(DeliveryError::ActorStopped);
        }
        // Never blocks its caller, so it never needs an admission slot.
        let key = self.work_key_for_envelope(&envelope);
        let command = StreamDomainCommand::Deliver(envelope, reply_tx, None);
        if high_priority {
            runtime.try_enqueue_control(family, command)
        } else if let Some(key) = key {
            runtime.try_enqueue(family, key, command)
        } else {
            runtime.try_enqueue_control(family, command)
        }
        .map_err(Self::family_enqueue_error)?;

        // Client delivery stays enqueue-only because this synchronous boundary
        // is called by the async transport edge. Cleanup is control-plane work:
        // ingress must retain its retry ticket until the mutation executes.
        if confirm_execution {
            reply_rx
                .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
                .unwrap_or_else(|error| {
                    Err(crate::runtime::reply_wait::map_reply_wait_error(error))
                })
        } else {
            drop(reply_rx);
            Ok(())
        }
    }

    fn work_key_for_envelope(&self, envelope: &Envelope) -> Option<StreamWorkKey> {
        if envelope
            .payload::<crate::runtime::SessionCleanup>()
            .is_some()
        {
            return None;
        }
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return Some(StreamWorkKey::Notification(event.route.as_str().to_owned()));
        }
        let request = StreamDomainCore::request_from_envelope(envelope)?;
        let session_id = request.meta.session_id;
        match request.frame {
            Err(_) => Some(StreamWorkKey::UnresolvedSession(session_id)),
            Ok(StreamClientFrame::Sub(message)) => match message {
                crate::domains::stream::protocol::StreamSubscriptionMessage::Subscribe {
                    session_id,
                    ..
                }
                | crate::domains::stream::protocol::StreamSubscriptionMessage::Unsubscribe {
                    session_id,
                    ..
                } => Some(StreamWorkKey::SubscriptionSession(session_id)),
            },
            Ok(StreamClientFrame::Op(message)) => {
                use crate::domains::stream::protocol::StreamMessage;
                match message {
                    StreamMessage::Begin {
                        family_id, route, ..
                    } => StreamDomainCore::actor_key_for_route(family_id, &route)
                        .ok()
                        .map(StreamWorkKey::Resource),
                    StreamMessage::Read {
                        family_id, route, ..
                    }
                    | StreamMessage::Last { family_id, route }
                    | StreamMessage::GetMetadata { family_id, route } => {
                        Some(Self::selector_work_key(family_id, &route))
                    }
                    StreamMessage::Append { session_id, .. }
                    | StreamMessage::Commit { session_id, .. }
                    | StreamMessage::Rollback { session_id } => self
                        .core
                        .session_owners
                        .lock()
                        .get(&session_id)
                        .map_or_else(
                            || Some(StreamWorkKey::UnresolvedSession(session_id)),
                            |owner| Some(StreamWorkKey::Resource(owner.key.clone())),
                        ),
                }
            }
        }
    }

    fn selector_work_key(family: RouteFamily, route: &Route) -> StreamWorkKey {
        match crate::domains::stream::route_grammar::classify_stream_route_shape(route.as_str()) {
            Ok(crate::domains::stream::route_grammar::StreamRouteShape::Resource { .. }) => {
                StreamDomainCore::actor_key_for_route(family, route).map_or_else(
                    |_| StreamWorkKey::Selector(route.as_str().to_owned()),
                    StreamWorkKey::Resource,
                )
            }
            Ok(_) | Err(_) => StreamWorkKey::Selector(route.as_str().to_owned()),
        }
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
                self.actor
                    .as_ref()
                    .is_some_and(crate::runtime::ManagedActor::is_running),
            )?)
        };

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = StreamDomainCommand::Deliver(envelope, reply_tx, admission);
        let enqueue_result = if high_priority {
            self.actor
                .as_ref()
                .expect("direct Stream mode has a managed actor")
                .try_send_high_priority(command)
        } else {
            self.actor
                .as_ref()
                .expect("direct Stream mode has a managed actor")
                .try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}

#[cfg(test)]
mod work_key_tests {
    use super::*;

    #[test]
    fn should_share_resource_key_between_exact_selectors_and_writes() {
        // Arrange
        let family = RouteFamily::new(7);
        let route = Route::new("stream://acme/orders/42");
        let write_key = StreamDomainCore::actor_key_for_route(family, &route)
            .map(StreamWorkKey::Resource)
            .unwrap();

        // Act
        let read_key = StreamDomainSink::selector_work_key(family, &route);

        // Assert
        assert_eq!(read_key, write_key);
    }

    #[test]
    fn should_keep_broad_selectors_on_selector_keys() {
        // Arrange
        let route = Route::new("stream://acme/orders/*");

        // Act
        let key = StreamDomainSink::selector_work_key(RouteFamily::new(7), &route);

        // Assert
        assert_eq!(key, StreamWorkKey::Selector(route.as_str().to_owned()));
    }
}
