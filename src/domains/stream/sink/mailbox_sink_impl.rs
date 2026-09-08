use super::model::{
    Arc, DeliveryError, Envelope, MailboxSink, Mutex, Ordering, PayloadEncoder, Route, RouteFamily,
    RoutedSubscriptionSet, StreamActor, StreamClientFrame, StreamClientRequest,
    StreamClientResponseBody, StreamDomainCommand, StreamDomainCore, StreamDomainSink,
    StreamReadExecution, StreamSessionOwner, StreamSubscription, STREAM_ACTOR_REPLY_TIMEOUT,
    STREAM_OPERATIONS_TOTAL,
};
#[cfg(test)]
use crate::dispatch::protocol::FrameContext;
use crate::domains::stream::protocol::{IngestMetadata, StreamDiscriminator};
use crate::domains::stream::store::StreamStoreError;
#[cfg(test)]
use crate::runtime::routing::RouteAddress;

mod envelope_dispatch;
mod session_operations;
mod subscription_frames;

impl MailboxSink for StreamDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_family(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_family(envelope, true)
    }
}

impl StreamDomainSink {
    fn deliver_to_family(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let runtime = &self.family_runtime;
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = *envelope.destination().family();
        let command = StreamDomainCommand::Deliver(envelope, reply_tx);
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)?;

        reply_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
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
}
