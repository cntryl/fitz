use super::model::{
    StreamDomain, StreamDomainCommand, StreamFamilyState, StreamReadExecution, StreamSessionOwner,
    StreamSubscription, STREAM_ACTOR_REPLY_TIMEOUT, STREAM_OPERATIONS_TOTAL,
};
use crate::dispatch::protocol::payload_codec::PayloadEncoder;
#[cfg(test)]
use crate::dispatch::protocol::FrameContext;
use crate::domains::stream::protocol::{IngestMetadata, StreamDiscriminator};
use crate::domains::stream::store::StreamStoreError;
use crate::domains::stream::{StreamClientFrame, StreamClientRequest, StreamClientResponseBody};
use crate::domains::subscription_state::RoutedSubscriptionSet;
#[cfg(test)]
use crate::runtime::routing::RouteAddress;
use crate::runtime::routing::{Route, RouteFamily};
use crate::runtime::{DeliveryError, Envelope, MailboxSink};
use std::sync::atomic::Ordering;

mod envelope_dispatch;
mod session_operations;
mod subscription_frames;

impl MailboxSink for StreamDomain {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_family(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_family(envelope, true)
    }
}

impl StreamDomain {
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
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)?;

        reply_rx
            .recv_timeout(STREAM_ACTOR_REPLY_TIMEOUT)
            .unwrap_or_else(|error| Err(crate::runtime::reply_wait::map_reply_wait_error(error)))
    }
}
