use super::model::{
    StreamDomain, StreamDomainCommand, StreamFamilyState, StreamReadExecution,
    STREAM_ACTOR_REPLY_TIMEOUT, STREAM_CLIENT_ACTOR_REPLY_TIMEOUT, STREAM_OPERATIONS_TOTAL,
};
use super::session_owners::StreamSessionOwner;
use crate::domain::protocol::{IngestMetadata, StreamDiscriminator};
use crate::domain::store::StreamStoreError;
use crate::domain::{StreamClientFrame, StreamClientRequest, StreamClientResponseBody};
use fitz_core::protocol::payload_codec::PayloadEncoder;
#[cfg(test)]
use fitz_core::protocol::FrameContext;
#[cfg(test)]
use fitz_core::runtime::routing::RouteAddress;
use fitz_core::runtime::routing::{Route, RouteFamily};
use fitz_core::runtime::{DeliveryError, Envelope, MailboxSink};
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
        let envelope = self.config.router.retain_session_work(envelope);
        let runtime = &self.family_runtime;
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = *envelope.destination().family();
        let command = StreamDomainCommand::Deliver(envelope, reply_tx);
        let lane = if high_priority {
            fitz_core::runtime::FamilyActorLane::Control
        } else {
            fitz_core::runtime::FamilyActorLane::Normal
        };
        runtime
            .try_enqueue(family, lane, command)
            .map_err(fitz_core::runtime::family_actor_enqueue_error_to_delivery_error)?;

        let reply_timeout = if high_priority {
            STREAM_ACTOR_REPLY_TIMEOUT
        } else {
            STREAM_CLIENT_ACTOR_REPLY_TIMEOUT
        };
        reply_rx
            .recv_timeout(reply_timeout)
            .unwrap_or_else(|error| {
                Err(fitz_core::runtime::reply_wait::map_reply_wait_error(error))
            })
    }
}
