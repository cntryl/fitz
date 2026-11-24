//! Common decoded frame metadata used to build domain envelopes.

use crate::protocol::frame::ChannelId;
use crate::protocol::message::MessageType;
use crate::runtime::routing::{RouteAddress, RouteFamily};
use crate::runtime::{ClientChannel, ClientFrameMeta};
use bytes::Bytes;

/// Input to the synchronous wire-to-domain adapter.
///
/// The API edge owns session and transport orchestration; this value is the
/// complete immutable frame context handed to the dispatch boundary.
pub struct DomainEnvelopeBuildRequest {
    pub session_id: u64,
    pub channel_id: ChannelId,
    pub route_family: RouteFamily,
    pub msg_type: MessageType,
    pub payload: Bytes,
    pub source: RouteAddress,
    pub destination: RouteAddress,
    /// Correlation the client attached to this frame, if any.
    pub correlation: Option<std::num::NonZeroU64>,
    /// The client set `ACK_REQUESTED` in the message header.
    pub ack_requested: bool,
    pub extensions: crate::protocol::wire3::extensions::KnownFields,
    /// Monotonic time the transport received the complete frame. Retries reuse it so
    /// redelivery never renews a request deadline.
    pub received_at: std::time::Instant,
}

pub fn frame_context(request: &DomainEnvelopeBuildRequest) -> crate::protocol::FrameContext {
    crate::protocol::FrameContext::new(
        request.session_id,
        request.channel_id,
        request.msg_type,
        request.payload.clone(),
        request.route_family,
    )
    .with_correlation(request.correlation)
    .with_extensions(request.extensions)
}

fn client_channel(channel: ChannelId) -> ClientChannel {
    match channel {
        ChannelId::Control => ClientChannel::Control,
        ChannelId::Pub => ClientChannel::Pub,
        ChannelId::Sub => ClientChannel::Sub,
        ChannelId::Rpc => ClientChannel::Rpc,
        ChannelId::Lease => ClientChannel::Lease,
        ChannelId::Internal => ClientChannel::Internal,
    }
}

pub fn client_frame_meta(request: &DomainEnvelopeBuildRequest) -> ClientFrameMeta {
    ClientFrameMeta::new(
        request.session_id,
        client_channel(request.channel_id),
        request.msg_type.as_u16(),
        request.route_family,
    )
    .with_correlation(request.correlation)
    .with_ack_requested(request.ack_requested)
}
