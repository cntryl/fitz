//! Synchronous wire-to-domain dispatch boundary.
//!
//! Protocol owns wire codecs and the exact manifest. Domains own commands,
//! responses, and state. This small adapter is the only shared mapping from
//! manifest domain names to runtime domain kinds.

use crate::protocol::frame::ChannelId;
use crate::protocol::manifest::{client_entry, ManifestAuthorization, ManifestDecoder};
use crate::protocol::tlv::MessageType;
use crate::runtime::routing::{RouteAddress, RouteFamily};
use crate::runtime::{ClientChannel, ClientFrameMeta, DomainKind, Envelope};
use bytes::Bytes;

/// Wire/domain DTO contracts used by codecs.
///
/// Protocol codecs reach domain-owned command DTOs only through this adapter
/// namespace. Keeping these imports here makes the dependency crossing
/// explicit and gives architecture checks one allowlisted boundary to audit.
pub mod wire {
    pub mod kv {
        pub use crate::domains::kv::*;
    }

    pub mod lease {
        pub use crate::domains::lease::protocol::*;
    }

    pub mod notice {
        pub use crate::domains::notice::protocol::*;
    }

    pub mod queue {
        pub use crate::domains::queue::*;
    }

    pub mod rpc {
        pub use crate::domains::rpc::protocol::*;
    }

    pub mod schedule {
        pub use crate::domains::schedule::*;
    }

    pub mod stream {
        pub use crate::domains::stream::protocol::*;
    }
}

/// Protocol contracts exposed to domain adapters.
///
/// Domains use this adapter namespace for frame metadata, codecs, and wire
/// error encoders. The protocol module itself remains the owner of those
/// implementations; this re-export keeps the dependency direction explicit.
pub mod protocol {
    pub use crate::protocol::*;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientDispatch {
    pub domain: DomainKind,
    pub authorization: ManifestAuthorization,
    pub decoder: ManifestDecoder,
    pub route_scheme: Option<&'static str>,
}

/// Input to the synchronous wire-to-domain adapter.
///
/// The API edge owns session and transport orchestration; this value is the
/// complete immutable frame context handed to the dispatch boundary.
pub(crate) struct DomainEnvelopeBuildRequest {
    pub(crate) session_id: u64,
    pub(crate) channel_id: ChannelId,
    pub(crate) route_family: RouteFamily,
    pub(crate) msg_type: MessageType,
    pub(crate) payload: Bytes,
    pub(crate) source: RouteAddress,
    pub(crate) destination: RouteAddress,
}

fn frame_context(request: &DomainEnvelopeBuildRequest) -> crate::protocol::FrameContext {
    crate::protocol::FrameContext::new(
        request.session_id,
        request.channel_id,
        request.msg_type,
        request.payload.clone(),
        request.route_family,
    )
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

fn client_frame_meta(request: &DomainEnvelopeBuildRequest) -> ClientFrameMeta {
    ClientFrameMeta::new(
        request.session_id,
        client_channel(request.channel_id),
        request.msg_type.as_u16(),
        request.route_family,
    )
}

/// Parse one manifest-selected client frame and adapt it to a domain command.
///
/// This is intentionally the only production conversion point that imports
/// both wire codecs and domain request DTOs. The API edge remains responsible
/// only for auth, retries, and handing the completed envelope to the router.
fn build_kv_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        source,
        destination,
        ..
    } = request;
    let parsed = crate::protocol::kv::parse_frame(
        ctx,
        &ctx.payload,
        route_family,
        session_id,
        source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::kv::ParsedKvFrame::Op(message) => {
            crate::domains::kv::KvClientFrame::Op(message)
        }
        crate::protocol::kv::ParsedKvFrame::Sub(message) => {
            crate::domains::kv::KvClientFrame::Sub(message)
        }
    });
    Envelope::from_route(
        source,
        destination,
        crate::domains::kv::KvClientRequest::new(meta, parsed),
    )
}

fn build_queue_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        source,
        destination,
        ..
    } = request;
    let parsed = crate::protocol::queue_codec::parse_frame(
        ctx,
        &ctx.payload,
        route_family,
        session_id,
        source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::queue_codec::ParsedQueueFrame::Op(message) => {
            crate::domains::queue::QueueClientFrame::Op(message)
        }
        crate::protocol::queue_codec::ParsedQueueFrame::Sub(message) => {
            crate::domains::queue::QueueClientFrame::Sub(message)
        }
    });
    Envelope::from_route(
        source,
        destination,
        crate::domains::queue::QueueClientRequest::new(meta, parsed),
    )
}

fn build_notice_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        source,
        destination,
        ..
    } = request;
    let parsed = crate::protocol::notice_codec::parse_request(
        ctx,
        &ctx.payload,
        route_family,
        crate::session::SessionId(session_id),
        source.clone(),
    );
    Envelope::from_route(
        source,
        destination,
        crate::domains::notice::NoticeClientRequest::new(meta, parsed),
    )
}

fn build_stream_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        source,
        destination,
        ..
    } = request;
    let parsed = crate::protocol::stream_codec::parse_request(
        ctx,
        &ctx.payload,
        route_family,
        crate::session::SessionId(session_id),
        source.clone(),
    );
    Envelope::from_route(
        source,
        destination,
        crate::domains::stream::StreamClientRequest::new(meta, parsed),
    )
}

fn build_rpc_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        route_family,
        source,
        destination,
        payload,
        ..
    } = request;
    let parsed = crate::protocol::rpc_codec::parse_request(ctx, &ctx.payload, route_family);
    Envelope::from_route(
        source,
        destination,
        crate::domains::rpc::RpcClientRequest::new_with_payload(meta, parsed, payload),
    )
}

fn build_lease_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        msg_type,
        payload,
        source,
        destination,
        ..
    } = request;
    let msg_type = msg_type.as_u16();
    if matches!(
        msg_type,
        crate::protocol::lease_codec::msg_type::ACQUIRE
            | crate::protocol::lease_codec::msg_type::RENEW
            | crate::protocol::lease_codec::msg_type::RELEASE
            | crate::protocol::lease_codec::msg_type::QUERY
    ) {
        let parsed = crate::protocol::lease_codec::parse_prepared_request(
            msg_type,
            route_family,
            session_id,
            &payload,
        );
        return Envelope::from_route(
            source,
            destination,
            crate::domains::lease::protocol::PreparedLeaseClientRequest::new(meta, parsed),
        );
    }

    let parsed = crate::protocol::lease_codec::parse_frame(
        ctx,
        &ctx.payload,
        route_family,
        session_id,
        source.clone(),
    )
    .map(|frame| match frame {
        crate::protocol::lease_codec::ParsedLeaseFrame::Op(message) => {
            crate::domains::lease::LeaseClientFrame::Op(message)
        }
        crate::protocol::lease_codec::ParsedLeaseFrame::Sub(message) => {
            crate::domains::lease::LeaseClientFrame::Sub(message)
        }
    });
    Envelope::from_route(
        source,
        destination,
        crate::domains::lease::LeaseClientRequest::new(meta, parsed),
    )
}

fn build_schedule_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &crate::protocol::FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        session_id,
        route_family,
        source,
        destination,
        ..
    } = request;
    let parsed = crate::protocol::schedule_codec::parse_request(
        ctx,
        &ctx.payload,
        route_family,
        crate::session::SessionId(session_id),
        source.clone(),
    );
    Envelope::from_route(
        source,
        destination,
        crate::domains::schedule::ScheduleClientRequest::new(meta, parsed),
    )
}

pub(crate) fn build_request_envelope(
    domain: DomainKind,
    request: DomainEnvelopeBuildRequest,
) -> Envelope {
    let meta = client_frame_meta(&request);
    let ctx = frame_context(&request);

    match domain {
        DomainKind::Kv => build_kv_envelope(request, meta, &ctx),
        DomainKind::Queue => build_queue_envelope(request, meta, &ctx),
        DomainKind::Notice => build_notice_envelope(request, meta, &ctx),
        DomainKind::Stream => build_stream_envelope(request, meta, &ctx),
        DomainKind::Rpc => build_rpc_envelope(request, meta, &ctx),
        DomainKind::Lease => build_lease_envelope(request, meta, &ctx),
        DomainKind::Schedule => build_schedule_envelope(request, meta, &ctx),
    }
}

/// Resolve an inbound client message through the exact protocol manifest.
/// Unknown IDs are errors; numeric range membership is never a fallback.
///
/// # Errors
///
/// Returns an error when the message ID is absent from the manifest or names
/// a domain without a registered runtime adapter.
pub fn client_dispatch(message_type: MessageType) -> Result<Option<ClientDispatch>, &'static str> {
    let entry = client_entry(message_type)?;
    if entry.domain == "control"
        || entry.direction != crate::protocol::manifest::ManifestDirection::ClientToServer
    {
        return Ok(None);
    }

    let domain = match entry.domain {
        "kv" => DomainKind::Kv,
        "queue" => DomainKind::Queue,
        "notice" => DomainKind::Notice,
        "stream" => DomainKind::Stream,
        "rpc" => DomainKind::Rpc,
        "lease" => DomainKind::Lease,
        "schedule" => DomainKind::Schedule,
        _ => return Err("manifest domain has no runtime dispatch adapter"),
    };

    Ok(Some(ClientDispatch {
        domain,
        authorization: entry.authorization,
        decoder: entry.decoder,
        route_scheme: entry.route_scheme,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_resolve_client_message_from_manifest() {
        // Arrange
        let message_type = MessageType::new(crate::protocol::kv::msg_type::BEGIN);

        // Act
        let dispatch = client_dispatch(message_type)
            .expect("manifest entry")
            .expect("client dispatch");

        // Assert
        assert_eq!(dispatch.domain, DomainKind::Kv);
        assert_eq!(dispatch.route_scheme, Some("kv"));
    }

    #[test]
    fn should_reject_unknown_message_without_range_fallback() {
        // Arrange
        let message_type = MessageType::new(9999);

        // Act
        let result = client_dispatch(message_type);

        // Assert
        assert_eq!(result, Err("unsupported message type"));
    }
}
