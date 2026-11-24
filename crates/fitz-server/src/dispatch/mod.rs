//! Synchronous wire-to-domain dispatch boundary.
//!
//! Protocol owns wire codecs and the exact manifest. Domains own commands,
//! responses, and state. This small adapter is the only shared mapping from
//! manifest domain names to runtime domain kinds.

#[cfg(test)]
use crate::protocol::frame::ChannelId;
use crate::protocol::manifest::{client_entry, ManifestAuthorization, ManifestDecoder};
use crate::protocol::message::MessageType;
#[cfg(test)]
use crate::runtime::routing::{RouteAddress, RouteFamily};
use crate::runtime::{DomainKind, Envelope};
#[cfg(test)]
use bytes::Bytes;

/// Domain-owned wire contracts used by codecs.
///
/// Protocol codecs reach domain-owned command DTOs and route grammar through
/// this adapter namespace. Keeping these imports here makes the dependency crossing
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

pub(crate) use fitz_core::dispatch::DomainEnvelopeBuildRequest;
use fitz_core::dispatch::{client_frame_meta, frame_context};

pub(crate) fn build_request_envelope(
    domain: DomainKind,
    request: DomainEnvelopeBuildRequest,
) -> Envelope {
    let meta = client_frame_meta(&request);
    let ctx = frame_context(&request);

    match domain {
        DomainKind::Kv => fitz_kv::kv_codec::build_envelope(request, meta, &ctx),
        DomainKind::Queue => fitz_queue::queue_codec::build_envelope(request, meta, &ctx),
        DomainKind::Notice => fitz_notice::notice_codec::build_envelope(request, meta, &ctx),
        DomainKind::Stream => fitz_stream::stream_codec::build_envelope(request, meta, &ctx),
        DomainKind::Rpc => fitz_rpc::rpc_codec::build_envelope(request, meta, &ctx),
        DomainKind::Lease => fitz_lease::lease_codec::build_envelope(request, meta, &ctx),
        DomainKind::Schedule => fitz_schedule::schedule_codec::build_envelope(request, meta, &ctx),
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

    let domain = DomainKind::from_scheme(entry.domain)
        .ok_or("manifest domain has no runtime dispatch adapter")?;

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

    fn build_request(correlation: Option<std::num::NonZeroU64>) -> DomainEnvelopeBuildRequest {
        let family = RouteFamily::new(1);
        DomainEnvelopeBuildRequest {
            session_id: 11,
            channel_id: ChannelId::Pub,
            route_family: family,
            msg_type: MessageType::new(crate::protocol::kv::msg_type::BEGIN),
            payload: Bytes::from_static(b"body"),
            source: RouteAddress::new(
                family,
                crate::runtime::routing::Route::new("inbox://session/11"),
            ),
            destination: RouteAddress::new(
                family,
                crate::runtime::routing::Route::new("kv://acme/app/users"),
            ),
            correlation,
            ack_requested: false,
            extensions: crate::protocol::wire3::extensions::KnownFields::none(),
            received_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn should_carry_correlation_into_both_frame_metadata_shapes() {
        // Arrange
        // `dispatch` is the only production conversion point from the wire into
        // domain metadata, so both shapes must pick the correlation up here or
        // no domain can echo it back onto its response.
        let correlation = std::num::NonZeroU64::new(4_242);
        let request = build_request(correlation);

        // Act
        let context = frame_context(&request);
        let meta = client_frame_meta(&request);

        // Assert
        assert_eq!(context.correlation, correlation);
        assert_eq!(meta.correlation, correlation);
    }

    #[test]
    fn should_leave_frame_metadata_uncorrelated_given_no_client_correlation() {
        // Arrange
        // The broker must never invent a correlation: emitting one the client
        // did not send is what would break every client shipping today.
        let request = build_request(None);

        // Act
        let context = frame_context(&request);
        let meta = client_frame_meta(&request);

        // Assert
        assert!(context.correlation.is_none());
        assert!(meta.correlation.is_none());
    }
}
