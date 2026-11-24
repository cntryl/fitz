use fitz_core::dispatch::DomainEnvelopeBuildRequest;
use fitz_core::protocol::FrameContext;
use fitz_core::runtime::{ClientFrameMeta, Envelope};

pub fn build_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &FrameContext,
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
        crate::lease_codec::msg_type::ACQUIRE
            | crate::lease_codec::msg_type::RENEW
            | crate::lease_codec::msg_type::RELEASE
            | crate::lease_codec::msg_type::QUERY
    ) {
        let parsed = crate::lease_codec::parse_prepared_request(
            msg_type,
            route_family,
            session_id,
            &payload,
        );
        return Envelope::from_route(
            source,
            destination,
            crate::domain::protocol::PreparedLeaseClientRequest::new(meta, parsed),
        );
    }

    let parsed = crate::lease_codec::parse_frame(
        ctx,
        &ctx.payload,
        route_family,
        session_id,
        source.clone(),
    )
    .map(|frame| match frame {
        crate::lease_codec::ParsedLeaseFrame::Op(message) => crate::LeaseClientFrame::Op(message),
        crate::lease_codec::ParsedLeaseFrame::Sub(message) => crate::LeaseClientFrame::Sub(message),
    });
    Envelope::from_route(
        source,
        destination,
        crate::LeaseClientRequest::new(meta, parsed),
    )
}
