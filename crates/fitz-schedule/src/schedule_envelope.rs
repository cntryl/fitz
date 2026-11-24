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
        source,
        destination,
        ..
    } = request;
    let parsed = crate::schedule_codec::parse_request(
        ctx,
        &ctx.payload,
        route_family,
        fitz_core::session::SessionId(session_id),
        source.clone(),
    );
    Envelope::from_route(
        source,
        destination,
        crate::ScheduleClientRequest::new(meta, parsed),
    )
}
