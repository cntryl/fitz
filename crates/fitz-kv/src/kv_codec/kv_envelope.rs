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
    let parsed =
        crate::kv_codec::parse_frame(ctx, &ctx.payload, route_family, session_id, source.clone());
    Envelope::from_route(
        source,
        destination,
        crate::KvClientRequest::new(meta, parsed),
    )
}
