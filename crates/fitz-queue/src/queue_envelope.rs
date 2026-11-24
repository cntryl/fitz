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
    let parsed = crate::queue_codec::parse_frame(
        ctx,
        &ctx.payload,
        route_family,
        session_id,
        source.clone(),
    )
    .map(|frame| match frame {
        crate::queue_codec::ParsedQueueFrame::Op(message) => crate::QueueClientFrame::Op(message),
        crate::queue_codec::ParsedQueueFrame::Sub(message) => crate::QueueClientFrame::Sub(message),
    });
    Envelope::from_route(
        source,
        destination,
        crate::QueueClientRequest::new(meta, parsed),
    )
}
