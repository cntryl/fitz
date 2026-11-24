use fitz_core::dispatch::DomainEnvelopeBuildRequest;
use fitz_core::protocol::FrameContext;
use fitz_core::runtime::{ClientFrameMeta, Envelope};

pub fn build_envelope(
    request: DomainEnvelopeBuildRequest,
    meta: ClientFrameMeta,
    ctx: &FrameContext,
) -> Envelope {
    let DomainEnvelopeBuildRequest {
        route_family,
        source,
        destination,
        payload,
        received_at,
        ..
    } = request;
    let parsed = crate::rpc_codec::parse_request(ctx, &ctx.payload, route_family);
    Envelope::from_route(
        source,
        destination,
        crate::domain::protocol::RpcClientRequestIngress {
            request: crate::RpcClientRequest::new_with_payload(meta, parsed, payload),
            received_at,
        },
    )
}
