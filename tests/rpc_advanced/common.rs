//! RPC domain advanced tests - Tier 2
//!
//! Advanced RPC scenarios covering:
//! - Lease management and tracking
//! - Fault tolerance and timeout handling
//! - Strict contiguous streaming response ordering
//! - Gap detection and duplicate failure handling

pub(crate) use crate::fixtures::transport::{
    build_rpc_request, build_rpc_response_delivery, build_rpc_subscribe,
    parse_rpc_request_delivery, parse_rpc_response_delivery,
};
pub(crate) use bytes::Bytes;
pub(crate) use fitz::benchkit::{
    create_bench_rpc_sink, create_bench_rpc_sink_with_route_pending_capacity,
    create_bench_rpc_sink_with_timeout, extract_single_tlv_field,
};
pub(crate) use fitz::boot::domains::DomainHandles;
pub(crate) use fitz::domains::kv::sink::KvDomainSink;
pub(crate) use fitz::domains::lease::sink::LeaseDomainSink;
pub(crate) use fitz::domains::notice::sink::NoticeDomainSink;
pub(crate) use fitz::domains::queue::sink::QueueDomainSink;
pub(crate) use fitz::domains::rpc::sink::RpcDomainSink;
pub(crate) use fitz::domains::rpc::{
    InboxMessage, ReplyInboxActor, RpcError, RpcErrorCode, RpcResponse as RpcResponseMsg,
};
pub(crate) use fitz::domains::schedule::sink::ScheduleDomainSink;
pub(crate) use fitz::domains::stream::sink::StreamDomainSink;
pub(crate) use fitz::protocol::frame::ChannelId;
pub(crate) use fitz::protocol::tlv::MessageType;
pub(crate) use fitz::protocol::FrameContext;
pub(crate) use fitz::runtime::actor::{Actor, Context};
pub(crate) use fitz::runtime::routing::{session_inbox_address, Route, RouteAddress, RouteFamily};
pub(crate) use fitz::runtime::{DeliveryError, Envelope, MailboxSink, Router, SessionCleanup};
pub(crate) use parking_lot::Mutex;
pub(crate) use std::sync::atomic::{AtomicUsize, Ordering};
pub(crate) use std::sync::Arc;
pub(crate) use std::time::Duration;
pub(crate) use uuid::Uuid;

// ============================================================================
//                         STREAMING & ORDERING HELPERS
// ============================================================================

pub(crate) fn create_inbox() -> ReplyInboxActor {
    ReplyInboxActor::new(RouteFamily::new(1))
}

pub(crate) fn create_response(correlation_id: Uuid, seq: u64, stream_end: bool) -> RpcResponseMsg {
    RpcResponseMsg::chunk(
        correlation_id,
        seq,
        Bytes::from(vec![u8::try_from(seq).unwrap_or(u8::MAX)]),
        stream_end,
    )
}

pub(crate) fn create_inbox_context() -> Context<ReplyInboxActor> {
    let router = std::sync::Arc::new(fitz::runtime::router::Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new("inbox://session/123"));
    Context::new(addr, router)
}

#[derive(Default)]
pub(crate) struct CaptureFrameSink {
    frames: Mutex<Vec<FrameContext>>,
}

impl CaptureFrameSink {
    pub(crate) fn snapshot(&self) -> Vec<FrameContext> {
        self.frames.lock().clone()
    }
}

pub(crate) fn rpc_frame_from_envelope(envelope: &Envelope) -> Option<FrameContext> {
    if let Some(frame) = envelope.payload::<FrameContext>() {
        return Some(frame.clone());
    }

    if let Some(response) = envelope.payload::<fitz::domains::rpc::RpcClientResponse>() {
        let payload = fitz::protocol::rpc_codec::encode_response(&response.response);
        return Some(FrameContext::new(
            response.meta.session_id,
            ChannelId::Rpc,
            MessageType::new(response.meta.message_type),
            Bytes::from(payload),
            response.meta.route_family,
        ));
    }

    if let Some(delivery) = envelope.payload::<fitz::domains::rpc::RpcWorkerRequestDelivery>() {
        let mut payload_encoder = fitz::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        let payload =
            fitz::protocol::rpc_codec::encode_request_into(&delivery.request, &mut payload_encoder);
        return Some(FrameContext::new(
            delivery.session_id,
            ChannelId::Rpc,
            MessageType::new(302),
            Bytes::from(payload),
            delivery.route_family,
        ));
    }

    if let Some(forwarded) = envelope.payload::<fitz::domains::rpc::RpcClientForwardedResponse>() {
        let payload = match &forwarded.body {
            fitz::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                fitz::protocol::rpc_codec::encode_response_message(response)
            }
            fitz::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            } => {
                let error_body = fitz::protocol::rpc_codec::encode_error_body(*code, message);
                let response = fitz::domains::rpc::RpcResponse::single(
                    *correlation_id,
                    Bytes::from(error_body),
                );
                fitz::protocol::rpc_codec::encode_response_message(&response)
            }
        };
        return Some(FrameContext::new(
            forwarded.session_id,
            ChannelId::Rpc,
            MessageType::new(303),
            Bytes::from(payload),
            forwarded.route_family,
        ));
    }

    None
}

impl MailboxSink for CaptureFrameSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        let frame = rpc_frame_from_envelope(&envelope).expect("rpc frame payload");
        self.frames.lock().push(frame);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[derive(Default)]
pub(crate) struct CountingFrameSink {
    deliveries: AtomicUsize,
}

impl CountingFrameSink {
    pub(crate) fn delivery_count(&self) -> usize {
        self.deliveries.load(Ordering::Relaxed)
    }
}

impl MailboxSink for CountingFrameSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        rpc_frame_from_envelope(&envelope).expect("rpc frame payload");
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

pub(crate) fn register_capture_sink(
    router: &Arc<Router>,
    address: RouteAddress,
) -> Arc<CaptureFrameSink> {
    let sink = Arc::new(CaptureFrameSink::default());
    router.register(address, sink.clone() as Arc<dyn MailboxSink>);
    sink
}

pub(crate) fn register_counting_sink(
    router: &Arc<Router>,
    address: RouteAddress,
) -> Arc<CountingFrameSink> {
    let sink = Arc::new(CountingFrameSink::default());
    router.register(address, sink.clone() as Arc<dyn MailboxSink>);
    sink
}

pub(crate) fn deliver_rpc_frame(
    sink: &RpcDomainSink,
    source: RouteAddress,
    destination: RouteAddress,
    session_id: u64,
    family: RouteFamily,
    frame: &[u8],
) {
    let (msg_type, payload) = extract_single_tlv_field(frame);
    let frame_ctx = FrameContext::new(
        session_id,
        ChannelId::Rpc,
        MessageType::new(msg_type),
        payload,
        family,
    );
    let parsed = fitz::protocol::rpc_codec::parse_request(&frame_ctx, &frame_ctx.payload, family);
    let meta = fitz::runtime::ClientFrameMeta::new(
        session_id,
        fitz::runtime::ClientChannel::Rpc,
        msg_type,
        family,
    );
    sink.deliver(Envelope::from_route(
        source,
        destination,
        fitz::domains::rpc::RpcClientRequest::new(meta, parsed),
    ))
    .expect("deliver rpc frame");
}

pub(crate) fn encode_captured_frame(frame: &FrameContext) -> Vec<u8> {
    let mut builder = fitz::testkit::TlvFrameBuilder::new();
    builder.encode_field(frame.msg_type.as_u16(), &frame.payload);
    builder.build()
}

pub(crate) async fn wait_for_pending_requests_to_clear(sink: &RpcDomainSink) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if sink.pending_request_count() == 0 {
                break;
            }

            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("pending requests should clear");
}

pub(crate) fn domain_handles_with_rpc_sink(
    router: Arc<Router>,
    rpc: Arc<RpcDomainSink>,
) -> Arc<DomainHandles> {
    let admin_runtime = fitz::boot::Runtime::new(router.clone());
    let admin_read_model = admin_runtime.admin_read_model();
    let store = fitz::testkit::create_test_engine_with_cfs(vec![1]);

    Arc::new(DomainHandles::new(
        Arc::new(KvDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
            fitz::utils::idempotency::default_dedup_store(),
        )),
        Arc::new(NoticeDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(StreamDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
            fitz::domains::stream::sink::StreamStorageWriteOptions::local(),
        )),
        rpc,
        Arc::new(LeaseDomainSink::new(
            router.clone(),
            admin_read_model.clone(),
        )),
        Arc::new(ScheduleDomainSink::new(
            store,
            router,
            admin_read_model.clone(),
        )),
    ))
}

// ============================================================================
//                      LEASE & FAULT TOLERANCE TESTS
// ============================================================================
