//! Live domain-sink benchmark helpers.
//!
//! These helpers let benchmarks drive the same `FrameContext -> Envelope -> Router ->
//! DomainSink` path that the live server uses, without standing up TCP/WS transport.

use super::create_bench_store;
use crate::boot::domains::{
    LeaseDomainSink, NoticeDomainSink, QueueDomainSink, RpcDomainSink, ScheduleDomainSink,
    StreamDomainSink,
};
use crate::observability::metrics::MetricsCollector;
use crate::protocol::frame::ChannelId;
use crate::protocol::frame_context::FrameContext;
use crate::protocol::tlv::MessageType;
use crate::runtime::envelope::Envelope;
use crate::runtime::router::{DeliveryError, MailboxSink, RouteError, Router};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Queue-style sink that keeps delivered frame contexts until explicitly drained.
#[derive(Default)]
pub struct FrameQueueSink {
    frames: Mutex<Vec<FrameContext>>,
}

impl FrameQueueSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn drain(&self) -> Vec<FrameContext> {
        std::mem::take(&mut *self.frames.lock())
    }

    pub fn clear(&self) {
        self.frames.lock().clear();
    }

    pub fn count(&self) -> usize {
        self.frames.lock().len()
    }
}

impl MailboxSink for FrameQueueSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(frame) = frame_context_from_envelope(&envelope) {
            self.frames.lock().push(frame);
        }
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

/// Counting sink for fanout-heavy benches where we do not need to keep payloads.
#[derive(Default)]
pub struct CountingSink {
    deliveries: AtomicUsize,
}

impl CountingSink {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn count(&self) -> usize {
        self.deliveries.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.deliveries.store(0, Ordering::Relaxed);
    }
}

impl MailboxSink for CountingSink {
    fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliveries.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[must_use]
pub fn session_inbox_route(family: RouteFamily, session_id: u64) -> RouteAddress {
    RouteAddress::new(family, Route::new(format!("inbox://session/{session_id}")))
}

#[must_use]
pub fn register_session_queue_sink(
    router: &Arc<Router>,
    family: RouteFamily,
    session_id: u64,
) -> (RouteAddress, Arc<FrameQueueSink>) {
    let route = session_inbox_route(family, session_id);
    let sink = Arc::new(FrameQueueSink::new());
    router.register(route.clone(), sink.clone());
    (route, sink)
}

#[must_use]
pub fn register_session_counting_sink(
    router: &Arc<Router>,
    family: RouteFamily,
    session_id: u64,
) -> (RouteAddress, Arc<CountingSink>) {
    let route = session_inbox_route(family, session_id);
    let sink = Arc::new(CountingSink::new());
    router.register(route.clone(), sink.clone());
    (route, sink)
}

#[allow(clippy::too_many_arguments)]
pub fn route_frame(
    router: &Router,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    channel_id: ChannelId,
    msg_type: u16,
    payload: Bytes,
    family: RouteFamily,
) -> Result<(), RouteError> {
    let destination = RouteAddress::new(family, Route::new(destination));
    let msg_type = MessageType::new(msg_type);
    let envelope = crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_msg_type(msg_type)
        .ok()
        .flatten()
        .map_or_else(|| {
            let frame = FrameContext::new(session_id, channel_id, msg_type, payload.clone(), family);
            Envelope::from_route(source.clone(), destination.clone(), frame)
        }, |descriptor| {
            descriptor.build_request_envelope(
                crate::api::runtime_ingress::domain_registry::DomainEnvelopeBuildRequest {
                    session_id,
                    channel_id,
                    route_family: family,
                    msg_type,
                    payload: payload.clone(),
                    source: source.clone(),
                    destination: destination.clone(),
                },
            )
        });
    router.route(envelope)
}

#[allow(clippy::too_many_arguments)]
pub fn route_raw_frame(
    router: &Router,
    source: &RouteAddress,
    destination: &str,
    session_id: u64,
    channel_id: ChannelId,
    msg_type: u16,
    payload: Bytes,
    family: RouteFamily,
) -> Result<(), RouteError> {
    let destination = RouteAddress::new(family, Route::new(destination));
    let frame = FrameContext::new(
        session_id,
        channel_id,
        MessageType::new(msg_type),
        payload,
        family,
    );
    router.route(Envelope::from_route(source.clone(), destination, frame))
}

fn frame_context_from_envelope(envelope: &Envelope) -> Option<FrameContext> {
    if let Some(frame) = envelope.payload::<FrameContext>() {
        return Some(frame.clone());
    }

    if let Some(response) = envelope.payload::<crate::domains::kv::KvClientResponse>() {
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::kv::encode_response(&response.response)),
        ));
    }

    if let Some(notification) = envelope.payload::<crate::domains::kv::KvClientNotification>() {
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(crate::protocol::kv::msg_type::NOTIFY),
            Bytes::from(crate::protocol::kv::encode_notify(
                notification.subscription_id,
                &notification.route,
                notification.notification,
            )),
            notification.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::queue::QueueClientResponse>() {
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::queue_codec::encode_response(
                &response.response,
            )),
        ));
    }

    if let Some(notification) = envelope.payload::<crate::domains::queue::QueueClientNotification>()
    {
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(crate::protocol::queue_codec::msg_type::NOTIFY),
            Bytes::from(crate::protocol::queue_codec::encode_notify(
                notification.subscription_id,
                &notification.route,
                notification.notification,
            )),
            notification.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::notice::NoticeClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::notice_codec::encode_response_into(
                &response.response,
                &mut encoder,
            )),
        ));
    }

    if let Some(notification) =
        envelope.payload::<crate::domains::notice::NoticeClientNotification>()
    {
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(504),
            Bytes::from(crate::protocol::notice_codec::encode_notify(
                notification.subscription_id,
                &notification.route,
                notification.payload.as_ref(),
            )),
            notification.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::stream::StreamClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::stream_codec::encode_response_into(
                &mut encoder,
                &response.response,
            )),
        ));
    }

    if let Some(notification) =
        envelope.payload::<crate::domains::stream::StreamClientNotification>()
    {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(609),
            Bytes::from(crate::protocol::stream_codec::encode_notify_into(
                &mut encoder,
                notification.subscription_id,
                &notification.route,
                notification.payload.as_ref(),
            )),
            notification.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::rpc::RpcClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::rpc_codec::encode_response_into(
                &response.response,
                &mut encoder,
            )),
        ));
    }

    if let Some(delivery) = envelope.payload::<crate::domains::rpc::RpcWorkerRequestDelivery>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(FrameContext::new(
            delivery.session_id,
            ChannelId::Rpc,
            MessageType::new(302),
            Bytes::from(crate::protocol::rpc_codec::encode_request_into(
                &delivery.request,
                &mut encoder,
            )),
            delivery.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::rpc::RpcClientForwardedResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        let response_payload = match &response.body {
            crate::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                crate::protocol::rpc_codec::encode_response_message_into(response, &mut encoder)
            }
            crate::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            } => {
                let mut error_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(96);
                let error_body = crate::protocol::rpc_codec::encode_error_body_into(
                    *code,
                    message,
                    &mut error_encoder,
                );
                let response = crate::domains::rpc::RpcResponse::single(
                    *correlation_id,
                    Bytes::from(error_body),
                );
                crate::protocol::rpc_codec::encode_response_message_into(&response, &mut encoder)
            }
        };
        return Some(FrameContext::new(
            response.session_id,
            ChannelId::Rpc,
            MessageType::new(303),
            Bytes::from(response_payload),
            response.route_family,
        ));
    }

    if let Some(ack) = envelope.payload::<crate::domains::rpc::RpcWorkerAck>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(64);
        return Some(FrameContext::new(
            ack.session_id,
            ChannelId::Rpc,
            MessageType::new(304),
            Bytes::from(crate::protocol::rpc_codec::encode_ack_into(
                &ack.correlation_id,
                &mut encoder,
            )),
            ack.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::lease::LeaseClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::lease_codec::encode_domain_response_into(
                &mut encoder,
                &response.response,
            )),
        ));
    }

    if let Some(notification) = envelope.payload::<crate::domains::lease::LeaseClientNotification>()
    {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(crate::protocol::lease_codec::msg_type::NOTIFY),
            Bytes::from(crate::protocol::lease_codec::encode_notify_into(
                &mut encoder,
                notification.subscription_id,
                notification.route.as_str(),
                notification.payload.as_ref(),
            )),
            notification.route_family,
        ));
    }

    if let Some(response) = envelope.payload::<crate::domains::schedule::ScheduleClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::schedule_codec::encode_response_into(
                &mut encoder,
                &response.response,
            )),
        ));
    }

    if let Some(notification) =
        envelope.payload::<crate::domains::schedule::ScheduleClientNotification>()
    {
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(705),
            Bytes::from(crate::protocol::schedule_codec::encode_notify(
                notification.subscription_id,
                notification.payload.as_ref(),
            )),
            notification.route_family,
        ));
    }

    None
}

fn client_response_frame(meta: crate::runtime::ClientFrameMeta, payload: Bytes) -> FrameContext {
    FrameContext::new(
        meta.session_id,
        protocol_channel_from_client(meta.channel),
        MessageType::new(meta.message_type),
        payload,
        meta.route_family,
    )
}

fn protocol_channel_from_client(channel: crate::runtime::ClientChannel) -> ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => ChannelId::Control,
        crate::runtime::ClientChannel::Pub => ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => ChannelId::Internal,
    }
}

#[must_use]
pub fn create_bench_notice_sink(router: Arc<Router>) -> Arc<NoticeDomainSink> {
    Arc::new(NoticeDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    ))
}

#[must_use]
pub fn create_bench_queue_sink(router: Arc<Router>) -> Arc<QueueDomainSink> {
    Arc::new(QueueDomainSink::new(
        create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
        cntryl_midge::WriteOptions::best_effort(),
        crate::utils::idempotency::default_dedup_store(),
    ))
}

#[must_use]
pub fn create_bench_lease_sink(router: Arc<Router>) -> Arc<LeaseDomainSink> {
    Arc::new(LeaseDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    ))
}

#[must_use]
pub fn create_bench_stream_sink(router: Arc<Router>) -> Arc<StreamDomainSink> {
    create_bench_stream_sink_with_layout(
        router,
        crate::domains::stream::StreamStorageLayout::default(),
    )
}

#[must_use]
pub fn create_bench_stream_sink_with_layout(
    router: Arc<Router>,
    stream_storage_layout: crate::domains::stream::StreamStorageLayout,
) -> Arc<StreamDomainSink> {
    Arc::new(
        StreamDomainSink::new_with_layout(
            create_bench_store(),
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
            stream_storage_layout,
        )
        .expect("create bench stream sink"),
    )
}

#[must_use]
pub fn create_bench_rpc_sink(router: Arc<Router>) -> Arc<RpcDomainSink> {
    Arc::new(RpcDomainSink::new(
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    ))
}

#[must_use]
pub fn create_bench_rpc_sink_with_timeout(
    router: Arc<Router>,
    request_timeout: Duration,
) -> Arc<RpcDomainSink> {
    Arc::new(
        RpcDomainSink::new(
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
        )
        .with_request_timeout(request_timeout),
    )
}

#[must_use]
pub fn create_bench_rpc_sink_with_route_pending_capacity(
    router: Arc<Router>,
    route_pending_capacity: usize,
) -> Arc<RpcDomainSink> {
    Arc::new(
        RpcDomainSink::new(
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
        )
        .with_route_pending_capacity(route_pending_capacity),
    )
}

#[must_use]
pub fn create_bench_rpc_sink_with_metrics(
    router: Arc<Router>,
    metrics: MetricsCollector,
) -> Arc<RpcDomainSink> {
    Arc::new(
        RpcDomainSink::new(
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
        )
        .with_metrics(metrics),
    )
}

#[must_use]
pub fn create_bench_schedule_sink(router: Arc<Router>) -> Arc<ScheduleDomainSink> {
    Arc::new(ScheduleDomainSink::new(
        create_bench_store(),
        router,
        crate::control::admin::read_model::AdminReadModel::new(),
    ))
}
