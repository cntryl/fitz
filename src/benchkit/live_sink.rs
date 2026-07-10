//! Live domain-sink benchmark helpers.
//!
//! These helpers let benchmarks drive the same `FrameContext -> Envelope -> Router ->
//! DomainSink` path that the live server uses, without standing up TCP/WS transport.

use super::{create_bench_store, create_local_bench_store, create_write_heavy_bench_store};
use crate::domains::lease::protocol::{LeaseKey, LeaseResponse};
use crate::domains::lease::sink::LeaseDomainSink;
use crate::domains::notice::sink::NoticeDomainSink;
use crate::domains::queue::sink::QueueDomainSink;
use crate::domains::rpc::sink::RpcDomainSink;
use crate::domains::schedule::sink::ScheduleDomainSink;
use crate::domains::stream::sink::StreamDomainSink;
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
use std::time::{Duration, Instant};

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

    pub fn drain_after_count(&self, min_count: usize, timeout: Duration) -> Vec<FrameContext> {
        let started = Instant::now();
        loop {
            {
                let mut frames = self.frames.lock();
                if frames.len() >= min_count || started.elapsed() >= timeout {
                    return std::mem::take(&mut *frames);
                }
            }
            std::thread::yield_now();
        }
    }

    pub fn clear(&self) {
        self.frames.lock().clear();
    }

    pub fn count(&self) -> usize {
        self.frames.lock().len()
    }
}

pub fn drain_frame_queue_sinks_after_total_count(
    sinks: &[Arc<FrameQueueSink>],
    min_total_count: usize,
    timeout: Duration,
) -> Vec<FrameContext> {
    let started = Instant::now();
    loop {
        let total_count: usize = sinks.iter().map(|sink| sink.count()).sum();
        if total_count >= min_total_count || started.elapsed() >= timeout {
            return sinks.iter().flat_map(|sink| sink.drain()).collect();
        }
        std::thread::yield_now();
    }
}

pub fn drain_frame_queue_sinks_after_each_count(
    sinks: &[Arc<FrameQueueSink>],
    min_count_per_sink: usize,
    timeout: Duration,
) -> Vec<FrameContext> {
    let started = Instant::now();
    loop {
        let all_sinks_ready = sinks.iter().all(|sink| sink.count() >= min_count_per_sink);
        if all_sinks_ready || started.elapsed() >= timeout {
            return sinks.iter().flat_map(|sink| sink.drain()).collect();
        }
        std::thread::yield_now();
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

    pub fn wait_for_count(&self, min_count: usize, timeout: Duration) -> usize {
        let started = Instant::now();
        loop {
            let count = self.count();
            if count >= min_count || started.elapsed() >= timeout {
                return count;
            }
            std::thread::yield_now();
        }
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

pub fn wait_for_counting_sinks_total_count(
    sinks: &[Arc<CountingSink>],
    min_total_count: usize,
    timeout: Duration,
) -> usize {
    let started = Instant::now();
    loop {
        let total_count: usize = sinks.iter().map(|sink| sink.count()).sum();
        if total_count >= min_total_count || started.elapsed() >= timeout {
            return total_count;
        }
        std::thread::yield_now();
    }
}

pub fn wait_for_counting_sinks_each_count(
    sinks: &[Arc<CountingSink>],
    min_count_per_sink: usize,
    timeout: Duration,
) -> usize {
    let started = Instant::now();
    loop {
        let mut total_count = 0usize;
        let mut all_sinks_ready = true;
        for sink in sinks {
            let count = sink.count();
            total_count += count;
            all_sinks_ready &= count >= min_count_per_sink;
        }
        if all_sinks_ready || started.elapsed() >= timeout {
            return total_count;
        }
        std::thread::yield_now();
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

#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
/// Route a decoded frame through the live sink benchmark path.
///
/// # Errors
///
/// Returns `RouteError` when the router rejects the envelope.
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
    route_frame_to_address(
        router,
        source,
        &destination,
        session_id,
        channel_id,
        msg_type,
        payload,
    )
}

/// Route a decoded frame through the live sink benchmark path to a prebuilt destination.
///
/// Use this in stress benches when route/address construction belongs to setup rather than the
/// measured operation.
///
/// # Errors
///
/// Returns `RouteError` when the router rejects the envelope.
pub fn route_frame_to_address(
    router: &Router,
    source: &RouteAddress,
    destination: &RouteAddress,
    session_id: u64,
    channel_id: ChannelId,
    msg_type: u16,
    payload: Bytes,
) -> Result<(), RouteError> {
    let family = *destination.family();
    let msg_type = MessageType::new(msg_type);
    if let Ok(Some(descriptor)) =
        crate::api::runtime_ingress::domain_registry::IngressDomainRegistry::descriptor_for_msg_type(
            msg_type,
        )
    {
        let envelope = descriptor.build_request_envelope(
            crate::api::runtime_ingress::domain_registry::DomainEnvelopeBuildRequest {
                session_id,
                channel_id,
                route_family: family,
                msg_type,
                payload,
                source: source.clone(),
                destination: destination.clone(),
            },
        );
        return router.route_to_domain(descriptor.domain_name(), envelope);
    }

    let frame = FrameContext::new(session_id, channel_id, msg_type, payload, family);
    router.route(Envelope::from_route(
        source.clone(),
        destination.clone(),
        frame,
    ))
}

#[allow(clippy::too_many_arguments)]
/// Route a raw frame directly through the router.
///
/// # Errors
///
/// Returns `RouteError` when the router rejects the envelope.
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

#[allow(clippy::too_many_lines)]
fn frame_context_from_envelope(envelope: &Envelope) -> Option<FrameContext> {
    if let Some(frame) = envelope.payload::<FrameContext>() {
        return Some(frame.clone());
    }

    if let Some(response) = envelope.payload::<crate::domains::rpc::RpcClientResponse>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::response_body_capacity(&response.response),
        );
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::protocol::rpc_codec::encode_response_into(
                &response.response,
                &mut encoder,
            )),
        ));
    }

    if let Some(delivery) = envelope.payload::<crate::domains::rpc::RpcWorkerRequestDelivery>() {
        let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
            crate::protocol::rpc_codec::request_payload_capacity(&delivery.request),
        );
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
        let response_payload = match &response.body {
            crate::domains::rpc::RpcClientForwardedResponseBody::Response(response) => {
                let mut encoder = crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                    crate::protocol::rpc_codec::response_message_capacity(response),
                );
                crate::protocol::rpc_codec::encode_response_message_into(response, &mut encoder)
            }
            crate::domains::rpc::RpcClientForwardedResponseBody::TerminalError {
                correlation_id,
                code,
                message,
            } => {
                let mut error_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                        crate::protocol::rpc_codec::error_body_capacity(message),
                    );
                let mut response_encoder =
                    crate::protocol::payload_codec::PayloadEncoder::with_capacity(
                        crate::protocol::rpc_codec::terminal_error_response_message_capacity(
                            message,
                        ),
                    );
                crate::protocol::rpc_codec::encode_terminal_error_response_message_into(
                    correlation_id,
                    *code,
                    message,
                    &mut response_encoder,
                    &mut error_encoder,
                )
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

    if let Some(request) =
        envelope.payload::<crate::domains::lease::protocol::PreparedLeaseClientRequest>()
    {
        return Some(client_response_frame(request.meta, Bytes::new()));
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

pub struct DirectLeaseAcquireRelease {
    sink: Arc<LeaseDomainSink>,
    key: LeaseKey,
    route_family: RouteFamily,
    owner_session_id: u64,
    owner_id: String,
    ttl_secs: u64,
}

impl DirectLeaseAcquireRelease {
    /// Create a direct lease acquire/release benchmark driver.
    ///
    /// # Panics
    ///
    /// Panics if `route` is not a valid lease route.
    #[must_use]
    pub fn new(
        route_family: RouteFamily,
        route: &str,
        owner_session_id: u64,
        owner_id: &str,
        ttl_secs: u64,
    ) -> Self {
        let router = Arc::new(Router::new());
        let sink = create_bench_lease_sink(router);
        let route = Route::new(route);
        let key = LeaseKey::from_route(route_family, &route).expect("valid lease route");

        Self {
            sink,
            key,
            route_family,
            owner_session_id,
            owner_id: scoped_lease_owner(owner_session_id, owner_id),
            ttl_secs,
        }
    }

    /// Complete one acquire/release roundtrip against the direct lease domain path.
    ///
    /// # Panics
    ///
    /// Panics if the lease domain does not acquire or release the lease.
    pub fn complete_roundtrip(&self) {
        let acquire_response = self.sink.acquire_direct_for_bench(
            &self.key,
            self.owner_session_id,
            self.owner_id.as_str(),
            self.ttl_secs,
            self.route_family,
        );
        let token = match acquire_response {
            LeaseResponse::Acquired { fencing_token } => fencing_token,
            other => panic!("expected direct lease acquire, got {other:?}"),
        };

        let release_response =
            self.sink
                .release_direct_for_bench(&self.key, self.owner_id.as_str(), token);
        assert!(
            matches!(release_response, LeaseResponse::Released),
            "expected direct lease release, got {release_response:?}"
        );
    }
}

#[must_use]
fn scoped_lease_owner(session_id: u64, owner_id: &str) -> String {
    if owner_id.is_empty() {
        format!("session:{session_id}")
    } else {
        format!("session:{session_id}:{owner_id}")
    }
}

#[must_use]
pub fn create_bench_stream_sink(router: Arc<Router>) -> Arc<StreamDomainSink> {
    create_bench_stream_sink_with_layout(
        router,
        crate::domains::stream::StreamStorageLayout::default(),
    )
}

/// Create a benchmark `StreamDomainSink` backed by an isolated local-disk store.
///
/// The returned temporary directory must stay alive for the lifetime of the sink.
///
/// # Panics
///
/// Panics if the benchmark Stream sink cannot be constructed.
#[must_use]
pub fn create_local_bench_stream_sink(
    router: Arc<Router>,
) -> (Arc<StreamDomainSink>, tempfile::TempDir) {
    let (store, temp_dir) = create_local_bench_store();
    let sink = Arc::new(
        StreamDomainSink::new_with_layout(
            store,
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
            crate::domains::stream::StreamStorageLayout::default(),
        )
        .expect("create local-disk bench Stream sink"),
    );
    (sink, temp_dir)
}

#[must_use]
/// Create a benchmark `StreamDomainSink` backed by a write-heavy bench store.
///
/// # Panics
///
/// Panics if the benchmark stream sink cannot be constructed.
pub fn create_write_heavy_bench_stream_sink(router: Arc<Router>) -> Arc<StreamDomainSink> {
    // Panics are acceptable here because this is benchmark-only setup and the
    // caller cannot meaningfully recover from a sink construction failure.
    Arc::new(
        StreamDomainSink::new_with_layout(
            create_write_heavy_bench_store(),
            router,
            crate::control::admin::read_model::AdminReadModel::new(),
            crate::domains::stream::StreamStorageLayout::default(),
        )
        .expect("create write-heavy bench stream sink"),
    )
}

#[must_use]
/// Create a benchmark `StreamDomainSink` with an explicit storage layout.
///
/// # Panics
///
/// Panics if the benchmark stream sink cannot be constructed.
pub fn create_bench_stream_sink_with_layout(
    router: Arc<Router>,
    stream_storage_layout: crate::domains::stream::StreamStorageLayout,
) -> Arc<StreamDomainSink> {
    // Panics are acceptable here because this is benchmark-only setup and the
    // caller cannot meaningfully recover from a sink construction failure.
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
