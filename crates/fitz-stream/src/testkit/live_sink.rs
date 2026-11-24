use bytes::Bytes;
use fitz_core::protocol::{ChannelId, FrameContext, MessageType};
use fitz_core::runtime::routing::{Route, RouteAddress, RouteFamily};
use fitz_core::runtime::{DeliveryError, Envelope, MailboxSink, RouteError, Router};
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

#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
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

#[allow(clippy::needless_pass_by_value, clippy::too_many_arguments)]
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
    if fitz_core::protocol::manifest::client_entry(msg_type)
        .is_ok_and(|entry| entry.domain == "stream")
    {
        let request = fitz_core::dispatch::DomainEnvelopeBuildRequest {
            session_id,
            channel_id,
            route_family: family,
            msg_type,
            payload,
            source: source.clone(),
            destination: destination.clone(),
            correlation: None,
            ack_requested: false,
            extensions: crate::wire::wire3::extensions::KnownFields::none(),
            received_at: Instant::now(),
        };
        let meta = fitz_core::dispatch::client_frame_meta(&request);
        let ctx = fitz_core::dispatch::frame_context(&request);
        return router.route_to_domain(
            "stream",
            crate::stream_codec::build_envelope(request, meta, &ctx),
        );
    }

    let frame = FrameContext::new(session_id, channel_id, msg_type, payload, family);
    router.route(Envelope::from_route(
        source.clone(),
        destination.clone(),
        frame,
    ))
}

fn frame_context_from_envelope(envelope: &Envelope) -> Option<FrameContext> {
    if let Some(frame) = envelope.payload::<FrameContext>() {
        return Some(frame.clone());
    }

    if let Some(response) = envelope.payload::<crate::domain::StreamClientResponse>() {
        let mut encoder = crate::wire::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(client_response_frame(
            response.meta,
            Bytes::from(crate::wire::stream_codec::encode_response_into(
                &mut encoder,
                response.meta.message_type,
                &response.response,
            )),
        ));
    }

    if let Some(notification) = envelope.payload::<crate::domain::StreamClientNotification>() {
        let mut encoder = crate::wire::payload_codec::PayloadEncoder::with_capacity(256);
        return Some(FrameContext::new(
            notification.session_id,
            ChannelId::Sub,
            MessageType::new(609),
            Bytes::from(crate::wire::stream_codec::encode_notify_into(
                &mut encoder,
                notification.subscription_id,
                &notification.route,
                notification.payload.as_ref(),
            )),
            notification.route_family,
        ));
    }

    None
}

fn client_response_frame(
    meta: fitz_core::runtime::ClientFrameMeta,
    payload: Bytes,
) -> FrameContext {
    FrameContext::new(
        meta.session_id,
        protocol_channel_from_client(meta.channel),
        MessageType::new(meta.message_type),
        payload,
        meta.route_family,
    )
}

fn protocol_channel_from_client(channel: fitz_core::runtime::ClientChannel) -> ChannelId {
    match channel {
        fitz_core::runtime::ClientChannel::Control => ChannelId::Control,
        fitz_core::runtime::ClientChannel::Pub => ChannelId::Pub,
        fitz_core::runtime::ClientChannel::Sub => ChannelId::Sub,
        fitz_core::runtime::ClientChannel::Rpc => ChannelId::Rpc,
        fitz_core::runtime::ClientChannel::Lease => ChannelId::Lease,
        fitz_core::runtime::ClientChannel::Internal => ChannelId::Internal,
    }
}
