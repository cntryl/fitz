//! Live domain-sink benchmark helpers.
//!
//! These helpers let benchmarks drive the same `FrameContext -> Envelope -> Router ->
//! DomainSink` path that the live server uses, without standing up TCP/WS transport.

use super::create_bench_store;
use crate::boot::domains::{LeaseDomainSink, NoticeDomainSink, RpcDomainSink, StreamDomainSink};
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

/// Queue-style sink that keeps delivered frame contexts until explicitly drained.
#[derive(Default)]
pub struct FrameQueueSink {
    frames: Mutex<Vec<FrameContext>>,
}

impl FrameQueueSink {
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
        if let Some(frame) = envelope.into_payload::<FrameContext>() {
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

pub fn session_inbox_route(family: RouteFamily, session_id: u64) -> RouteAddress {
    RouteAddress::new(
        family,
        Route::new(format!("inbox://session/{}", session_id)),
    )
}

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
    let frame = FrameContext::new(
        session_id,
        channel_id,
        MessageType::new(msg_type),
        payload,
        family,
    );
    router.route(Envelope::from_route(source.clone(), destination, frame))
}

pub fn create_bench_notice_sink(router: Arc<Router>) -> Arc<NoticeDomainSink> {
    Arc::new(NoticeDomainSink::new(
        router,
        crate::api::admin::read_model::AdminReadModel::new(),
    ))
}

pub fn create_bench_lease_sink(router: Arc<Router>) -> Arc<LeaseDomainSink> {
    Arc::new(LeaseDomainSink::new(
        router,
        crate::api::admin::read_model::AdminReadModel::new(),
    ))
}

pub fn create_bench_stream_sink(router: Arc<Router>) -> Arc<StreamDomainSink> {
    Arc::new(StreamDomainSink::new(
        create_bench_store(),
        router,
        crate::api::admin::read_model::AdminReadModel::new(),
    ))
}

pub fn create_bench_rpc_sink(router: Arc<Router>) -> Arc<RpcDomainSink> {
    Arc::new(RpcDomainSink::new(
        router,
        crate::api::admin::read_model::AdminReadModel::new(),
    ))
}
