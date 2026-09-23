//! Multi-family Stream admin projection fixture for in-process benchmarks.

use super::create_bench_store_with_cfs;
use crate::control::admin::read_model::AdminReadModel;
use crate::domains::stream::sink::{StreamDomain, StreamStorageWriteOptions};
use crate::runtime::router::{DeliveryError, MailboxSink, Router};
use crate::runtime::routing::RouteFamily;
use crate::runtime::Envelope;
use std::sync::Arc;

/// The production Stream domain and its admin read model behind a benchmark-only seam.
pub struct StreamAdminBench {
    sink: StreamDomain,
    read_model: Arc<AdminReadModel>,
}

impl StreamAdminBench {
    /// Ask every family actor to refresh its admin projection if dirty.
    pub fn refresh(&self) {
        self.sink.refresh_admin_snapshot_if_dirty();
    }

    #[must_use]
    /// Read projected row and committed-event counts for correctness checks outside timing.
    pub fn snapshot_counts(&self) -> (usize, usize) {
        (
            self.read_model.streams(None).len(),
            self.read_model.stream_events_total(),
        )
    }

    #[must_use]
    /// Count live append sessions in the projected rows.
    pub fn sessions_active_total(&self) -> usize {
        self.read_model
            .streams(None)
            .iter()
            .map(|stream| stream.sessions_active)
            .sum()
    }
}

impl MailboxSink for StreamAdminBench {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.sink.deliver(envelope)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.sink.deliver_high_priority(envelope)
    }
}

impl Drop for StreamAdminBench {
    fn drop(&mut self) {
        self.sink.stop();
    }
}

/// Create a real multi-family Stream domain over an isolated in-memory store.
///
/// # Panics
///
/// Panics if no families are provided or the Stream domain cannot be constructed.
#[must_use]
pub fn create_bench_stream_admin_sink(
    router: Arc<Router>,
    families: &[RouteFamily],
) -> Arc<StreamAdminBench> {
    assert!(!families.is_empty(), "benchmark requires Stream families");
    let store = create_bench_store_with_cfs(families.iter().map(RouteFamily::id));
    let read_model = AdminReadModel::new();
    let sink = StreamDomain::new_with_storage_layout_and_families(
        store,
        router,
        Arc::clone(&read_model),
        crate::domains::stream::StreamStorageLayout::default(),
        Some(families),
        StreamStorageWriteOptions::local(),
    )
    .expect("create multi-family Stream bench sink");
    Arc::new(StreamAdminBench { sink, read_model })
}
