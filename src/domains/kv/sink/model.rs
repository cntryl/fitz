// KV domain sink for session-scoped transaction dispatch.
//
// Committed KV writes flow straight to Midge and persist according to the
// `WriteOptions` selected when the transaction commits. Active `tx_id`
// handles, resource locks, and admin snapshot entries are separate live
// in-memory state owned by the current broker process. `cleanup_session`
// intentionally discards that state on disconnect, and broker restart clears
// it wholesale instead of attempting transaction recovery.

pub(super) use crate::domains::kv::{KvClientFrame, KvClientRequest};
pub(super) use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
#[cfg(test)]
pub(super) use crate::protocol::frame_context::FrameContext;
pub(super) use crate::runtime::routing::RouteFamily;
pub(super) use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
pub(super) use bytes::Bytes;
pub(super) use chrono::Utc;
pub(super) use parking_lot::Mutex;
pub(super) use std::collections::{HashMap, VecDeque};
pub(super) use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
pub(super) use std::sync::Arc;

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

pub type AdminKvCommittedPair = (Vec<u8>, Vec<u8>);
pub type AdminKvPrefixScanResult = (Vec<AdminKvCommittedPair>, bool);
pub type AdminKvRowsResult = (Vec<AdminKvCommittedPair>, Option<Vec<u8>>, bool);

pub struct AdminKvRowsRequest<'a> {
    pub route_family: RouteFamily,
    pub realm: &'a str,
    pub area: &'a str,
    pub resource: &'a str,
    pub starts_with: &'a [u8],
    pub cursor: Option<&'a [u8]>,
    pub limit: usize,
}

pub(super) const ADMIN_INVENTORY_REFRESH_LIMIT: usize = 10_000;
pub(super) const KV_LATENCY_SAMPLE_LIMIT: usize = 256;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct KvResourceLockKey {
    pub(super) family_id: u64,
    pub(super) realm: String,
    pub(super) area: String,
    pub(super) resource: String,
}

impl KvResourceLockKey {
    pub(super) fn new(family_id: u64, realm: &str, area: &str, resource: &str) -> Self {
        Self {
            family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
        }
    }
}

pub(super) struct KvSubscription {
    pub(super) pattern: crate::runtime::matcher::Pattern,
    pub(super) session_id: u64,
    pub(super) subscription_id: u64,
    pub(super) subscriber: crate::runtime::routing::RouteAddress,
}

#[derive(Default)]
pub(super) struct KvRollingLatency {
    pub(super) samples: VecDeque<f64>,
}

impl KvRollingLatency {
    pub(super) fn record(&mut self, latency_ms: f64) {
        if self.samples.len() >= KV_LATENCY_SAMPLE_LIMIT {
            self.samples.pop_front();
        }
        self.samples.push_back(latency_ms);
    }

    pub(super) fn snapshot(&self) -> crate::control::admin::KvLatencySnapshot {
        if self.samples.is_empty() {
            return crate::control::admin::KvLatencySnapshot::default();
        }

        let mut samples = self.samples.iter().copied().collect::<Vec<_>>();
        samples.sort_by(f64::total_cmp);
        let sum = samples.iter().sum::<f64>();
        let p95_index = samples
            .len()
            .saturating_mul(95)
            .saturating_add(99)
            .saturating_div(100)
            .saturating_sub(1);

        crate::control::admin::KvLatencySnapshot {
            avg_ms: sum / usize_to_f64(samples.len()),
            p95_ms: samples[p95_index],
        }
    }
}

#[derive(Default)]
pub(super) struct KvResourceLatency {
    pub(super) reads: KvRollingLatency,
    pub(super) writes: KvRollingLatency,
}

impl RoutedSubscription for KvSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

pub struct KvDomainSink {
    pub(super) store: Arc<cntryl_midge::Engine>,
    pub(super) actors: Arc<Mutex<HashMap<u64, crate::domains::kv::KvActor>>>,
    pub(super) families: Mutex<HashMap<u64, RoutedSubscriptionSet<KvSubscription>>>,
    pub(super) latencies: Mutex<HashMap<KvResourceLockKey, KvResourceLatency>>,
    pub(super) next_sub_id: AtomicU64,
    pub(super) router: Arc<Router>,
    pub(super) admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    pub(super) metrics: Option<crate::domains::kv::KvMetrics>,
    pub(super) sync_write_options: cntryl_midge::WriteOptions,
    pub(super) active: AtomicBool,
}
