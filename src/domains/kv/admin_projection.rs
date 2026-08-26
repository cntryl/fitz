//! Live KV transaction and latency projection for the admin read model.

use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::{KvLatencySnapshot, KvTransaction};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::sink::KvResourceLockKey;

const KV_LATENCY_SAMPLE_LIMIT: usize = 256;

#[allow(clippy::cast_precision_loss)]
fn usize_to_f64(value: usize) -> f64 {
    value as f64
}

#[derive(Default)]
struct KvRollingLatency {
    samples: VecDeque<f64>,
}

impl KvRollingLatency {
    fn record(&mut self, latency_ms: f64) {
        if self.samples.len() >= KV_LATENCY_SAMPLE_LIMIT {
            self.samples.pop_front();
        }
        self.samples.push_back(latency_ms);
    }

    fn snapshot(&self) -> KvLatencySnapshot {
        if self.samples.is_empty() {
            return KvLatencySnapshot::default();
        }

        let mut samples = self.samples.iter().copied().collect::<Vec<_>>();
        samples.sort_by(f64::total_cmp);
        let sum = samples.iter().sum::<f64>();
        // Nearest-rank p95 is one-based: ceil(n * 0.95), converted back to a
        // zero-based index after saturating arithmetic keeps small samples safe.
        let p95_index = samples
            .len()
            .saturating_mul(95)
            .saturating_add(99)
            .saturating_div(100)
            .saturating_sub(1);

        KvLatencySnapshot {
            avg_ms: sum / usize_to_f64(samples.len()),
            p95_ms: samples[p95_index],
        }
    }
}

#[derive(Default)]
struct KvResourceLatency {
    reads: KvRollingLatency,
    writes: KvRollingLatency,
}

/// Admin projection for the KV domain.
///
/// Applies live transaction changes incrementally and keeps admin state
/// synchronized in production by replaying runtime updates.
/// Projection failure must never affect domain correctness.
pub(crate) struct KvAdminProjection {
    read_model: Arc<AdminReadModel>,
    #[cfg(test)]
    dirty: AtomicBool,
    latencies: Mutex<HashMap<KvResourceLockKey, KvResourceLatency>>,
}

impl KvAdminProjection {
    #[must_use]
    pub(crate) fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            #[cfg(test)]
            dirty: AtomicBool::new(false),
            latencies: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    #[cfg(test)]
    pub(crate) fn refresh_if_dirty<F>(&self, build_transactions: F)
    where
        F: FnOnce() -> Vec<KvTransaction>,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.read_model
                .replace_kv_transactions(build_transactions());
        }
    }

    pub(crate) fn upsert_transaction(&self, transaction: KvTransaction) {
        self.read_model.upsert_kv_transaction(transaction);
    }

    pub(crate) fn remove_transaction(&self, session_id: u64, tx_id: u64) {
        self.read_model.remove_kv_transaction(session_id, tx_id);
    }

    pub(crate) fn remove_session_transactions(&self, session_id: u64) {
        self.read_model
            .remove_kv_transactions_for_session(session_id);
    }

    pub(crate) fn active_transaction_count(&self) -> usize {
        self.read_model.kv_transaction_count()
    }

    pub(crate) fn active_transactions_for_resource(
        &self,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> usize {
        self.read_model
            .kv_transaction_count_for_resource(family_id, realm, area, resource)
    }

    pub(crate) fn record_read_latency(&self, key: &KvResourceLockKey, latency_ms: f64) {
        self.latencies
            .lock()
            .entry(key.clone())
            .or_default()
            .reads
            .record(latency_ms);
    }

    pub(crate) fn record_write_latency(&self, key: &KvResourceLockKey, latency_ms: f64) {
        self.latencies
            .lock()
            .entry(key.clone())
            .or_default()
            .writes
            .record(latency_ms);
    }

    pub(crate) fn latency_snapshots(
        &self,
        key: &KvResourceLockKey,
    ) -> (KvLatencySnapshot, KvLatencySnapshot) {
        self.latencies
            .lock()
            .get(key)
            .map(|latency| (latency.reads.snapshot(), latency.writes.snapshot()))
            .unwrap_or_default()
    }
}

#[cfg(test)]
#[path = "tests/admin_projection.rs"]
mod tests;
