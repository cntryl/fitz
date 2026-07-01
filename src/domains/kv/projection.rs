use crate::control::admin::read_model::AdminReadModel;
use crate::control::admin::{KvLatencySnapshot, KvTransaction};
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

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
/// Tracks dirty state and rebuilds the admin read model snapshot on demand.
/// Projection failure must never affect domain correctness.
pub struct KvAdminProjection<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    read_model: Arc<AdminReadModel>,
    dirty: AtomicBool,
    latencies: Mutex<HashMap<K, KvResourceLatency>>,
}

impl<K> KvAdminProjection<K>
where
    K: Clone + Eq + std::hash::Hash,
{
    pub fn new(read_model: Arc<AdminReadModel>) -> Self {
        Self {
            read_model,
            dirty: AtomicBool::new(false),
            latencies: Mutex::new(HashMap::new()),
        }
    }

    pub fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }

    pub fn refresh_if_dirty<F>(&self, build_transactions: F)
    where
        F: FnOnce() -> Vec<KvTransaction>,
    {
        if self.dirty.swap(false, Ordering::AcqRel) {
            self.read_model
                .replace_kv_transactions(build_transactions());
        }
    }

    pub fn record_read_latency(&self, key: &K, latency_ms: f64) {
        self.latencies
            .lock()
            .entry(key.clone())
            .or_default()
            .reads
            .record(latency_ms);
    }

    pub fn record_write_latency(&self, key: &K, latency_ms: f64) {
        self.latencies
            .lock()
            .entry(key.clone())
            .or_default()
            .writes
            .record(latency_ms);
    }

    pub fn latency_snapshots(&self, key: &K) -> (KvLatencySnapshot, KvLatencySnapshot) {
        self.latencies
            .lock()
            .get(key)
            .map(|latency| (latency.reads.snapshot(), latency.writes.snapshot()))
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_refresh_projection_when_marked_dirty() {
        // Arrange
        let read_model = AdminReadModel::new();
        let projection = KvAdminProjection::<String>::new(read_model.clone());
        projection.mark_dirty();

        // Act
        projection.refresh_if_dirty(|| {
            vec![KvTransaction::snapshot(
                1,
                41,
                7,
                "acme",
                "app",
                "users",
                "2026-07-01T00:00:00Z",
            )]
        });

        // Assert
        assert_eq!(read_model.kv_transactions(None).len(), 1);
    }

    #[test]
    fn should_record_projection_latency_by_operation_kind() {
        // Arrange
        let read_model = AdminReadModel::new();
        let projection = KvAdminProjection::<String>::new(read_model);
        let key = "kv://acme/app/users".to_string();

        // Act
        projection.record_write_latency(&key, 5.0);
        projection.record_read_latency(&key, 3.0);
        let (reads, writes) = projection.latency_snapshots(&key);

        // Assert
        assert!((reads.avg_ms - 3.0).abs() < f64::EPSILON);
        assert!((writes.avg_ms - 5.0).abs() < f64::EPSILON);
    }
}
