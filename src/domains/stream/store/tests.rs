use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

struct TestStreamClock {
    epoch_ms: AtomicU64,
}

impl TestStreamClock {
    fn new(epoch_ms: u64) -> Self {
        Self {
            epoch_ms: AtomicU64::new(epoch_ms),
        }
    }

    fn set(&self, epoch_ms: u64) {
        self.epoch_ms.store(epoch_ms, Ordering::Release);
    }
}

impl crate::runtime::clock::Clock for TestStreamClock {
    fn now_instant(&self) -> std::time::Instant {
        std::time::Instant::now()
    }

    fn now_epoch_ms(&self) -> u64 {
        self.epoch_ms.load(Ordering::Acquire)
    }
}

fn drain_maintenance(store: &StreamStore, family: u64) -> StreamMaintenanceResult {
    let mut total = StreamMaintenanceResult::default();
    for _ in 0..128 {
        let slice = store
            .run_maintenance(family)
            .expect("run bounded Stream maintenance slice");
        total.buckets_compacted = total
            .buckets_compacted
            .saturating_add(slice.buckets_compacted);
        total.records_compacted = total
            .records_compacted
            .saturating_add(slice.records_compacted);
        total.bytes_examined = total.bytes_examined.saturating_add(slice.bytes_examined);
        if !store.has_pending_maintenance(family) {
            return total;
        }
    }
    panic!("Stream maintenance did not drain within the test slice bound");
}

mod sessions_layout_and_watermarks;
use sessions_layout_and_watermarks::*;
mod filters_ttl_and_metadata;
mod global_ordering;
mod global_recovery_and_filters;
mod maintenance_and_payloads;
mod model_based;
mod offsets_and_reads;
mod overflow_and_recovery;
mod ttl_cursor_regressions;
