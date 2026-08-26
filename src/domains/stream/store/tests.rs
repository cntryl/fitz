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
