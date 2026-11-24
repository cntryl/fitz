//! Deadlines for Queue's broker-local maintenance work.

use super::model::{QUEUE_DEDUP_SWEEP_INTERVAL, QUEUE_IDLE_SWEEP_INTERVAL};
use std::time::{Duration, Instant};

pub(super) struct QueueMaintenanceClock {
    next_idle_sweep_at: Instant,
    next_dedup_sweep_at: Instant,
    next_fast_flush_at: Instant,
    fast_flush_interval: Option<Duration>,
}

impl QueueMaintenanceClock {
    pub(super) fn new(now: Instant, fast_flush_interval: Option<Duration>) -> Self {
        Self {
            next_idle_sweep_at: now,
            next_dedup_sweep_at: now,
            next_fast_flush_at: fast_flush_interval.map_or(now, |interval| now + interval),
            fast_flush_interval,
        }
    }

    pub(super) fn fast_flush_enabled(&self) -> bool {
        self.fast_flush_interval.is_some()
    }

    pub(super) fn idle_sweep_due(&mut self, now: Instant) -> bool {
        if now < self.next_idle_sweep_at {
            return false;
        }
        self.next_idle_sweep_at = now + QUEUE_IDLE_SWEEP_INTERVAL;
        true
    }

    pub(super) fn dedup_sweep_due(&mut self, now: Instant) -> bool {
        if now < self.next_dedup_sweep_at {
            return false;
        }
        self.next_dedup_sweep_at = now + QUEUE_DEDUP_SWEEP_INTERVAL;
        true
    }

    pub(super) fn fast_flush_due(&mut self, now: Instant) -> bool {
        let Some(interval) = self.fast_flush_interval else {
            return false;
        };
        if now < self.next_fast_flush_at {
            return false;
        }
        self.next_fast_flush_at = now + interval;
        true
    }

    #[cfg(test)]
    pub(super) fn set_next_dedup_sweep_at(&mut self, now: Instant) {
        self.next_dedup_sweep_at = now;
    }
}
