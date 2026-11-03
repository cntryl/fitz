use crate::runtime::clock::Clock;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Mock clock for deterministic testing
#[derive(Clone)]
pub(crate) struct MockClock {
    state: Arc<std::sync::Mutex<MockClockState>>,
}

#[derive(Clone, Copy)]
struct MockClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl Default for MockClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MockClock {
    pub(crate) fn new() -> Self {
        Self {
            state: Arc::new(std::sync::Mutex::new(MockClockState {
                instant: Instant::now(),
                epoch_ms: 1_700_000_000_000, // deterministic-ish base
            })),
        }
    }

    pub(crate) fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().unwrap();
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u64::try_from(duration.as_millis()).unwrap_or(u64::MAX));
    }
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().unwrap().instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().unwrap().epoch_ms
    }
}
