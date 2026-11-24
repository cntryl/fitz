//! Opt-in, high-resolution samples for the Stream subscription benchmark.

use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static ACTIVE: AtomicBool = AtomicBool::new(false);
static SAMPLES: Mutex<Vec<Duration>> = Mutex::new(Vec::new());

pub fn start_sampling(capacity: usize) {
    *SAMPLES.lock() = Vec::with_capacity(capacity);
    ACTIVE.store(true, Ordering::Relaxed);
}

pub fn finish_sampling() -> Vec<Duration> {
    ACTIVE.store(false, Ordering::Relaxed);
    std::mem::take(&mut *SAMPLES.lock())
}

pub(crate) fn start() -> Option<Instant> {
    ACTIVE.load(Ordering::Relaxed).then(Instant::now)
}

pub(crate) fn record(started: Option<Instant>) {
    if let Some(started) = started {
        let elapsed = started.elapsed();
        SAMPLES.lock().push(elapsed);
    }
}
