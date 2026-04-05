use std::time::Duration;

pub const MAX_WAIT_SECONDS: u64 = 30;
pub const MAX_WAIT_QUEUE_DEPTH: usize = 100;
pub const WAIT_SWEEP_INTERVAL: Duration = Duration::from_millis(50);