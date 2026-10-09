use crate::stress_support::histogram::Latencies;
use serde::Serialize;
use std::time::Duration;

#[derive(Clone, Default, Serialize)]
pub(super) struct Aggregate {
    pub total_ns: u128,
    pub min_ns: Option<u128>,
    pub distribution: Latencies,
}

impl Aggregate {
    pub fn record(&mut self, duration: Duration) {
        self.total_ns = self.total_ns.saturating_add(duration.as_nanos());
        self.min_ns = Some(
            self.min_ns
                .map_or(duration.as_nanos(), |min| min.min(duration.as_nanos())),
        );
        self.distribution.record(duration);
    }
}

#[derive(Clone, Default, Serialize)]
pub(super) struct ConsumerTiming {
    pub reserve: Aggregate,
    pub ack: Aggregate,
    pub pause: Aggregate,
    pub worker_pause: Aggregate,
    pub handoff: Aggregate,
    pub overshoot: Aggregate,
    pub cycle: Aggregate,
}
