//! MetricsMsg messages.

use crate::actor::ActorRef;

/// Messages for MetricsActor.
#[derive(Debug)]
pub enum MetricsMsg {
    /// Increment a counter.
    IncrementCounter { realm: String, metric_name: String, value: i64 },
}
