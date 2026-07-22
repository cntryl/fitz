/// Semantic state transitions emitted by the schedule domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum ScheduleEvent {
    ScheduleCreated {
        route: String,
        cron: String,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    },
    ScheduleUpdated {
        route: String,
        cron: String,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    },
    ScheduleDeleted {
        route: String,
    },
    ScheduleFired {
        route: String,
        fire_ms: u64,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    },
    ScheduleAcknowledged {
        route: String,
        acknowledged_at_ms: u64,
    },
    ScheduleFireFailed {
        route: String,
    },
}
