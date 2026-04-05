/// Semantic state transitions emitted by the notice domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum NoticeEvent {
    Published {
        route: String,
        subscriber_count: usize,
    },
    Subscribed {
        route: String,
        session_id: u64,
        subscription_id: u64,
    },
    Unsubscribed {
        route: String,
        session_id: u64,
        subscription_id: u64,
    },
}
