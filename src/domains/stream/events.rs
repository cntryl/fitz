/// Semantic state transitions emitted by the stream domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    EventAppended {
        realm: String,
        area: String,
        resource: String,
        offset: u64,
        committed_at_epoch_ms: u64,
    },
    Subscribed {
        realm: String,
        area: String,
        resource: String,
        session_id: u64,
    },
    Unsubscribed {
        realm: String,
        area: String,
        resource: String,
        session_id: u64,
    },
    WatermarkAdvanced {
        realm: String,
        area: String,
        resource: String,
        offset: u64,
    },
}
