/// Semantic state transitions emitted by the lease domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum LeaseEvent {
    LeaseAcquired {
        realm: String,
        area: String,
        resource: String,
        session_id: u64,
        expires_at_epoch_ms: u64,
    },
    LeaseRenewed {
        realm: String,
        area: String,
        resource: String,
        session_id: u64,
        expires_at_epoch_ms: u64,
    },
    LeaseReleased {
        realm: String,
        area: String,
        resource: String,
        session_id: u64,
    },
    LeaseExpired {
        realm: String,
        area: String,
        resource: String,
    },
}
