/// Semantic state transitions emitted by the KV domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum KvEvent {
    KeySet {
        realm: String,
        area: String,
        resource: String,
        key: String,
    },
    KeyDeleted {
        realm: String,
        area: String,
        resource: String,
        key: String,
    },
    TransactionCommitted {
        realm: String,
        area: String,
        resource: String,
        op_count: usize,
    },
    TransactionRolledBack {
        realm: String,
        area: String,
        resource: String,
    },
}
