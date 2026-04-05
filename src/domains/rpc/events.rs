/// Semantic state transitions emitted by the RPC domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum RpcEvent {
    WorkerRegistered {
        route: String,
        session_id: u64,
    },
    WorkerDeregistered {
        route: String,
        session_id: u64,
    },
    RequestDispatched {
        route: String,
        request_id: u64,
    },
    RequestCompleted {
        route: String,
        request_id: u64,
        latency_ms: u64,
    },
    RequestTimedOut {
        route: String,
        request_id: u64,
    },
}
