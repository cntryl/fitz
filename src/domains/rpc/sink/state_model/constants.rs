/// How long a client-path RPC delivery waits for the actor's reply.
///
/// Named here rather than inlined so the queue/stream/rpc budgets are visible
/// together; see `QUEUE_ACTOR_REPLY_TIMEOUT`. Note this sits far below midge's
/// own runtime response deadline, so it will always fire first - which is safe
/// only because a timeout is now a retryable error, not a session close.
pub(in crate::domains::rpc::sink) const RPC_ACTOR_REPLY_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(1);

use super::Duration;

pub(in crate::domains::rpc::sink) const RPC_MSG_TYPE_REQUEST: u16 = 302;
#[cfg(test)]
pub(in crate::domains::rpc::sink) const RPC_MSG_TYPE_RESPONSE: u16 = 303;

pub(in crate::domains::rpc::sink) const RPC_BACKPRESSURE_ERROR: &str =
    "RPC backpressure: too many pending requests";
pub(in crate::domains::rpc::sink) const RPC_NO_WORKERS_ERROR: &str =
    "No workers registered for route";
pub(in crate::domains::rpc::sink) const RPC_WORKER_NOT_FOUND_ERROR: &str =
    "Worker disconnected or unregistered";
pub(in crate::domains::rpc::sink) const RPC_CORRELATION_NOT_FOUND_ERROR: &str =
    "Correlation ID not found (orphaned response)";
pub(in crate::domains::rpc::sink) const RPC_DUPLICATE_CORRELATION_ERROR: &str =
    "Correlation ID already has a live pending request";
pub(in crate::domains::rpc::sink) const RPC_WRONG_WORKER_ERROR: &str =
    "Worker does not own this correlation ID";
pub(in crate::domains::rpc::sink) const RPC_INVALID_SEQUENCE_ERROR: &str =
    "RPC response sequence must start at seq=0 and advance contiguously";
pub(in crate::domains::rpc::sink) const RPC_RESPONSE_UNDELIVERABLE_ERROR: &str =
    "RPC stream terminated: a response chunk could not be delivered to the caller";
pub(in crate::domains::rpc::sink) const RPC_TIMEOUT_ERROR: &str =
    "Worker did not reply within timeout period";
// RPC remains broker-local and in-memory. Worker registrations and pending
// requests disappear on disconnect cleanup or broker restart; this bound keeps
// that live coordination state from growing without limit in one process.
pub(in crate::domains::rpc::sink) const RPC_MAX_PENDING_REQUESTS: usize = 4096;
pub(in crate::domains::rpc::sink) const RPC_DEFAULT_ROUTE_PENDING_CAPACITY: usize = 1000;
// Admin endpoints read from a coalesced in-memory snapshot so hot-path request
// dispatch does not rewrite the current-process read model on every mutation.
#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
pub(in crate::domains::rpc::sink) const RPC_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;
pub(in crate::domains::rpc::sink) const RPC_DEFAULT_REQUEST_TIMEOUT: Duration =
    Duration::from_secs(30);
pub(in crate::domains::rpc::sink) const RPC_MIN_TIMEOUT_SWEEP_INTERVAL: Duration =
    Duration::from_millis(10);
pub(in crate::domains::rpc::sink) const RPC_MAX_TIMEOUT_SWEEP_INTERVAL: Duration =
    Duration::from_millis(250);
