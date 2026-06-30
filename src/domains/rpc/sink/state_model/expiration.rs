use super::{
    Duration, HeapOrdering, Instant, RPC_MAX_TIMEOUT_SWEEP_INTERVAL, RPC_MIN_TIMEOUT_SWEEP_INTERVAL,
};

#[derive(Debug, Clone, Eq, PartialEq)]
pub(in crate::domains::rpc::sink) struct ExpiringPendingRequest {
    pub(super) expires_at: Instant,
    pub(super) correlation_id: uuid::Uuid,
}

impl Ord for ExpiringPendingRequest {
    fn cmp(&self, other: &Self) -> HeapOrdering {
        other
            .expires_at
            .cmp(&self.expires_at)
            .then_with(|| self.correlation_id.cmp(&other.correlation_id))
    }
}

impl PartialOrd for ExpiringPendingRequest {
    fn partial_cmp(&self, other: &Self) -> Option<HeapOrdering> {
        Some(self.cmp(other))
    }
}

pub(in crate::domains::rpc::sink) fn rpc_timeout_sweep_interval(
    request_timeout: Duration,
) -> Duration {
    request_timeout
        .checked_div(4)
        .unwrap_or(RPC_MIN_TIMEOUT_SWEEP_INTERVAL)
        .max(RPC_MIN_TIMEOUT_SWEEP_INTERVAL)
        .min(RPC_MAX_TIMEOUT_SWEEP_INTERVAL)
}
