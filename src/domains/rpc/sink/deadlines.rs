//! Admission-to-dispatch deadline guard; no worker has executed these calls.
use super::state_model::{RpcFamilyRuntime, RpcPendingErrorDelivery, RPC_BUDGET_EXPIRED_ERROR};
use crate::domains::rpc::RpcRequest;
use std::time::Instant;

#[cfg(test)]
use std::time::Duration;

#[cfg(test)]
pub(super) fn remaining_budget_at(deadline: Instant, now: Instant) -> Duration {
    deadline.saturating_duration_since(now)
}

impl RpcFamilyRuntime<'_> {
    pub(super) fn finish_expired_undispatched_request(
        &mut self,
        request: &RpcRequest,
        expires_at: Instant,
    ) -> bool {
        if expires_at > Instant::now() {
            return false;
        }
        let Some((pending, _)) =
            self.remove_pending_request_for_family(request.family_id, &request.correlation_id)
        else {
            return true;
        };
        self.counter_inc("rpc_requests_rejected_expired_budget_total");
        self.schedule_admin_snapshot(false);
        if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
            self.forward_pending_error_deliveries(
                vec![RpcPendingErrorDelivery {
                    correlation_id: request.correlation_id,
                    caller_session_id: pending.dispatch_info.caller_session_id,
                    caller_inbox_addr,
                }],
                crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
                RPC_BUDGET_EXPIRED_ERROR,
                "rpc_timeout_errors_forwarded_total",
                "rpc_timeout_errors_dropped_total",
            );
        }
        true
    }
}
