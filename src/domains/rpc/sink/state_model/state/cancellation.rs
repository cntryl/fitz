use super::{
    HashSet, Instant, RouteFamily, RpcCancellationAckDisposition, RpcCancellationDisposition,
    RpcCancellationReason, RpcPendingDispatchInfo, RpcPendingErrorDelivery,
    RpcPendingTimeoutResult, RpcState, RpcWorkerCancellation,
};

impl RpcState {
    pub(in crate::domains::rpc::sink) fn cancel_request_for_caller(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        caller_session_id: u64,
        now: Instant,
        cancellation_grace: std::time::Duration,
    ) -> RpcCancellationDisposition {
        if let Some(queued_caller_session_id) = self
            .pending
            .queued_caller_session_id(family, correlation_id)
        {
            if queued_caller_session_id != caller_session_id {
                return RpcCancellationDisposition::UnauthorizedOrUnknown;
            }
            self.remove_queued_request_for_family(family, correlation_id)
                .expect("queued request checked above");
            return RpcCancellationDisposition::QueuedRemoved;
        }
        self.pending.cancel_pending_for_caller(
            family,
            correlation_id,
            caller_session_id,
            now + cancellation_grace,
        )
    }

    pub(in crate::domains::rpc::sink) fn acknowledge_cancellation(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        worker_session_id: u64,
    ) -> Option<RpcPendingDispatchInfo> {
        let pending =
            match self
                .pending
                .acknowledge_cancellation(family, correlation_id, worker_session_id)
            {
                RpcCancellationAckDisposition::Acknowledged(pending) => pending,
                RpcCancellationAckDisposition::Rejected => return None,
            };
        let dispatch_info = pending.dispatch_info();
        self.release_registration_for_pending(&pending, None);
        Some(dispatch_info)
    }

    pub(in crate::domains::rpc::sink) fn retry_close_for_worker(
        &mut self,
        family: RouteFamily,
        worker_session_id: u64,
        retry_at: Instant,
    ) {
        self.pending
            .retry_close_for_worker(family, worker_session_id, retry_at);
    }

    pub(in crate::domains::rpc::sink) fn mark_worker_close_requested(
        &mut self,
        family: RouteFamily,
        worker_session_id: u64,
    ) {
        self.pending
            .mark_worker_close_requested(family, worker_session_id);
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn expire_timed_out(
        &mut self,
        now: Instant,
    ) -> RpcPendingTimeoutResult {
        self.expire_timed_out_with_grace(
            now,
            super::super::RPC_DEFAULT_CANCELLATION_GRACE,
            |_, _| None,
        )
    }

    /// `frame_loop_busy_time` reports a worker session's frame-loop busy time;
    /// an expired cancellation grace is deferred by any busy time not yet credited.
    pub(in crate::domains::rpc::sink) fn expire_timed_out_with_grace(
        &mut self,
        now: Instant,
        cancellation_grace: std::time::Duration,
        frame_loop_busy_time: impl Fn(RouteFamily, u64) -> Option<std::time::Duration>,
    ) -> RpcPendingTimeoutResult {
        let mut timeout_deliveries = Vec::new();
        let mut removed_pending = 0usize;
        let mut closed_caller_drops = 0usize;
        let mut timed_out_requests = 0usize;
        let mut cancellations = Vec::new();
        let mut close_worker_sessions = HashSet::new();

        while let Some(key) = self.pending.next_expired_pending_key(now) {
            let pending = self
                .pending
                .pending_for_key(&key)
                .expect("tracked pending request")
                .clone();
            if pending.cancelled {
                let busy_time = frame_loop_busy_time(key.family, pending.worker_session_id);
                if self.pending.defer_grace_for_busy_frame_loop(key, busy_time) {
                    continue;
                }
                if let Some(worker_session_id) = self.pending.mark_close_requested(key) {
                    close_worker_sessions.insert((key.family, worker_session_id));
                }
            } else if pending.supports_cancellation {
                if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                    timeout_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: key.correlation_id,
                        caller_session_id: pending.dispatch_info.caller_session_id,
                        caller_inbox_addr,
                    });
                } else {
                    closed_caller_drops = closed_caller_drops.saturating_add(1);
                }
                if let RpcCancellationDisposition::Dispatched {
                    worker_session_id,
                    supports_cancellation: true,
                } = self.pending.cancel_pending_for_caller(
                    key.family,
                    &key.correlation_id,
                    pending.dispatch_info.caller_session_id,
                    now + cancellation_grace,
                ) {
                    cancellations.push(RpcWorkerCancellation {
                        family: key.family,
                        correlation_id: key.correlation_id,
                        worker_session_id,
                        reason: RpcCancellationReason::BrokerDeadline,
                    });
                }
                timed_out_requests = timed_out_requests.saturating_add(1);
            } else {
                let pending = self.pending.remove(&key).expect("tracked pending request");
                self.release_registration_for_pending(&pending, None);
                removed_pending = removed_pending.saturating_add(1);
                timed_out_requests = timed_out_requests.saturating_add(1);
                if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                    timeout_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: key.correlation_id,
                        caller_session_id: pending.dispatch_info.caller_session_id,
                        caller_inbox_addr,
                    });
                } else {
                    closed_caller_drops = closed_caller_drops.saturating_add(1);
                }
            }
        }

        while let Some(key) = self.pending.next_expired_queued_key(now) {
            let queued = self
                .remove_queued_request_for_family(key.family, &key.correlation_id)
                .expect("tracked queued request");
            removed_pending = removed_pending.saturating_add(1);
            timed_out_requests = timed_out_requests.saturating_add(1);
            timeout_deliveries.push(RpcPendingErrorDelivery {
                correlation_id: key.correlation_id,
                caller_session_id: queued.caller_session_id,
                caller_inbox_addr: queued.caller_inbox_addr,
            });
        }

        RpcPendingTimeoutResult {
            removed_pending,
            pending_len: self.live_request_count(),
            closed_caller_drops,
            timeout_deliveries,
            timed_out_requests,
            cancellations,
            close_worker_sessions: close_worker_sessions.into_iter().collect(),
        }
    }
}
