use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, RouteAddress, RpcFastMap,
    RpcPendingCleanupResult, RpcPendingErrorDelivery, RpcPendingRequest,
};

pub(in crate::domains::rpc::sink) struct RpcPendingTable {
    pub(in crate::domains::rpc::sink) pending: RpcFastMap<uuid::Uuid, RpcPendingRequest>,
    pub(in crate::domains::rpc::sink) expirations: BinaryHeap<ExpiringPendingRequest>,
}

#[derive(Debug)]
pub(in crate::domains::rpc::sink) enum RpcPendingResponseDisposition {
    Missing,
    Forward {
        pending: RpcPendingRequest,
        removed_pending: bool,
    },
    InvalidSequence {
        pending: RpcPendingRequest,
        expected_seq: u64,
    },
}

impl RpcPendingTable {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            pending: HashMap::with_capacity_and_hasher(256, FxBuildHasher::default()),
            expirations: BinaryHeap::with_capacity(256),
        }
    }

    pub(in crate::domains::rpc::sink) fn track_pending(
        &mut self,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) -> usize {
        let expires_at = pending.expires_at;
        self.pending.insert(correlation_id, pending);
        self.expirations.push(ExpiringPendingRequest {
            expires_at,
            correlation_id,
        });
        self.pending.len()
    }

    pub(in crate::domains::rpc::sink) fn pending_for_response(
        &mut self,
        correlation_id: &uuid::Uuid,
        seq: u64,
        stream_end: bool,
    ) -> RpcPendingResponseDisposition {
        let Some(expected_seq) = self
            .pending
            .get(correlation_id)
            .map(|pending| pending.next_expected_seq)
        else {
            return RpcPendingResponseDisposition::Missing;
        };

        if seq != expected_seq {
            let pending = self
                .pending
                .remove(correlation_id)
                .expect("tracked pending request for invalid sequence");
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                expected_seq,
            };
        }

        if stream_end {
            let pending = self
                .pending
                .remove(correlation_id)
                .expect("tracked pending request for terminal response");
            return RpcPendingResponseDisposition::Forward {
                pending,
                removed_pending: true,
            };
        }

        let tracked = {
            let pending = self
                .pending
                .get_mut(correlation_id)
                .expect("tracked pending request for non-terminal response");
            let tracked = pending.clone();
            pending.next_expected_seq = pending.next_expected_seq.saturating_add(1);
            tracked
        };
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            removed_pending: false,
        }
    }

    pub(in crate::domains::rpc::sink) fn contains_correlation(
        &self,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.pending.contains_key(correlation_id)
    }

    pub(in crate::domains::rpc::sink) fn worker_session_id(
        &self,
        correlation_id: &uuid::Uuid,
    ) -> Option<u64> {
        self.pending
            .get(correlation_id)
            .map(|pending| pending.worker_session_id)
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcPendingCleanupResult {
        let mut detached_callers = 0;
        let mut disconnect_deliveries = Vec::new();
        let before = self.pending.len();

        self.pending.retain(|correlation_id, pending| {
            if pending.worker_session_id == session_id {
                if pending.caller_session_id != session_id {
                    if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                        disconnect_deliveries.push(RpcPendingErrorDelivery {
                            correlation_id: correlation_id.to_owned(),
                            caller_session_id: pending.caller_session_id,
                            caller_inbox_addr,
                        });
                    }
                }

                return false;
            }

            if pending.caller_session_id == session_id && pending.caller_inbox_addr.is_some() {
                pending.caller_inbox_addr = None;
                detached_callers += 1;
            }

            true
        });

        let removed_pending = before.saturating_sub(self.pending.len());
        RpcPendingCleanupResult {
            detached_callers,
            removed_pending,
            disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_worker(
        &mut self,
        worker_addr: &RouteAddress,
        worker_session_id: u64,
    ) -> RpcPendingCleanupResult {
        let mut disconnect_deliveries = Vec::new();
        let before = self.pending.len();

        self.pending.retain(|correlation_id, pending| {
            if pending.worker_session_id == worker_session_id && pending.worker_addr == *worker_addr
            {
                if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                    disconnect_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: correlation_id.to_owned(),
                        caller_session_id: pending.caller_session_id,
                        caller_inbox_addr,
                    });
                }

                return false;
            }

            true
        });

        RpcPendingCleanupResult {
            detached_callers: 0,
            removed_pending: before.saturating_sub(self.pending.len()),
            disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn len(&self) -> usize {
        self.pending.len()
    }
}
