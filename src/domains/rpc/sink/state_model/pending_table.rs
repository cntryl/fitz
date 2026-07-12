use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, RouteAddress, RouteFamily,
    RpcCorrelationKey, RpcFastMap, RpcPendingCleanupResult, RpcPendingDispatchInfo,
    RpcPendingErrorDelivery, RpcPendingRequest,
};

pub(in crate::domains::rpc::sink) struct RpcPendingTable {
    pub(in crate::domains::rpc::sink) pending: RpcFastMap<RpcCorrelationKey, RpcPendingRequest>,
    pub(in crate::domains::rpc::sink) expirations: BinaryHeap<ExpiringPendingRequest>,
}

#[derive(Debug)]
pub(in crate::domains::rpc::sink) enum RpcPendingResponseDisposition {
    Missing,
    WrongWorker {
        owner_worker_session_id: u64,
    },
    Forward {
        pending: RpcPendingDispatchInfo,
        removed_pending: bool,
    },
    InvalidSequence {
        pending: RpcPendingDispatchInfo,
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

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn track_pending(
        &mut self,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) -> usize {
        self.track_pending_for_family(RouteFamily::new(1), correlation_id, pending)
    }

    pub(in crate::domains::rpc::sink) fn track_pending_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: uuid::Uuid,
        pending: RpcPendingRequest,
    ) -> usize {
        let key = RpcCorrelationKey {
            family,
            correlation_id,
        };
        let expires_at = pending.expires_at;
        self.pending.insert(key, pending);
        self.expirations
            .push(ExpiringPendingRequest { expires_at, key });
        self.pending.len()
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn pending_for_response(
        &mut self,
        correlation_id: &uuid::Uuid,
        worker_session_id: u64,
        seq: u64,
        stream_end: bool,
    ) -> RpcPendingResponseDisposition {
        self.pending_for_response_in_family(
            RouteFamily::new(1),
            correlation_id,
            worker_session_id,
            seq,
            stream_end,
        )
    }

    pub(in crate::domains::rpc::sink) fn pending_for_response_in_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        worker_session_id: u64,
        seq: u64,
        stream_end: bool,
    ) -> RpcPendingResponseDisposition {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        let std::collections::hash_map::Entry::Occupied(mut entry) = self.pending.entry(key) else {
            return RpcPendingResponseDisposition::Missing;
        };

        let pending = entry.get_mut();
        if pending.worker_session_id != worker_session_id {
            return RpcPendingResponseDisposition::WrongWorker {
                owner_worker_session_id: pending.worker_session_id,
            };
        }

        let expected_seq = pending.next_expected_seq;
        if seq != expected_seq {
            let pending = entry.remove().into_dispatch_info();
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                expected_seq,
            };
        }

        if stream_end {
            let pending = entry.remove().into_dispatch_info();
            return RpcPendingResponseDisposition::Forward {
                pending,
                removed_pending: true,
            };
        }

        let tracked = pending.dispatch_info();
        let Some(next_expected_seq) = pending.next_expected_seq.checked_add(1) else {
            let pending = entry.remove().into_dispatch_info();
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                expected_seq,
            };
        };
        pending.next_expected_seq = next_expected_seq;
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            removed_pending: false,
        }
    }

    pub(in crate::domains::rpc::sink) fn contains_correlation_in_family(
        &self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        self.pending.contains_key(&RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        })
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcPendingCleanupResult {
        let mut detached_callers = 0;
        let mut disconnect_deliveries = Vec::new();
        let before = self.pending.len();

        self.pending.retain(|key, pending| {
            if pending.worker_session_id == session_id {
                if pending.caller_session_id != session_id {
                    if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                        disconnect_deliveries.push(RpcPendingErrorDelivery {
                            correlation_id: key.correlation_id,
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

        self.pending.retain(|key, pending| {
            if pending.worker_session_id == worker_session_id && pending.worker_addr == *worker_addr
            {
                if let Some(caller_inbox_addr) = pending.caller_inbox_addr.clone() {
                    disconnect_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: key.correlation_id,
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
