use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, HashSet, Route, RouteFamily,
    RpcCorrelationKey, RpcFastMap, RpcPendingCleanupResult, RpcPendingDispatchInfo,
    RpcPendingErrorDelivery, RpcPendingRequest, RpcRegistrationId,
};

pub(in crate::domains::rpc::sink) struct RpcPendingTable {
    pub(in crate::domains::rpc::sink) pending: RpcFastMap<RpcCorrelationKey, RpcPendingRequest>,
    pub(in crate::domains::rpc::sink) expirations: BinaryHeap<ExpiringPendingRequest>,
    route_counts: RpcFastMap<(RouteFamily, Route), usize>,
}

#[derive(Debug)]
pub(in crate::domains::rpc::sink) enum RpcPendingResponseDisposition {
    Missing,
    WrongWorker {
        owner_worker_session_id: u64,
    },
    Forward {
        pending: RpcPendingDispatchInfo,
        stream_end: bool,
    },
    InvalidSequence {
        pending: RpcPendingDispatchInfo,
        expected_seq: u64,
    },
}

impl RpcPendingTable {
    pub(in crate::domains::rpc::sink) fn new() -> Self {
        Self {
            pending: HashMap::with_capacity_and_hasher(256, FxBuildHasher),
            expirations: BinaryHeap::with_capacity(256),
            route_counts: HashMap::with_capacity_and_hasher(64, FxBuildHasher),
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
        let route = pending.dispatch_info.route.clone();
        if let Some(replaced) = self.pending.insert(key, pending) {
            self.decrement_route_count(family, &replaced.dispatch_info.route);
        }
        *self.route_counts.entry((family, route)).or_default() += 1;
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
        let Some(pending) = self.pending.get_mut(&key) else {
            return RpcPendingResponseDisposition::Missing;
        };
        if pending.worker_session_id != worker_session_id {
            return RpcPendingResponseDisposition::WrongWorker {
                owner_worker_session_id: pending.worker_session_id,
            };
        }

        let expected_seq = pending.next_expected_seq;
        if seq != expected_seq {
            let pending = self
                .remove(&key)
                .expect("RPC pending request checked above")
                .into_dispatch_info();
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                expected_seq,
            };
        }

        // Deliberately does not advance the sequence or drop the request here.
        // Delivery can still fail, and a stream whose cursor moved past a
        // chunk the caller never received presents later chunks as
        // contiguous. `commit_response_delivery` runs once the chunk is
        // actually handed over.
        if stream_end {
            return RpcPendingResponseDisposition::Forward {
                pending: pending.dispatch_info(),
                stream_end: true,
            };
        }

        let tracked = pending.dispatch_info();
        if pending.next_expected_seq.checked_add(1).is_none() {
            let pending = self
                .remove(&key)
                .expect("RPC pending request checked above")
                .into_dispatch_info();
            return RpcPendingResponseDisposition::InvalidSequence {
                pending,
                expected_seq,
            };
        }
        RpcPendingResponseDisposition::Forward {
            pending: tracked,
            stream_end: false,
        }
    }

    /// Advance past a chunk the caller has actually received.
    ///
    /// Returns `false` when the request is already gone. A `stream_end` chunk
    /// completes the request and drops it.
    pub(in crate::domains::rpc::sink) fn commit_response_delivery(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        stream_end: bool,
    ) -> bool {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        if stream_end {
            return self.remove(&key).is_some();
        }
        let Some(pending) = self.pending.get_mut(&key) else {
            return false;
        };
        pending.next_expected_seq = pending.next_expected_seq.saturating_add(1);
        pending.delivery_retries = 0;
        true
    }

    /// Record that a chunk could not be handed to the caller, returning how
    /// many consecutive delivery failures this request has now seen.
    ///
    /// The sequence stays put, so the worker may resend the same chunk.
    pub(in crate::domains::rpc::sink) fn record_delivery_failure(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> u32 {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        let Some(pending) = self.pending.get_mut(&key) else {
            return u32::MAX;
        };
        pending.delivery_retries = pending.delivery_retries.saturating_add(1);
        pending.delivery_retries
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

    pub(in crate::domains::rpc::sink) fn has_pending_for_route(
        &self,
        family: RouteFamily,
        route: &Route,
    ) -> bool {
        self.route_counts.contains_key(&(family, route.clone()))
    }

    pub(in crate::domains::rpc::sink) fn remove(
        &mut self,
        key: &RpcCorrelationKey,
    ) -> Option<RpcPendingRequest> {
        let pending = self.pending.remove(key)?;
        self.decrement_route_count(key.family, &pending.dispatch_info.route);
        Some(pending)
    }

    fn decrement_route_count(&mut self, family: RouteFamily, route: &Route) {
        let key = (family, route.clone());
        let Some(count) = self.route_counts.get_mut(&key) else {
            return;
        };
        *count = count.saturating_sub(1);
        if *count == 0 {
            self.route_counts.remove(&key);
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcPendingCleanupResult {
        let mut detached_callers = 0;
        let mut disconnect_deliveries = Vec::new();
        let worker_owned: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.worker_session_id == session_id).then_some(*key))
            .collect();
        for key in &worker_owned {
            let pending = self
                .remove(key)
                .expect("collected worker-owned pending request");
            if pending.dispatch_info.caller_session_id != session_id {
                if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                    disconnect_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: key.correlation_id,
                        caller_session_id: pending.dispatch_info.caller_session_id,
                        caller_inbox_addr,
                    });
                }
            }
        }
        for pending in self.pending.values_mut() {
            if pending.dispatch_info.caller_session_id == session_id
                && pending.dispatch_info.caller_inbox_addr.is_some()
            {
                pending.dispatch_info.caller_inbox_addr = None;
                detached_callers += 1;
            }
        }

        let removed_pending = worker_owned.len();
        RpcPendingCleanupResult {
            detached_callers,
            removed_pending,
            disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn cleanup_registrations(
        &mut self,
        registration_ids: &HashSet<RpcRegistrationId>,
    ) -> RpcPendingCleanupResult {
        let mut disconnect_deliveries = Vec::new();
        let registration_owned: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(key, pending)| {
                registration_ids
                    .contains(&pending.dispatch_info.registration_id)
                    .then_some(*key)
            })
            .collect();
        for key in &registration_owned {
            let pending = self
                .remove(key)
                .expect("collected registration-owned pending request");
            if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                disconnect_deliveries.push(RpcPendingErrorDelivery {
                    correlation_id: key.correlation_id,
                    caller_session_id: pending.dispatch_info.caller_session_id,
                    caller_inbox_addr,
                });
            }
        }

        RpcPendingCleanupResult {
            detached_callers: 0,
            removed_pending: registration_owned.len(),
            disconnect_deliveries,
        }
    }

    pub(in crate::domains::rpc::sink) fn len(&self) -> usize {
        self.pending.len()
    }
}
