use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, HashSet, Route, RouteFamily,
    RpcCorrelationKey, RpcFastMap, RpcPendingCleanupResult, RpcPendingDispatchInfo,
    RpcPendingErrorDelivery, RpcPendingRequest, RpcQueuedRequest, RpcRegistrationId,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Owns live queued and dispatched requests and their expiration indexes.
pub(in crate::domains::rpc::sink) struct RpcPendingTable {
    pending: RpcFastMap<RpcCorrelationKey, RpcPendingRequest>,
    expirations: BinaryHeap<ExpiringPendingRequest>,
    route_counts: RpcFastMap<(RouteFamily, Route), usize>,
    queued: RpcFastMap<RpcCorrelationKey, RpcQueuedRequest>,
    queued_expirations: BinaryHeap<ExpiringPendingRequest>,
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
            queued: HashMap::with_capacity_and_hasher(256, FxBuildHasher),
            queued_expirations: BinaryHeap::with_capacity(256),
        }
    }

    pub(in crate::domains::rpc::sink) fn track_queued_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: uuid::Uuid,
        queued: RpcQueuedRequest,
    ) {
        let key = RpcCorrelationKey {
            family,
            correlation_id,
        };
        self.queued_expirations.push(ExpiringPendingRequest {
            expires_at: queued.expires_at,
            key,
        });
        self.queued.insert(key, queued);
    }

    pub(in crate::domains::rpc::sink) fn remove_queued_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcQueuedRequest> {
        self.queued.remove(&RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        })
    }

    pub(in crate::domains::rpc::sink) fn queued_keys_for_session(
        &self,
        session_id: u64,
    ) -> Vec<RpcCorrelationKey> {
        self.queued
            .iter()
            .filter_map(|(key, queued)| (queued.caller_session_id == session_id).then_some(*key))
            .collect()
    }

    pub(in crate::domains::rpc::sink) fn contains_correlation_in_family(
        &self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> bool {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        self.pending.contains_key(&key) || self.queued.contains_key(&key)
    }

    pub(in crate::domains::rpc::sink) fn live_len(&self) -> usize {
        self.pending.len() + self.queued.len()
    }

    /// Reserves one unit of global pending capacity without oversubscribing the shared limit.
    pub(in crate::domains::rpc::sink) fn reserve_global_capacity(
        &self,
        global_pending_count: Option<&AtomicUsize>,
        global_pending_capacity: usize,
    ) -> Option<bool> {
        let Some(global_pending_count) = global_pending_count else {
            return (self.live_len() < global_pending_capacity).then_some(false);
        };
        let mut current = global_pending_count.load(Ordering::Acquire);
        loop {
            if current >= global_pending_capacity {
                return None;
            }
            match global_pending_count.compare_exchange_weak(
                current,
                current.saturating_add(1),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Some(true),
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn queued_len(&self) -> usize {
        self.queued.len()
    }

    pub(in crate::domains::rpc::sink) fn iter_queued(
        &self,
    ) -> impl Iterator<Item = (&RpcCorrelationKey, &RpcQueuedRequest)> {
        self.queued.iter()
    }

    pub(in crate::domains::rpc::sink) fn iter_pending(
        &self,
    ) -> impl Iterator<Item = (&RpcCorrelationKey, &RpcPendingRequest)> {
        self.pending.iter()
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn get_pending(
        &self,
        key: &RpcCorrelationKey,
    ) -> Option<&RpcPendingRequest> {
        self.pending.get(key)
    }

    pub(in crate::domains::rpc::sink) fn next_expired_pending_key(
        &mut self,
        now: Instant,
    ) -> Option<RpcCorrelationKey> {
        while self
            .expirations
            .peek()
            .is_some_and(|entry| entry.expires_at <= now)
        {
            let expiring = self.expirations.pop().expect("pending expiration entry");
            if self
                .pending
                .get(&expiring.key)
                .is_some_and(|pending| pending.expires_at == expiring.expires_at)
            {
                return Some(expiring.key);
            }
        }
        None
    }

    pub(in crate::domains::rpc::sink) fn next_expired_queued_key(
        &mut self,
        now: Instant,
    ) -> Option<RpcCorrelationKey> {
        while self
            .queued_expirations
            .peek()
            .is_some_and(|entry| entry.expires_at <= now)
        {
            let expiring = self
                .queued_expirations
                .pop()
                .expect("queued expiration entry");
            if self
                .queued
                .get(&expiring.key)
                .is_some_and(|queued| queued.expires_at == expiring.expires_at)
            {
                return Some(expiring.key);
            }
        }
        None
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

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn len(&self) -> usize {
        self.pending.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::routing::RouteAddress;
    use std::time::{Duration, Instant};

    #[test]
    fn should_reserve_correlation_while_request_is_queued() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = uuid::Uuid::new_v4();
        let route = Route::new("rpc://bench/system/resource/operation");
        let request = crate::domains::rpc::protocol::RpcRequest::new(
            family,
            correlation_id,
            route,
            bytes::Bytes::from_static(b"queued"),
        );
        let queued = RpcQueuedRequest::from_request(
            request,
            7,
            RouteAddress::new(
                family,
                Route::new("inbox://bench/system/resource/operation"),
            ),
            Instant::now() + Duration::from_secs(30),
        );
        let mut table = RpcPendingTable::new();

        // Act
        table.track_queued_for_family(family, correlation_id, queued);

        // Assert
        assert!(table.contains_correlation_in_family(family, &correlation_id));
        assert_eq!(table.live_len(), 1);
        assert!(table
            .remove_queued_for_family(family, &correlation_id)
            .is_some());
        assert_eq!(table.live_len(), 0);
    }

    #[test]
    fn should_ignore_stale_queued_expiration_after_replacement() {
        // Arrange
        let family = RouteFamily::new(1);
        let correlation_id = uuid::Uuid::new_v4();
        let route = Route::new("rpc://bench/system/resource/operation");
        let now = Instant::now();
        let make_queued = |expires_at| {
            RpcQueuedRequest::from_request(
                crate::domains::rpc::protocol::RpcRequest::new(
                    family,
                    correlation_id,
                    route.clone(),
                    bytes::Bytes::from_static(b"queued"),
                ),
                7,
                RouteAddress::new(
                    family,
                    Route::new("inbox://bench/system/resource/operation"),
                ),
                expires_at,
            )
        };
        let mut table = RpcPendingTable::new();
        table.track_queued_for_family(
            family,
            correlation_id,
            make_queued(now + Duration::from_secs(1)),
        );
        table.track_queued_for_family(
            family,
            correlation_id,
            make_queued(now + Duration::from_secs(60)),
        );

        // Act
        let early = table.next_expired_queued_key(now + Duration::from_secs(2));
        let due = table.next_expired_queued_key(now + Duration::from_secs(61));

        // Assert
        assert_eq!(early, None);
        assert_eq!(due.map(|key| key.correlation_id), Some(correlation_id));
    }
}
