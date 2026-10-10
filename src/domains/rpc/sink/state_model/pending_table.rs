use super::{
    BinaryHeap, ExpiringPendingRequest, FxBuildHasher, HashMap, HashSet, Route, RouteFamily,
    RpcCancellationAckDisposition, RpcCancellationDisposition, RpcCorrelationKey, RpcFastMap,
    RpcPendingCleanupResult, RpcPendingDispatchInfo, RpcPendingErrorDelivery, RpcPendingRequest,
    RpcQueuedRequest, RpcRegistrationId, RpcWorkerCancellation,
    RPC_MAX_CANCELLATION_GRACE_DEFERRAL,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const EXPIRATION_HEAP_STALE_ENTRY_ALLOWANCE: usize = 256;
const EXPIRATION_HEAP_LIVE_ENTRY_MULTIPLIER: usize = 2;

/// Owns live queued and dispatched requests and their expiration indexes.
pub(in crate::domains::rpc::sink) struct RpcPendingTable {
    pending: RpcFastMap<RpcCorrelationKey, RpcPendingRequest>,
    worker_invocations: RpcFastMap<RpcCorrelationKey, RpcCorrelationKey>,
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
    CancelledResponse {
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
            worker_invocations: HashMap::with_capacity_and_hasher(256, FxBuildHasher),
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
        self.compact_queued_expirations_if_needed();
    }

    pub(in crate::domains::rpc::sink) fn remove_queued_for_family(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<RpcQueuedRequest> {
        let queued = self.queued.remove(&RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        })?;
        self.compact_queued_expirations_if_needed();
        Some(queued)
    }

    pub(in crate::domains::rpc::sink) fn queued_caller_session_id(
        &self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
    ) -> Option<u64> {
        self.queued
            .get(&RpcCorrelationKey {
                family,
                correlation_id: *correlation_id,
            })
            .map(|queued| queued.caller_session_id)
    }

    pub(in crate::domains::rpc::sink) fn cancel_pending_for_caller(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        caller_session_id: u64,
        expires_at: Instant,
    ) -> RpcCancellationDisposition {
        let key = RpcCorrelationKey {
            family,
            correlation_id: *correlation_id,
        };
        let Some(pending) = self.pending.get_mut(&key) else {
            return RpcCancellationDisposition::UnauthorizedOrUnknown;
        };
        if pending.dispatch_info.caller_session_id != caller_session_id {
            return RpcCancellationDisposition::UnauthorizedOrUnknown;
        }
        if pending.cancelled {
            return RpcCancellationDisposition::AlreadyCancelled;
        }
        let worker_session_id = pending.worker_session_id;
        let supports_cancellation = pending.supports_cancellation;
        if !supports_cancellation {
            return RpcCancellationDisposition::Dispatched {
                worker_session_id,
                supports_cancellation,
            };
        }
        pending.cancelled = true;
        pending.close_requested = false;
        pending.dispatch_info.caller_inbox_addr = None;
        pending.expires_at = expires_at;
        self.expirations
            .push(ExpiringPendingRequest { expires_at, key });
        self.compact_pending_expirations_if_needed();
        RpcCancellationDisposition::Dispatched {
            worker_session_id,
            supports_cancellation,
        }
    }

    pub(in crate::domains::rpc::sink) fn acknowledge_cancellation(
        &mut self,
        family: RouteFamily,
        correlation_id: &uuid::Uuid,
        worker_session_id: u64,
    ) -> RpcCancellationAckDisposition {
        let Some(key) = self
            .worker_invocations
            .get(&RpcCorrelationKey {
                family,
                correlation_id: *correlation_id,
            })
            .copied()
        else {
            return RpcCancellationAckDisposition::Rejected;
        };
        let Some(pending) = self.pending.get(&key) else {
            return RpcCancellationAckDisposition::Rejected;
        };
        if pending.worker_session_id != worker_session_id
            || !pending.supports_cancellation
            || !pending.cancelled
        {
            return RpcCancellationAckDisposition::Rejected;
        }
        RpcCancellationAckDisposition::Acknowledged(Box::new(
            self.remove(&key).expect("acknowledged pending request"),
        ))
    }

    pub(in crate::domains::rpc::sink) fn mark_close_requested(
        &mut self,
        key: RpcCorrelationKey,
    ) -> Option<u64> {
        let pending = self.pending.get_mut(&key)?;
        if !pending.cancelled || pending.close_requested {
            return None;
        }
        pending.close_requested = true;
        let worker_session_id = pending.worker_session_id;
        Some(worker_session_id)
    }

    pub(in crate::domains::rpc::sink) fn mark_worker_close_requested(
        &mut self,
        family: RouteFamily,
        worker_session_id: u64,
    ) {
        for (key, pending) in &mut self.pending {
            if key.family == family
                && pending.worker_session_id == worker_session_id
                && pending.cancelled
                && !pending.close_requested
            {
                pending.close_requested = true;
            }
        }
    }

    /// Records the worker's frame-loop busy time when a cancellation grace starts.
    pub(in crate::domains::rpc::sink) fn start_grace_busy_accounting(
        &mut self,
        key: RpcCorrelationKey,
        busy_time: Option<std::time::Duration>,
    ) {
        if let Some(pending) = self.pending.get_mut(&key) {
            if pending.cancelled && !pending.close_requested {
                pending.grace_busy_baseline = busy_time;
            }
        }
    }

    /// Pushes an expired grace back by the time the worker's frame loop spent
    /// busy since the grace was last credited, so the grace only runs while the
    /// loop is free to read the cleanup ACK. Total deferral per cancellation is
    /// capped at `RPC_MAX_CANCELLATION_GRACE_DEFERRAL`; once the cap is reached
    /// this returns `false` and the worker is closed. Returns whether it was deferred.
    pub(in crate::domains::rpc::sink) fn defer_grace_for_busy_frame_loop(
        &mut self,
        key: RpcCorrelationKey,
        busy_time: Option<std::time::Duration>,
    ) -> bool {
        let Some(pending) = self.pending.get_mut(&key) else {
            return false;
        };
        let (Some(busy_time), Some(baseline)) = (busy_time, pending.grace_busy_baseline) else {
            return false;
        };
        let remaining_ceiling =
            RPC_MAX_CANCELLATION_GRACE_DEFERRAL.saturating_sub(pending.grace_deferred);
        let unpaid = busy_time.saturating_sub(baseline).min(remaining_ceiling);
        if unpaid.is_zero() {
            return false;
        }
        pending.grace_busy_baseline = Some(busy_time);
        pending.grace_deferred += unpaid;
        pending.expires_at += unpaid;
        let expires_at = pending.expires_at;
        self.expirations
            .push(ExpiringPendingRequest { expires_at, key });
        self.compact_pending_expirations_if_needed();
        true
    }

    pub(in crate::domains::rpc::sink) fn retry_close_for_worker(
        &mut self,
        family: RouteFamily,
        worker_session_id: u64,
        expires_at: Instant,
    ) {
        let keys: Vec<_> = self
            .pending
            .iter_mut()
            .filter_map(|(key, pending)| {
                if key.family == family
                    && pending.worker_session_id == worker_session_id
                    && pending.cancelled
                {
                    pending.close_requested = false;
                    pending.grace_busy_baseline = None;
                    pending.expires_at = expires_at;
                    Some(*key)
                } else {
                    None
                }
            })
            .collect();
        self.expirations.extend(
            keys.into_iter()
                .map(|key| ExpiringPendingRequest { expires_at, key }),
        );
        self.compact_pending_expirations_if_needed();
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
        self.pending.contains_key(&key)
            || self.queued.contains_key(&key)
            || self.worker_invocations.contains_key(&key)
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

    pub(in crate::domains::rpc::sink) fn pending_for_key(
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
            if self.pending.get(&expiring.key).is_some_and(|pending| {
                !pending.close_requested && pending.expires_at == expiring.expires_at
            }) {
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
        mut pending: RpcPendingRequest,
    ) -> usize {
        let key = RpcCorrelationKey {
            family,
            correlation_id,
        };
        let expires_at = pending.expires_at;
        let route = pending.dispatch_info.route.clone();
        pending.dispatch_info.worker_correlation_id = if pending.supports_cancellation {
            loop {
                let id = uuid::Uuid::new_v4();
                if !self.contains_correlation_in_family(family, &id) && id != correlation_id {
                    break id;
                }
            }
        } else {
            correlation_id
        };
        let invocation = RpcCorrelationKey {
            family,
            correlation_id: pending.dispatch_info.worker_correlation_id,
        };
        if let Some(replaced) = self.pending.insert(key, pending) {
            self.worker_invocations.remove(&RpcCorrelationKey {
                family,
                correlation_id: replaced.dispatch_info.worker_correlation_id,
            });
            self.decrement_route_count(family, &replaced.dispatch_info.route);
        }
        self.worker_invocations.insert(invocation, key);
        *self.route_counts.entry((family, route)).or_default() += 1;
        self.expirations
            .push(ExpiringPendingRequest { expires_at, key });
        self.compact_pending_expirations_if_needed();
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

        if pending.cancelled {
            // A terminal frame does not prove that the handler's cleanup has
            // finished. Retain execution credit until its cleanup acknowledgment
            // or worker session cleanup, including cancel/completion races.
            return RpcPendingResponseDisposition::CancelledResponse { stream_end };
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
        self.worker_invocations.remove(&RpcCorrelationKey {
            family: key.family,
            correlation_id: pending.dispatch_info.worker_correlation_id,
        });
        self.decrement_route_count(key.family, &pending.dispatch_info.route);
        self.compact_pending_expirations_if_needed();
        Some(pending)
    }

    pub(in crate::domains::rpc::sink) fn resolve_worker_response_correlation(
        &self,
        family: RouteFamily,
        worker_session_id: u64,
        worker_id: uuid::Uuid,
    ) -> Option<uuid::Uuid> {
        self.worker_invocations
            .get(&RpcCorrelationKey {
                family,
                correlation_id: worker_id,
            })
            .filter(|key| {
                self.pending
                    .get(key)
                    .is_some_and(|pending| pending.worker_session_id == worker_session_id)
            })
            .map(|key| key.correlation_id)
            .or_else(|| {
                // Preserve legacy wrong-worker diagnostics without accepting a
                // caller correlation as a negotiated invocation identity.
                self.pending
                    .get(&RpcCorrelationKey {
                        family,
                        correlation_id: worker_id,
                    })
                    .filter(|pending| !pending.supports_cancellation)
                    .map(|_| worker_id)
            })
    }

    fn compact_pending_expirations_if_needed(&mut self) {
        let limit = expiration_heap_limit(self.pending.len());
        if self.expirations.len() <= limit {
            return;
        }
        self.expirations = self
            .pending
            .iter()
            .map(|(key, pending)| ExpiringPendingRequest {
                expires_at: pending.expires_at,
                key: *key,
            })
            .collect();
    }

    fn compact_queued_expirations_if_needed(&mut self) {
        let limit = expiration_heap_limit(self.queued.len());
        if self.queued_expirations.len() <= limit {
            return;
        }
        self.queued_expirations = self
            .queued
            .iter()
            .map(|(key, queued)| ExpiringPendingRequest {
                expires_at: queued.expires_at,
                key: *key,
            })
            .collect();
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

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn cleanup_session(
        &mut self,
        session_id: u64,
    ) -> RpcPendingCleanupResult {
        self.cleanup_session_with_expiration(
            session_id,
            Instant::now() + std::time::Duration::from_secs(5),
        )
    }

    pub(in crate::domains::rpc::sink) fn cleanup_session_with_expiration(
        &mut self,
        session_id: u64,
        expires_at: Instant,
    ) -> RpcPendingCleanupResult {
        let mut detached_callers = 0;
        let mut disconnect_deliveries = Vec::new();
        let mut cancellations = Vec::new();
        let worker_owned: Vec<_> = self
            .pending
            .iter()
            .filter_map(|(key, pending)| (pending.worker_session_id == session_id).then_some(*key))
            .collect();
        for key in &worker_owned {
            let pending = self
                .remove(key)
                .expect("collected worker-owned pending request");
            if pending.dispatch_info.caller_session_id != session_id
                && pending.dispatch_info.caller_inbox_addr.is_some()
            {
                if let Some(caller_inbox_addr) = pending.dispatch_info.caller_inbox_addr {
                    disconnect_deliveries.push(RpcPendingErrorDelivery {
                        correlation_id: key.correlation_id,
                        caller_session_id: pending.dispatch_info.caller_session_id,
                        caller_inbox_addr,
                    });
                }
            }
        }
        let mut rescheduled = Vec::new();
        for (key, pending) in &mut self.pending {
            if pending.dispatch_info.caller_session_id == session_id
                && pending.dispatch_info.caller_inbox_addr.is_some()
            {
                pending.dispatch_info.caller_inbox_addr = None;
                detached_callers += 1;
                if pending.supports_cancellation && !pending.cancelled {
                    pending.cancelled = true;
                    pending.expires_at = expires_at;
                    pending.close_requested = false;
                    cancellations.push(RpcWorkerCancellation {
                        family: key.family,
                        correlation_id: key.correlation_id,
                        worker_session_id: pending.worker_session_id,
                        reason:
                            crate::protocol::rpc_codec::RpcCancellationReason::CallerDisconnected,
                    });
                    rescheduled.push(*key);
                }
            }
        }
        self.expirations.extend(
            rescheduled
                .into_iter()
                .map(|key| ExpiringPendingRequest { expires_at, key }),
        );
        self.compact_pending_expirations_if_needed();

        let removed_pending = worker_owned.len();
        RpcPendingCleanupResult {
            detached_callers,
            removed_pending,
            disconnect_deliveries,
            cancellations,
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
                // Unregistering stops future dispatch; it is not a cleanup
                // acknowledgment for execution already canceled by the broker.
                (!pending.cancelled
                    && registration_ids.contains(&pending.dispatch_info.registration_id))
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
            cancellations: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(in crate::domains::rpc::sink) fn len(&self) -> usize {
        self.pending.len()
    }
}

fn expiration_heap_limit(live_entries: usize) -> usize {
    live_entries
        .saturating_mul(EXPIRATION_HEAP_LIVE_ENTRY_MULTIPLIER)
        .saturating_add(EXPIRATION_HEAP_STALE_ENTRY_ALLOWANCE)
}

#[cfg(test)]
mod tests;
