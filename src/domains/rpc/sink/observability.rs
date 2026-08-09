use super::state_model::{
    Arc, Instant, Mutex, RpcDomainCore, RpcDomainRuntime, RpcLiveCounts, RpcState,
};

impl RpcDomainCore {
    pub(super) fn registered_family_cores(&self) -> Vec<Arc<RpcDomainCore>> {
        let mut family_cores = self.family_cores.lock();
        let mut live = Vec::with_capacity(family_cores.len());
        family_cores.retain(|_, weak| {
            if let Some(core) = weak.upgrade() {
                live.push(core);
                true
            } else {
                false
            }
        });
        live
    }

    pub(super) fn aggregate_live_counts(&self) -> RpcLiveCounts {
        let family_cores = self.registered_family_cores();
        if family_cores.is_empty() {
            let state = self.state.lock();
            return RpcLiveCounts {
                workers: state.registration_count(),
                pending_requests: state.live_request_count(),
            };
        }

        family_cores
            .into_iter()
            .fold(RpcLiveCounts::default(), |mut total, family_core| {
                let state = family_core.state.lock();
                total.workers = total.workers.saturating_add(state.registration_count());
                total.pending_requests = total
                    .pending_requests
                    .saturating_add(state.live_request_count());
                total
            })
    }
}

impl RpcDomainRuntime<'_> {
    /// Copy a point-in-time view of live in-memory RPC state into the admin read
    /// model for the current broker process only.
    ///
    /// This snapshot is intentionally coalesced and may lag very recent subscribe,
    /// unsubscribe, timeout, or cleanup mutations by up to the current sync
    /// interval. It is an operational view, not a durable recovery log.
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) fn sync_admin_snapshot(&self) {
        let snapshot_now = Instant::now();
        let family_cores = self.core.registered_family_cores();
        let mut workers = Vec::new();
        let mut pending = Vec::new();
        if family_cores.is_empty() {
            Self::append_admin_snapshot_for_state(
                &self.state,
                snapshot_now,
                &mut workers,
                &mut pending,
            );
        } else {
            for family_core in family_cores {
                Self::append_admin_snapshot_for_state(
                    &family_core.state,
                    snapshot_now,
                    &mut workers,
                    &mut pending,
                );
            }
        }
        self.admin_read_model.replace_rpc_workers(workers);
        self.admin_read_model.replace_rpc_pending(pending);
    }

    fn append_admin_snapshot_for_state(
        state: &Mutex<RpcState>,
        snapshot_now: Instant,
        workers: &mut Vec<crate::control::admin::RpcWorker>,
        pending: &mut Vec<crate::control::admin::RpcPendingRequest>,
    ) {
        let state = state.lock();
        workers.extend(state.registrations.values().filter_map(|worker| {
            let route = worker.addr.route().as_str();
            let realm = route.strip_prefix("rpc://")?.split('/').next()?;
            let registered_at = worker.registered_at_rfc3339();
            Some(crate::control::admin::RpcWorker::snapshot(
                worker.addr.family().as_u64(),
                worker.session_id,
                realm,
                route,
                &registered_at,
                worker.requests_handled,
                worker.average_latency_ms(),
            ))
        }));
        pending.extend(state.queued.iter().map(|(correlation_key, queued)| {
            crate::control::admin::RpcPendingRequest::snapshot(
                queued.caller_inbox_addr.family().as_u64(),
                &correlation_key.correlation_id,
                queued.request.route.as_str(),
                &queued.submitted_at_rfc3339(),
                queued.age_seconds(snapshot_now),
                None,
            )
        }));
        pending.extend(
            state
                .pending
                .pending
                .iter()
                .map(|(correlation_key, pending)| {
                    crate::control::admin::RpcPendingRequest::snapshot(
                        pending.worker_addr.family().as_u64(),
                        &correlation_key.correlation_id,
                        pending.dispatch_info.route.as_str(),
                        &pending.submitted_at_rfc3339(),
                        pending.age_seconds(snapshot_now),
                        Some(pending.worker_session_id.to_string()),
                    )
                }),
        );
    }
}
