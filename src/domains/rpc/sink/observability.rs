use super::state_model::{
    rpc_admin_snapshot_due, rpc_timeout_sweep_interval, Arc, Duration, Instant, Mutex, Ordering,
    RpcDomainCore, RpcDomainRuntime, RpcLiveCounts, RpcState, RPC_TIMEOUT_ERROR,
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
    fn u64_to_usize_saturating(value: u64) -> usize {
        usize::try_from(value).unwrap_or(usize::MAX)
    }

    pub(super) fn elapsed_us_saturating(start: Instant) -> u64 {
        start.elapsed().as_micros().try_into().unwrap_or(u64::MAX)
    }

    pub(super) fn release_global_pending(&self, count: usize) {
        if count == 0 {
            return;
        }
        let _ = self.global_pending_count.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| Some(current.saturating_sub(count)),
        );
    }

    pub(crate) fn timeout_sweep_interval(&self) -> Duration {
        rpc_timeout_sweep_interval(self.request_timeout)
    }

    pub(super) fn live_counts(&self) -> RpcLiveCounts {
        let state = self.state.lock();
        let workers = state.registration_count();
        RpcLiveCounts {
            workers,
            pending_requests: state.live_request_count(),
        }
    }

    pub(super) fn counter_inc(&self, name: &str) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_inc(name);
        }
    }

    pub(super) fn counter_add(&self, name: &str, amount: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.counter_add(name, amount);
        }
    }

    pub(super) fn gauge_set(&self, name: &str, value: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.gauge_set(name, value);
            if name == "rpc_pending_requests" {
                metrics.set_pending_request_count(Self::u64_to_usize_saturating(value));
            }
        }
    }

    pub(super) fn histogram_observe_us(&self, name: &str, value_us: u64) {
        if let Some(ref metrics) = self.metrics {
            metrics.histogram_observe_us(name, value_us);
        }
    }

    pub(super) fn histogram_observe_elapsed_us(&self, name: &str, start: Instant) {
        self.histogram_observe_us(name, Self::elapsed_us_saturating(start));
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            let counts = self.core.aggregate_live_counts();
            metrics.set_worker_count(counts.workers);
            metrics.set_pending_request_count(counts.pending_requests);
        }
    }

    pub(super) fn expire_timed_out_requests_inline_if_due(&self) {
        let now_elapsed_us = Self::elapsed_us_saturating(self.snapshot_epoch);
        let interval_us = self
            .timeout_sweep_interval()
            .as_micros()
            .try_into()
            .unwrap_or(u64::MAX);
        let last_elapsed_us = self.last_inline_timeout_elapsed_us.load(Ordering::Relaxed);

        if now_elapsed_us.saturating_sub(last_elapsed_us) < interval_us {
            return;
        }

        if self
            .last_inline_timeout_elapsed_us
            .compare_exchange(
                last_elapsed_us,
                now_elapsed_us,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_ok()
        {
            self.expire_timed_out_requests_at(Instant::now());
        }
    }

    pub(super) fn expire_timed_out_requests_at(&self, now: Instant) {
        let timeout_result = {
            let mut state = self.state.lock();
            state.expire_timed_out(now)
        };

        if timeout_result.removed_pending == 0 {
            return;
        }

        self.release_global_pending(timeout_result.removed_pending);
        let timeout_delivery_count = timeout_result.timeout_deliveries.len();
        self.gauge_set("rpc_pending_requests", timeout_result.pending_len as u64);
        self.counter_add(
            "rpc_request_timeouts_total",
            timeout_result.removed_pending as u64,
        );
        self.counter_add(
            "rpc_cleanup_pending_removed_total",
            timeout_result.removed_pending as u64,
        );
        if timeout_result.closed_caller_drops > 0 {
            self.counter_add(
                "rpc_timeout_errors_dropped_total",
                timeout_result.closed_caller_drops as u64,
            );
            self.counter_add(
                "rpc_responses_dropped_closed_caller_total",
                timeout_result.closed_caller_drops as u64,
            );
        }
        self.schedule_admin_snapshot(false);
        self.dispatch_all_queued_requests();

        tracing::debug!(
            domain = "rpc",
            removed_pending = timeout_result.removed_pending,
            delivered_timeouts = timeout_delivery_count,
            closed_caller_drops = timeout_result.closed_caller_drops,
            pending_len = timeout_result.pending_len,
            "RPC request timeout sweep applied"
        );

        self.forward_pending_error_deliveries(
            timeout_result.timeout_deliveries,
            crate::dispatch::protocol::error_codes::rpc::ERR_RPC_TIMEOUT,
            RPC_TIMEOUT_ERROR,
            "rpc_timeout_errors_forwarded_total",
            "rpc_timeout_errors_dropped_total",
        );
    }

    pub(super) fn pending_request_count(&self) -> usize {
        self.live_counts().pending_requests
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(false);
    }

    /// Mark the admin snapshot dirty. Forced calls refresh immediately; regular
    /// hot-path updates coalesce until an admin read or another forced refresh.
    pub(super) fn schedule_admin_snapshot(&self, force: bool) {
        if force {
            self.snapshot_dirty.store(true, Ordering::Relaxed);
            self.maybe_sync_admin_snapshot(true);
            return;
        }

        if self
            .snapshot_dirty
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        self.maybe_sync_admin_snapshot(false);
    }

    /// Sync the admin snapshot when the snapshot interval elapses or a caller forces it.
    ///
    /// Even forced snapshots are still point-in-time copies of the sink's current
    /// in-memory state, not linearizable reads of concurrent RPC activity.
    pub(super) fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        if !force {
            return;
        }

        let now_elapsed_us = Self::elapsed_us_saturating(self.snapshot_epoch);
        let last_snapshot_elapsed_us = self.last_snapshot_elapsed_us.load(Ordering::Relaxed);
        let snapshot_dirty = self.snapshot_dirty.load(Ordering::Relaxed);

        if !rpc_admin_snapshot_due(
            snapshot_dirty,
            force,
            now_elapsed_us,
            last_snapshot_elapsed_us,
        ) {
            return;
        }

        if self
            .snapshot_syncing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        if !self.snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.snapshot_syncing.store(false, Ordering::Release);
            return;
        }

        let snapshot_start = Instant::now();
        self.sync_admin_snapshot();
        let snapshot_time_us = Self::elapsed_us_saturating(snapshot_start);
        self.last_snapshot_elapsed_us.store(
            Self::elapsed_us_saturating(self.snapshot_epoch),
            Ordering::Relaxed,
        );
        self.snapshot_syncing.store(false, Ordering::Release);
        self.histogram_observe_us("rpc_admin_snapshot_us", snapshot_time_us);
    }

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
