use super::model::*;

impl ScheduleDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_storage(
            crate::storage::FitzStorageEngine::new(store),
            router,
            admin_read_model,
        )
    }

    pub(crate) fn new_with_storage(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            store,
            actors: Mutex::new(HashMap::new()),
            sub_families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
            snapshot_dirty: AtomicBool::new(false),
            snapshot_syncing: AtomicBool::new(false),
            last_snapshot_elapsed_us: AtomicU64::new(0),
            snapshot_epoch: Instant::now(),
            live_publish_failures: AtomicU64::new(0),
            ack_failures: AtomicU64::new(0),
            pending_ack_retries: Mutex::new(HashMap::new()),
            pending_claim_ttl_ms: SCHEDULE_PENDING_CLAIM_TTL_MS,
            last_pending_claim_cleanup_elapsed_ms: AtomicU64::new(0),
            recent_acknowledgement_ms: Mutex::new(VecDeque::new()),
            write_options: cntryl_midge::WriteOptions::buffered(),
            metrics: None,
        }
    }

    pub fn with_write_options(mut self, write_options: cntryl_midge::WriteOptions) -> Self {
        self.write_options = write_options;
        self
    }

    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(ScheduleMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn preload_persisted_families(&self) -> Result<(), String> {
        let column_families = self
            .store
            .list_column_families()
            .map_err(|e| format!("list schedule column families failed: {}", e))?;

        let mut actors = self.actors.lock();
        for column_family in column_families {
            if column_family.id() == 0 {
                continue;
            }

            let family = crate::runtime::routing::RouteFamily::new(column_family.id().into());
            if actors.contains_key(&family) {
                continue;
            }

            let actor = crate::domains::schedule::ScheduleActor::try_new_with_storage(
                family,
                self.store.clone(),
                self.write_options,
            )?;
            actors.insert(family, actor);
        }

        // Seed the rolling-window acknowledgement counter from persisted
        // last_fire_ms values. This preserves the legacy
        // executions-per-minute metric across broker restarts for occurrences
        // that were already acknowledged within the last 60 seconds.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff_ms = now_ms.saturating_sub(60_000);
        let mut deque = self.recent_acknowledgement_ms.lock();
        for actor in actors.values() {
            for ts in actor.last_fire_timestamps_since(cutoff_ms) {
                deque.push_back(ts);
            }
        }
        deque.make_contiguous().sort_unstable();
        drop(deque);

        drop(actors);

        self.schedule_admin_snapshot(true);
        Ok(())
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_due_schedules(&self) {
        let mut live_publish_candidates = Vec::new();
        let mut ack_retry_candidates =
            HashMap::<crate::runtime::routing::RouteFamily, Vec<PendingFireKey>>::new();
        let mut snapshot_dirty = false;
        let cleanup_due = self.pending_claim_cleanup_due();
        {
            let mut actors = self.actors.lock();
            let mut pending_ack_retries = self.pending_ack_retries.lock();
            for (family, actor) in actors.iter_mut() {
                if cleanup_due {
                    match actor.cleanup_stale_pending_claims(self.pending_claim_ttl_ms) {
                        Ok(expired) if expired > 0 => {
                            snapshot_dirty = true;
                            if let Some(metrics) = &self.metrics {
                                metrics.record_pending_claims_expired(expired);
                            }
                        }
                        Ok(_) => {}
                        Err(error) => {
                            if let Some(metrics) = &self.metrics {
                                metrics.record_pending_claim_cleanup_failure();
                            }
                            tracing::warn!(
                                route_family = family.as_u64(),
                                error = %error,
                                "Failed to cleanup stale pending schedule fires"
                            );
                        }
                    }
                }

                let claimed = actor.claim_due_fires();
                if !claimed.is_empty() {
                    snapshot_dirty = true;
                }

                let family_id = family.as_u64();
                let pending_fires = actor.pending_claimed_occurrences_for_publish();
                let mut pending_keys = HashSet::with_capacity(pending_fires.len());
                let remove_retry_entry = {
                    let tracked_retries = pending_ack_retries.entry(family_id).or_default();
                    for pending_fire in pending_fires {
                        let pending_key = (pending_fire.fire_ms, pending_fire.route.clone());
                        pending_keys.insert(pending_key.clone());

                        if tracked_retries.contains(&pending_key) {
                            ack_retry_candidates
                                .entry(*family)
                                .or_default()
                                .push(pending_key);
                            continue;
                        }

                        live_publish_candidates.push((
                            *family,
                            pending_fire.fire_ms,
                            pending_fire.route,
                            pending_fire.payload,
                        ));
                    }

                    tracked_retries.retain(|pending_key| pending_keys.contains(pending_key));
                    tracked_retries.is_empty()
                };

                if remove_retry_entry {
                    pending_ack_retries.remove(&family_id);
                }
            }
        }

        let mut had_live_handoffs = false;
        for (family, fire_ms, route, payload) in live_publish_candidates {
            let route_value = crate::runtime::routing::Route::new(route.clone());
            let event =
                crate::runtime::DomainPublishEvent::new(family, route_value.clone(), payload);
            let destination = crate::runtime::routing::RouteAddress::new(family, route_value);
            // Ack is keyed to a successful handoff into the live publish path,
            // not to downstream subscriber receipt.
            if self.router.route(Envelope::new(destination, event)).is_ok() {
                had_live_handoffs = true;
                ack_retry_candidates
                    .entry(family)
                    .or_default()
                    .push((fire_ms, route));
            } else {
                self.live_publish_failures.fetch_add(1, Ordering::Relaxed);
            }
        }

        let mut acknowledged_handoffs = false;

        if !ack_retry_candidates.is_empty() {
            let mut actors = self.actors.lock();
            let mut pending_ack_retries = self.pending_ack_retries.lock();
            for (family, ack_candidates) in ack_retry_candidates {
                if let Some(actor) = actors.get_mut(&family) {
                    let family_id = family.as_u64();
                    match actor.ack_pending_fire_claims(&ack_candidates) {
                        Ok((acked, acknowledged_at_ms)) if acked > 0 => {
                            let remove_retry_entry = if let Some(tracked_retries) =
                                pending_ack_retries.get_mut(&family_id)
                            {
                                for pending_key in &ack_candidates {
                                    tracked_retries.remove(pending_key);
                                }
                                tracked_retries.is_empty()
                            } else {
                                false
                            };
                            if remove_retry_entry {
                                pending_ack_retries.remove(&family_id);
                            }

                            acknowledged_handoffs = true;
                            let mut deque = self.recent_acknowledgement_ms.lock();
                            let cutoff = acknowledged_at_ms.saturating_sub(60_000);
                            while deque.front().copied().is_some_and(|t| t < cutoff) {
                                deque.pop_front();
                            }
                            for _ in 0..acked {
                                deque.push_back(acknowledged_at_ms);
                            }
                        }
                        Ok(_) => {
                            let remove_retry_entry = if let Some(tracked_retries) =
                                pending_ack_retries.get_mut(&family_id)
                            {
                                for pending_key in &ack_candidates {
                                    tracked_retries.remove(pending_key);
                                }
                                tracked_retries.is_empty()
                            } else {
                                false
                            };
                            if remove_retry_entry {
                                pending_ack_retries.remove(&family_id);
                            }
                        }
                        Err(error) => {
                            self.ack_failures.fetch_add(1, Ordering::Relaxed);
                            let tracked_retries = pending_ack_retries.entry(family_id).or_default();
                            for pending_key in ack_candidates {
                                tracked_retries.insert(pending_key);
                            }
                            tracing::warn!(
                                route_family = family.as_u64(),
                                error = %error,
                                "Failed to acknowledge pending schedule fires"
                            );
                        }
                    }
                }
            }
        }

        if snapshot_dirty || had_live_handoffs || acknowledged_handoffs {
            self.schedule_admin_snapshot(false);
        }

        self.refresh_metrics_gauges();
    }

    pub(crate) fn force_due_scan_for_tests(&self, ready_count: usize) {
        {
            let mut actors = self.actors.lock();
            for actor in actors.values_mut() {
                actor.bench_prepare_scan(ready_count);
            }
        }

        self.scan_due_schedules();
        self.schedule_admin_snapshot(true);
    }

    pub(super) fn get_or_create_actor<'a>(
        &'a self,
        actors: &'a mut HashMap<
            crate::runtime::routing::RouteFamily,
            crate::domains::schedule::ScheduleActor,
        >,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<&'a mut crate::domains::schedule::ScheduleActor, String> {
        match actors.entry(route_family) {
            Entry::Occupied(entry) => Ok(entry.into_mut()),
            Entry::Vacant(entry) => {
                let actor = crate::domains::schedule::ScheduleActor::try_new_with_storage(
                    route_family,
                    self.store.clone(),
                    self.write_options,
                )?;
                Ok(entry.insert(actor))
            }
        }
    }

    pub(super) fn route_live_notify(
        &self,
        subscription: &ScheduleSubscription,
        payload: &bytes::Bytes,
    ) {
        #[cfg(test)]
        let notify_payload = crate::protocol::schedule_codec::encode_notify(
            subscription.subscription_id,
            payload.as_ref(),
        );

        #[cfg(test)]
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(705),
            bytes::Bytes::from(notify_payload),
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );

        #[cfg(test)]
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);

        #[cfg(not(test))]
        let notify_envelope = Envelope::new(
            subscription.subscriber.clone(),
            crate::domains::schedule::ScheduleClientNotification::new(
                subscription.session_id,
                crate::runtime::routing::RouteFamily::from_u32(
                    subscription.subscriber.family().id(),
                ),
                subscription.subscription_id,
                payload.clone(),
            ),
        );

        // Subscriber notify routing is best-effort and must not redefine the
        // schedule domain's durable acknowledgement boundary.
        let _ = self.router.route(notify_envelope);
    }

    pub(super) fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            state.for_each_route(event.route.as_str(), |subscription| {
                self.route_live_notify(subscription, &event.payload);
            });
        }
        Ok(())
    }

    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.sub_families.lock();
        for state in families.values_mut() {
            state.remove_session(session_id);
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.sub_families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }

    pub fn schedule_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.schedule_count()).sum()
    }

    pub fn pending_fire_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|actor| actor.pending_fire_count())
            .sum()
    }

    /// Legacy metric name: counts acknowledged live handoffs over the last minute.
    pub fn executions_per_minute(&self) -> f64 {
        let now_ms = now_epoch_ms();
        let cutoff = now_ms.saturating_sub(60_000);
        let mut deque = self.recent_acknowledgement_ms.lock();
        while deque.front().copied().is_some_and(|t| t < cutoff) {
            deque.pop_front();
        }
        deque.len() as f64
    }

    pub fn notify_failure_count(&self) -> u64 {
        self.live_publish_failures.load(Ordering::Relaxed)
    }

    pub fn ack_failure_count(&self) -> u64 {
        self.ack_failures.load(Ordering::Relaxed)
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        let pending_ack_retries = self.pending_ack_retries.lock();
        pending_ack_retries
            .values()
            .map(|tracked| tracked.len())
            .sum()
    }

    pub fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let actors = self.actors.lock();
        actors
            .get(&route_family)
            .map(|actor| actor.admin_pending_claims())
            .unwrap_or_default()
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        let now_ms = now_epoch_ms();
        let actors = self.actors.lock();
        actors
            .values()
            .map(|actor| actor.oldest_pending_claim_age_seconds(now_ms))
            .max()
            .unwrap_or(0)
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|actor| actor.overdue_normalization_count())
            .sum()
    }

    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    pub(super) fn sync_admin_snapshot(&self) {
        let snapshot = {
            let actors = self.actors.lock();
            let mut schedules = Vec::new();
            for actor in actors.values() {
                schedules.extend(actor.admin_snapshot());
            }
            schedules
        };

        self.admin_read_model.replace_schedules(snapshot);
        self.refresh_metrics_gauges();
    }

    pub(super) fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_schedule_count(self.schedule_count());
            metrics.set_pending_fire_count(self.pending_fire_count());
        }
    }

    pub(super) fn pending_claim_cleanup_due(&self) -> bool {
        let now_elapsed_ms = self.snapshot_epoch.elapsed().as_millis() as u64;
        let mut last_elapsed_ms = self
            .last_pending_claim_cleanup_elapsed_ms
            .load(Ordering::Relaxed);

        loop {
            if last_elapsed_ms == 0 {
                let first_elapsed_ms = now_elapsed_ms.max(1);
                match self.last_pending_claim_cleanup_elapsed_ms.compare_exchange(
                    0,
                    first_elapsed_ms,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(observed) => {
                        last_elapsed_ms = observed;
                        continue;
                    }
                }
            }

            if now_elapsed_ms.saturating_sub(last_elapsed_ms)
                < SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS
            {
                return false;
            }

            match self.last_pending_claim_cleanup_elapsed_ms.compare_exchange(
                last_elapsed_ms,
                now_elapsed_ms,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => last_elapsed_ms = observed,
            }
        }
    }

    pub(super) fn schedule_response_is_failure(
        response: &crate::domains::schedule::ScheduleResponse,
    ) -> bool {
        matches!(
            response,
            crate::domains::schedule::ScheduleResponse::Error(_)
        )
    }

    pub(super) fn schedule_admin_snapshot(&self, force: bool) {
        self.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    pub(super) fn maybe_sync_admin_snapshot(&self, force: bool) {
        #[cfg(feature = "bench-no-snapshot")]
        if !force {
            return;
        }

        let now_elapsed_us = self.snapshot_epoch.elapsed().as_micros() as u64;
        let last_snapshot_elapsed_us = self.last_snapshot_elapsed_us.load(Ordering::Relaxed);
        let snapshot_dirty = self.snapshot_dirty.load(Ordering::Relaxed);

        if !schedule_admin_snapshot_due(
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

        self.sync_admin_snapshot();
        self.last_snapshot_elapsed_us.store(
            self.snapshot_epoch.elapsed().as_micros() as u64,
            Ordering::Relaxed,
        );
        self.snapshot_syncing.store(false, Ordering::Release);
    }

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        self.maybe_sync_admin_snapshot(true);
    }

    #[doc(hidden)]
    pub fn bench_publish_event(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        self.handle_domain_publish(event)
    }
}
