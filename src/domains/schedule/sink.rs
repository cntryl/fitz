use crate::domains::schedule::ScheduleMetrics;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
const SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US: u64 = 250_000;
const SCHEDULE_PENDING_CLAIM_TTL_MS: u64 = 24 * 60 * 60 * 1000;
const SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS: u64 = 60_000;

fn now_epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

type PendingFireKey = (u64, String);

#[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
fn schedule_admin_snapshot_due(
    snapshot_dirty: bool,
    force: bool,
    now_elapsed_us: u64,
    last_snapshot_elapsed_us: u64,
) -> bool {
    snapshot_dirty
        && (force
            || now_elapsed_us.saturating_sub(last_snapshot_elapsed_us)
                >= SCHEDULE_ADMIN_SNAPSHOT_INTERVAL_US)
}

struct ScheduleSubscription {
    route: String,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

struct ScheduleSubscriptionSet {
    subscriptions: HashMap<u64, ScheduleSubscription>,
    session_routes: HashMap<u64, HashMap<String, u64>>,
    exact_routes: HashMap<String, Vec<u64>>,
}

impl ScheduleSubscriptionSet {
    fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            session_routes: HashMap::new(),
            exact_routes: HashMap::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }

    fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    fn find_existing_id(&self, session_id: u64, route: &str) -> Option<u64> {
        self.session_routes
            .get(&session_id)
            .and_then(|routes| routes.get(route).copied())
    }

    fn insert(&mut self, subscription: ScheduleSubscription) {
        let subscription_id = subscription.subscription_id;
        let session_id = subscription.session_id;
        let route = subscription.route.clone();

        self.session_routes
            .entry(session_id)
            .or_default()
            .insert(route.clone(), subscription_id);
        self.exact_routes
            .entry(route)
            .or_default()
            .push(subscription_id);
        self.subscriptions.insert(subscription_id, subscription);
    }

    fn remove_session_route(&mut self, session_id: u64, route: &str) -> usize {
        let Some(subscription_id) = self.find_existing_id(session_id, route) else {
            return 0;
        };

        usize::from(self.remove_subscription(subscription_id))
    }

    fn remove_session(&mut self, session_id: u64) -> usize {
        let Some(routes) = self.session_routes.remove(&session_id) else {
            return 0;
        };

        let removed_ids: Vec<u64> = routes.into_values().collect();
        for subscription_id in &removed_ids {
            self.remove_subscription(*subscription_id);
        }

        removed_ids.len()
    }

    fn for_each_route(&self, route: &str, mut visit: impl FnMut(&ScheduleSubscription)) -> usize {
        let Some(subscription_ids) = self.exact_routes.get(route) else {
            return 0;
        };

        let mut matched = 0;
        for subscription_id in subscription_ids {
            if let Some(subscription) = self.subscriptions.get(subscription_id) {
                matched += 1;
                visit(subscription);
            }
        }

        matched
    }

    fn remove_subscription(&mut self, subscription_id: u64) -> bool {
        let Some(subscription) = self.subscriptions.remove(&subscription_id) else {
            return false;
        };

        let session_routes_empty =
            if let Some(routes) = self.session_routes.get_mut(&subscription.session_id) {
                routes.remove(subscription.route.as_str());
                routes.is_empty()
            } else {
                false
            };
        if session_routes_empty {
            self.session_routes.remove(&subscription.session_id);
        }

        let route_entries_empty =
            if let Some(route_entries) = self.exact_routes.get_mut(subscription.route.as_str()) {
                route_entries.retain(|id| *id != subscription_id);
                route_entries.is_empty()
            } else {
                false
            };
        if route_entries_empty {
            self.exact_routes.remove(subscription.route.as_str());
        }

        true
    }
}

pub struct ScheduleDomainSink {
    store: Arc<cntryl_midge::Engine>,
    actors: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crate::domains::schedule::ScheduleActor>,
    >,
    sub_families: Mutex<HashMap<u64, ScheduleSubscriptionSet>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
    snapshot_dirty: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    snapshot_syncing: AtomicBool,
    #[cfg_attr(feature = "bench-no-snapshot", allow(dead_code))]
    last_snapshot_elapsed_us: AtomicU64,
    snapshot_epoch: Instant,
    /// Total number of live publish handoffs that failed to route.
    live_publish_failures: AtomicU64,
    /// Total number of pending-fire acknowledgement persistence failures.
    ack_failures: AtomicU64,
    /// Pending fire claims already handed off to the live publish path in this
    /// broker process but still waiting for durable acknowledgement retry.
    pending_ack_retries: Mutex<HashMap<u64, HashSet<PendingFireKey>>>,
    /// Maximum age for a pending claimed fire before cleanup removes it.
    pending_claim_ttl_ms: u64,
    /// Last monotonic cleanup sweep time, measured relative to `snapshot_epoch`.
    last_pending_claim_cleanup_elapsed_ms: AtomicU64,
    /// Rolling window of acknowledged handoff timestamps for the legacy
    /// executions-per-minute metric.
    recent_acknowledgement_ms: Mutex<VecDeque<u64>>,
    /// Write options for schedule persistence.
    write_options: cntryl_midge::WriteOptions,
    metrics: Option<ScheduleMetrics>,
}

impl ScheduleDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
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

            let actor = crate::domains::schedule::ScheduleActor::try_new(
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

    fn get_or_create_actor<'a>(
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
                let actor = crate::domains::schedule::ScheduleActor::try_new(
                    route_family,
                    self.store.clone(),
                    self.write_options,
                )?;
                Ok(entry.insert(actor))
            }
        }
    }

    fn route_live_notify(
        &self,
        subscription: &ScheduleSubscription,
        payload: &[u8],
        payload_encoder: &mut crate::protocol::payload_codec::PayloadEncoder,
    ) {
        let notify_payload = crate::protocol::schedule_codec::encode_notify_into(
            payload_encoder,
            subscription.subscription_id,
            payload,
        );
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(705),
            bytes::Bytes::from(notify_payload),
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);
        // Subscriber notify routing is best-effort and must not redefine the
        // schedule domain's durable acknowledgement boundary.
        let _ = self.router.route(notify_envelope);
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        let families = self.sub_families.lock();
        if let Some(state) = families.get(&family_id) {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            state.for_each_route(event.route.as_str(), |subscription| {
                self.route_live_notify(subscription, &event.payload, &mut payload_encoder);
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
    ) -> Vec<crate::api::admin::SchedulePendingClaimInfo> {
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
    fn sync_admin_snapshot(&self) {
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

    fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_schedule_count(self.schedule_count());
            metrics.set_pending_fire_count(self.pending_fire_count());
        }
    }

    fn pending_claim_cleanup_due(&self) -> bool {
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

    fn schedule_response_is_failure(response: &crate::domains::schedule::ScheduleResponse) -> bool {
        matches!(
            response,
            crate::domains::schedule::ScheduleResponse::Error(_)
        )
    }

    fn schedule_admin_snapshot(&self, force: bool) {
        self.snapshot_dirty.store(true, Ordering::Relaxed);
        self.maybe_sync_admin_snapshot(force);
    }

    fn maybe_sync_admin_snapshot(&self, force: bool) {
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

impl MailboxSink for ScheduleDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        tracing::debug!(
            domain = "schedule",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Schedule domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "schedule", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let schedule_msg = match crate::protocol::schedule_codec::parse_request(
            &frame_ctx,
            &frame_ctx.payload,
            *envelope.destination().family(),
            crate::session::SessionId(frame_ctx.session_id),
            if let Some(src) = envelope.source() {
                src.clone()
            } else {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            },
        ) {
            Ok(msg) => msg,
            Err(e) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(
                    domain = "schedule",
                    error = %e,
                    "Failed to parse schedule message"
                );
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();

        use crate::domains::schedule::{ScheduleMessage, ScheduleResponse};
        let mut schedule_snapshot_dirty = false;

        let response = {
            let mut actors = self.actors.lock();
            let actor = match self.get_or_create_actor(&mut actors, route_family) {
                Ok(actor) => actor,
                Err(error) => {
                    let response = ScheduleResponse::Error(error);
                    let response_bytes = crate::protocol::schedule_codec::encode_response_into(
                        &mut payload_encoder,
                        &response,
                    );
                    let response_ctx = FrameContext::new(
                        frame_ctx.session_id,
                        frame_ctx.channel_id,
                        crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
                        bytes::Bytes::from(response_bytes),
                        frame_ctx.route_family,
                    );
                    if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
                        let _ = self.router.route(response_envelope);
                    }
                    if let (Some(metrics), Some(started_at)) =
                        (self.metrics.as_ref(), request_started)
                    {
                        metrics.record_failure(started_at);
                    }
                    return Ok(());
                }
            };

            match schedule_msg {
                ScheduleMessage::Create {
                    route,
                    cron,
                    payload,
                } => match actor.create_schedule(route, cron, payload) {
                    Ok(changed) => {
                        if changed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::CreateBatch { entries } => match actor.create_schedules(entries) {
                    Ok(changed) => {
                        if changed > 0 {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::Cancel { route } => match actor.delete_schedule(route) {
                    Ok(removed) => {
                        if removed {
                            schedule_snapshot_dirty = true;
                        }
                        ScheduleResponse::Ok
                    }
                    Err(e) => ScheduleResponse::Error(e),
                },
                ScheduleMessage::List { offset, limit } => {
                    let (entries, total_count) = actor.list_entries(offset, limit);

                    ScheduleResponse::ListDefs {
                        entries,
                        total_count,
                    }
                }
                ScheduleMessage::Subscribe {
                    family_id,
                    route,
                    session_id,
                    subscriber,
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            route.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();

                        let mut families = self.sub_families.lock();
                        let state = families
                            .entry(fam_id)
                            .or_insert_with(ScheduleSubscriptionSet::new);

                        let existing_sub_id = state.find_existing_id(session_id, route.as_str());

                        let sub_id = if let Some(id) = existing_sub_id {
                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = id,
                                route = route.as_str(),
                                "Schedule subscription already exists (idempotent)"
                            );
                            id
                        } else {
                            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(ScheduleSubscription {
                                route: route.as_str().to_string(),
                                session_id,
                                subscription_id: new_id,
                                subscriber,
                            });

                            tracing::debug!(
                                domain = "schedule",
                                session = session_id,
                                subscription_id = new_id,
                                route = route.as_str(),
                                "Schedule subscription added"
                            );
                            new_id
                        };

                        ScheduleResponse::SubscribeOk {
                            subscription_id: sub_id,
                        }
                    }
                }
                ScheduleMessage::Unsubscribe {
                    family_id,
                    route,
                    session_id,
                    ..
                } => {
                    if let Err(error) =
                        crate::domains::schedule::protocol::validate_concrete_schedule_route(
                            route.as_str(),
                        )
                    {
                        ScheduleResponse::Error(error)
                    } else {
                        let fam_id = family_id.as_u64();
                        let mut families = self.sub_families.lock();
                        let remove_family = if let Some(state) = families.get_mut(&fam_id) {
                            state.remove_session_route(session_id, route.as_str());
                            state.is_empty()
                        } else {
                            false
                        };
                        if remove_family {
                            families.remove(&fam_id);
                        }
                        ScheduleResponse::Ok
                    }
                }
                ScheduleMessage::UnsubscribeAll { session_id, .. } => {
                    self.unsubscribe_all(session_id);
                    ScheduleResponse::Ok
                }
            }
        };

        if schedule_snapshot_dirty {
            self.schedule_admin_snapshot(false);
        }

        let response_bytes =
            crate::protocol::schedule_codec::encode_response_into(&mut payload_encoder, &response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::schedule_response_is_failure(&response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domains::schedule::metrics::METRIC_PENDING_CLAIMS_EXPIRED_TOTAL;
    use crate::observability::metrics::MetricsCollector;
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
    use crate::protocol::tlv::MessageType;
    use crate::runtime::clock::Clock;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use bytes::Bytes;
    use std::sync::Arc;
    use std::time::Duration;

    #[derive(Clone)]
    struct MockClock {
        state: Arc<std::sync::Mutex<MockClockState>>,
    }

    #[derive(Clone, Copy)]
    struct MockClockState {
        instant: Instant,
        epoch_ms: u64,
    }

    impl MockClock {
        fn new(epoch_ms: u64) -> Self {
            Self {
                state: Arc::new(std::sync::Mutex::new(MockClockState {
                    instant: Instant::now(),
                    epoch_ms,
                })),
            }
        }

        fn advance(&self, duration: Duration) {
            let mut state = self.state.lock().expect("lock mock clock");
            state.instant += duration;
            state.epoch_ms = state.epoch_ms.saturating_add(duration.as_millis() as u64);
        }
    }

    impl Clock for MockClock {
        fn now_instant(&self) -> Instant {
            self.state.lock().expect("lock mock clock").instant
        }

        fn now_epoch_ms(&self) -> u64 {
            self.state.lock().expect("lock mock clock").epoch_ms
        }
    }

    fn encode_schedule_create(route: &str, cron: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_string(cron);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn encode_schedule_subscribe(route: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        Bytes::from(encoder.finish())
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    #[test]
    fn should_create_schedule_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = ScheduleDomainSink::new(store, router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_publish_schedule_notify_to_subscribers_when_due() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/nightly/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", sink.clone());

        let create_ctx = FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(700),
            encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
            family,
        );
        let subscribe_ctx = FrameContext::new(
            session_id,
            ChannelId::Sub,
            MessageType::new(703),
            encode_schedule_subscribe(schedule_route),
            family,
        );

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            create_ctx,
        ))
        .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            subscribe_ctx,
        ))
        .expect("subscribe schedule");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
        let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
        let subscription_id = subscribe_decoder
            .get_optional_u64()
            .expect("subscription id")
            .expect("subscription id present");

        {
            let mut actors = sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }

        sink.scan_due_schedules();

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("schedule notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);

        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");

        assert_eq!(notified_subscription_id, subscription_id);
        assert_eq!(notified_payload.as_ref(), b"nightly");
        assert!(notify_decoder.is_complete());
    }

    #[test]
    fn should_retry_pending_claim_after_restart_given_initial_live_publish_failure() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/replay/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let initial_sink = Arc::new(ScheduleDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model.clone(),
        ));

        initial_sink
            .deliver(Envelope::from_route(
                subscriber_address.clone(),
                schedule_address.clone(),
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(700),
                    encode_schedule_create(schedule_route, "* * * * *", b"replay"),
                    family,
                ),
            ))
            .expect("create schedule");

        {
            let mut actors = initial_sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }

        // Act
        initial_sink.scan_due_schedules();

        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let restarted_sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", restarted_sink.clone());
        restarted_sink
            .preload_persisted_families()
            .expect("preload persisted families");

        restarted_sink
            .deliver(Envelope::from_route(
                subscriber_address.clone(),
                schedule_address,
                FrameContext::new(
                    session_id,
                    ChannelId::Sub,
                    MessageType::new(703),
                    encode_schedule_subscribe(schedule_route),
                    family,
                ),
            ))
            .expect("subscribe schedule");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("subscribe ack frame");
        let mut subscribe_decoder = PayloadDecoder::new(&subscribe_frame.payload);
        let _subscribe_status = subscribe_decoder.get_u8().expect("subscribe status");
        let subscription_id = subscribe_decoder
            .get_optional_u64()
            .expect("subscription id")
            .expect("subscription id present");

        restarted_sink.scan_due_schedules();

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("schedule notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);

        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let notified_subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");

        assert_eq!(notified_subscription_id, subscription_id);
        assert_eq!(notified_payload.as_ref(), b"replay");
        assert!(notify_decoder.is_complete());

        restarted_sink.scan_due_schedules();
        assert!(
            subscriber_mailbox.receiver().try_recv().is_err(),
            "pending claimed occurrence should be acknowledged after a successful live handoff retry"
        );
    }

    #[test]
    fn should_retry_ack_without_republishing_given_same_broker_ack_persist_failure() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/retry/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", sink.clone());

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"retry"),
                family,
            ),
        ))
        .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
        let _subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("subscribe ack envelope");
        drain_mailbox(&subscriber_mailbox);

        {
            let mut actors = sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
            let claimed = actor.bench_claim_due_fires();
            assert_eq!(claimed.len(), 1);
            actor.fail_next_store_commit_for_tests();
        }

        // Act
        sink.scan_due_schedules();
        let first_notify = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("first schedule notify envelope");
        let pending_after_failed_ack = sink.pending_fire_count();
        let pending_ack_retries = sink.pending_ack_retry_count();
        sink.scan_due_schedules();

        // Assert
        let notify_frame = first_notify
            .into_payload::<FrameContext>()
            .expect("first schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_payload.as_ref(), b"retry");
        assert!(notify_decoder.is_complete());
        assert_eq!(sink.ack_failure_count(), 1);
        assert_eq!(pending_after_failed_ack, 1);
        assert_eq!(pending_ack_retries, 1);
        assert_eq!(sink.pending_fire_count(), 0);
        assert!(
            subscriber_mailbox.receiver().try_recv().is_err(),
            "ack retry should not republish a schedule notify on the same broker"
        );
    }

    #[test]
    fn should_store_cloud_strict_write_options_given_strict_cloud_policy() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = ScheduleDomainSink::new(store, router, admin_read_model)
            .with_write_options(cntryl_midge::WriteOptions::cloud_strict());

        // Assert
        assert!(sink.write_options.is_cloud_strict());
    }

    #[test]
    fn should_increment_expired_pending_claim_metric_when_cleanup_removes_orphans() {
        // Arrange
        let family = RouteFamily::new(1);
        let clock = Arc::new(MockClock::new(1_700_000_000_000));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let metrics = MetricsCollector::new();
        let mut sink = ScheduleDomainSink::new(store.clone(), router, admin_read_model)
            .with_metrics(metrics.clone());
        let mut actor = crate::domains::schedule::ScheduleActor::new_with_clock(
            family,
            store,
            cntryl_midge::WriteOptions::buffered(),
            clock.clone(),
        );
        let create_response = actor.handle(crate::domains::schedule::ScheduleMessage::Create {
            route: "schedule://acme/jobs/cleanup/run".to_string(),
            cron: "* * * * *".to_string(),
            payload: Bytes::from_static(b"cleanup"),
        });
        assert!(matches!(
            create_response,
            crate::domains::schedule::ScheduleResponse::Ok
        ));
        actor.bench_prepare_scan(1);
        let claimed = actor.bench_claim_due_fires();
        assert_eq!(claimed.len(), 1);
        clock.advance(Duration::from_millis(11));
        sink.pending_claim_ttl_ms = 10;
        let now_elapsed_ms = sink.snapshot_epoch.elapsed().as_millis() as u64;
        sink.last_pending_claim_cleanup_elapsed_ms.store(
            now_elapsed_ms.saturating_sub(SCHEDULE_PENDING_CLAIM_CLEANUP_INTERVAL_MS),
            Ordering::Relaxed,
        );
        sink.actors.lock().insert(family, actor);

        // Act
        sink.scan_due_schedules();

        // Assert
        assert_eq!(metrics.counter_get(METRIC_PENDING_CLAIMS_EXPIRED_TOTAL), 1);
        let actors = sink.actors.lock();
        assert_eq!(
            actors
                .get(&family)
                .expect("schedule actor")
                .pending_fire_count(),
            0
        );
    }

    #[test]
    fn should_remove_schedule_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let schedule_route = "schedule://acme/jobs/nightly/run";
        let schedule_address = RouteAddress::new(family, Route::new(schedule_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store,
            router.clone(),
            admin_read_model,
        ));
        router.register_domain_pattern("schedule", sink.clone());

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(700),
                encode_schedule_create(schedule_route, "* * * * *", b"nightly"),
                family,
            ),
        ))
        .expect("create schedule");
        drain_mailbox(&subscriber_mailbox);
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            schedule_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(schedule_route),
                family,
            ),
        ))
        .expect("subscribe schedule");
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://cleanup")),
            crate::runtime::SessionCleanup { session_id },
        ))
        .expect("cleanup session");
        {
            let mut actors = sink.actors.lock();
            let actor = actors.get_mut(&family).expect("schedule actor");
            actor.bench_prepare_scan(1);
        }
        sink.scan_due_schedules();

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
        assert!(sink.sub_families.lock().is_empty());
    }

    #[test]
    fn should_retain_other_schedule_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let removed_route = "schedule://acme/jobs/nightly/run";
        let retained_route = "schedule://acme/jobs/weekly/report";
        let removed_address = RouteAddress::new(family, Route::new(removed_route));
        let retained_address = RouteAddress::new(family, Route::new(retained_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = ScheduleDomainSink::new(store, router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            removed_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed schedule route");
        let _removed_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("removed subscribe ack envelope");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            retained_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(703),
                encode_schedule_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained schedule route");
        let _retained_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained subscribe ack envelope");
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            removed_address,
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(704),
                encode_schedule_subscribe(removed_route),
                family,
            ),
        ))
        .expect("unsubscribe removed schedule route");
        let unsubscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_frame = unsubscribe_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        let mut unsubscribe_decoder = PayloadDecoder::new(&unsubscribe_frame.payload);
        let unsubscribe_status = unsubscribe_decoder.get_u8().expect("unsubscribe status");
        assert_eq!(unsubscribe_status, 0);
        assert!(unsubscribe_decoder.is_complete());
        assert_eq!(sink.subscription_count(), 1);

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://events/removed")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(removed_route),
                Bytes::from_static(b"nightly"),
            ),
        ))
        .expect("deliver removed schedule event");
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("schedule://events/retained")),
            crate::runtime::DomainPublishEvent::new(
                family,
                Route::new(retained_route),
                Bytes::from_static(b"weekly"),
            ),
        ))
        .expect("deliver retained schedule event");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained schedule notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained schedule notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 705);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_payload.as_ref(), b"weekly");
        assert!(notify_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_count_live_publish_failure_given_domain_routing_error() {
        // Arrange — create a due schedule but do NOT register the "schedule" domain
        // handler so that router.route() returns an error when the live publish
        // handoff is attempted.
        let family = RouteFamily::new(1);
        let schedule_route = "schedule://acme/jobs/orphan/run";
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = Arc::new(ScheduleDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model,
        ));
        // Intentionally do NOT register the "schedule" domain handler so routing fails.

        {
            let mut actors = sink.actors.lock();
            let mut actor = crate::domains::schedule::ScheduleActor::new(
                family,
                store,
                cntryl_midge::WriteOptions::buffered(),
            );
            actor
                .create_schedule(
                    schedule_route.to_string(),
                    "* * * * *".to_string(),
                    Bytes::from_static(b"orphan"),
                )
                .expect("create schedule");
            actor.bench_prepare_scan(1);
            actors.insert(family, actor);
        }

        assert_eq!(sink.notify_failure_count(), 0, "no failures before scan");

        // Act — scan claims an occurrence but the live publish handoff cannot be routed
        sink.scan_due_schedules();

        // Assert
        assert_eq!(
            sink.notify_failure_count(),
            1,
            "live publish handoff failure should be counted when domain routing returns an error"
        );
        assert_eq!(
            sink.ack_failure_count(),
            0,
            "ack failure counter must remain zero when the publish itself failed"
        );
    }
}
