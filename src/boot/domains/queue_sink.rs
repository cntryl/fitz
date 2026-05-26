use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::domains::queue::{
    projection::{QueueAdminProjection, QueueProjectionEntry, QueueProjectionState},
    QueueAdminSnapshot, QueueMetrics, QueueNotification, QueueSubscriptionMessage,
};
use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct WarmQueueActor {
    actor: Arc<Mutex<crate::domains::queue::QueueActor>>,
    last_used: Instant,
}

struct QueueSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for QueueSubscription {
    fn pattern(&self) -> &crate::runtime::matcher::Pattern {
        &self.pattern
    }

    fn session_id(&self) -> u64 {
        self.session_id
    }

    fn subscription_id(&self) -> u64 {
        self.subscription_id
    }
}

#[derive(Clone, Copy)]
struct QueueReadyNotification {
    family_id: crate::runtime::routing::RouteFamily,
    snapshot: QueueAdminSnapshot,
}

const QUEUE_ACTOR_IDLE_TTL: Duration = Duration::from_secs(5 * 60);
const QUEUE_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);
const QUEUE_RUNTIME_SWEEP_INTERVAL: Duration = Duration::from_millis(50);
const QUEUE_DEDUP_SWEEP_INTERVAL: Duration = Duration::from_secs(30);

/// Queue domain sink with per-queue QueueActor instances
///
/// This sink:
/// - Maintains per-queue QueueActor instances keyed by QueueKey
/// - Parses TLV frames to QueueMessage
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks queue-local watch subscriptions for the current broker process
/// - Exposes only warm in-memory queue/admin state for the current broker process
pub struct QueueDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Commit policy for queue persistence on this runtime.
    queue_write_options: cntryl_midge::WriteOptions,
    /// Deduplication store shared by warm actors created through this sink.
    dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    /// Per-queue actors keyed by QueueKey
    actors: Mutex<HashMap<crate::domains::queue::QueueKey, WarmQueueActor>>,
    /// Queue-local watch subscriptions scoped to this broker process.
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<QueueSubscription>>>,
    next_sub_id: AtomicU64,
    ready_states: Mutex<HashMap<crate::domains::queue::QueueKey, bool>>,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    projection: QueueAdminProjection,
    metrics: Option<QueueMetrics>,
    active: AtomicBool,
    next_idle_sweep_at: Mutex<Instant>,
    next_dedup_sweep_at: Mutex<Instant>,
}

impl QueueDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self {
            store,
            queue_write_options,
            dedup_store,
            actors: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            ready_states: Mutex::new(HashMap::new()),
            router,
            projection: QueueAdminProjection::new(admin_read_model),
            metrics: None,
            active: AtomicBool::new(true),
            next_idle_sweep_at: Mutex::new(Instant::now()),
            next_dedup_sweep_at: Mutex::new(Instant::now()),
        }
    }

    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(QueueMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    pub fn start_runtime_sweep(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("Queue runtime sweep not started: no Tokio runtime available");
            return;
        };

        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            let mut interval = tokio::time::interval(QUEUE_RUNTIME_SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                sink.sweep_runtime_state();
            }
        });
    }

    fn queue_key_for_route(
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
    ) -> Result<crate::domains::queue::QueueKey, crate::domains::queue::QueueResponse> {
        crate::domains::queue::QueueKey::from_route(family_id, route).ok_or_else(|| {
            crate::domains::queue::QueueResponse::BadRequest {
                reason: format!("invalid queue route: {}", route.as_str()),
            }
        })
    }

    fn parse_frame(
        frame_ctx: &FrameContext,
        route_family: crate::runtime::routing::RouteFamily,
        subscriber: crate::runtime::routing::RouteAddress,
    ) -> Result<crate::protocol::queue_codec::ParsedQueueFrame, crate::domains::queue::QueueResponse>
    {
        crate::protocol::queue_codec::parse_frame(
            frame_ctx,
            &frame_ctx.payload,
            route_family,
            frame_ctx.session_id,
            subscriber,
        )
        .map_err(|reason| crate::domains::queue::QueueResponse::BadRequest { reason })
    }

    fn session_inbox_address(
        family_id: crate::runtime::routing::RouteFamily,
        session_id: u64,
    ) -> crate::runtime::routing::RouteAddress {
        crate::runtime::routing::RouteAddress::new(
            family_id,
            crate::runtime::routing::Route::new(format!("inbox://session/{session_id}")),
        )
    }

    fn route_queue_response(
        &self,
        request_envelope: &Envelope,
        frame_ctx: &FrameContext,
        response: &crate::domains::queue::QueueResponse,
    ) {
        let response_bytes = crate::protocol::queue_codec::encode_response(response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );

        if let Some(response_envelope) = request_envelope.try_reply_to(response_ctx) {
            if let Err(error) = self.router.route(response_envelope) {
                tracing::warn!(
                    domain = "queue",
                    session = frame_ctx.session_id,
                    error = ?error,
                    "Failed to route queue response"
                );
            }
        }
    }

    fn queue_ready_route(key: &crate::domains::queue::QueueKey) -> crate::runtime::routing::Route {
        crate::runtime::routing::Route::new(format!(
            "queue://{}/{}/{}/ready",
            key.realm, key.area, key.resource
        ))
    }

    fn record_ready_state(
        &self,
        key: &crate::domains::queue::QueueKey,
        snapshot: QueueAdminSnapshot,
    ) -> Option<QueueReadyNotification> {
        let is_ready = snapshot.messages_ready > 0;
        let mut ready_states = self.ready_states.lock();
        let was_ready = ready_states.get(key).copied().unwrap_or(false);

        if snapshot.messages_total == 0 && snapshot.messages_inflight == 0 {
            ready_states.remove(key);
        } else {
            ready_states.insert(key.clone(), is_ready);
        }

        if !was_ready && is_ready {
            Some(QueueReadyNotification {
                family_id: key.family,
                snapshot,
            })
        } else {
            None
        }
    }

    fn route_queue_notify_to_subscription(
        &self,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
        route: &crate::runtime::routing::Route,
        snapshot: QueueAdminSnapshot,
    ) {
        let payload = crate::protocol::queue_codec::encode_notify(
            subscription_id,
            route,
            QueueNotification {
                ready_messages: snapshot.messages_ready as u64,
                delayed_messages: snapshot.messages_delayed as u64,
                inflight_messages: snapshot.messages_inflight as u64,
            },
        );
        let notify_ctx = FrameContext::new(
            session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(crate::protocol::queue_codec::msg_type::NOTIFY),
            bytes::Bytes::from(payload),
            *subscriber.family(),
        );
        let notify_envelope = Envelope::new(subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::boot::observability::counter_inc("fitz_queue_notify_drops_total");
        }
    }

    fn route_queue_ready_notification(
        &self,
        key: &crate::domains::queue::QueueKey,
        notification: QueueReadyNotification,
    ) {
        let route = Self::queue_ready_route(key);
        let families = self.families.lock();
        if let Some(state) = families.get(&notification.family_id.as_u64()) {
            state.for_each_matching_route(notification.family_id, route.as_str(), |subscription| {
                self.route_queue_notify_to_subscription(
                    subscription.session_id,
                    subscription.subscription_id,
                    &subscription.subscriber,
                    &route,
                    notification.snapshot,
                );
            });
        }
    }

    fn emit_current_ready_notifications_for_watch(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        pattern: &crate::runtime::matcher::Pattern,
        session_id: u64,
        subscription_id: u64,
        subscriber: &crate::runtime::routing::RouteAddress,
    ) {
        let actors = self.actors.lock();
        let ready_snapshots: Vec<_> = actors
            .iter()
            .filter(|(key, _)| key.family == family_id)
            .filter_map(|(key, warm_actor)| {
                let snapshot = warm_actor.actor.lock().admin_snapshot();
                let route = Self::queue_ready_route(key);
                (snapshot.messages_ready > 0 && pattern.matches(&route))
                    .then_some((route, snapshot))
            })
            .collect();
        drop(actors);

        for (route, snapshot) in ready_snapshots {
            self.route_queue_notify_to_subscription(
                session_id,
                subscription_id,
                subscriber,
                &route,
                snapshot,
            );
        }
    }

    fn sweep_runtime_state(&self) {
        self.sweep_runtime_state_at(Instant::now());
    }

    fn sweep_runtime_state_at(&self, now: Instant) {
        self.sweep_idle_actors_at(now);
        self.maybe_cleanup_dedup_at(now);
    }

    fn maybe_cleanup_dedup_at(&self, now: Instant) {
        let should_cleanup = {
            let mut next_dedup_sweep_at = self.next_dedup_sweep_at.lock();
            if now < *next_dedup_sweep_at {
                false
            } else {
                *next_dedup_sweep_at = now + QUEUE_DEDUP_SWEEP_INTERVAL;
                true
            }
        };

        if should_cleanup {
            self.dedup_store.cleanup();
        }
    }

    fn get_or_create_actor(
        &self,
        key: crate::domains::queue::QueueKey,
    ) -> (Arc<Mutex<crate::domains::queue::QueueActor>>, bool) {
        use std::collections::hash_map::Entry;

        let now = Instant::now();
        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().last_used = now;
                (entry.get().actor.clone(), false)
            }
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(
                    crate::domains::queue::QueueActor::new_with_write_options(
                        key.family,
                        key,
                        self.store.clone(),
                        None,
                        self.dedup_store.clone(),
                        self.queue_write_options,
                    ),
                ));
                entry.insert(WarmQueueActor {
                    actor: actor.clone(),
                    last_used: now,
                });
                (actor, true)
            }
        }
    }

    fn mark_admin_snapshot_dirty(&self) {
        self.projection.mark_dirty();
        self.refresh_metrics_gauges();
    }

    fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_ready_messages(self.ready_message_count());
            metrics.set_delayed_messages(self.delayed_message_count());
            metrics.set_inflight_messages(self.active_inflight_count());
        }
    }

    fn observe_histogram_us(&self, name: &str, value_us: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.histogram_observe_us(name, value_us);
        } else {
            crate::boot::observability::histogram_observe_us(name, value_us);
        }
    }

    fn queue_response_is_failure(response: &crate::domains::queue::QueueResponse) -> bool {
        matches!(
            response,
            crate::domains::queue::QueueResponse::InvalidToken
                | crate::domains::queue::QueueResponse::InflightExpired
                | crate::domains::queue::QueueResponse::NotFound
                | crate::domains::queue::QueueResponse::QueueNotFound
                | crate::domains::queue::QueueResponse::BadRequest { .. }
                | crate::domains::queue::QueueResponse::Error { .. }
        )
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        self.sweep_idle_actors();
        self.projection
            .refresh_if_dirty(|| self.collect_projection_state());
    }

    fn collect_projection_state(&self) -> QueueProjectionState {
        let actors = self.actors.lock();
        let entries = actors
            .iter()
            .map(|(key, warm_actor)| {
                let actor = warm_actor.actor.lock();
                QueueProjectionEntry {
                    key: key.clone(),
                    snapshot: actor.admin_snapshot(),
                    inflight: actor.admin_inflight(),
                    dead_letters: actor.admin_dead_letters(),
                }
            })
            .collect();

        QueueProjectionState::from_entries(entries)
    }

    fn sweep_idle_actors(&self) {
        self.sweep_idle_actors_at(Instant::now());
    }

    fn maybe_sweep_idle_actors(&self) {
        let now = Instant::now();

        {
            let mut next_idle_sweep_at = self.next_idle_sweep_at.lock();
            if now < *next_idle_sweep_at {
                return;
            }
            *next_idle_sweep_at = now + QUEUE_IDLE_SWEEP_INTERVAL;
        }

        self.sweep_idle_actors_at(now);
    }

    fn sweep_idle_actors_at(&self, now: Instant) {
        let mut changed = false;
        let mut notifications = Vec::new();
        let mut removed_keys = Vec::new();
        let mut actors = self.actors.lock();

        actors.retain(|key, warm_actor| {
            let mut actor = warm_actor.actor.lock();
            let ready_before = actor.ready_len();
            let inflight_before = actor.inflight.len();
            actor.process_due_work();
            let ready_after = actor.ready_len();
            let inflight_after = actor.inflight.len();
            let snapshot = actor.admin_snapshot();
            if ready_before != ready_after || inflight_before != inflight_after {
                changed = true;
            }

            if let Some(notification) = self.record_ready_state(key, snapshot) {
                notifications.push((key.clone(), notification));
            }

            let idle_for = now.saturating_duration_since(warm_actor.last_used);
            let should_keep = idle_for < QUEUE_ACTOR_IDLE_TTL
                || snapshot.messages_delayed > 0
                || snapshot.messages_inflight > 0;
            if !should_keep {
                changed = true;
                removed_keys.push(key.clone());
            }
            should_keep
        });

        drop(actors);
        if !removed_keys.is_empty() {
            let mut ready_states = self.ready_states.lock();
            for key in removed_keys {
                ready_states.remove(&key);
            }
        }
        if changed {
            self.mark_admin_snapshot_dirty();
        }
        for (key, notification) in notifications {
            self.route_queue_ready_notification(&key, notification);
        }
    }

    /// Drop all live queue inflight entries owned by the disconnected session and return
    /// those committed messages to the ready queue. Inflight ownership is
    /// broker-local runtime state only.
    pub fn cleanup_session(&self, session_id: u64) {
        let mut released_any = false;
        let mut notifications = Vec::new();
        let mut actors = self.actors.lock();
        for (key, warm_actor) in actors.iter_mut() {
            let mut actor = warm_actor.actor.lock();
            if actor.cleanup_session_inflight(session_id) > 0 {
                released_any = true;
                if let Some(notification) = self.record_ready_state(key, actor.admin_snapshot()) {
                    notifications.push((key.clone(), notification));
                }
            }
        }
        drop(actors);

        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);

        if released_any {
            self.mark_admin_snapshot_dirty();
        }

        for (key, notification) in notifications {
            self.route_queue_ready_notification(&key, notification);
        }

        tracing::debug!(
            domain = "queue",
            session = session_id,
            "Queue session cleanup completed"
        );
    }

    pub fn pending_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| {
                let snapshot = warm_actor.actor.lock().admin_snapshot();
                snapshot.messages_ready + snapshot.messages_delayed
            })
            .sum()
    }

    pub fn ready_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().admin_snapshot().messages_ready)
            .sum()
    }

    pub fn delayed_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().admin_snapshot().messages_delayed)
            .sum()
    }

    pub fn active_inflight_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| warm_actor.actor.lock().inflight.len())
            .sum()
    }

    pub fn dead_letter_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|warm_actor| {
                warm_actor
                    .actor
                    .lock()
                    .admin_snapshot()
                    .messages_dead_lettered
            })
            .sum()
    }

    pub fn replay_dead_letter(
        &self,
        key: crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key.clone());
        let result = {
            let mut actor = actor_handle.lock();
            actor.replay_dead_letter(id)
        };

        if matches!(result, Ok(true)) {
            let snapshot = actor_handle.lock().admin_snapshot();
            let notification = self.record_ready_state(&key, snapshot);
            self.mark_admin_snapshot_dirty();
            if let Some(notification) = notification {
                self.route_queue_ready_notification(&key, notification);
            }
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.admin_snapshot().messages_total == 0 && actor.inflight.is_empty()
            };
            if should_remove {
                self.actors.lock().remove(&key);
                self.ready_states.lock().remove(&key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }

    pub fn purge_dead_letter(
        &self,
        key: crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        let (actor_handle, created_actor) = self.get_or_create_actor(key.clone());
        let result = {
            let mut actor = actor_handle.lock();
            actor.purge_dead_letter(id)
        };

        if matches!(result, Ok(true)) {
            self.mark_admin_snapshot_dirty();
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.admin_snapshot().messages_total == 0 && actor.inflight.is_empty()
            };
            if should_remove {
                self.actors.lock().remove(&key);
                self.ready_states.lock().remove(&key);
                self.mark_admin_snapshot_dirty();
            }
        }

        result
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return Ok(());
        }

        tracing::debug!(
            domain = "queue",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Queue domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "queue", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };

        let route_addr = envelope.destination();
        let route_family = *route_addr.family();
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());
        let subscriber = envelope
            .source()
            .cloned()
            .unwrap_or_else(|| Self::session_inbox_address(route_family, frame_ctx.session_id));

        let parsed_frame = match Self::parse_frame(&frame_ctx, route_family, subscriber.clone()) {
            Ok(msg) => msg,
            Err(response) => {
                self.route_queue_response(&envelope, &frame_ctx, &response);
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                return Ok(());
            }
        };

        tracing::debug!(
            domain = "queue",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed Queue message successfully"
        );

        self.maybe_sweep_idle_actors();

        if let crate::protocol::queue_codec::ParsedQueueFrame::Sub(sub_msg) = parsed_frame {
            let (response, initial_watch_snapshot) = match sub_msg {
                QueueSubscriptionMessage::Watch {
                    family_id,
                    pattern,
                    session_id,
                    subscriber,
                } => {
                    let pattern_str = pattern.as_str();
                    let parsed_pattern = crate::runtime::matcher::Pattern::new(pattern_str);
                    let subscription_id = {
                        let mut families = self.families.lock();
                        let state = families
                            .entry(family_id.as_u64())
                            .or_insert_with(RoutedSubscriptionSet::new);

                        if let Some(id) = state.find_existing_id(session_id, pattern_str) {
                            id
                        } else {
                            let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                            state.insert(
                                family_id,
                                QueueSubscription {
                                    pattern: parsed_pattern.clone(),
                                    session_id,
                                    subscription_id: id,
                                    subscriber: subscriber.clone(),
                                },
                            );
                            id
                        }
                    };

                    (
                        crate::domains::queue::QueueResponse::WatchOk { subscription_id },
                        Some((
                            family_id,
                            parsed_pattern,
                            session_id,
                            subscription_id,
                            subscriber,
                        )),
                    )
                }
                QueueSubscriptionMessage::Unwatch {
                    family_id,
                    pattern,
                    session_id,
                    ..
                } => {
                    let mut families = self.families.lock();
                    let remove_family = if let Some(state) = families.get_mut(&family_id.as_u64()) {
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                        state.is_empty()
                    } else {
                        false
                    };
                    if remove_family {
                        families.remove(&family_id.as_u64());
                    }
                    (crate::domains::queue::QueueResponse::UnwatchOk, None)
                }
            };

            self.route_queue_response(&envelope, &frame_ctx, &response);
            if let Some((family_id, pattern, session_id, subscription_id, subscriber)) =
                initial_watch_snapshot
            {
                self.emit_current_ready_notifications_for_watch(
                    family_id,
                    &pattern,
                    session_id,
                    subscription_id,
                    &subscriber,
                );
            }

            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if Self::queue_response_is_failure(&response) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
            return Ok(());
        }

        use crate::domains::queue::protocol::QueueMessage;

        let queue_msg = match parsed_frame {
            crate::protocol::queue_codec::ParsedQueueFrame::Op(msg) => msg,
            crate::protocol::queue_codec::ParsedQueueFrame::Sub(_) => unreachable!(),
        };

        // Capture the operation kind before the consuming match for operation-specific metrics.
        #[derive(Clone, Copy)]
        enum QueueOpKind {
            Send,
            Receive,
            Extend,
            Ack,
            Other,
        }
        let op_kind = match &queue_msg {
            QueueMessage::Send { .. } => QueueOpKind::Send,
            QueueMessage::Receive { .. } => QueueOpKind::Receive,
            QueueMessage::Extend { .. } => QueueOpKind::Extend,
            QueueMessage::Ack { .. } => QueueOpKind::Ack,
            _ => QueueOpKind::Other,
        };

        let (response, ready_notification, should_mark_admin_snapshot_dirty) = match queue_msg {
            QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = self.get_or_create_actor(key);
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let resp = actor.handle_send(body, delay_seconds);
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        resp,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Receive {
                family_id,
                route,
                inflight_seconds,
                batch_size,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = self.get_or_create_actor(key);
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_receive_for_session(
                        frame_ctx.session_id,
                        inflight_seconds,
                        batch_size,
                    );
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;

                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Extend {
                family_id,
                route,
                id,
                token,
                inflight_seconds,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = self.get_or_create_actor(key);
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_extend(id, token, inflight_seconds);
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::Ack {
                family_id,
                route,
                id,
                token,
            } => match Self::queue_key_for_route(family_id, &route) {
                Ok(key) => {
                    let notification_key = key.clone();
                    let actor_lock_start = Instant::now();
                    let (actor_handle, created_actor) = self.get_or_create_actor(key);
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                        actor_lock_start.elapsed().as_micros() as u64,
                    );
                    let mut actor = actor_handle.lock();
                    let actor_exec_start = Instant::now();
                    actor.process_due_work();
                    let response = actor.handle_ack(id, token);
                    let notification =
                        self.record_ready_state(&notification_key, actor.admin_snapshot());
                    self.observe_histogram_us(
                        obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                        actor_exec_start.elapsed().as_micros() as u64,
                    );
                    let _ = created_actor;
                    (
                        response,
                        notification.map(|event| (notification_key.clone(), event)),
                        true,
                    )
                }
                Err(response) => (response, None, false),
            },
            QueueMessage::InflightExpired { .. } => (
                crate::domains::queue::QueueResponse::Error {
                    message: "InflightExpired is an internal message".to_string(),
                },
                None,
                false,
            ),
        };
        if should_mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
        }

        if let Some((key, notification)) = ready_notification {
            self.route_queue_ready_notification(&key, notification);
        }

        self.route_queue_response(&envelope, &frame_ctx, &response);

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::queue_response_is_failure(&response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
                match op_kind {
                    QueueOpKind::Send => metrics.record_enqueue(started_at),
                    QueueOpKind::Receive => metrics.record_reserve(started_at),
                    QueueOpKind::Ack => metrics.record_complete(),
                    QueueOpKind::Extend => metrics.record_extend(),
                    QueueOpKind::Other => {}
                }
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
    use crate::protocol::frame::ChannelId;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::Mailbox;
    use bytes::{BufMut, Bytes};

    fn encode_route_pattern(pattern: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(pattern.len() as u32);
        payload.put_slice(pattern.as_bytes());
        Bytes::from(payload)
    }

    fn encode_queue_send(route: &str, body: &[u8]) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u32(body.len() as u32);
        payload.put_slice(body);
        Bytes::from(payload)
    }

    fn encode_queue_send_with_delay(route: &str, body: &[u8], delay_seconds: u64) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u32(body.len() as u32);
        payload.put_slice(body);
        payload.put_u8(1);
        payload.put_u64(delay_seconds);
        Bytes::from(payload)
    }

    fn encode_queue_reserve(route: &str, inflight_seconds: u64, batch_size: u32) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(inflight_seconds);
        payload.put_u8(1);
        payload.put_u32(batch_size);
        Bytes::from(payload)
    }

    fn encode_queue_watch(pattern: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(pattern.len() as u32);
        payload.put_slice(pattern.as_bytes());
        Bytes::from(payload)
    }

    fn encode_queue_unwatch(pattern: &str) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(pattern.len() as u32);
        payload.put_slice(pattern.as_bytes());
        Bytes::from(payload)
    }

    fn encode_queue_extend(route: &str, id: u64, token: u64, inflight_seconds: u64) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(id);
        payload.put_u64(token);
        payload.put_u64(inflight_seconds);
        Bytes::from(payload)
    }

    fn encode_queue_ack(route: &str, id: u64, token: u64) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(id);
        payload.put_u64(token);
        Bytes::from(payload)
    }

    fn bad_request_reason(frame: &FrameContext) -> String {
        assert_eq!(frame.payload[0], 1, "expected error status");
        let reason_len = u32::from_be_bytes(
            frame.payload[1..5]
                .try_into()
                .expect("bad request payload should include reason length"),
        ) as usize;
        String::from_utf8(frame.payload[5..5 + reason_len].to_vec())
            .expect("bad request reason should be valid utf-8")
    }

    fn new_queue_domain_sink(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
    ) -> QueueDomainSink {
        QueueDomainSink::new(
            store,
            router,
            admin_read_model,
            queue_write_options,
            crate::utils::idempotency::default_dedup_store(),
        )
    }

    fn receive_response_message_count(frame: &FrameContext) -> u32 {
        assert_eq!(frame.payload[0], 0, "expected success status");
        u32::from_be_bytes(
            frame.payload[1..5]
                .try_into()
                .expect("receive payload should include count"),
        )
    }

    fn watch_response_subscription_id(frame: &FrameContext) -> u64 {
        assert_eq!(frame.payload[0], 0, "expected success status");
        u64::from_be_bytes(
            frame.payload[1..9]
                .try_into()
                .expect("watch payload should include subscription id"),
        )
    }

    fn decode_queue_watch_delivery(frame: &FrameContext) -> (u64, String, u64) {
        let subscription_id = u64::from_be_bytes(frame.payload[0..8].try_into().unwrap());
        let route_len = u32::from_be_bytes(frame.payload[8..12].try_into().unwrap()) as usize;
        let route = String::from_utf8(frame.payload[12..12 + route_len].to_vec())
            .expect("queue watch route should be utf-8");
        let offset = 12 + route_len;
        let ready_messages =
            u64::from_be_bytes(frame.payload[offset..offset + 8].try_into().unwrap());
        (subscription_id, route, ready_messages)
    }

    fn force_actor_idle(sink: &QueueDomainSink, queue_route: &str, family: RouteFamily) {
        let key = crate::domains::queue::QueueKey::from_route(family, &Route::new(queue_route))
            .expect("queue key");
        let mut actors = sink.actors.lock();
        let warm_actor = actors.get_mut(&key).expect("warm queue actor");
        warm_actor.last_used = Instant::now() - QUEUE_ACTOR_IDLE_TTL - Duration::from_secs(1);
    }

    #[test]
    fn should_create_queue_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_reject_send_given_malformed_queue_route() {
        // Arrange
        let family = RouteFamily::new(1);
        let invalid_route = "queue://acme/jobs";
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let queue_sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Act
        queue_sink
            .deliver(Envelope::from_route(
                sender_address,
                queue_address,
                FrameContext::new(
                    7,
                    ChannelId::Pub,
                    MessageType::new(200),
                    encode_queue_send(invalid_route, b"email"),
                    family,
                ),
            ))
            .expect("reject malformed send");
        queue_sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let response_envelope = sender_mailbox
            .receiver()
            .try_recv()
            .expect("send response envelope");
        let response_frame = response_envelope
            .into_payload::<FrameContext>()
            .expect("send response frame");
        assert_eq!(response_frame.msg_type.as_u16(), 200);
        assert_eq!(
            bad_request_reason(&response_frame),
            "invalid queue route: queue://acme/jobs"
        );
        assert!(queue_sink.actors.lock().is_empty());
        assert!(admin_read_model.queues(None).is_empty());
    }

    #[test]
    fn should_reject_receive_given_malformed_queue_route() {
        // Arrange
        let family = RouteFamily::new(1);
        let invalid_route = "queue://acme/jobs";
        let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let client_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(client_address.clone(), client_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            client_address,
            queue_address,
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(invalid_route, 30, 1),
                family,
            ),
        ))
        .expect("reject malformed receive");
        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let response_envelope = client_mailbox
            .receiver()
            .try_recv()
            .expect("receive response envelope");
        let response_frame = response_envelope
            .into_payload::<FrameContext>()
            .expect("receive response frame");
        assert_eq!(response_frame.msg_type.as_u16(), 202);
        assert_eq!(
            bad_request_reason(&response_frame),
            "invalid queue route: queue://acme/jobs"
        );
        assert!(sink.actors.lock().is_empty());
        assert!(admin_read_model.queues(None).is_empty());
    }

    #[test]
    fn should_reject_extend_given_malformed_queue_route() {
        // Arrange
        let family = RouteFamily::new(1);
        let invalid_route = "queue://acme/jobs";
        let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let client_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(client_address.clone(), client_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            client_address,
            queue_address,
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(203),
                encode_queue_extend(invalid_route, 1, 99, 30),
                family,
            ),
        ))
        .expect("reject malformed extend");
        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let response_envelope = client_mailbox
            .receiver()
            .try_recv()
            .expect("extend response envelope");
        let response_frame = response_envelope
            .into_payload::<FrameContext>()
            .expect("extend response frame");
        assert_eq!(response_frame.msg_type.as_u16(), 203);
        assert_eq!(
            bad_request_reason(&response_frame),
            "invalid queue route: queue://acme/jobs"
        );
        assert!(sink.actors.lock().is_empty());
        assert!(admin_read_model.queues(None).is_empty());
    }

    #[test]
    fn should_reject_ack_given_malformed_queue_route() {
        // Arrange
        let family = RouteFamily::new(1);
        let invalid_route = "queue://acme/jobs";
        let client_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let client_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(client_address.clone(), client_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            client_address,
            queue_address,
            FrameContext::new(
                7,
                ChannelId::Pub,
                MessageType::new(204),
                encode_queue_ack(invalid_route, 1, 99),
                family,
            ),
        ))
        .expect("reject malformed ack");
        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let response_envelope = client_mailbox
            .receiver()
            .try_recv()
            .expect("ack response envelope");
        let response_frame = response_envelope
            .into_payload::<FrameContext>()
            .expect("ack response frame");
        assert_eq!(response_frame.msg_type.as_u16(), 204);
        assert_eq!(
            bad_request_reason(&response_frame),
            "invalid queue route: queue://acme/jobs"
        );
        assert!(sink.actors.lock().is_empty());
        assert!(admin_read_model.queues(None).is_empty());
    }

    #[test]
    fn should_notify_queue_watch_given_queue_send_when_queue_transitions_to_ready() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let queue_sink = Arc::new(new_queue_domain_sink(
            store.clone(),
            router.clone(),
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        ));
        let family = RouteFamily::new(1);
        let receiver_addr = RouteAddress::new(family, Route::new("inbox://session/1"));
        let sender_addr = RouteAddress::new(family, Route::new("inbox://session/2"));
        let queue_inbound_addr = RouteAddress::new(family, Route::new("queue://inbound"));
        let receiver_mailbox = Arc::new(Mailbox::new(8));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        router.register(receiver_addr.clone(), receiver_mailbox.clone());
        router.register(sender_addr.clone(), sender_mailbox.clone());
        router.register_domain_pattern("queue", queue_sink as Arc<dyn MailboxSink>);
        let route = "queue://realm/area/resource";
        let watch_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(207),
            encode_queue_watch("queue://realm/area/resource/ready"),
            family,
        );
        let watch_env =
            Envelope::from_route(receiver_addr.clone(), queue_inbound_addr.clone(), watch_ctx);
        let body: &[u8] = b"x";
        let mut send_payload = Vec::new();
        send_payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(route.as_bytes());
        send_payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(body);
        let send_ctx = FrameContext::new(
            2,
            ChannelId::Pub,
            MessageType::new(200),
            Bytes::from(send_payload),
            family,
        );
        let send_env = Envelope::from_route(sender_addr, queue_inbound_addr, send_ctx);

        // Act
        router.route(watch_env).expect("route queue watch");
        router.route(send_env).expect("route send");

        // Assert
        let watch_ack = receiver_mailbox
            .receiver()
            .try_recv()
            .expect("watch ack envelope")
            .into_payload::<FrameContext>()
            .expect("watch ack frame");
        assert_eq!(watch_ack.msg_type.as_u16(), 207);
        let subscription_id = watch_response_subscription_id(&watch_ack);

        let send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("send ack envelope")
            .into_payload::<FrameContext>()
            .expect("send ack frame");
        assert_eq!(send_ack.msg_type.as_u16(), 200);

        let notify_frame = receiver_mailbox
            .receiver()
            .try_recv()
            .expect("queue watch notify envelope")
            .into_payload::<FrameContext>()
            .expect("queue watch notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 209);
        let (delivered_subscription_id, delivered_route, ready_messages) =
            decode_queue_watch_delivery(&notify_frame);
        assert_eq!(delivered_subscription_id, subscription_id);
        assert_eq!(delivered_route, "queue://realm/area/resource/ready");
        assert_eq!(ready_messages, 1);
        assert!(receiver_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_register_queue_watch_given_watch_request() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let queue_route = "queue://acme/jobs/emails/ready";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            queue_address,
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(207),
                encode_route_pattern(queue_route),
                family,
            ),
        ))
        .expect("register queue watch path");

        // Assert
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("queue watch ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("queue watch ack frame");
        assert_eq!(subscribe_frame.msg_type.as_u16(), 207);
        assert!(watch_response_subscription_id(&subscribe_frame) > 0);
    }

    #[test]
    fn should_remove_queue_watch_given_unwatch_request() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let queue_route = "queue://acme/jobs/emails/ready";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/9"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(sender_address.clone(), sender_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(207),
                encode_queue_watch(queue_route),
                family,
            ),
        ))
        .expect("register queue watch path");
        let _ = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("watch ack envelope");

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            queue_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(208),
                encode_queue_unwatch(queue_route),
                family,
            ),
        ))
        .expect("remove queue watch path");

        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address,
            FrameContext::new(
                9,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send("queue://acme/jobs/emails", b"email"),
                family,
            ),
        ))
        .expect("enqueue watched queue message");

        // Assert
        let unsubscribe_ack_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_ack_frame = unsubscribe_ack_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        assert_eq!(unsubscribe_ack_frame.msg_type.as_u16(), 208);
        assert_eq!(
            unsubscribe_ack_frame.payload,
            bytes::Bytes::from_static(&[0])
        );
        let _ = sender_mailbox
            .receiver()
            .try_recv()
            .expect("send ack envelope");
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
        assert!(sink.families.lock().is_empty());
    }

    #[test]
    fn should_cleanup_expired_queue_dedup_entries_during_runtime_sweep() {
        // Arrange
        let family = RouteFamily::new(1);
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let dedup_store = Arc::new(crate::utils::idempotency::DedupStore::new(
            Duration::from_millis(1),
        ));
        let sink = QueueDomainSink::new(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
            dedup_store.clone(),
        );
        let dedup_key = crate::utils::idempotency::DedupKey {
            realm: "acme".to_string(),
            domain: crate::utils::idempotency::Domain::Queue,
            identifier: crate::utils::idempotency::DedupIdentifier::QueueComplete {
                family: family.as_u64(),
                area: "jobs".to_string(),
                resource: "emails".to_string(),
                message_id: 1,
                token: 99,
            },
        };
        dedup_store.record(dedup_key, vec![1, 2, 3]);
        std::thread::sleep(Duration::from_millis(5));

        let now = Instant::now();
        *sink.next_dedup_sweep_at.lock() = now;

        // Act
        sink.sweep_runtime_state_at(now);

        // Assert
        assert!(dedup_store.is_empty());
    }

    #[test]
    fn should_refresh_queue_admin_snapshot_with_live_queue_state() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let worker_session_id = 8;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue response");

        sink.deliver(Envelope::from_route(
            worker_address,
            queue_address,
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message");
        let reserve_envelope = worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response");
        let reserve_frame = reserve_envelope
            .into_payload::<FrameContext>()
            .expect("reserve response frame");
        assert_eq!(reserve_frame.msg_type.as_u16(), 202);
        assert_eq!(receive_response_message_count(&reserve_frame), 1);

        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let queues = admin_read_model.queues(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].realm, "acme");
        assert_eq!(queues[0].area, "jobs");
        assert_eq!(queues[0].resource, "emails");
        assert_eq!(queues[0].messages_ready, 0);
        assert_eq!(queues[0].messages_delayed, 0);
        assert_eq!(queues[0].messages_inflight, 1);
        assert_eq!(queues[0].messages_dead_lettered, 0);
        assert_eq!(queues[0].messages_total, 1);

        let inflight = admin_read_model.queue_inflight(None);
        assert_eq!(inflight.len(), 1);
        assert_eq!(inflight[0].realm, "acme");
        assert_eq!(inflight[0].area, "jobs");
        assert_eq!(inflight[0].resource, "emails");
        assert_eq!(inflight[0].message_id, 1);
        assert_eq!(inflight[0].session_id, worker_session_id.to_string());
        assert_eq!(inflight[0].attempts, 1);
        assert!(!inflight[0].expires_at.is_empty());
    }

    #[test]
    fn should_cleanup_queue_inflight_for_disconnected_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let worker_session_id = 8;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue response");

        sink.deliver(Envelope::from_route(
            worker_address,
            queue_address.clone(),
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message");
        let _reserve_ack = worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response");

        sink.refresh_admin_snapshot_if_dirty();
        assert_eq!(admin_read_model.queue_inflight(None).len(), 1);

        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("queue://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: worker_session_id,
            },
        ))
        .expect("cleanup queue session");

        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let queues = admin_read_model.queues(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].messages_ready, 1);
        assert_eq!(queues[0].messages_delayed, 0);
        assert_eq!(queues[0].messages_inflight, 0);
        assert_eq!(queues[0].messages_dead_lettered, 0);
        assert_eq!(queues[0].messages_total, 1);
        assert!(admin_read_model.queue_inflight(None).is_empty());
    }

    #[test]
    fn should_include_delayed_messages_in_queue_admin_snapshot() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
        );

        // Act
        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address,
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send_with_delay(queue_route, b"email", 60),
                family,
            ),
        ))
        .expect("enqueue delayed queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue delayed response");
        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        let queues = admin_read_model.queues(None);
        assert_eq!(queues.len(), 1);
        assert_eq!(queues[0].messages_ready, 0);
        assert_eq!(queues[0].messages_delayed, 1);
        assert_eq!(queues[0].messages_inflight, 0);
        assert_eq!(queues[0].messages_dead_lettered, 0);
        assert_eq!(queues[0].messages_total, 1);
    }

    #[test]
    fn should_evict_idle_queue_actor_without_losing_committed_state() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let worker_session_id = 8;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model.clone(),
            cntryl_midge::WriteOptions::buffered(),
        );

        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue response");
        assert_eq!(sink.actors.lock().len(), 1);

        // Act
        force_actor_idle(&sink, queue_route, family);
        sink.refresh_admin_snapshot_if_dirty();
        assert!(
            sink.actors.lock().is_empty(),
            "idle actor should be evicted"
        );
        assert!(
            admin_read_model.queues(None).is_empty(),
            "cold queue should disappear from warm admin snapshot"
        );

        sink.deliver(Envelope::from_route(
            worker_address,
            queue_address,
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message after eviction");
        let reserve_envelope = worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response after eviction");
        let reserve_frame = reserve_envelope
            .into_payload::<FrameContext>()
            .expect("reserve response frame after eviction");
        assert_eq!(receive_response_message_count(&reserve_frame), 1);

        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        assert_eq!(sink.actors.lock().len(), 1);
        assert_eq!(admin_read_model.queues(None)[0].messages_inflight, 1);
    }

    #[test]
    fn should_not_evict_idle_queue_actor_with_live_inflight() {
        // Arrange
        let family = RouteFamily::new(1);
        let sender_session_id = 7;
        let worker_session_id = 8;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let worker_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let worker_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(sender_address.clone(), sender_mailbox.clone());
        router.register(worker_address.clone(), worker_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = new_queue_domain_sink(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::buffered(),
        );

        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");
        let _send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("enqueue response");

        sink.deliver(Envelope::from_route(
            worker_address,
            queue_address,
            FrameContext::new(
                worker_session_id,
                ChannelId::Pub,
                MessageType::new(202),
                encode_queue_reserve(queue_route, 30, 1),
                family,
            ),
        ))
        .expect("reserve queue message");
        let _reserve_ack = worker_mailbox
            .receiver()
            .try_recv()
            .expect("reserve response");

        // Act
        force_actor_idle(&sink, queue_route, family);
        sink.refresh_admin_snapshot_if_dirty();

        // Assert
        assert_eq!(
            sink.actors.lock().len(),
            1,
            "actors with live inflight entries must stay warm until the inflight entry is gone"
        );
    }
}
