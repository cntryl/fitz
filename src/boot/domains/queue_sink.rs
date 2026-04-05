use super::queue_waiters::{PendingReceive, QueueWaiterRegistry};
use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use crate::domains::queue::{
    projection::{QueueAdminProjection, QueueProjectionEntry, QueueProjectionState},
    QueueMetrics,
};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

struct WarmQueueActor {
    actor: Arc<Mutex<crate::domains::queue::QueueActor>>,
    last_used: Instant,
}

const QUEUE_ACTOR_IDLE_TTL: Duration = Duration::from_secs(5 * 60);
const QUEUE_IDLE_SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// Queue domain sink with per-queue QueueActor instances
///
/// This sink:
/// - Maintains per-queue QueueActor instances keyed by QueueKey
/// - Parses TLV frames to QueueMessage
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks queue-local reserve waiters for the current broker process
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
    /// Queue-local reserve waiter registry scoped to this broker process.
    waiters: QueueWaiterRegistry,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    projection: QueueAdminProjection,
    metrics: Option<QueueMetrics>,
    active: AtomicBool,
    next_idle_sweep_at: Mutex<Instant>,
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
            waiters: QueueWaiterRegistry::new(),
            router,
            projection: QueueAdminProjection::new(admin_read_model),
            metrics: None,
            active: AtomicBool::new(true),
            next_idle_sweep_at: Mutex::new(Instant::now()),
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

    pub fn start_wait_loop(self: &Arc<Self>) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::debug!("Queue wait loop not started: no Tokio runtime available");
            return;
        };

        let weak = Arc::downgrade(self);
        handle.spawn(async move {
            let mut interval = tokio::time::interval(crate::domains::queue::WAIT_SWEEP_INTERVAL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                interval.tick().await;

                let Some(sink) = weak.upgrade() else {
                    break;
                };
                if !sink.active.load(Ordering::Relaxed) {
                    break;
                }

                sink.sweep_waiters_and_runtime_state();
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

    fn parse_frame_to_queue_message(
        frame_ctx: &FrameContext,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<crate::domains::queue::QueueMessage, crate::domains::queue::QueueResponse> {
        let msg_type = frame_ctx.msg_type.as_u16();

        if matches!(
            msg_type,
            crate::protocol::queue_codec::msg_type::ENQUEUE
                | crate::protocol::queue_codec::msg_type::RESERVE
                | crate::protocol::queue_codec::msg_type::EXTEND
                | crate::protocol::queue_codec::msg_type::COMPLETE
        ) {
            return crate::protocol::queue_codec::parse_request(
                msg_type,
                route_family,
                &frame_ctx.payload,
            )
            .map_err(|reason| crate::domains::queue::QueueResponse::BadRequest { reason });
        }

        let reason = match msg_type {
            207 => {
                "queue subscribe is not supported; use reserve wait_seconds on the queue route instead"
                    .to_string()
            }
            208 => {
                "queue unsubscribe is not supported; queued waits are scoped to the original reserve request"
                    .to_string()
            }
            209 => "QUEUE_NOTIFY is not supported by the queue domain".to_string(),
            _ => format!("unsupported queue message type: {}", msg_type),
        };

        Err(crate::domains::queue::QueueResponse::BadRequest { reason })
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

    fn route_waiter_response(
        &self,
        waiter: &PendingReceive,
        response: &crate::domains::queue::QueueResponse,
    ) {
        let response_ctx = FrameContext::new(
            waiter.session_id,
            waiter.channel_id,
            crate::protocol::tlv::MessageType::new(waiter.msg_type),
            bytes::Bytes::from(crate::protocol::queue_codec::encode_response(response)),
            waiter.route_family,
        );
        let response_envelope = Envelope::from_route(
            waiter.reply_source.clone(),
            waiter.reply_destination.clone(),
            response_ctx,
        );

        if let Err(error) = self.router.route(response_envelope) {
            tracing::warn!(
                domain = "queue",
                session = waiter.session_id,
                error = ?error,
                "Failed to route deferred queue receive response"
            );
            if let Some(metrics) = &self.metrics {
                metrics.record_failure(waiter.requested_at);
            }
        } else if let Some(metrics) = &self.metrics {
            if Self::queue_response_is_failure(response) {
                metrics.record_failure(waiter.requested_at);
            } else {
                metrics.record_success(waiter.requested_at);
            }
        }
    }

    fn try_receive_for_waiter(
        &self,
        key: &crate::domains::queue::QueueKey,
        waiter: &PendingReceive,
    ) -> Option<crate::domains::queue::QueueResponse> {
        let actor_handle = {
            let mut actors = self.actors.lock();
            let warm_actor = actors.get_mut(key)?;
            warm_actor.last_used = Instant::now();
            warm_actor.actor.clone()
        };

        let mut actor = actor_handle.lock();
        actor.process_due_work();
        let response =
            actor.handle_receive_for_session(waiter.session_id, waiter.lease_seconds, waiter.batch_size);
        match &response {
            crate::domains::queue::QueueResponse::Received { messages }
                if messages.is_empty() => None,
            _ => Some(response),
        }
    }

    fn grant_waiters_for_key(&self, key: &crate::domains::queue::QueueKey, now: Instant) {
        let expired_waiters = self.waiters.expire_timed_out_for_key(key, now);
        for waiter in expired_waiters {
            self.route_waiter_response(
                &waiter,
                &crate::domains::queue::QueueResponse::Received { messages: vec![] },
            );
        }

        loop {
            let waiter = self.waiters.pop_next_for_key(key);
            let Some(waiter) = waiter else {
                break;
            };

            if let Some(response) = self.try_receive_for_waiter(key, &waiter) {
                self.waiters.complete(key, &waiter);
                self.route_waiter_response(&waiter, &response);
                self.mark_admin_snapshot_dirty();
            } else {
                self.waiters.requeue_front(key, waiter);
                break;
            }
        }
    }

    fn sweep_waiters_and_runtime_state(&self) {
        let now = Instant::now();
        self.sweep_idle_actors_at(now);
        let keys = self.waiters.keys();
        for key in keys {
            self.grant_waiters_for_key(&key, now);
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
            metrics.set_inflight_messages(self.active_lease_count());
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
                | crate::domains::queue::QueueResponse::LeaseExpired
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
                    leases: actor.admin_leases(),
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
        let mut actors = self.actors.lock();

        actors.retain(|_, warm_actor| {
            let mut actor = warm_actor.actor.lock();
            let ready_before = actor.ready_len();
            let leases_before = actor.inflight.len();
            actor.process_due_work();
            let ready_after = actor.ready_len();
            let leases_after = actor.inflight.len();
            if ready_before != ready_after || leases_before != leases_after {
                changed = true;
            }

            let idle_for = now.saturating_duration_since(warm_actor.last_used);
            let should_keep = idle_for < QUEUE_ACTOR_IDLE_TTL || !actor.inflight.is_empty();
            if !should_keep {
                changed = true;
            }
            should_keep
        });

        drop(actors);
        if changed {
            self.mark_admin_snapshot_dirty();
        }
    }

    /// Drop all live queue leases owned by the disconnected session and return
    /// those committed messages to the ready queue. Lease ownership is
    /// broker-local runtime state only.
    pub fn cleanup_session(&self, session_id: u64) {
        let mut released_any = false;
        let mut released_keys = Vec::new();
        let mut actors = self.actors.lock();
        for (key, warm_actor) in actors.iter_mut() {
            let mut actor = warm_actor.actor.lock();
            if actor.cleanup_session_leases(session_id) > 0 {
                released_any = true;
                released_keys.push(key.clone());
            }
        }
        drop(actors);

        let removed_waiters = self.waiters.remove_session_waiters(session_id);

        if released_any {
            self.mark_admin_snapshot_dirty();
        }

        let now = Instant::now();
        for key in released_keys {
            self.grant_waiters_for_key(&key, now);
        }

        tracing::debug!(
            domain = "queue",
            session = session_id,
            waiters_removed = removed_waiters,
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

    pub fn active_lease_count(&self) -> usize {
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
            self.mark_admin_snapshot_dirty();
            self.grant_waiters_for_key(&key, Instant::now());
        }

        if created_actor {
            let should_remove = {
                let actor = actor_handle.lock();
                actor.admin_snapshot().messages_total == 0 && actor.inflight.is_empty()
            };
            if should_remove {
                self.actors.lock().remove(&key);
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
        let request_started = self.metrics.as_ref().map(|metrics| metrics.record_request_start());

        let queue_msg = match Self::parse_frame_to_queue_message(&frame_ctx, route_family) {
            Ok(msg) => msg,
            Err(response) => {
                self.route_queue_response(&envelope, &frame_ctx, &response);
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
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

        use crate::domains::queue::protocol::QueueMessage;

        let (response, wake_waiters_for_key, deferred_wait_for_key, should_mark_admin_snapshot_dirty) =
            match queue_msg {
                QueueMessage::Send {
                    family_id,
                    route,
                    body,
                    delay_seconds,
                } => match Self::queue_key_for_route(family_id, &route) {
                    Ok(key) => {
                        let wake_key = key.clone();
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
                        self.observe_histogram_us(
                            obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                            actor_exec_start.elapsed().as_micros() as u64,
                        );
                        let wake_waiters = actor.take_needs_wake_waiters().then_some(wake_key);
                        let _ = created_actor;
                        (resp, wake_waiters, None, true)
                    }
                    Err(response) => (response, None, None, false),
                }
                QueueMessage::Receive {
                    family_id,
                    route,
                    lease_seconds,
                    batch_size,
                    wait_seconds,
                } => match Self::queue_key_for_route(family_id, &route) {
                    Ok(key) => {
                        let wait_seconds = wait_seconds.unwrap_or(0);
                        let wait_key = key.clone();
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
                            lease_seconds,
                            batch_size,
                        );
                        self.observe_histogram_us(
                            obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                            actor_exec_start.elapsed().as_micros() as u64,
                        );
                        let _ = created_actor;

                        let should_defer = matches!(
                            &response,
                            crate::domains::queue::QueueResponse::Received { messages }
                                if messages.is_empty() && wait_seconds > 0
                        );

                        if should_defer {
                            match self.waiters.enqueue(
                                &wait_key,
                                &envelope,
                                &frame_ctx,
                                lease_seconds,
                                batch_size,
                                wait_seconds,
                            ) {
                                Ok(()) => (response, None, Some(wait_key), true),
                                Err(wait_error) => (wait_error, None, None, true),
                            }
                        } else {
                            (response, None, None, true)
                        }
                    }
                    Err(response) => (response, None, None, false),
                }
                QueueMessage::Extend {
                    family_id,
                    route,
                    id,
                    token,
                    lease_seconds,
                } => match Self::queue_key_for_route(family_id, &route) {
                    Ok(key) => {
                        let actor_lock_start = Instant::now();
                        let (actor_handle, created_actor) = self.get_or_create_actor(key);
                        self.observe_histogram_us(
                            obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                            actor_lock_start.elapsed().as_micros() as u64,
                        );
                        let mut actor = actor_handle.lock();
                        let actor_exec_start = Instant::now();
                        actor.process_due_work();
                        let response = actor.handle_extend(id, token, lease_seconds);
                        self.observe_histogram_us(
                            obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                            actor_exec_start.elapsed().as_micros() as u64,
                        );
                        let _ = created_actor;
                        (response, None, None, true)
                    }
                    Err(response) => (response, None, None, false),
                }
                QueueMessage::Ack {
                    family_id,
                    route,
                    id,
                    token,
                } => match Self::queue_key_for_route(family_id, &route) {
                    Ok(key) => {
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
                        self.observe_histogram_us(
                            obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                            actor_exec_start.elapsed().as_micros() as u64,
                        );
                        let _ = created_actor;
                        (response, None, None, true)
                    }
                    Err(response) => (response, None, None, false),
                }
                QueueMessage::LeaseExpired { .. } => (
                    crate::domains::queue::QueueResponse::Error {
                        message: "LeaseExpired is an internal message".to_string(),
                    },
                    None,
                    None,
                    false,
                ),
            };
        if should_mark_admin_snapshot_dirty {
            self.mark_admin_snapshot_dirty();
        }

        if let Some(key) = wake_waiters_for_key {
            self.grant_waiters_for_key(&key, Instant::now());
        }

        if deferred_wait_for_key.is_some() {
            return Ok(());
        }

        self.route_queue_response(&envelope, &frame_ctx, &response);

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::queue_response_is_failure(&response) {
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

    fn encode_queue_reserve(route: &str, lease_seconds: u64, batch_size: u32) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(lease_seconds);
        payload.put_u8(1);
        payload.put_u32(batch_size);
        payload.put_u8(0);
        Bytes::from(payload)
    }

    fn encode_queue_reserve_with_wait(
        route: &str,
        lease_seconds: u64,
        batch_size: u32,
        wait_seconds: u64,
    ) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(lease_seconds);
        payload.put_u8(1);
        payload.put_u32(batch_size);
        payload.put_u8(1);
        payload.put_u64(wait_seconds);
        Bytes::from(payload)
    }

    fn encode_queue_extend(route: &str, id: u64, token: u64, lease_seconds: u64) -> Bytes {
        let mut payload = Vec::new();
        payload.put_u32(route.len() as u32);
        payload.put_slice(route.as_bytes());
        payload.put_u64(id);
        payload.put_u64(token);
        payload.put_u64(lease_seconds);
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
        assert_eq!(bad_request_reason(&response_frame), "invalid queue route: queue://acme/jobs");
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
        assert_eq!(bad_request_reason(&response_frame), "invalid queue route: queue://acme/jobs");
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
        assert_eq!(bad_request_reason(&response_frame), "invalid queue route: queue://acme/jobs");
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
        assert_eq!(bad_request_reason(&response_frame), "invalid queue route: queue://acme/jobs");
        assert!(sink.actors.lock().is_empty());
        assert!(admin_read_model.queues(None).is_empty());
    }

    #[test]
    fn should_resume_waiting_receive_given_queue_send_when_queue_empty() {
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
        let wait_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(202),
            encode_queue_reserve_with_wait(route, 30, 1, 5),
            family,
        );
        let wait_env =
            Envelope::from_route(receiver_addr.clone(), queue_inbound_addr.clone(), wait_ctx);
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
        router.route(wait_env).expect("route waiting receive");
        assert!(receiver_mailbox.receiver().try_recv().is_err());
        router.route(send_env).expect("route send");

        // Assert
        let send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("send ack envelope")
            .into_payload::<FrameContext>()
            .expect("send ack frame");
        assert_eq!(send_ack.msg_type.as_u16(), 200);

        let wait_response = receiver_mailbox
            .receiver()
            .try_recv()
            .expect("waiting receive response envelope")
            .into_payload::<FrameContext>()
            .expect("waiting receive response frame");
        assert_eq!(wait_response.msg_type.as_u16(), 202);
        assert_eq!(receive_response_message_count(&wait_response), 1);
        assert!(receiver_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_reject_legacy_queue_subscribe_given_removed_queue_subscription_path() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let queue_route = "queue://acme/jobs/emails";
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
        .expect("reject removed queue subscribe path");

        // Assert
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("queue subscribe rejection envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("queue subscribe rejection frame");
        assert_eq!(subscribe_frame.msg_type.as_u16(), 207);
        assert_eq!(
            bad_request_reason(&subscribe_frame),
            "queue subscribe is not supported; use reserve wait_seconds on the queue route instead"
        );
    }

    #[test]
    fn should_reject_legacy_queue_unsubscribe_given_removed_queue_subscription_path() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
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
                MessageType::new(208),
                encode_route_pattern(queue_route),
                family,
            ),
        ))
        .expect("reject removed queue unsubscribe path");

        // Assert
        let unsubscribe_ack_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe rejection envelope");
        let unsubscribe_ack_frame = unsubscribe_ack_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe rejection frame");
        assert_eq!(unsubscribe_ack_frame.msg_type.as_u16(), 208);
        assert_eq!(
            bad_request_reason(&unsubscribe_ack_frame),
            "queue unsubscribe is not supported; queued waits are scoped to the original reserve request"
        );
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
        assert_eq!(queues[0].messages_leased, 1);
        assert_eq!(queues[0].messages_dead_lettered, 0);
        assert_eq!(queues[0].messages_total, 1);

        let leases = admin_read_model.queue_leases(None);
        assert_eq!(leases.len(), 1);
        assert_eq!(leases[0].realm, "acme");
        assert_eq!(leases[0].area, "jobs");
        assert_eq!(leases[0].resource, "emails");
        assert_eq!(leases[0].message_id, 1);
        assert_eq!(leases[0].session_id, worker_session_id.to_string());
        assert_eq!(leases[0].attempts, 1);
        assert!(!leases[0].expires_at.is_empty());
    }

    #[test]
    fn should_cleanup_queue_leases_for_disconnected_session() {
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
        assert_eq!(admin_read_model.queue_leases(None).len(), 1);

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
        assert_eq!(queues[0].messages_leased, 0);
        assert_eq!(queues[0].messages_dead_lettered, 0);
        assert_eq!(queues[0].messages_total, 1);
        assert!(admin_read_model.queue_leases(None).is_empty());
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
        assert_eq!(queues[0].messages_leased, 0);
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
        assert_eq!(admin_read_model.queues(None)[0].messages_leased, 1);
    }

    #[test]
    fn should_not_evict_idle_queue_actor_with_live_leases() {
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
            "actors with live leases must stay warm until the lease is gone"
        );
    }
}
