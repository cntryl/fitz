use super::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::observability as obs;
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// Queue subscription (for availability notifications)
struct QueueSubscription {
    /// Pattern to match (e.g., "queue://realm/area/resource")
    pattern: crate::runtime::matcher::Pattern,
    /// Session ID
    session_id: u64,
    /// Subscriber route address
    subscriber: crate::runtime::routing::RouteAddress,
    /// Subscription ID
    subscription_id: u64,
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

/// Queue domain sink with per-queue QueueActor instances
///
/// This sink:
/// - Maintains per-queue QueueActor instances keyed by QueueKey
/// - Parses TLV frames to QueueMessage
/// - Dispatches to the correct actor based on route
/// - Returns responses
/// - Tracks subscriptions for availability notifications (empty->non-empty transitions)
pub struct QueueDomainSink {
    /// Midge storage engine
    store: Arc<cntryl_midge::Engine>,
    /// Commit policy for queue persistence on this runtime.
    queue_write_options: cntryl_midge::WriteOptions,
    /// Per-queue actors keyed by QueueKey
    actors: Mutex<
        HashMap<crate::domains::queue::QueueKey, Arc<Mutex<crate::domains::queue::QueueActor>>>,
    >,
    /// Per-family subscription state for queue availability notifications
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<QueueSubscription>>>,
    /// Monotonic subscription ID counter
    next_sub_id: AtomicU64,
    /// Total active queue subscriptions across all families.
    subscription_count: AtomicUsize,
    /// Router for routing response envelopes back
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    active: AtomicBool,
}

impl QueueDomainSink {
    pub fn new(
        store: Arc<cntryl_midge::Engine>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
        queue_write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        Self {
            store,
            queue_write_options,
            actors: Mutex::new(HashMap::new()),
            families: Mutex::new(HashMap::new()),
            next_sub_id: AtomicU64::new(1),
            subscription_count: AtomicUsize::new(0),
            router,
            admin_read_model,
            active: AtomicBool::new(true),
        }
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

    fn queue_key_for_route(
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
    ) -> crate::domains::queue::QueueKey {
        crate::domains::queue::QueueKey::from_route(family_id, route).unwrap_or(
            crate::domains::queue::QueueKey {
                family: family_id,
                realm: String::new(),
                area: String::new(),
                resource: "default".to_string(),
            },
        )
    }

    fn get_or_create_actor(
        &self,
        key: crate::domains::queue::QueueKey,
    ) -> (Arc<Mutex<crate::domains::queue::QueueActor>>, bool) {
        use std::collections::hash_map::Entry;

        let mut actors = self.actors.lock();
        match actors.entry(key.clone()) {
            Entry::Occupied(entry) => (entry.get().clone(), false),
            Entry::Vacant(entry) => {
                let actor = Arc::new(Mutex::new(
                    crate::domains::queue::QueueActor::new_with_write_options(
                        key.family,
                        key,
                        self.store.clone(),
                        None,
                        crate::utils::idempotency::global_dedup_store(),
                        self.queue_write_options,
                    ),
                ));
                entry.insert(actor.clone());
                (actor, true)
            }
        }
    }

    fn sync_admin_snapshot(&self) {
        let queues = self
            .actors
            .lock()
            .keys()
            .map(|key| crate::api::admin::QueueInfo {
                realm: key.realm.clone(),
                area: key.area.clone(),
                resource: key.resource.clone(),
                messages_ready: 0,
                messages_leased: 0,
                messages_total: 0,
                oldest_message_age_seconds: 0,
            })
            .collect();
        self.admin_read_model.replace_queues(queues);
    }

    /// Handle a DomainPublishEvent from queue actors.
    /// Matches the event route against subscription patterns and fans out
    /// QUEUE_NOTIFY (209) frames to matching subscribers.
    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        let family_id = event.family_id.as_u64();
        tracing::info!(
            domain = "queue",
            family_id = family_id,
            route = %event.route,
            "Queue: handle_domain_publish called (ENTRY)"
        );
        let families = self.families.lock();
        if let Some(state) = families.get(&family_id) {
            tracing::info!(
                domain = "queue",
                family_id = family_id,
                subscription_count = state.subscription_count(),
                "Queue: found family state with subscriptions"
            );
            let matched = state.for_each_matching(event, |subscription| {
                self.route_availability_notify(subscription, event);
            });
            if matched == 0 {
                tracing::debug!(
                    domain = "queue",
                    family_id = family_id,
                    route = %event.route,
                    subscription_count = state.subscription_count(),
                    "Queue: no subscription matched event route"
                );
            }
        } else {
            tracing::debug!(
                domain = "queue",
                family_id = family_id,
                route = %event.route,
                "Queue: no family state for event (no subscriptions in this family)"
            );
        }
        Ok(())
    }

    fn route_availability_notify(
        &self,
        subscription: &QueueSubscription,
        event: &crate::runtime::DomainPublishEvent,
    ) {
        let notify_payload = crate::protocol::queue_codec::encode_notify(
            subscription.subscription_id,
            &event.route,
            &event.payload,
        );
        let notify_ctx = FrameContext::new(
            subscription.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(209),
            bytes::Bytes::from(notify_payload),
            crate::runtime::routing::RouteFamily::from_u32(subscription.subscriber.family().id()),
        );
        let notify_envelope = Envelope::new(subscription.subscriber.clone(), notify_ctx);
        if let Err(error) = self.router.route(notify_envelope) {
            tracing::warn!(
                domain = "queue",
                destination = %subscription.subscriber,
                error = ?error,
                "Queue: failed to route 209 to subscriber inbox"
            );
        } else {
            tracing::debug!(
                domain = "queue",
                session_id = subscription.session_id,
                destination = %subscription.subscriber,
                "Queue: routed 209 to subscriber"
            );
        }
    }

    /// Remove all subscriptions for a given session (called on disconnect cleanup).
    pub fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.families.lock();
        let mut removed_count = 0_usize;
        for (family_id, state) in families.iter_mut() {
            removed_count += state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
                session_id,
            );
        }
        if removed_count > 0 {
            self.subscription_count
                .fetch_sub(removed_count, Ordering::Relaxed);
        }
        tracing::debug!(
            domain = "queue",
            session = session_id,
            "All queue subscriptions removed for session"
        );
    }

    /// Get the total number of active queue subscriptions (for stats).
    pub fn subscription_count(&self) -> usize {
        self.subscription_count.load(Ordering::Relaxed)
    }

    pub fn pending_message_count(&self) -> usize {
        let actors = self.actors.lock();
        actors.values().map(|actor| actor.lock().ready_len()).sum()
    }

    pub fn active_lease_count(&self) -> usize {
        let actors = self.actors.lock();
        actors
            .values()
            .map(|actor| actor.lock().inflight.len())
            .sum()
    }
}

impl MailboxSink for QueueDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all(cleanup.session_id);
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

        let queue_msg = {
            let mt = frame_ctx.msg_type.as_u16();
            if mt == crate::protocol::queue_codec::msg_type::SUBSCRIBE {
                let subscriber = if let Some(src) = envelope.source() {
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        source = %src,
                        "Queue SUBSCRIBE: using envelope source as subscriber"
                    );
                    src.clone()
                } else {
                    let session_inbox = crate::runtime::routing::session_inbox_address(
                        route_family,
                        frame_ctx.session_id,
                    );
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        subscriber = %session_inbox,
                        "Queue SUBSCRIBE: using session inbox as subscriber (no envelope source)"
                    );
                    session_inbox
                };
                match crate::protocol::queue_codec::parse_subscribe(
                    route_family,
                    &frame_ctx.payload,
                    frame_ctx.session_id,
                    subscriber,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = mt,
                            error = %e,
                            "Failed to parse Queue SUBSCRIBE"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            } else if mt == crate::protocol::queue_codec::msg_type::UNSUBSCRIBE {
                let subscriber = if let Some(src) = envelope.source() {
                    src.clone()
                } else {
                    crate::runtime::routing::session_inbox_address(
                        route_family,
                        frame_ctx.session_id,
                    )
                };
                match crate::protocol::queue_codec::parse_unsubscribe(
                    route_family,
                    &frame_ctx.payload,
                    frame_ctx.session_id,
                    subscriber,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = mt,
                            error = %e,
                            "Failed to parse Queue UNSUBSCRIBE"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            } else {
                match crate::protocol::queue_codec::parse_request(
                    frame_ctx.msg_type.as_u16(),
                    route_family,
                    &frame_ctx.payload,
                ) {
                    Ok(msg) => msg,
                    Err(e) => {
                        tracing::warn!(
                            domain = "queue",
                            session = frame_ctx.session_id,
                            msg_type = frame_ctx.msg_type.as_u16(),
                            error = %e,
                            "Failed to parse Queue message"
                        );
                        return Err(DeliveryError::ActorStopped);
                    }
                }
            }
        };

        tracing::debug!(
            domain = "queue",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            "Parsed Queue message successfully"
        );

        use crate::domains::queue::protocol::QueueMessage;

        let (response, availability_notify_route, should_sync_admin_snapshot) = match queue_msg {
            QueueMessage::Send {
                family_id,
                route,
                body,
                delay_seconds,
            } => {
                let key = Self::queue_key_for_route(family_id, &route);
                let actor_lock_start = Instant::now();
                let (actor_handle, created_actor) = self.get_or_create_actor(key);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                    actor_lock_start.elapsed().as_micros() as u64,
                );
                let mut actor = actor_handle.lock();
                let actor_exec_start = Instant::now();
                actor.process_due_work();
                let resp = actor.handle_send(body, delay_seconds);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                    actor_exec_start.elapsed().as_micros() as u64,
                );
                let notify_route = if actor.take_needs_notify_availability() {
                    tracing::info!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        route = %route,
                        family_id = %family_id,
                        "Queue: SEND triggered availability notification"
                    );
                    Some(route)
                } else {
                    None
                };
                (resp, notify_route, created_actor)
            }
            QueueMessage::Receive {
                family_id,
                route,
                lease_seconds,
                batch_size,
                ..
            } => {
                let key = Self::queue_key_for_route(family_id, &route);
                let actor_lock_start = Instant::now();
                let (actor_handle, created_actor) = self.get_or_create_actor(key);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                    actor_lock_start.elapsed().as_micros() as u64,
                );
                let mut actor = actor_handle.lock();
                let actor_exec_start = Instant::now();
                actor.process_due_work();
                let response = actor.handle_receive(lease_seconds, batch_size);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                    actor_exec_start.elapsed().as_micros() as u64,
                );
                (response, None, created_actor)
            }
            QueueMessage::Extend {
                family_id,
                route,
                id,
                token,
                lease_seconds,
            } => {
                let key = Self::queue_key_for_route(family_id, &route);
                let actor_lock_start = Instant::now();
                let (actor_handle, created_actor) = self.get_or_create_actor(key);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                    actor_lock_start.elapsed().as_micros() as u64,
                );
                let mut actor = actor_handle.lock();
                let actor_exec_start = Instant::now();
                actor.process_due_work();
                let response = actor.handle_extend(id, token, lease_seconds);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                    actor_exec_start.elapsed().as_micros() as u64,
                );
                (response, None, created_actor)
            }
            QueueMessage::Ack {
                family_id,
                route,
                id,
                token,
            } => {
                let key = Self::queue_key_for_route(family_id, &route);
                let actor_lock_start = Instant::now();
                let (actor_handle, created_actor) = self.get_or_create_actor(key);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_LOCK_HOLD_LATENCY,
                    actor_lock_start.elapsed().as_micros() as u64,
                );
                let mut actor = actor_handle.lock();
                let actor_exec_start = Instant::now();
                actor.process_due_work();
                let response = actor.handle_ack(id, token);
                crate::boot::observability::histogram_observe_us(
                    obs::METRIC_QUEUE_ACTOR_EXECUTION_LATENCY,
                    actor_exec_start.elapsed().as_micros() as u64,
                );
                (response, None, created_actor)
            }
            QueueMessage::LeaseExpired { .. } => (
                crate::domains::queue::QueueResponse::Error {
                    message: "LeaseExpired is an internal message".to_string(),
                },
                None,
                false,
            ),
            QueueMessage::Subscribe {
                family_id,
                pattern,
                session_id,
                subscriber,
            } => {
                let fam_id = family_id.as_u64();

                let mut families = self.families.lock();
                let state = families
                    .entry(fam_id)
                    .or_insert_with(RoutedSubscriptionSet::new);

                let existing_sub_id = state.find_existing_id(session_id, pattern.as_str());

                let sub_id = if let Some(id) = existing_sub_id {
                    tracing::debug!(
                        domain = "queue",
                        session = session_id,
                        subscription_id = id,
                        pattern = pattern.as_str(),
                        "Queue subscription already exists (idempotent)"
                    );
                    id
                } else {
                    let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
                    state.insert(
                        family_id,
                        QueueSubscription {
                            pattern: crate::runtime::matcher::Pattern::new(pattern.as_str()),
                            session_id,
                            subscription_id: new_id,
                            subscriber,
                        },
                    );

                    tracing::debug!(
                        domain = "queue",
                        session = session_id,
                        subscription_id = new_id,
                        pattern = pattern.as_str(),
                        "Queue subscription added"
                    );
                    self.subscription_count.fetch_add(1, Ordering::Relaxed);
                    new_id
                };

                (
                    crate::domains::queue::QueueResponse::SubscribeOk {
                        subscription_id: sub_id,
                    },
                    None,
                    false,
                )
            }
            QueueMessage::Unsubscribe {
                family_id,
                pattern,
                session_id,
                ..
            } => {
                let fam_id = family_id.as_u64();
                let mut families = self.families.lock();
                if let Some(state) = families.get_mut(&fam_id) {
                    let removed_count =
                        state.remove_session_pattern(family_id, session_id, pattern.as_str());
                    if removed_count > 0 {
                        self.subscription_count
                            .fetch_sub(removed_count, Ordering::Relaxed);
                    }
                }

                tracing::debug!(
                    domain = "queue",
                    session = session_id,
                    pattern = pattern.as_str(),
                    "Queue subscription removed"
                );

                (
                    crate::domains::queue::QueueResponse::UnsubscribeOk,
                    None,
                    false,
                )
            }
            QueueMessage::UnsubscribeAll { session_id, .. } => {
                self.unsubscribe_all(session_id);
                (
                    crate::domains::queue::QueueResponse::UnsubscribeOk,
                    None,
                    false,
                )
            }
        };
        if should_sync_admin_snapshot {
            self.sync_admin_snapshot();
        }

        if let Some(notify_route) = availability_notify_route
            .filter(|_| self.subscription_count.load(Ordering::Relaxed) > 0)
        {
            tracing::info!(
                domain = "queue",
                route = %notify_route,
                route_family = route_family.id(),
                "Queue: fanning out availability notification (209) - CALLING handle_domain_publish"
            );
            let event = crate::runtime::DomainPublishEvent::new(
                route_family,
                notify_route,
                bytes::Bytes::from("{}"),
            );
            if let Err(e) = self.handle_domain_publish(&event) {
                tracing::warn!(domain = "queue", error = ?e, "Queue: handle_domain_publish FAILED");
            } else {
                tracing::info!(domain = "queue", "Queue: handle_domain_publish SUCCEEDED");
            }
        }

        let response_bytes = crate::protocol::queue_codec::encode_response(&response);
        let response_ctx = FrameContext::new(
            frame_ctx.session_id,
            frame_ctx.channel_id,
            crate::protocol::tlv::MessageType::new(frame_ctx.msg_type.as_u16()),
            bytes::Bytes::from(response_bytes),
            frame_ctx.route_family,
        );
        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            match self.router.route(response_envelope) {
                Ok(_) => {
                    tracing::debug!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        "Queue message handled and response routed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        domain = "queue",
                        session = frame_ctx.session_id,
                        error = ?e,
                        "Failed to route queue response"
                    );
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
    use crate::protocol::payload_codec::PayloadDecoder;
    use crate::protocol::tlv::MessageType;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use crate::runtime::Mailbox;
    use bytes::{BufMut, Bytes};
    use std::sync::Mutex as StdMutex;

    fn encode_queue_subscribe(pattern: &str) -> Bytes {
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

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    struct CaptureFrameContextSink {
        msg_types: Arc<StdMutex<Vec<u16>>>,
    }

    impl MailboxSink for CaptureFrameContextSink {
        fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            if let Some(ctx) = envelope.payload::<FrameContext>() {
                self.msg_types.lock().unwrap().push(ctx.msg_type.as_u16());
            }
            Ok(())
        }

        fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
            self.deliver(envelope)
        }
    }

    #[test]
    fn should_create_queue_domain_sink() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = QueueDomainSink::new(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_fan_out_queue_notify_209_after_send_when_subscribed() {
        // Arrange
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let queue_sink = Arc::new(QueueDomainSink::new(
            store.clone(),
            router.clone(),
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        ));
        let received: Arc<StdMutex<Vec<u16>>> = Arc::new(StdMutex::new(Vec::new()));
        let capture_sink = Arc::new(CaptureFrameContextSink {
            msg_types: received.clone(),
        });
        let family = RouteFamily::new(1);
        let inbox_addr = RouteAddress::new(family, Route::new("inbox://session/1"));
        let queue_inbound_addr = RouteAddress::new(family, Route::new("queue://inbound"));
        router.register(inbox_addr.clone(), capture_sink as Arc<dyn MailboxSink>);
        router.register_domain_pattern("queue", queue_sink as Arc<dyn MailboxSink>);
        let pattern = "queue://realm/area/resource";
        let mut sub_payload = Vec::new();
        sub_payload.extend_from_slice(&(pattern.len() as u32).to_be_bytes());
        sub_payload.extend_from_slice(pattern.as_bytes());
        let sub_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(207),
            Bytes::from(sub_payload),
            family,
        );
        let sub_env = Envelope::from_route(inbox_addr.clone(), queue_inbound_addr.clone(), sub_ctx);
        let route = "queue://realm/area/resource";
        let body: &[u8] = b"x";
        let mut send_payload = Vec::new();
        send_payload.extend_from_slice(&(route.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(route.as_bytes());
        send_payload.extend_from_slice(&(body.len() as u32).to_be_bytes());
        send_payload.extend_from_slice(body);
        let send_ctx = FrameContext::new(
            1,
            ChannelId::Pub,
            MessageType::new(200),
            Bytes::from(send_payload),
            family,
        );
        let send_env = Envelope::from_route(inbox_addr.clone(), queue_inbound_addr, send_ctx);

        // Act
        router.route(sub_env).expect("route subscribe");
        router.route(send_env).expect("route send");

        // Assert
        let msg_types = received.lock().unwrap();
        assert!(
            msg_types.contains(&209),
            "expected inbox to receive msg_type 209 (QUEUE_NOTIFY), got {:?}",
            *msg_types
        );
    }

    #[test]
    fn should_remove_queue_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let sender_session_id = 8;
        let queue_route = "queue://acme/jobs/emails";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        let sender_mailbox = Arc::new(Mailbox::new(8));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(sender_address.clone(), sender_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = QueueDomainSink::new(
            store,
            router,
            admin_read_model,
            cntryl_midge::WriteOptions::best_effort(),
        );

        sink.deliver(Envelope::from_route(
            subscriber_address,
            queue_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(207),
                encode_queue_subscribe(queue_route),
                family,
            ),
        ))
        .expect("subscribe queue route");
        let subscribe_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("queue subscribe ack envelope");
        let subscribe_frame = subscribe_envelope
            .into_payload::<FrameContext>()
            .expect("queue subscribe ack frame");
        assert_eq!(subscribe_frame.payload[0], 0);
        assert_eq!(sink.subscription_count(), 1);
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("queue://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: subscriber_session_id,
            },
        ))
        .expect("cleanup queue subscriptions");
        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address,
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(queue_route, b"email"),
                family,
            ),
        ))
        .expect("enqueue queue message");

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
        let send_ack_envelope = sender_mailbox
            .receiver()
            .try_recv()
            .expect("queue send ack envelope");
        let send_ack_frame = send_ack_envelope
            .into_payload::<FrameContext>()
            .expect("queue send ack frame");
        assert_eq!(send_ack_frame.msg_type.as_u16(), 200);
        assert_eq!(send_ack_frame.payload[0], 0);
    }

    #[test]
    fn should_retain_other_queue_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let sender_session_id = 8;
        let removed_route = "queue://acme/jobs/emails";
        let retained_route = "queue://acme/jobs/reports";
        let queue_address = RouteAddress::new(family, Route::new("queue://inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let sender_address = RouteAddress::new(family, Route::new("inbox://session/8"));
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        let sender_mailbox = Arc::new(Mailbox::new(16));
        let store = crate::testkit::create_test_engine_with_cfs(vec![1]);
        let router = Arc::new(Router::new());
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(sender_address.clone(), sender_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = QueueDomainSink::new(
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
                encode_queue_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed queue route");
        let _removed_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("removed subscribe ack envelope");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(207),
                encode_queue_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained queue route");
        let _retained_subscribe_ack = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained subscribe ack envelope");
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            queue_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Pub,
                MessageType::new(208),
                encode_queue_subscribe(removed_route),
                family,
            ),
        ))
        .expect("unsubscribe removed queue route");
        let unsubscribe_ack_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("unsubscribe ack envelope");
        let unsubscribe_ack_frame = unsubscribe_ack_envelope
            .into_payload::<FrameContext>()
            .expect("unsubscribe ack frame");
        assert_eq!(unsubscribe_ack_frame.payload[0], 0);
        assert_eq!(sink.subscription_count(), 1);
        drain_mailbox(&subscriber_mailbox);

        sink.deliver(Envelope::from_route(
            sender_address.clone(),
            queue_address.clone(),
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(removed_route, b"removed"),
                family,
            ),
        ))
        .expect("enqueue removed queue message");
        let _removed_send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("removed send ack envelope");
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::from_route(
            sender_address,
            queue_address,
            FrameContext::new(
                sender_session_id,
                ChannelId::Pub,
                MessageType::new(200),
                encode_queue_send(retained_route, b"retained"),
                family,
            ),
        ))
        .expect("enqueue retained queue message");
        let _retained_send_ack = sender_mailbox
            .receiver()
            .try_recv()
            .expect("retained send ack envelope");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained queue notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained queue notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 209);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_route = notify_decoder.get_string().expect("notify route");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_route, retained_route);
        assert_eq!(notified_payload.as_ref(), b"{}");
        assert!(notify_decoder.is_complete());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }
}
