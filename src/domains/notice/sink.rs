//! Live notice domain sink for the current broker process.
//!
//! Notice subscriptions are broker-local in-memory state only. They are
//! session-scoped, cleaned up on disconnect, and are never replayed or
//! restored after broker restart.

use crate::domains::notice::NoticeMetrics;
use crate::domains::subscription_state::RoutedSubscriptionSet;
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, RouteError, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod actor_runtime;
mod model;
#[cfg(test)]
mod test_channels;

use actor_runtime::{NoticeDomainActor, NoticeDomainCommand};
use model::{
    notice_route_realm, usize_to_u64, NoticeDeliveryTarget, NoticeDeliveryTargets,
    NoticeMatchedRoutePatterns, NoticeRouteStats, NoticeRouteStatsKey, NoticeSubscription,
    MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION,
};
#[cfg(test)]
use test_channels::{test_client_channel_from_protocol, test_protocol_channel_from_client};

/// Live notice pub/sub state for the current broker process.
///
/// This core owns the authoritative in-memory subscription index used for
/// delivery and admin snapshots. State disappears on session cleanup or broker
/// restart and is never durably recovered or replayed.
struct NoticeDomainCore {
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<NoticeSubscription>>>,
    route_stats: Mutex<HashMap<NoticeRouteStatsKey, NoticeRouteStats>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    admin_snapshot_dirty: AtomicBool,
    metrics: Option<NoticeMetrics>,
    active: AtomicBool,
}

pub struct NoticeDomainSink {
    core: Arc<NoticeDomainCore>,
    actor: ManagedActor<NoticeDomainCommand>,
}

impl NoticeDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let core = Arc::new(NoticeDomainCore {
            families: Mutex::new(HashMap::new()),
            route_stats: Mutex::new(HashMap::with_capacity(64)),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            admin_snapshot_dirty: AtomicBool::new(false),
            metrics: None,
            active: AtomicBool::new(true),
        });
        let actor = Self::spawn_actor(core.clone());
        Self { core, actor }
    }

    fn spawn_actor(
        core: Arc<NoticeDomainCore>,
    ) -> crate::runtime::ManagedActor<NoticeDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_supervised(
            router,
            NoticeDomainActor::route_address(),
            move || NoticeDomainActor::new(core.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.core.clone());
    }

    fn core_for_builder(&mut self) -> &mut NoticeDomainCore {
        Arc::get_mut(&mut self.core).expect("Notice sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        self.core_for_builder().metrics = Some(NoticeMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn subscription_family_count(&self) -> usize {
        self.core.families.lock().len()
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn route_stats_count(&self) -> usize {
        self.core.route_stats.lock().len()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(NoticeDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send(NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply_tx))
        {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice admin snapshot refresh enqueue failed"
            );
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(Duration::from_secs(1)) {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice admin snapshot refresh reply failed"
            );
        }
    }

    pub fn subscription_count(&self) -> usize {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(NoticeDomainCommand::ReadSubscriptionCount(reply_tx))
        {
            tracing::warn!(domain = "notice", error = %error, "Notice subscription-count query enqueue failed");
            return 0;
        }

        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap_or(0)
    }

    pub fn unsubscribe_all_for_session(&self, session_id: u64) -> usize {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(NoticeDomainCommand::UnsubscribeAllForSession(
                    session_id, reply_tx,
                ))
        {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice session cleanup command enqueue failed"
            );
            return 0;
        }

        reply_rx.recv_timeout(Duration::from_secs(1)).unwrap_or(0)
    }

    fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        if Self::can_accept_without_reply(&envelope) {
            let command = NoticeDomainCommand::DeliverAccepted(envelope);
            return if high_priority {
                self.actor.try_send_high_priority(command)
            } else {
                self.actor.try_send(command)
            };
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = NoticeDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::ActorStopped))
    }

    fn can_accept_without_reply(envelope: &Envelope) -> bool {
        if envelope
            .payload::<crate::runtime::DomainPublishEvent>()
            .is_some()
        {
            return true;
        }

        envelope
            .payload::<crate::domains::notice::NoticeClientRequest>()
            .is_some_and(|request| {
                matches!(
                    request.message,
                    Ok(crate::domains::notice::protocol::NotificationMessage::Publish(_))
                )
            })
    }
}

impl NoticeDomainCore {
    /// Rebuild the admin read model from the current in-memory subscription
    /// state only.
    fn sync_admin_snapshot(&self) {
        let families = self.families.lock();
        let now = Instant::now();
        let created_at = Utc::now().to_rfc3339();
        let mut subscriptions = Vec::new();
        let mut routes: HashMap<NoticeRouteStatsKey, usize> = HashMap::new();
        for (route_family, state) in families.iter() {
            for subscription in state.values() {
                let pattern = subscription.pattern.route().to_string();
                if let Some(realm) = notice_route_realm(&pattern) {
                    subscriptions.push(crate::control::admin::NoticeSubscription::snapshot(
                        *route_family,
                        subscription.subscription_id,
                        subscription.session_id,
                        realm,
                        pattern.clone(),
                        &created_at,
                    ));
                    *routes
                        .entry((*route_family, Arc::clone(&subscription.pattern_route)))
                        .or_insert(0) += 1;
                }
            }
        }
        drop(families);
        let mut route_stats = self.route_stats.lock();
        route_stats.retain(|route, stats| {
            let keep = routes.contains_key(route);
            if keep {
                stats.prune_recent_publishes(now);
            }
            keep
        });
        self.admin_read_model
            .replace_notice_subscriptions(subscriptions);
        self.admin_read_model.replace_notice_routes(
            routes
                .into_iter()
                .map(|((route_family, route), subscribers)| {
                    let (publishes_total, publishes_per_minute) = route_stats
                        .get_mut(&(route_family, Arc::clone(&route)))
                        .map_or((0, 0.0), |stats| {
                            (stats.publishes_total(), stats.publishes_per_minute(now))
                        });
                    let mut entry = crate::control::admin::NoticeRouteInfo::snapshot(
                        route_family,
                        route.to_string(),
                        subscribers,
                    );
                    entry.publishes_total = publishes_total;
                    entry.publishes_per_minute = publishes_per_minute;
                    entry
                })
                .collect(),
        );
        if let Some(metrics) = &self.metrics {
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    fn mark_admin_snapshot_dirty(&self) {
        self.admin_snapshot_dirty.store(true, Ordering::Relaxed);
        self.refresh_metrics_gauges();
    }

    fn refresh_metrics_gauges(&self) {
        if let Some(metrics) = &self.metrics {
            metrics.set_subscription_count(self.subscription_count());
        }
    }

    fn counter_add(&self, name: &str, amount: u64) {
        if let Some(metrics) = &self.metrics {
            metrics.counter_add(name, amount);
        } else {
            crate::observability::counter_add(name, amount);
        }
    }

    fn notice_response_is_failure(response: &crate::domains::notice::NoticeResponse) -> bool {
        matches!(response, crate::domains::notice::NoticeResponse::Error(_))
    }

    pub(super) fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    fn fan_out_notice_event(
        &self,
        targets: &NoticeDeliveryTargets,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        for target in targets {
            self.route_notice_notify(target, route, payload);
        }
    }

    fn record_route_publishes(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        routes: &[Arc<str>],
    ) {
        if routes.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut route_stats = self.route_stats.lock();
        let family = route_family.as_u64();

        for route in routes {
            route_stats
                .entry((family, Arc::clone(route)))
                .or_insert_with(NoticeRouteStats::new)
                .record_publish(now);
        }
    }

    fn route_notice_notify(
        &self,
        target: &NoticeDeliveryTarget,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        const MAX_RETRIES: usize = 200;

        #[cfg(test)]
        let notify_payload = crate::protocol::notice_codec::encode_notify(
            target.subscription_id,
            route,
            payload.as_ref(),
        );

        #[cfg(test)]
        let notify_ctx = FrameContext::new(
            target.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(504),
            notify_payload.into(),
            *target.subscriber.family(),
        );

        #[cfg(not(test))]
        let notification = crate::domains::notice::NoticeClientNotification::new(
            target.session_id,
            *target.subscriber.family(),
            target.subscription_id,
            route.clone(),
            payload.clone(),
        );

        let subscriber_sink = self.router.resolve_sink(&target.subscriber);

        for attempt in 0..=MAX_RETRIES {
            #[cfg(test)]
            let notify_envelope = Envelope::new(target.subscriber.clone(), notify_ctx.clone());

            #[cfg(not(test))]
            let notify_envelope = Envelope::new(target.subscriber.clone(), notification.clone());

            let delivery_result = if let Some(sink) = subscriber_sink.as_ref() {
                sink.deliver(notify_envelope)
                    .map_err(|error| RouteError::DeliveryFailed(target.subscriber.clone(), error))
            } else {
                self.router.route(notify_envelope)
            };

            match delivery_result {
                Ok(()) => return,
                Err(RouteError::DeliveryFailed(_, DeliveryError::MailboxFull { .. }))
                    if attempt < MAX_RETRIES =>
                {
                    std::thread::yield_now();
                }
                Err(_) => {
                    crate::observability::counter_inc("fitz_notice_delivery_drops_total");
                    return;
                }
            }
        }
    }

    fn collect_matching_targets_for_route(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &str,
    ) -> NoticeDeliveryTargets {
        let families = self.families.lock();
        let Some(state) = families.get(&family_id.as_u64()) else {
            return NoticeDeliveryTargets::new();
        };

        let mut targets = NoticeDeliveryTargets::with_capacity(state.matching_capacity_hint(route));
        let mut matching_routes = NoticeMatchedRoutePatterns::new();
        state.for_each_matching_route(family_id, route, |subscription| {
            targets.push(NoticeDeliveryTarget::from(subscription));
            let pattern_route = subscription.pattern_route.as_ref();
            if !matching_routes
                .iter()
                .any(|route| route.as_ref() == pattern_route)
            {
                matching_routes.push(Arc::clone(&subscription.pattern_route));
            }
        });
        self.record_route_publishes(family_id, &matching_routes);
        targets
    }

    fn publish_route_payload(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        let targets = self.collect_matching_targets_for_route(family_id, route.as_str());
        if targets.is_empty() {
            return;
        }

        self.fan_out_notice_event(&targets, route, payload);
        self.mark_admin_snapshot_dirty();
    }

    fn publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_route_payload(event.family_id, &event.route, &event.payload);
    }

    fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_event(event);
    }

    pub(super) fn unsubscribe_all_for_session(&self, session_id: u64) -> usize {
        let mut families = self.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("notice family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "notice",
            session = session_id,
            "All notice subscriptions removed for session (disconnect cleanup)"
        );
        drop(families);
        if removed > 0 {
            self.counter_add("fitz_notice_unsubscribes_total", usize_to_u64(removed));
            self.mark_admin_snapshot_dirty();
        }
        removed
    }

    pub(super) fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(RoutedSubscriptionSet::subscription_count)
            .sum()
    }

    fn wildcard_subscription_limit_reached(
        state: &RoutedSubscriptionSet<NoticeSubscription>,
        session_id: u64,
        pattern: &str,
    ) -> bool {
        pattern.contains('*')
            && state.wildcard_subscription_count_for_session(session_id)
                >= MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
    }
}

impl MailboxSink for NoticeDomainSink {
    fn deliver(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, false)
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver_to_actor(envelope, true)
    }
}

impl NoticeDomainCore {
    fn deliver_envelope(&self, envelope: &Envelope) -> Result<(), DeliveryError> {
        if self.handle_cleanup_envelope(envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(envelope) {
            return Ok(());
        }

        Self::log_delivery(envelope);

        let Some(request) = Self::extract_request(envelope)? else {
            return Ok(());
        };
        let meta = request.meta;
        let request_started = self.record_request_start();

        Self::log_parse_start(meta);

        let notice_msg = self.parse_notice_message(request.message, request_started)?;

        let (response_opt, should_sync_admin_snapshot) = self.dispatch_notice_message(notice_msg);
        if should_sync_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some(response) = response_opt {
            self.route_notice_response(envelope, meta, &response, request_started);
        } else if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_success(started_at);
        }

        Ok(())
    }

    fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all_for_session(cleanup.session_id);
            return true;
        }

        false
    }

    fn ensure_active(&self) -> Result<(), DeliveryError> {
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        Ok(())
    }

    fn handle_domain_publish_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            self.handle_domain_publish(event);
            return true;
        }

        false
    }

    fn log_delivery(envelope: &Envelope) {
        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );
    }

    fn extract_request(
        envelope: &Envelope,
    ) -> Result<Option<crate::domains::notice::NoticeClientRequest>, DeliveryError> {
        if let Some(request) = Self::request_from_envelope(envelope) {
            Ok(Some(request))
        } else {
            tracing::warn!(
                domain = "notice",
                "Envelope payload was not NoticeClientRequest"
            );
            Err(DeliveryError::ActorStopped)
        }
    }

    fn record_request_start(&self) -> Option<Instant> {
        self.metrics
            .as_ref()
            .map(NoticeMetrics::record_request_start)
    }

    fn log_parse_start(meta: crate::runtime::ClientFrameMeta) {
        tracing::debug!(
            domain = "notice",
            session = meta.session_id,
            msg_type = meta.message_type,
            "Notice: parsing request"
        );
    }

    fn parse_notice_message(
        &self,
        message: Result<crate::domains::notice::protocol::NotificationMessage, String>,
        request_started: Option<Instant>,
    ) -> Result<crate::domains::notice::protocol::NotificationMessage, DeliveryError> {
        match message {
            Ok(message) => Ok(message),
            Err(error) => {
                if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started)
                {
                    metrics.record_failure(started_at);
                }
                tracing::warn!(domain = "notice", error = %error, "Failed to parse notice message");
                Err(DeliveryError::ActorStopped)
            }
        }
    }

    fn dispatch_notice_message(
        &self,
        notice_msg: crate::domains::notice::protocol::NotificationMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        use crate::domains::notice::protocol::NotificationMessage;
        use crate::domains::notice::NoticeResponse;

        match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                self.publish_route_payload(pub_msg.family_id, &pub_msg.route, &pub_msg.payload);
                (None, false)
            }
            NotificationMessage::Subscribe(sub_msg) => self.handle_subscribe_message(&sub_msg),
            NotificationMessage::Unsubscribe(unsub_msg) => {
                let family_id = unsub_msg.family_id.as_u64();
                let mut families = self.families.lock();
                let removed = if let Some(state) = families.get_mut(&family_id) {
                    let removed = state.remove_subscription_for_session(
                        unsub_msg.family_id,
                        unsub_msg.session_id.0,
                        unsub_msg.subscription_id,
                    );
                    if state.is_empty() {
                        families.remove(&family_id);
                    }
                    removed
                } else {
                    false
                };
                if removed {
                    self.counter_add("fitz_notice_unsubscribes_total", 1);
                }
                (Some(NoticeResponse::Ok), removed)
            }
            NotificationMessage::UnsubscribeAll(unsub_all) => {
                let session_id = unsub_all.session_id.0;
                let removed = self.unsubscribe_all_for_session(session_id);
                tracing::debug!(
                    domain = "notice",
                    session = session_id,
                    "All subscriptions removed for session"
                );
                (Some(NoticeResponse::Ok), removed > 0)
            }
            NotificationMessage::Deliver(_) => (Some(NoticeResponse::Ok), false),
        }
    }

    fn handle_subscribe_message(
        &self,
        sub_msg: &crate::domains::notice::protocol::SubscribeMessage,
    ) -> (Option<crate::domains::notice::NoticeResponse>, bool) {
        use crate::domains::notice::NoticeResponse;

        let family_id = sub_msg.family_id.as_u64();
        let mut families = self.families.lock();
        let state = families
            .entry(family_id)
            .or_insert_with(RoutedSubscriptionSet::new);

        if sub_msg.pattern.as_str().is_empty() {
            tracing::warn!(
                domain = "notice",
                session = sub_msg.session_id.0,
                "Rejected empty subscription pattern"
            );
            return (
                Some(NoticeResponse::Error("empty pattern".to_string())),
                false,
            );
        }

        let existing_sub_id =
            state.find_existing_id(sub_msg.session_id.0, sub_msg.pattern.as_str());
        let (response, state_changed) = if let Some(id) = existing_sub_id {
            tracing::debug!(
                domain = "notice",
                session = sub_msg.session_id.0,
                subscription_id = id,
                pattern = sub_msg.pattern.as_str(),
                "Notice subscription already exists (idempotent)"
            );
            (
                NoticeResponse::SubscribeOk {
                    subscription_id: id,
                },
                false,
            )
        } else if Self::wildcard_subscription_limit_reached(
            state,
            sub_msg.session_id.0,
            sub_msg.pattern.as_str(),
        ) {
            tracing::warn!(
                domain = "notice",
                session = sub_msg.session_id.0,
                pattern = sub_msg.pattern.as_str(),
                limit = MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION,
                "Rejected wildcard notice subscription because session limit was exceeded"
            );
            crate::observability::counter_inc("fitz_notice_wildcard_limit_rejects_total");
            (
                NoticeResponse::Error(format!(
                    "wildcard subscription limit exceeded ({MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION} per session)"
                )),
                false,
            )
        } else {
            let new_id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
            state.insert(
                sub_msg.family_id,
                NoticeSubscription {
                    pattern: crate::runtime::matcher::Pattern::new(sub_msg.pattern.as_str()),
                    pattern_route: Arc::from(sub_msg.pattern.as_str()),
                    session_id: sub_msg.session_id.0,
                    subscription_id: new_id,
                    subscriber: sub_msg.subscriber.clone(),
                },
            );

            tracing::debug!(
                domain = "notice",
                session = sub_msg.session_id.0,
                subscription_id = new_id,
                pattern = sub_msg.pattern.as_str(),
                "Notice subscription added"
            );
            (
                NoticeResponse::SubscribeOk {
                    subscription_id: new_id,
                },
                true,
            )
        };

        (Some(response), state_changed)
    }

    fn route_notice_response(
        &self,
        envelope: &Envelope,
        meta: crate::runtime::ClientFrameMeta,
        response: &crate::domains::notice::NoticeResponse,
        request_started: Option<Instant>,
    ) {
        #[cfg(test)]
        let response_ctx = {
            let mut payload_encoder =
                crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);
            let response_bytes =
                crate::protocol::notice_codec::encode_response_into(response, &mut payload_encoder);
            FrameContext::new(
                meta.session_id,
                test_protocol_channel_from_client(meta.channel),
                crate::protocol::tlv::MessageType::new(meta.message_type),
                bytes::Bytes::from(response_bytes),
                meta.route_family,
            )
        };

        #[cfg(not(test))]
        let response_ctx =
            crate::domains::notice::NoticeClientResponse::new(meta, response.clone());

        if let Some(response_envelope) = envelope.try_reply_to(response_ctx) {
            let _ = self.router.route(response_envelope);
        }

        if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            if Self::notice_response_is_failure(response) {
                metrics.record_failure(started_at);
            } else {
                metrics.record_success(started_at);
            }
        }
    }

    fn request_from_envelope(
        envelope: &Envelope,
    ) -> Option<crate::domains::notice::NoticeClientRequest> {
        if let Some(request) = envelope.payload::<crate::domains::notice::NoticeClientRequest>() {
            return Some(request.clone());
        }

        #[cfg(test)]
        {
            let frame_ctx = envelope.payload::<FrameContext>()?.clone();
            let subscriber = envelope.source().cloned().unwrap_or_else(|| {
                crate::runtime::routing::RouteAddress::new(
                    *envelope.destination().family(),
                    crate::runtime::routing::Route::new(format!(
                        "inbox://session/{}",
                        frame_ctx.session_id
                    )),
                )
            });
            let meta = crate::runtime::ClientFrameMeta::new(
                frame_ctx.session_id,
                test_client_channel_from_protocol(frame_ctx.channel_id),
                frame_ctx.msg_type.as_u16(),
                frame_ctx.route_family,
            );
            let parsed = crate::protocol::notice_codec::parse_request(
                &frame_ctx,
                &frame_ctx.payload,
                *envelope.destination().family(),
                crate::session::SessionId(frame_ctx.session_id),
                subscriber,
            );
            Some(crate::domains::notice::NoticeClientRequest::new(
                meta, parsed,
            ))
        }

        #[cfg(not(test))]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests;
