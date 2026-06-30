//! Live notice domain sink for the current broker process.
//!
//! Notice subscriptions are broker-local in-memory state only. They are
//! session-scoped, cleaned up on disconnect, and are never replayed or
//! restored after broker restart.

use crate::domains::notice::NoticeMetrics;
use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
#[cfg(test)]
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, RouteError, Router};
use chrono::Utc;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Per-session wildcard cap used to keep the in-memory matcher bounded.
const MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION: usize = 128;
type NoticeDeliveryTargets = SmallVec<[NoticeDeliveryTarget; 8]>;
type NoticeRouteStatsKey = (u64, String);

#[derive(Clone)]
struct NoticeDeliveryTarget {
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

struct NoticeRouteStats {
    publishes_total: u64,
    recent_publishes: VecDeque<Instant>,
}

impl NoticeRouteStats {
    fn new() -> Self {
        Self {
            publishes_total: 0,
            recent_publishes: VecDeque::new(),
        }
    }

    fn record_publish(&mut self, now: Instant) {
        self.prune_recent_publishes(now);
        self.publishes_total = self.publishes_total.saturating_add(1);
        self.recent_publishes.push_back(now);
    }

    fn publishes_total(&self) -> u64 {
        self.publishes_total
    }

    fn publishes_per_minute(&mut self, now: Instant) -> f64 {
        self.prune_recent_publishes(now);
        usize_to_f64(self.recent_publishes.len())
    }

    fn prune_recent_publishes(&mut self, now: Instant) {
        while let Some(oldest) = self.recent_publishes.front().copied() {
            if now.saturating_duration_since(oldest) <= Duration::from_mins(1) {
                break;
            }
            self.recent_publishes.pop_front();
        }
    }
}

struct NoticeSubscription {
    pattern: crate::runtime::matcher::Pattern,
    session_id: u64,
    subscription_id: u64,
    subscriber: crate::runtime::routing::RouteAddress,
}

impl RoutedSubscription for NoticeSubscription {
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

impl From<&NoticeSubscription> for NoticeDeliveryTarget {
    fn from(subscription: &NoticeSubscription) -> Self {
        Self {
            session_id: subscription.session_id,
            subscription_id: subscription.subscription_id,
            subscriber: subscription.subscriber.clone(),
        }
    }
}

fn notice_route_realm(route: &str) -> Option<&str> {
    let path = route.split_once("://").map_or(route, |(_, path)| path);
    path.trim_start_matches('/')
        .split('/')
        .find(|segment| !segment.is_empty())
}

fn usize_to_f64(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn usize_to_u64(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Live notice pub/sub state for the current broker process.
///
/// This sink owns the authoritative in-memory subscription index used for
/// delivery and admin snapshots. State disappears on session cleanup or broker
/// restart and is never durably recovered or replayed.
pub struct NoticeDomainSink {
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<NoticeSubscription>>>,
    route_stats: Mutex<HashMap<NoticeRouteStatsKey, NoticeRouteStats>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    admin_snapshot_dirty: AtomicBool,
    metrics: Option<NoticeMetrics>,
    active: AtomicBool,
}

impl NoticeDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self {
            families: Mutex::new(HashMap::new()),
            route_stats: Mutex::new(HashMap::with_capacity(64)),
            next_sub_id: AtomicU64::new(1),
            router,
            admin_read_model,
            admin_snapshot_dirty: AtomicBool::new(false),
            metrics: None,
            active: AtomicBool::new(true),
        }
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.metrics = Some(NoticeMetrics::new(collector));
        self.refresh_metrics_gauges();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
    }

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
                    *routes.entry((*route_family, pattern)).or_insert(0) += 1;
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
                        .get_mut(&(route_family, route.clone()))
                        .map_or((0, 0.0), |stats| {
                            (stats.publishes_total(), stats.publishes_per_minute(now))
                        });
                    let mut entry = crate::control::admin::NoticeRouteInfo::snapshot(
                        route_family,
                        route,
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

    pub fn refresh_admin_snapshot_if_dirty(&self) {
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
        routes: &[String],
    ) {
        if routes.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut route_stats = self.route_stats.lock();
        let family = route_family.as_u64();

        for route in routes {
            let key = (family, route.clone());
            if let Some(stats) = route_stats.get_mut(&key) {
                stats.record_publish(now);
            } else {
                let mut stats = NoticeRouteStats::new();
                stats.record_publish(now);
                route_stats.insert(key, stats);
            }
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

        for attempt in 0..=MAX_RETRIES {
            #[cfg(test)]
            let notify_envelope = Envelope::new(target.subscriber.clone(), notify_ctx.clone());

            #[cfg(not(test))]
            let notify_envelope = Envelope::new(target.subscriber.clone(), notification.clone());

            match self.router.route(notify_envelope) {
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
    ) -> (NoticeDeliveryTargets, Vec<String>) {
        let families = self.families.lock();
        let Some(state) = families.get(&family_id.as_u64()) else {
            return (NoticeDeliveryTargets::new(), Vec::new());
        };

        let mut targets = NoticeDeliveryTargets::with_capacity(state.matching_capacity_hint(route));
        let mut matching_routes: HashSet<String> =
            HashSet::with_capacity(state.matching_capacity_hint(route));
        state.for_each_matching_route(family_id, route, |subscription| {
            targets.push(NoticeDeliveryTarget::from(subscription));
            matching_routes.insert(subscription.pattern.route().to_string());
        });
        (targets, matching_routes.into_iter().collect())
    }

    fn publish_route_payload(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &crate::runtime::routing::Route,
        payload: &bytes::Bytes,
    ) {
        let (targets, matching_routes) =
            self.collect_matching_targets_for_route(family_id, route.as_str());
        if targets.is_empty() {
            return;
        }

        self.record_route_publishes(family_id, &matching_routes);

        self.fan_out_notice_event(&targets, route, payload);
        self.mark_admin_snapshot_dirty();
    }

    fn publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_route_payload(event.family_id, &event.route, &event.payload);
    }

    fn handle_domain_publish(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_event(event);
    }

    pub fn unsubscribe_all_for_session(&self, session_id: u64) -> usize {
        let mut families = self.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::new(*family_id),
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

    pub fn subscription_count(&self) -> usize {
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
        if self.handle_cleanup_envelope(&envelope) {
            return Ok(());
        }
        self.ensure_active()?;

        if self.handle_domain_publish_envelope(&envelope) {
            return Ok(());
        }

        Self::log_delivery(&envelope);

        let Some(request) = Self::extract_request(&envelope)? else {
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
            self.route_notice_response(&envelope, meta, &response, request_started);
        } else if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_success(started_at);
        }

        Ok(())
    }

    fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
        self.deliver(envelope)
    }
}

impl NoticeDomainSink {
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
fn test_client_channel_from_protocol(
    channel: crate::protocol::frame::ChannelId,
) -> crate::runtime::ClientChannel {
    match channel {
        crate::protocol::frame::ChannelId::Control => crate::runtime::ClientChannel::Control,
        crate::protocol::frame::ChannelId::Pub => crate::runtime::ClientChannel::Pub,
        crate::protocol::frame::ChannelId::Sub => crate::runtime::ClientChannel::Sub,
        crate::protocol::frame::ChannelId::Rpc => crate::runtime::ClientChannel::Rpc,
        crate::protocol::frame::ChannelId::Lease => crate::runtime::ClientChannel::Lease,
        crate::protocol::frame::ChannelId::Internal => crate::runtime::ClientChannel::Internal,
    }
}

#[cfg(test)]
fn test_protocol_channel_from_client(
    channel: crate::runtime::ClientChannel,
) -> crate::protocol::frame::ChannelId {
    match channel {
        crate::runtime::ClientChannel::Control => crate::protocol::frame::ChannelId::Control,
        crate::runtime::ClientChannel::Pub => crate::protocol::frame::ChannelId::Pub,
        crate::runtime::ClientChannel::Sub => crate::protocol::frame::ChannelId::Sub,
        crate::runtime::ClientChannel::Rpc => crate::protocol::frame::ChannelId::Rpc,
        crate::runtime::ClientChannel::Lease => crate::protocol::frame::ChannelId::Lease,
        crate::runtime::ClientChannel::Internal => crate::protocol::frame::ChannelId::Internal,
    }
}

#[cfg(test)]
mod tests;
