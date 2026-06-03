//! Live notice domain sink for the current broker process.
//!
//! Notice subscriptions are broker-local in-memory state only. They are
//! session-scoped, cleaned up on disconnect, and are never replayed or
//! restored after broker restart.

use crate::domains::notice::NoticeMetrics;
use crate::domains::subscription_state::{RoutedSubscription, RoutedSubscriptionSet};
use crate::protocol::frame_context::FrameContext;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, Router};
use chrono::Utc;
use parking_lot::Mutex;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Per-session wildcard cap used to keep the in-memory matcher bounded.
const MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION: usize = 128;
type NoticeDeliveryTargets = SmallVec<[NoticeDeliveryTarget; 8]>;

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
        self.recent_publishes.len() as f64
    }

    fn prune_recent_publishes(&mut self, now: Instant) {
        while let Some(oldest) = self.recent_publishes.front().copied() {
            if now.saturating_duration_since(oldest) <= Duration::from_secs(60) {
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

/// Live notice pub/sub state for the current broker process.
///
/// This sink owns the authoritative in-memory subscription index used for
/// delivery and admin snapshots. State disappears on session cleanup or broker
/// restart and is never durably recovered or replayed.
pub struct NoticeDomainSink {
    families: Mutex<HashMap<u64, RoutedSubscriptionSet<NoticeSubscription>>>,
    route_stats: Mutex<HashMap<String, NoticeRouteStats>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
    admin_snapshot_dirty: AtomicBool,
    metrics: Option<NoticeMetrics>,
    active: AtomicBool,
}

impl NoticeDomainSink {
    pub fn new(
        router: Arc<Router>,
        admin_read_model: Arc<crate::api::admin::read_model::AdminReadModel>,
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
        let mut routes: HashMap<String, usize> = HashMap::new();
        for state in families.values() {
            for subscription in state.values() {
                let pattern = subscription.pattern.route().to_string();
                if let Some(realm) = notice_route_realm(&pattern) {
                    subscriptions.push(crate::api::admin::NoticeSubscription::snapshot(
                        subscription.subscription_id,
                        subscription.session_id,
                        realm,
                        pattern.clone(),
                        &created_at,
                    ));
                    *routes.entry(pattern).or_insert(0) += 1;
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
                .map(|(route, subscribers)| {
                    let (publishes_total, publishes_per_minute) = route_stats
                        .get_mut(route.as_str())
                        .map(|stats| (stats.publishes_total(), stats.publishes_per_minute(now)))
                        .unwrap_or((0, 0.0));
                    let mut entry =
                        crate::api::admin::NoticeRouteInfo::snapshot(route, subscribers);
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

    fn notice_response_is_failure(
        response: &crate::protocol::notice_codec::NoticeResponse,
    ) -> bool {
        matches!(
            response,
            crate::protocol::notice_codec::NoticeResponse::Error(_)
        )
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        if self.admin_snapshot_dirty.swap(false, Ordering::AcqRel) {
            self.sync_admin_snapshot();
        }
    }

    fn fan_out_notice_event(&self, targets: &NoticeDeliveryTargets, shared_suffix: &bytes::Bytes) {
        for target in targets {
            self.route_notice_notify(target, shared_suffix);
        }
    }

    fn record_route_publishes(&self, routes: &[String]) {
        if routes.is_empty() {
            return;
        }

        let now = Instant::now();
        let mut route_stats = self.route_stats.lock();

        for route in routes {
            if let Some(stats) = route_stats.get_mut(route.as_str()) {
                stats.record_publish(now);
            } else {
                let mut stats = NoticeRouteStats::new();
                stats.record_publish(now);
                route_stats.insert(route.clone(), stats);
            }
        }
    }

    fn route_notice_notify(&self, target: &NoticeDeliveryTarget, shared_suffix: &bytes::Bytes) {
        let notify_payload = crate::protocol::notice_codec::encode_notify_with_shared_suffix(
            target.subscription_id,
            shared_suffix,
        );
        let notify_ctx = FrameContext::new(
            target.session_id,
            crate::protocol::frame::ChannelId::Sub,
            crate::protocol::tlv::MessageType::new(504),
            notify_payload,
            *target.subscriber.family(),
        );
        let notify_envelope = Envelope::new(target.subscriber.clone(), notify_ctx);
        if self.router.route(notify_envelope).is_err() {
            crate::observability::counter_inc("fitz_notice_delivery_drops_total");
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
        route: &str,
        payload: &[u8],
    ) {
        let (targets, matching_routes) = self.collect_matching_targets_for_route(family_id, route);
        if targets.is_empty() {
            return;
        }

        self.record_route_publishes(&matching_routes);

        let shared_suffix =
            crate::protocol::notice_codec::encode_notify_shared_suffix(route, payload);
        self.fan_out_notice_event(&targets, &shared_suffix);
        self.mark_admin_snapshot_dirty();
    }

    fn publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        self.publish_route_payload(
            event.family_id,
            event.route.as_str(),
            event.payload.as_ref(),
        );
    }

    fn handle_frame_publish(
        &self,
        frame_ctx: &FrameContext,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Result<(), DeliveryError> {
        let mut decoder = crate::protocol::payload_codec::PayloadDecoder::new(&frame_ctx.payload);
        let route = decoder.get_string_ref().map_err(|error| {
            tracing::warn!(domain = "notice", error = %error, "Failed to parse notice publish route");
            DeliveryError::ActorStopped
        })?;
        let payload_range = decoder.get_bytes_range().map_err(|error| {
            tracing::warn!(domain = "notice", error = %error, "Failed to parse notice publish payload");
            DeliveryError::ActorStopped
        })?;
        if !decoder.is_complete() {
            tracing::warn!(domain = "notice", "Trailing data in notice publish message");
            return Err(DeliveryError::ActorStopped);
        }

        let payload = frame_ctx.payload.slice(payload_range);
        self.publish_route_payload(route_family, route, payload.as_ref());
        Ok(())
    }

    fn handle_domain_publish(
        &self,
        event: &crate::runtime::DomainPublishEvent,
    ) -> Result<(), DeliveryError> {
        self.publish_event(event);
        Ok(())
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
            self.counter_add("fitz_notice_unsubscribes_total", removed as u64);
            self.mark_admin_snapshot_dirty();
        }
        removed
    }

    pub fn subscription_count(&self) -> usize {
        let families = self.families.lock();
        families
            .values()
            .map(|state| state.subscription_count())
            .sum()
    }

    fn wildcard_subscription_limit_reached(
        &self,
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
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.unsubscribe_all_for_session(cleanup.session_id);
            return Ok(());
        }
        if !self.active.load(Ordering::Relaxed) {
            return Err(DeliveryError::ActorStopped);
        }

        if let Some(event) = envelope.payload::<crate::runtime::DomainPublishEvent>() {
            return self.handle_domain_publish(event);
        }

        tracing::debug!(
            domain = "notice",
            destination = %envelope.destination(),
            source = ?envelope.source(),
            "Notice domain sink: received envelope"
        );

        let frame_ctx = match envelope.payload::<FrameContext>() {
            Some(ctx) => ctx.clone(),
            None => {
                tracing::warn!(domain = "notice", "Envelope payload was not FrameContext");
                return Err(DeliveryError::ActorStopped);
            }
        };
        let request_started = self
            .metrics
            .as_ref()
            .map(|metrics| metrics.record_request_start());

        tracing::debug!(
            domain = "notice",
            session = frame_ctx.session_id,
            msg_type = frame_ctx.msg_type.as_u16(),
            payload_len = frame_ctx.payload.len(),
            "Notice: parsing request"
        );

        if frame_ctx.msg_type.as_u16() == 500 {
            let result = self.handle_frame_publish(&frame_ctx, *envelope.destination().family());
            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if result.is_ok() {
                    metrics.record_success(started_at);
                } else {
                    metrics.record_failure(started_at);
                }
            }
            return result;
        }

        let notice_msg = match crate::protocol::notice_codec::parse_request(
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
                tracing::warn!(domain = "notice", error = %e, "Failed to parse notice message");
                return Err(DeliveryError::ActorStopped);
            }
        };

        use crate::domains::notice::protocol::NotificationMessage;
        use crate::protocol::notice_codec::NoticeResponse;
        let mut payload_encoder =
            crate::protocol::payload_codec::PayloadEncoder::with_capacity(256);

        let (response_opt, should_sync_admin_snapshot) = match notice_msg {
            NotificationMessage::Publish(pub_msg) => {
                let event = crate::runtime::DomainPublishEvent::new(
                    pub_msg.family_id,
                    pub_msg.route.clone(),
                    pub_msg.payload.clone(),
                );
                self.publish_event(&event);
                (None, false)
            }
            NotificationMessage::Subscribe(sub_msg) => {
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
                    (
                        Some(NoticeResponse::Error("empty pattern".to_string())),
                        false,
                    )
                } else {
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
                    } else if self.wildcard_subscription_limit_reached(
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
                        crate::observability::counter_inc(
                            "fitz_notice_wildcard_limit_rejects_total",
                        );
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
                                pattern: crate::runtime::matcher::Pattern::new(
                                    sub_msg.pattern.as_str(),
                                ),
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

                    let should_sync_admin_snapshot = state_changed;

                    (Some(response), should_sync_admin_snapshot)
                }
            }
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
        };
        if should_sync_admin_snapshot {
            self.mark_admin_snapshot_dirty();
        }

        if let Some(response) = response_opt {
            let response_bytes = crate::protocol::notice_codec::encode_response_into(
                &response,
                &mut payload_encoder,
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

            if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
                if Self::notice_response_is_failure(&response) {
                    metrics.record_failure(started_at);
                } else {
                    metrics.record_success(started_at);
                }
            }
        } else if let (Some(metrics), Some(started_at)) = (self.metrics.as_ref(), request_started) {
            metrics.record_success(started_at);
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
    use crate::api::admin::{NoticeRouteInfo, NoticeSubscription as AdminNoticeSubscription};
    use crate::protocol::frame::ChannelId;
    use crate::protocol::frame_context::FrameContext;
    use crate::protocol::payload_codec::{PayloadDecoder, PayloadEncoder};
    use crate::protocol::tlv::MessageType;
    use crate::runtime::mailbox::Mailbox;
    use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
    use bytes::Bytes;
    use std::sync::Arc;

    fn encode_notice_subscribe(pattern: &str) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(pattern);
        Bytes::from(encoder.finish())
    }

    fn encode_notice_publish(route: &str, payload: &[u8]) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_string(route);
        encoder.put_bytes(payload);
        Bytes::from(encoder.finish())
    }

    fn encode_notice_unsubscribe(subscription_id: u64) -> Bytes {
        let mut encoder = PayloadEncoder::new();
        encoder.put_u64(subscription_id);
        Bytes::from(encoder.finish())
    }

    fn drain_mailbox(mailbox: &Mailbox) {
        while mailbox.receiver().try_recv().is_ok() {}
    }

    struct NoticeResponsePayload {
        status: u8,
        subscription_id: Option<u64>,
        error: Option<String>,
    }

    fn decode_notice_response(mailbox: &Mailbox) -> NoticeResponsePayload {
        let response_envelope = mailbox
            .receiver()
            .try_recv()
            .expect("notice response envelope");
        let response_frame = response_envelope
            .into_payload::<FrameContext>()
            .expect("notice response frame");
        let mut decoder = PayloadDecoder::new(&response_frame.payload);
        let status = decoder.get_u8().expect("notice response status");

        if status == 0 {
            let subscription_id = if decoder.remaining() > 0 {
                Some(
                    decoder
                        .get_optional_u64()
                        .expect("notice response subscription id")
                        .expect("notice response subscription id value"),
                )
            } else {
                None
            };
            assert!(decoder.is_complete());
            NoticeResponsePayload {
                status,
                subscription_id,
                error: None,
            }
        } else {
            let error = decoder.get_string().expect("notice response error");
            assert!(decoder.is_complete());
            NoticeResponsePayload {
                status,
                subscription_id: None,
                error: Some(error),
            }
        }
    }

    fn subscribe_notice_pattern(
        sink: &NoticeDomainSink,
        subscriber_address: &RouteAddress,
        notice_address: &RouteAddress,
        session_id: u64,
        pattern: &str,
        family: RouteFamily,
    ) {
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(pattern),
                family,
            ),
        ))
        .expect("subscribe notice pattern");
    }

    fn unsubscribe_notice_pattern(
        sink: &NoticeDomainSink,
        subscriber_address: &RouteAddress,
        notice_address: &RouteAddress,
        session_id: u64,
        subscription_id: u64,
        family: RouteFamily,
    ) {
        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                session_id,
                ChannelId::Sub,
                MessageType::new(502),
                encode_notice_unsubscribe(subscription_id),
                family,
            ),
        ))
        .expect("unsubscribe notice pattern");
    }

    fn assert_notice_admin_subscriptions(
        actual: &[AdminNoticeSubscription],
        expected_patterns: &[&str],
    ) {
        let mut actual_patterns: Vec<&str> =
            actual.iter().map(|entry| entry.pattern.as_str()).collect();
        actual_patterns.sort_unstable();

        let mut expected_patterns = expected_patterns.to_vec();
        expected_patterns.sort_unstable();

        assert_eq!(actual_patterns, expected_patterns);
    }

    fn assert_notice_admin_routes(actual: &[NoticeRouteInfo], expected_routes: &[&str]) {
        let mut actual_routes: Vec<&str> =
            actual.iter().map(|entry| entry.route.as_str()).collect();
        actual_routes.sort_unstable();

        let mut expected_routes = expected_routes.to_vec();
        expected_routes.sort_unstable();

        assert_eq!(actual_routes, expected_routes);
    }

    fn refresh_notice_admin_snapshot(sink: &NoticeDomainSink) {
        sink.refresh_admin_snapshot_if_dirty();
    }

    #[test]
    fn should_create_notice_domain_sink() {
        // Arrange
        let router = Arc::new(Router::new());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();

        // Act
        let sink = NoticeDomainSink::new(router, admin_read_model);

        // Assert
        assert!(sink.active.load(Ordering::Relaxed));
    }

    #[test]
    fn should_include_notice_subscription_given_flexible_route_shape() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let notice_route = "notice://acme/events";
        let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model.clone());

        // Act
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            notice_route,
            family,
        );
        let subscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(subscribe_response.status, 0);
        refresh_notice_admin_snapshot(&sink);

        // Assert
        let subscriptions = admin_read_model.notice_subscriptions(None, None);
        let routes = admin_read_model.notice_routes(None);
        assert_notice_admin_subscriptions(&subscriptions, &[notice_route]);
        assert_notice_admin_routes(&routes, &[notice_route]);
        assert_eq!(subscriptions[0].realm, "acme");
    }

    #[test]
    fn should_track_notice_publish_activity_given_matching_publish() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let publisher_session_id = 11;
        let notice_route = "notice://acme/app/events";
        let notice_address = RouteAddress::new(family, Route::new("notice://acme/inbound"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model.clone());

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            subscriber_session_id,
            notice_route,
            family,
        );
        let subscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(subscribe_response.status, 0);

        // Act
        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish notice event");
        refresh_notice_admin_snapshot(&sink);

        // Assert
        let routes = admin_read_model.notice_routes(None);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].route, notice_route);
        assert_eq!(routes[0].subscribers, 1);
        assert_eq!(routes[0].publishes_total, 1);
        assert_eq!(routes[0].publishes_per_minute, 1.0);
    }

    #[test]
    fn should_remove_notice_subscriptions_given_session_cleanup() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let publisher_session_id = 11;
        let notice_route = "notice://acme/app/events";
        let notice_address = RouteAddress::new(family, Route::new(notice_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        let publisher_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(publisher_address.clone(), publisher_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(notice_route),
                family,
            ),
        ))
        .expect("subscribe notice route");
        let subscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(subscribe_response.status, 0);
        assert!(subscribe_response.subscription_id.is_some());

        // Act
        sink.deliver(Envelope::new(
            RouteAddress::new(family, Route::new("notice://cleanup")),
            crate::runtime::SessionCleanup {
                session_id: subscriber_session_id,
            },
        ))
        .expect("cleanup notice subscriber");
        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish notice event");

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
        assert!(publisher_mailbox.receiver().try_recv().is_err());
        assert!(sink.families.lock().is_empty());
    }

    #[test]
    fn should_clear_notice_admin_snapshot_given_session_cleanup_with_mixed_subscriptions() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let exact_route = "notice://acme/app/events";
        let wildcard_route = "notice://acme/app/*";
        let notice_address = RouteAddress::new(family, Route::new(exact_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model.clone());

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            exact_route,
            family,
        );
        let exact_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(exact_response.status, 0);

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            wildcard_route,
            family,
        );
        let wildcard_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(wildcard_response.status, 0);

        refresh_notice_admin_snapshot(&sink);

        let before_subscriptions = admin_read_model.notice_subscriptions(None, None);
        let before_routes = admin_read_model.notice_routes(None);
        assert_notice_admin_subscriptions(&before_subscriptions, &[exact_route, wildcard_route]);
        assert_notice_admin_routes(&before_routes, &[exact_route, wildcard_route]);

        // Act
        sink.unsubscribe_all_for_session(session_id);

        // Assert
        assert_eq!(sink.subscription_count(), 0);
        refresh_notice_admin_snapshot(&sink);
        assert!(admin_read_model.notice_subscriptions(None, None).is_empty());
        assert!(admin_read_model.notice_routes(None).is_empty());
        assert!(sink.families.lock().is_empty());
    }

    #[test]
    fn should_prune_notice_route_stats_after_last_subscription_is_removed() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let publisher_session_id = 11;
        let notice_route = "notice://acme/app/events";
        let notice_address = RouteAddress::new(family, Route::new(notice_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        let publisher_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(publisher_address.clone(), publisher_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model);

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            notice_route,
            family,
        );
        let _subscribe_response = decode_notice_response(&subscriber_mailbox);
        sink.deliver(Envelope::from_route(
            publisher_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"hello"),
                family,
            ),
        ))
        .expect("publish notice event");
        assert_eq!(sink.route_stats.lock().len(), 1);
        drain_mailbox(&subscriber_mailbox);
        drain_mailbox(&publisher_mailbox);

        // Act
        sink.unsubscribe_all_for_session(session_id);
        refresh_notice_admin_snapshot(&sink);

        // Assert
        assert!(sink.route_stats.lock().is_empty());
    }

    #[test]
    fn should_retain_other_notice_subscription_given_unsubscribe_on_same_session() {
        // Arrange
        let family = RouteFamily::new(1);
        let subscriber_session_id = 7;
        let publisher_session_id = 11;
        let removed_route = "notice://acme/app/events";
        let retained_route = "notice://acme/app/audits";
        let notice_address = RouteAddress::new(family, Route::new(removed_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(16));
        let publisher_mailbox = Arc::new(Mailbox::new(16));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        router.register(publisher_address.clone(), publisher_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model);

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(removed_route),
                family,
            ),
        ))
        .expect("subscribe removed notice route");
        let removed_subscribe_response = decode_notice_response(&subscriber_mailbox);
        let removed_subscription_id = removed_subscribe_response
            .subscription_id
            .expect("removed subscribe subscription id");

        sink.deliver(Envelope::from_route(
            subscriber_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(501),
                encode_notice_subscribe(retained_route),
                family,
            ),
        ))
        .expect("subscribe retained notice route");
        let _retained_subscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(sink.subscription_count(), 2);
        drain_mailbox(&subscriber_mailbox);
        drain_mailbox(&publisher_mailbox);

        // Act
        sink.deliver(Envelope::from_route(
            subscriber_address,
            notice_address.clone(),
            FrameContext::new(
                subscriber_session_id,
                ChannelId::Sub,
                MessageType::new(502),
                encode_notice_unsubscribe(removed_subscription_id),
                family,
            ),
        ))
        .expect("unsubscribe removed notice route");
        let unsubscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(unsubscribe_response.status, 0);
        assert!(unsubscribe_response.subscription_id.is_none());
        assert_eq!(sink.subscription_count(), 1);

        sink.deliver(Envelope::from_route(
            publisher_address.clone(),
            notice_address.clone(),
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(removed_route, b"removed"),
                family,
            ),
        ))
        .expect("publish removed notice event");
        assert!(publisher_mailbox.receiver().try_recv().is_err());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());

        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                publisher_session_id,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(retained_route, b"retained"),
                family,
            ),
        ))
        .expect("publish retained notice event");

        // Assert
        let notify_envelope = subscriber_mailbox
            .receiver()
            .try_recv()
            .expect("retained notice notify envelope");
        let notify_frame = notify_envelope
            .into_payload::<FrameContext>()
            .expect("retained notice notify frame");
        assert_eq!(notify_frame.msg_type.as_u16(), 504);
        let mut notify_decoder = PayloadDecoder::new(&notify_frame.payload);
        let _subscription_id = notify_decoder.get_u64().expect("notify subscription id");
        let notified_route = notify_decoder.get_string().expect("notify route");
        let notified_payload = notify_decoder.get_bytes().expect("notify payload");
        assert_eq!(notified_route, retained_route);
        assert_eq!(notified_payload.as_ref(), b"retained");
        assert!(notify_decoder.is_complete());

        assert!(publisher_mailbox.receiver().try_recv().is_err());
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_retain_notice_admin_snapshot_entry_given_unsubscribe_of_sibling_pattern() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let removed_route = "notice://acme/app/events";
        let retained_route = "notice://acme/app/audits";
        let notice_address = RouteAddress::new(family, Route::new(removed_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model.clone());

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            removed_route,
            family,
        );
        let removed_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(removed_response.status, 0);
        let removed_subscription_id = removed_response
            .subscription_id
            .expect("removed subscription id");

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            retained_route,
            family,
        );
        let retained_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(retained_response.status, 0);

        // Act
        unsubscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            removed_subscription_id,
            family,
        );
        let unsubscribe_response = decode_notice_response(&subscriber_mailbox);

        // Assert
        assert_eq!(unsubscribe_response.status, 0);
        assert_eq!(sink.subscription_count(), 1);

        refresh_notice_admin_snapshot(&sink);

        let subscriptions = admin_read_model.notice_subscriptions(None, None);
        let routes = admin_read_model.notice_routes(None);
        assert_notice_admin_subscriptions(&subscriptions, &[retained_route]);
        assert_notice_admin_routes(&routes, &[retained_route]);
    }

    #[test]
    fn should_increment_delivery_drop_counter_given_failing_subscriber_route() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let notice_route = "notice://acme/app/events";
        let notice_address = RouteAddress::new(family, Route::new(notice_route));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let publisher_address = RouteAddress::new(family, Route::new("inbox://session/11"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(8));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let sink = NoticeDomainSink::new(
            router.clone(),
            crate::api::admin::read_model::AdminReadModel::new(),
        );

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            notice_route,
            family,
        );
        let subscribe_response = decode_notice_response(&subscriber_mailbox);
        assert_eq!(subscribe_response.status, 0);

        let before_drops =
            crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total");

        struct FailingSink;

        impl MailboxSink for FailingSink {
            fn deliver(&self, _envelope: Envelope) -> Result<(), DeliveryError> {
                Err(DeliveryError::ActorStopped)
            }

            fn deliver_high_priority(&self, envelope: Envelope) -> Result<(), DeliveryError> {
                self.deliver(envelope)
            }
        }

        router.register(subscriber_address.clone(), Arc::new(FailingSink));

        // Act
        sink.deliver(Envelope::from_route(
            publisher_address,
            notice_address,
            FrameContext::new(
                11,
                ChannelId::Sub,
                MessageType::new(500),
                encode_notice_publish(notice_route, b"dropped"),
                family,
            ),
        ))
        .expect("publish notice event");

        // Assert
        assert_eq!(
            crate::observability::metrics().counter_get("fitz_notice_delivery_drops_total"),
            before_drops + 1
        );
        assert_eq!(sink.subscription_count(), 1);
        assert!(subscriber_mailbox.receiver().try_recv().is_err());
    }

    #[test]
    fn should_reject_wildcard_subscription_when_session_limit_is_exceeded() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION + 4));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let admin_read_model = crate::api::admin::read_model::AdminReadModel::new();
        let sink = NoticeDomainSink::new(router, admin_read_model.clone());

        for pattern_index in 0..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
            let pattern = format!("notice://acme/app/{pattern_index}/*");
            subscribe_notice_pattern(
                &sink,
                &subscriber_address,
                &notice_address,
                session_id,
                &pattern,
                family,
            );
            let response = decode_notice_response(&subscriber_mailbox);
            assert_eq!(response.status, 0);
        }

        let overflow_pattern = "notice://acme/app/overflow/*";

        // Act
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            overflow_pattern,
            family,
        );
        let overflow_response = decode_notice_response(&subscriber_mailbox);

        // Assert
        assert_eq!(overflow_response.status, 1);
        assert_eq!(
            overflow_response.error.as_deref(),
            Some("wildcard subscription limit exceeded (128 per session)")
        );
        assert_eq!(
            sink.subscription_count(),
            MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
        );
        refresh_notice_admin_snapshot(&sink);
        assert_eq!(
            admin_read_model.notice_subscriptions(None, None).len(),
            MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
        );
    }

    #[test]
    fn should_return_existing_subscription_id_given_idempotent_wildcard_subscribe_at_limit() {
        // Arrange
        let family = RouteFamily::new(1);
        let session_id = 7;
        let duplicated_pattern = "notice://acme/app/dupe/*";
        let notice_address = RouteAddress::new(family, Route::new("notice://acme/app/events"));
        let subscriber_address = RouteAddress::new(family, Route::new("inbox://session/7"));
        let router = Arc::new(Router::new());
        let subscriber_mailbox = Arc::new(Mailbox::new(MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION + 5));
        router.register(subscriber_address.clone(), subscriber_mailbox.clone());
        let sink =
            NoticeDomainSink::new(router, crate::api::admin::read_model::AdminReadModel::new());

        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            duplicated_pattern,
            family,
        );
        let first_response = decode_notice_response(&subscriber_mailbox);
        let first_subscription_id = first_response
            .subscription_id
            .expect("first subscription id");

        for pattern_index in 1..MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION {
            let pattern = format!("notice://acme/app/{pattern_index}/*");
            subscribe_notice_pattern(
                &sink,
                &subscriber_address,
                &notice_address,
                session_id,
                &pattern,
                family,
            );
            let response = decode_notice_response(&subscriber_mailbox);
            assert_eq!(response.status, 0);
        }

        // Act
        subscribe_notice_pattern(
            &sink,
            &subscriber_address,
            &notice_address,
            session_id,
            duplicated_pattern,
            family,
        );
        let duplicate_response = decode_notice_response(&subscriber_mailbox);

        // Assert
        assert_eq!(duplicate_response.status, 0);
        assert_eq!(
            duplicate_response.subscription_id,
            Some(first_subscription_id)
        );
        assert_eq!(
            sink.subscription_count(),
            MAX_WILDCARD_SUBSCRIPTIONS_PER_SESSION
        );
    }
}
