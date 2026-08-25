//! Live notice domain sink for the current broker process.
//!
//! Notice subscriptions are broker-local in-memory state only. They are
//! session-scoped, cleaned up on disconnect, and are never replayed or
//! restored after broker restart.

use crate::domains::notice::NoticeMetrics;
use crate::domains::subscription_state::RoutedSubscriptionSet;
use crate::runtime::{DeliveryError, Envelope, MailboxSink, ManagedActor, Router};
use chrono::Utc;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

mod actor_runtime;
mod delivery_worker;
mod domain_sink_impl;
mod mailbox_sink_impl;
mod model;
#[cfg(test)]
mod test_channels;
mod validation;

use actor_runtime::{NoticeDomainActor, NoticeDomainCommand};
use delivery_worker::{notice_delivery_worker, NoticeDeliveryJob, NOTICE_DELIVERY_HANDOFF_TIMEOUT};
use model::{
    notice_route_realm, usize_to_u64, NoticeDeliveryTarget, NoticeDeliveryTargets,
    NoticeMatchedRoutePatterns, NoticeRouteStats, NoticeRouteStatsKey, NoticeSubscription,
};
#[cfg(test)]
use test_channels::{test_client_channel_from_protocol, test_protocol_channel_from_client};
use validation::subscription_limit_error;

/// Live notice pub/sub state for the current broker process.
///
/// This core owns the authoritative in-memory subscription index used for
/// delivery and admin snapshots. State disappears on session cleanup or broker
/// restart and is never durably recovered or replayed.
struct NoticeDomainCore {
    /// Actor-owned single-writer state. The mutex supports immutable facade
    /// methods; production mutation remains serialized by `NoticeDomainActor`.
    families: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, RoutedSubscriptionSet<NoticeSubscription>>,
    >,
    /// Actor-owned single-writer route telemetry guarded for facade reads.
    route_stats: Mutex<HashMap<NoticeRouteStatsKey, NoticeRouteStats>>,
    next_sub_id: AtomicU64,
    router: Arc<Router>,
    admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    admin_snapshot_dirty: AtomicBool,
    metrics: Option<NoticeMetrics>,
    active: AtomicBool,
    /// One bounded, ordered delivery lane per route family prevents a blocked
    /// subscriber from stalling unrelated families on the Notice actor.
    delivery_workers: Mutex<
        HashMap<crate::runtime::routing::RouteFamily, crossbeam_channel::Sender<NoticeDeliveryJob>>,
    >,
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
            delivery_workers: Mutex::new(HashMap::new()),
        });
        let actor = Self::spawn_actor(core.clone());
        Self { core, actor }
    }

    fn spawn_actor(
        core: Arc<NoticeDomainCore>,
    ) -> crate::runtime::ManagedActor<NoticeDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
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

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.actor
            .try_send_high_priority(NoticeDomainCommand::BlockForTests(entered, release))
            .expect("enqueue Notice actor test block");
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

    /// Return the actor-owned live Notice subscription count.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// actor does not reply before the bounded query deadline.
    pub fn subscription_count(&self) -> Result<usize, DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(NoticeDomainCommand::ReadSubscriptionCount(reply_tx))
        {
            tracing::warn!(domain = "notice", error = %error, "Notice subscription-count query enqueue failed");
            return Err(error);
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| DeliveryError::Timeout)
    }

    /// Remove every Notice registration owned by one ephemeral session.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// actor does not reply before the bounded cleanup deadline.
    pub fn unsubscribe_all_for_session(&self, session_id: u64) -> Result<usize, DeliveryError> {
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
            return Err(error);
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| DeliveryError::Timeout)
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
            .unwrap_or(Err(DeliveryError::Timeout))
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
                let Ok(crate::domains::notice::protocol::NotificationMessage::Publish(publish)) =
                    &request.message
                else {
                    return false;
                };
                request.meta.route_family == *envelope.destination().family()
                    && envelope
                        .source()
                        .is_none_or(|source| *source.family() == request.meta.route_family)
                    && publish.family_id == request.meta.route_family
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
                        route_family.as_u64(),
                        subscription.subscription_id,
                        subscription.session_id,
                        realm,
                        pattern.clone(),
                        &created_at,
                    ));
                    let subscribers = routes
                        .entry((*route_family, Arc::clone(&subscription.pattern_route)))
                        .or_insert(0);
                    *subscribers = subscribers.saturating_add(1);
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
                        route_family.as_u64(),
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
        for route in routes {
            route_stats
                .entry((route_family, Arc::clone(route)))
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
        let family = *target.subscriber.family();
        let worker = notice_delivery_worker(&self.delivery_workers, &self.router, family);
        let Some(worker) = worker else {
            crate::observability::counter_inc(
                crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
            );
            return;
        };
        let (completed_tx, completed_rx) = crossbeam_channel::bounded(1);
        let job =
            NoticeDeliveryJob::new(target.clone(), route.clone(), payload.clone(), completed_tx);
        if worker.try_send(job).is_err() {
            crate::observability::counter_inc(
                crate::domains::notice::metrics::METRIC_DELIVERY_DROPS_TOTAL,
            );
            return;
        }
        model::record_delivery_handoff_outcome(
            completed_rx.recv_timeout(NOTICE_DELIVERY_HANDOFF_TIMEOUT),
        );
    }

    fn collect_matching_targets_for_route(
        &self,
        family_id: crate::runtime::routing::RouteFamily,
        route: &str,
    ) -> NoticeDeliveryTargets {
        let families = self.families.lock();
        let Some(state) = families.get(&family_id) else {
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
            removed += state.remove_session(*family_id, session_id);
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
}

#[cfg(test)]
mod tests;
#[cfg(test)]
use crate::dispatch::protocol::frame_context::FrameContext;
