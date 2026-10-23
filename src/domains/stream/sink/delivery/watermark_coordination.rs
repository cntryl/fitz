use super::super::model::{
    StreamAreaScope, StreamFamilyRuntime, StreamRealmScope, WatermarkCommit, WatermarkTargets,
};
use crate::domains::stream::metrics::{
    METRIC_WATERMARK_COORDINATION_DEFERRALS_TOTAL, METRIC_WATERMARK_COORDINATION_DROPS_TOTAL,
};
use crate::prelude::Actor;
use crate::runtime::routing::{Route, RouteFamily};

impl StreamFamilyRuntime {
    pub(in crate::domains::stream::sink) fn enqueue_watermark_commit(
        &mut self,
        commit: super::super::model::WatermarkCommit,
    ) {
        if self.pending_watermark_commits.len() >= crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY
        {
            self.core
                .counter_inc(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
            tracing::warn!(
                domain = "stream",
                route_family = commit.family.id(),
                realm = commit.realm,
                area = commit.area,
                "Stream watermark coordination queue was full"
            );
            return;
        }
        self.pending_watermark_commits.push(commit);
    }

    #[cfg(test)]
    pub(in crate::domains::stream::sink) fn notify_area_batch_committed(
        &mut self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        self.notify_batch_committed(WatermarkCommit {
            family,
            realm: realm.to_owned(),
            area: area.to_owned(),
            batch: commit.clone(),
            targets: WatermarkTargets::both(),
        });
    }

    /// Deliver a commit to its area and realm coordinators. A target that
    /// cannot be reached because every coordinator slot at the cap is busy is
    /// re-queued, so the range is folded in on a later sweep instead of being
    /// lost and wedging the watermark.
    fn notify_batch_committed(&mut self, commit: WatermarkCommit) {
        let remaining = WatermarkTargets {
            area: commit.targets.area
                && !self.notify_area_coordinator(
                    commit.family,
                    &commit.realm,
                    &commit.area,
                    &commit.batch,
                ),
            realm: commit.targets.realm
                && !self.notify_realm_coordinator(commit.family, &commit.realm, &commit.batch),
        };
        if !remaining.is_empty() {
            self.enqueue_watermark_commit(WatermarkCommit {
                targets: remaining,
                ..commit
            });
        }
    }

    fn notify_area_coordinator(
        &mut self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) -> bool {
        let scope = StreamAreaScope {
            family,
            realm: realm.to_owned(),
            area: area.to_owned(),
        };
        if !self.watermark_coordinators.area.contains_key(&scope)
            && self.watermark_coordinators.area.len()
                >= crate::domains::stream::MAX_WATERMARK_COORDINATORS
            && !evict_idle_coordinator(&mut self.watermark_coordinators.area, |actor| {
                actor.is_idle()
            })
        {
            self.record_capacity_deferral(family, realm, Some(area), "area");
            return false;
        }
        let router = self.watermark_router.clone();
        let store = self.core.stream_store.clone();
        let metrics = self.core.observability.durable_metrics();
        let coordinator = self
            .watermark_coordinators
            .area
            .entry(scope)
            .or_insert_with(|| {
                let address = crate::runtime::routing::RouteAddress::new(
                    family,
                    Route::new(format!(
                        "stream://{realm}/{area}/{}",
                        crate::domains::stream::INTERNAL_AREA_SEGMENT
                    )),
                );
                (
                    crate::domains::stream::area_actor::AreaActor::new(
                        family,
                        realm.to_owned(),
                        area.to_owned(),
                        store,
                        metrics,
                    ),
                    crate::runtime::actor::Context::new(address, router),
                )
            });
        coordinator.0.receive(
            crate::domains::stream::protocol::StreamCoordinationMessage::BatchCommitted(
                commit.clone(),
            ),
            &mut coordinator.1,
        );
        true
    }

    fn notify_realm_coordinator(
        &mut self,
        family: RouteFamily,
        realm: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) -> bool {
        let scope = StreamRealmScope {
            family,
            realm: realm.to_owned(),
        };
        if !self.watermark_coordinators.realm.contains_key(&scope)
            && self.watermark_coordinators.realm.len()
                >= crate::domains::stream::MAX_WATERMARK_COORDINATORS
            && !evict_idle_coordinator(&mut self.watermark_coordinators.realm, |actor| {
                actor.is_idle()
            })
        {
            self.record_capacity_deferral(family, realm, None, "realm");
            return false;
        }
        let router = self.watermark_router.clone();
        let store = self.core.stream_store.clone();
        let metrics = self.core.observability.durable_metrics();
        let coordinator = self
            .watermark_coordinators
            .realm
            .entry(scope)
            .or_insert_with(|| {
                let address = crate::runtime::routing::RouteAddress::new(
                    family,
                    Route::new(format!(
                        "stream://{realm}/{}",
                        crate::domains::stream::INTERNAL_REALM_SEGMENT
                    )),
                );
                (
                    crate::domains::stream::realm_actor::RealmActor::new(
                        family,
                        realm.to_owned(),
                        store,
                        metrics,
                    ),
                    crate::runtime::actor::Context::new(address, router),
                )
            });
        coordinator.0.receive(
            crate::domains::stream::protocol::StreamCoordinationMessage::BatchCommitted(
                commit.clone(),
            ),
            &mut coordinator.1,
        );
        true
    }

    fn record_capacity_deferral(
        &mut self,
        family: RouteFamily,
        realm: &str,
        area: Option<&str>,
        coordinator: &'static str,
    ) {
        self.core
            .counter_inc(METRIC_WATERMARK_COORDINATION_DEFERRALS_TOTAL);
        tracing::debug!(
            domain = "stream",
            route_family = family.id(),
            realm,
            area = area.unwrap_or(""),
            coordinator,
            "Stream watermark coordinators are all busy at the cap; commit deferred"
        );
    }

    pub(in crate::domains::stream::sink) fn service_watermark_timers(&mut self) {
        let commits = std::mem::take(&mut self.pending_watermark_commits);
        for commit in commits {
            self.notify_batch_committed(commit);
        }
        // This runs after every delivered envelope, so read the clock once
        // and skip coordinators with no armed timer: the timer wheel only
        // short-circuits when a deadline exists and lies in the future.
        let now = std::time::Instant::now();
        for (actor, context) in self.watermark_coordinators.area.values_mut() {
            if context.timer_manager().next_deadline().is_none() {
                continue;
            }
            for timer in context.timer_manager().fired_timers_at(now) {
                actor.on_timer(timer, context);
            }
        }
        for (actor, context) in self.watermark_coordinators.realm.values_mut() {
            if context.timer_manager().next_deadline().is_none() {
                continue;
            }
            for timer in context.timer_manager().fired_timers_at(now) {
                actor.on_timer(timer, context);
            }
        }
        while let Ok(event) = self.watermark_events.try_recv() {
            self.core.handle_domain_publish(&event);
            let destination =
                crate::runtime::routing::RouteAddress::new(event.family_id, event.route.clone());
            let _ = self
                .core
                .router
                .route_exact(crate::runtime::Envelope::new(destination, event));
        }
        let watermark_generation = self.core.observability.watermark_generation();
        if watermark_generation != self.last_watermark_generation {
            self.core.mark_committed_admin_snapshot_dirty();
            self.last_watermark_generation = watermark_generation;
        }
    }
}

/// Make room at the coordinator cap by dropping one coordinator whose state is
/// fully persisted. Returns false when every coordinator still holds unfolded
/// ranges or an armed timer.
fn evict_idle_coordinator<K, A>(
    coordinators: &mut std::collections::HashMap<K, (A, crate::runtime::actor::Context<A>)>,
    is_idle: impl Fn(&A) -> bool,
) -> bool
where
    K: Clone + Eq + std::hash::Hash,
    A: Actor,
{
    let idle = coordinators
        .iter()
        .find(|(_, (actor, _))| is_idle(actor))
        .map(|(scope, _)| scope.clone());
    idle.is_some_and(|scope| coordinators.remove(&scope).is_some())
}
