use super::super::model::{
    Route, RouteFamily, StreamAreaScope, StreamFamilyState, StreamRealmScope,
};
use crate::domains::stream::metrics::METRIC_WATERMARK_COORDINATION_DROPS_TOTAL;
use crate::prelude::Actor;

impl StreamFamilyState {
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

    pub(in crate::domains::stream::sink) fn notify_area_batch_committed(
        &mut self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        self.notify_area_coordinator(family, realm, area, commit);
        self.notify_realm_coordinator(family, realm, commit);
    }

    fn notify_area_coordinator(
        &mut self,
        family: RouteFamily,
        realm: &str,
        area: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        let scope = StreamAreaScope {
            family,
            realm: realm.to_owned(),
            area: area.to_owned(),
        };
        if !self.watermark_coordinators.area.contains_key(&scope)
            && self.watermark_coordinators.area.len()
                >= crate::domains::stream::MAX_WATERMARK_COORDINATORS
        {
            self.record_capacity_drop(family, realm, Some(area), "area");
            return;
        }
        let router = self.watermark_router.clone();
        let store = self.core.stream_store.clone();
        let metrics = self.core.durable_metrics.clone();
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
    }

    fn notify_realm_coordinator(
        &mut self,
        family: RouteFamily,
        realm: &str,
        commit: &crate::domains::stream::protocol::BatchCommitted,
    ) {
        let scope = StreamRealmScope {
            family,
            realm: realm.to_owned(),
        };
        if !self.watermark_coordinators.realm.contains_key(&scope)
            && self.watermark_coordinators.realm.len()
                >= crate::domains::stream::MAX_WATERMARK_COORDINATORS
        {
            self.record_capacity_drop(family, realm, None, "realm");
            return;
        }
        let router = self.watermark_router.clone();
        let store = self.core.stream_store.clone();
        let metrics = self.core.durable_metrics.clone();
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
    }

    fn record_capacity_drop(
        &self,
        family: RouteFamily,
        realm: &str,
        area: Option<&str>,
        coordinator: &'static str,
    ) {
        self.core
            .counter_inc(METRIC_WATERMARK_COORDINATION_DROPS_TOTAL);
        tracing::warn!(
            domain = "stream",
            route_family = family.id(),
            realm,
            area = area.unwrap_or(""),
            coordinator,
            "Stream watermark coordinator capacity was exhausted"
        );
    }

    pub(in crate::domains::stream::sink) fn service_watermark_timers(&mut self) {
        let commits = std::mem::take(&mut self.pending_watermark_commits);
        for commit in commits {
            self.notify_area_batch_committed(
                commit.family,
                &commit.realm,
                &commit.area,
                &commit.batch,
            );
        }
        for (actor, context) in self.watermark_coordinators.area.values_mut() {
            for timer in context.timer_manager().fired_timers() {
                actor.on_timer(timer, context);
            }
        }
        for (actor, context) in self.watermark_coordinators.realm.values_mut() {
            for timer in context.timer_manager().fired_timers() {
                actor.on_timer(timer, context);
            }
        }
        let events = std::mem::take(&mut *self.watermark_events.lock());
        for event in events {
            self.core.handle_domain_publish(&event);
            let destination =
                crate::runtime::routing::RouteAddress::new(event.family_id, event.route.clone());
            let _ = self
                .core
                .router
                .route_exact(crate::runtime::Envelope::new(destination, event));
        }
    }
}
