use super::model::{Ordering, ScheduleDomainSink, ScheduleSubscriptionSet};
use crate::runtime::routing::RouteFamily;

impl ScheduleDomainSink {
    pub(super) fn write_options_are_cloud_strict_for_tests(&self) -> bool {
        self.state.core.write_options.is_cloud_strict()
    }

    pub(super) fn insert_actor_for_tests(
        &self,
        family: RouteFamily,
        actor: crate::domains::schedule::ScheduleActor,
    ) {
        self.state.core.actors.lock().insert(family, actor);
    }

    pub(super) fn actor_count_for_tests(&self) -> usize {
        self.state.core.actors.lock().len()
    }

    pub(super) fn actor_pending_fire_count_for_tests(&self, family: RouteFamily) -> usize {
        self.state.core.actors.lock().get(&family).map_or(
            0,
            crate::domains::schedule::ScheduleActor::pending_fire_count,
        )
    }

    pub(super) fn prepare_actor_scan_for_tests(&self, family: RouteFamily, ready_count: usize) {
        let mut actors = self.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(ready_count);
    }

    pub(super) fn prepare_actor_scan_claim_and_fail_next_commit_for_tests(
        &self,
        family: RouteFamily,
        ready_count: usize,
    ) -> usize {
        let mut actors = self.state.core.actors.lock();
        let actor = actors.get_mut(&family).expect("schedule actor");
        actor.bench_prepare_scan(ready_count);
        let claimed = actor.bench_claim_due_fires();
        actor.fail_next_store_commit_for_tests();
        claimed.len()
    }

    pub(super) fn subscriptions_are_empty_for_tests(&self) -> bool {
        self.state.core.sub_families.lock().is_empty()
    }

    pub(super) fn set_snapshot_dirty_for_tests(&self, dirty: bool) {
        self.state
            .core
            .snapshot_dirty
            .store(dirty, Ordering::Relaxed);
    }

    pub(super) fn snapshot_dirty_for_tests(&self) -> bool {
        self.state.core.snapshot_dirty.load(Ordering::Relaxed)
    }

    pub(super) fn insert_subscriptions_for_tests(
        &self,
        family_id: u64,
        subscriptions: ScheduleSubscriptionSet,
    ) {
        self.state
            .core
            .sub_families
            .lock()
            .insert(family_id, subscriptions);
    }

    pub(super) fn push_recent_acknowledgement_for_tests(&self, epoch_ms: u64) {
        self.state
            .core
            .recent_acknowledgement_ms
            .lock()
            .push_back(epoch_ms);
    }

    pub(super) fn set_live_publish_failures_for_tests(&self, failures: u64) {
        self.state
            .core
            .live_publish_failures
            .store(failures, Ordering::Relaxed);
    }

    pub(super) fn set_ack_failures_for_tests(&self, failures: u64) {
        self.state
            .core
            .ack_failures
            .store(failures, Ordering::Relaxed);
    }

    pub(super) fn insert_pending_ack_retry_for_tests(
        &self,
        family_id: u64,
        fire_id: u64,
        route: &str,
    ) {
        self.state
            .core
            .pending_ack_retries
            .lock()
            .entry(family_id)
            .or_default()
            .insert((fire_id, route.to_string()));
    }
}
