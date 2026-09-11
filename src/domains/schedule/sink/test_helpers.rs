use super::model::{
    ScheduleDomain, ScheduleDomainCommand, ScheduleFamilyState, ScheduleSubscriptionSet,
};
use crate::runtime::routing::RouteFamily;
use std::sync::atomic::Ordering;

impl ScheduleDomain {
    pub(super) fn route_families_for_tests(&self) -> &[RouteFamily] {
        &self.route_families
    }

    fn inspect_family_for_tests(
        &self,
        family: RouteFamily,
        inspect: impl FnOnce(&mut ScheduleFamilyState) + Send + 'static,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::InspectForTests(Box::new(inspect), reply_tx),
        )
        .expect("enqueue Schedule family test inspection");
        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("Schedule family test inspection reply");
    }

    fn read_family_for_tests<T: Send + 'static>(
        &self,
        family: RouteFamily,
        read: impl FnOnce(&mut ScheduleFamilyState) -> T + Send + 'static,
    ) -> T {
        let (value_tx, value_rx) = crossbeam_channel::bounded(1);
        self.inspect_family_for_tests(family, move |core| {
            let _ = value_tx.send(read(core));
        });
        value_rx.recv().expect("Schedule family test read")
    }

    pub(super) fn write_policy_are_cloud_strict_for_tests(&self) -> bool {
        self.config.write_policy == crate::domains::WritePolicy::CloudStrict
    }

    pub(super) fn insert_actor_for_tests(
        &self,
        family: RouteFamily,
        actor: crate::domains::schedule::ScheduleActor,
    ) {
        self.inspect_family_for_tests(family, move |core| core.actor = Some(actor));
    }

    pub(super) fn actor_count_for_tests(&self) -> usize {
        self.route_families
            .iter()
            .map(|family| {
                self.read_family_for_tests(*family, |core| usize::from(core.actor.is_some()))
            })
            .sum()
    }

    pub(super) fn actor_pending_fire_count_for_tests(&self, family: RouteFamily) -> usize {
        self.read_family_for_tests(family, |core| {
            core.actor.as_ref().map_or(
                0,
                crate::domains::schedule::ScheduleActor::pending_fire_count,
            )
        })
    }

    pub(super) fn prepare_actor_scan_for_tests(&self, family: RouteFamily, ready_count: usize) {
        self.inspect_family_for_tests(family, move |core| {
            core.actor
                .as_mut()
                .expect("schedule actor")
                .bench_prepare_scan(ready_count);
        });
    }

    pub(super) fn prepare_actor_scan_claim_and_fail_next_commit_for_tests(
        &self,
        family: RouteFamily,
        ready_count: usize,
    ) -> usize {
        self.read_family_for_tests(family, move |core| {
            let actor = core.actor.as_mut().expect("schedule actor");
            actor.bench_prepare_scan(ready_count);
            let claimed = actor.bench_claim_due_fires().len();
            actor.fail_next_store_commit_for_tests();
            claimed
        })
    }

    pub(super) fn subscriptions_are_empty_for_tests(&self) -> bool {
        self.route_families
            .iter()
            .all(|family| self.read_family_for_tests(*family, |core| core.subscriptions.is_empty()))
    }

    pub(super) fn set_snapshot_dirty_for_tests(&self, dirty: bool) {
        for family in &self.route_families {
            self.inspect_family_for_tests(*family, move |core| {
                core.snapshot_dirty.store(dirty, Ordering::Relaxed);
            });
        }
    }

    pub(super) fn snapshot_dirty_for_tests(&self) -> bool {
        self.read_family_for_tests(self.route_families[0], |core| {
            core.snapshot_dirty.load(Ordering::Relaxed)
        })
    }

    pub(super) fn insert_subscriptions_for_tests(
        &self,
        family: RouteFamily,
        subscriptions: ScheduleSubscriptionSet,
    ) {
        self.inspect_family_for_tests(family, move |core| core.subscriptions = subscriptions);
    }

    pub(super) fn round_robin_cursor_for_tests(
        &self,
        family: RouteFamily,
        route: &str,
    ) -> Option<usize> {
        let route = route.to_string();
        self.read_family_for_tests(family, move |core| {
            core.subscriptions.round_robin_cursors.get(&route).copied()
        })
    }

    pub(super) fn push_recent_acknowledgement_for_tests(&self, epoch_ms: u64) {
        self.inspect_family_for_tests(self.route_families[0], move |core| {
            core.recent_acknowledgement_ms.push_back(epoch_ms);
        });
    }

    pub(super) fn set_live_publish_failures_for_tests(&self, failures: u64) {
        self.inspect_family_for_tests(self.route_families[0], move |core| {
            core.live_publish_failures = failures;
        });
    }

    pub(super) fn set_ack_failures_for_tests(&self, failures: u64) {
        self.inspect_family_for_tests(self.route_families[0], move |core| {
            core.ack_failures = failures;
        });
    }

    pub(super) fn insert_pending_ack_retry_for_tests(
        &self,
        family_id: u64,
        fire_id: u64,
        route: &str,
    ) {
        let family = RouteFamily::new(u32::try_from(family_id).expect("family ID"));
        let route = route.to_string();
        self.inspect_family_for_tests(family, move |core| {
            core.pending_ack_retries
                .entry(family_id)
                .or_default()
                .insert((fire_id, route), super::model::PendingFireState::HandedOff);
        });
    }
}
