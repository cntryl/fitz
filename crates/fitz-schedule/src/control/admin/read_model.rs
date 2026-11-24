use super::model::ScheduleInfo;
use parking_lot::{RwLock, RwLockReadGuard};
use std::collections::BTreeMap;
use std::sync::Arc;
type ScheduleIdentity = (u64, String, String, String, String);
fn schedule_identity_key(
    route_family: u64,
    realm: &str,
    area: &str,
    resource: &str,
    operation: &str,
) -> ScheduleIdentity {
    (
        route_family,
        realm.to_string(),
        area.to_string(),
        resource.to_string(),
        operation.to_string(),
    )
}

fn schedule_identity_for(info: &ScheduleInfo) -> ScheduleIdentity {
    schedule_identity_key(
        info.route_family,
        &info.realm,
        &info.area,
        &info.resource,
        &info.operation,
    )
}

use chrono::Utc;

fn matches_realm(realm: Option<&str>, value: &str) -> bool {
    realm.is_none_or(|needle| value == needle)
}

#[derive(Default)]
pub struct AdminReadModel {
    schedules: RwLock<BTreeMap<ScheduleIdentity, ScheduleInfo>>,
    schedule_pending_fire_counts: RwLock<BTreeMap<u64, usize>>,
}

impl AdminReadModel {
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn replace_schedules(&self, schedules: Vec<ScheduleInfo>) {
        *self.schedules.write() = schedules
            .into_iter()
            .map(|schedule| (schedule_identity_for(&schedule), schedule))
            .collect();
    }

    pub fn replace_schedule_family(&self, route_family: u64, schedules: Vec<ScheduleInfo>) {
        let mut current = self.schedules.write();
        current.retain(|identity, _| identity.0 != route_family);
        current.extend(
            schedules
                .into_iter()
                .map(|schedule| (schedule_identity_for(&schedule), schedule)),
        );
    }

    pub fn schedule_count(&self) -> usize {
        self.schedules.read().len()
    }

    pub fn set_schedule_family_pending_fire_count(&self, route_family: u64, count: usize) -> usize {
        let mut counts = self.schedule_pending_fire_counts.write();
        counts.insert(route_family, count);
        counts.values().sum()
    }

    pub fn upsert_schedule(&self, schedule: ScheduleInfo) {
        let mut schedules = self.schedules.write();
        let identity = schedule_identity_for(&schedule);
        if let Some(existing) = schedules.get_mut(&identity) {
            // Fast path for idempotent create/upsert calls: avoid rewriting
            // the admin model when durable schedule identity is unchanged.
            if existing.cron == schedule.cron
                && existing.delivery_mode == schedule.delivery_mode
                && existing.next_run == schedule.next_run
                && existing.last_run == schedule.last_run
                && existing.executions_total == schedule.executions_total
                && existing.enabled == schedule.enabled
            {
                return;
            }
            *existing = schedule;
        } else {
            schedules.insert(identity, schedule);
        }
    }

    pub fn upsert_schedule_fields(
        &self,
        route_family: u64,
        realm: String,
        area: String,
        resource: String,
        operation: String,
        cron: String,
    ) {
        let next_run = Utc::now().to_rfc3339();
        self.upsert_schedule(ScheduleInfo::enabled_snapshot(
            route_family,
            realm,
            area,
            resource,
            operation,
            cron,
            &next_run,
        ));
    }

    pub fn remove_schedule(
        &self,
        route_family: u64,
        realm: &str,
        area: &str,
        resource: &str,
        operation: &str,
    ) {
        self.schedules.write().remove(&schedule_identity_key(
            route_family,
            realm,
            area,
            resource,
            operation,
        ));
    }

    pub fn schedules(&self, realm: Option<&str>) -> Vec<ScheduleInfo> {
        let schedules = self.schedules.read();
        schedules
            .values()
            .filter(|item| matches_realm(realm, &item.realm))
            .cloned()
            .collect()
    }

    pub fn schedules_for_route_family(&self, route_family: u64) -> Vec<ScheduleInfo> {
        let schedules = self.schedules.read();
        schedules
            .values()
            .filter(|item| item.route_family == route_family)
            .cloned()
            .collect()
    }

    #[doc(hidden)]
    pub fn schedules_rows(&self) -> RwLockReadGuard<'_, BTreeMap<ScheduleIdentity, ScheduleInfo>> {
        self.schedules.read()
    }

    #[doc(hidden)]
    pub fn schedule_pending_fire_counts_rows(&self) -> RwLockReadGuard<'_, BTreeMap<u64, usize>> {
        self.schedule_pending_fire_counts.read()
    }
}
