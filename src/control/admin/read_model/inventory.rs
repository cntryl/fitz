//! Names from shared projections, selected and paginated without copying payloads.
use super::{family_rows::ResourceIdentity, AdminReadModel};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Default, Clone, Copy)]
pub(crate) struct InventoryScope<'a> {
    pub family: Option<u64>,
    pub realm: Option<&'a str>,
    pub area: Option<&'a str>,
    pub resource: Option<&'a str>,
    pub scheme: Option<&'a str>,
}

impl InventoryScope<'_> {
    pub(super) fn start(&self) -> ResourceIdentity {
        (
            self.family.unwrap_or(0),
            self.realm.unwrap_or_default().into(),
            self.area.unwrap_or_default().into(),
            self.resource.unwrap_or_default().into(),
        )
    }
    pub(super) fn contains(&self, key: &ResourceIdentity) -> bool {
        self.family.is_none_or(|family| key.0 == family)
            && self.realm.is_none_or(|realm| key.1 == realm)
            && self.area.is_none_or(|area| key.2 == area)
            && self.resource.is_none_or(|resource| key.3 == resource)
    }
    fn allows_domain(&self, scheme: &str) -> bool {
        self.scheme.is_none_or(|requested| requested == scheme)
    }
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
pub(crate) struct InventoryEntry {
    pub route_family: u64,
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub scheme: String,
}

impl InventoryEntry {
    fn identity(&self) -> ResourceIdentity {
        (
            self.route_family,
            self.realm.clone(),
            self.area.clone(),
            self.resource.clone(),
        )
    }
}

impl AdminReadModel {
    pub(crate) fn inventory_page(
        &self,
        scope: InventoryScope<'_>,
        after: Option<&InventoryEntry>,
        limit: usize,
    ) -> (Vec<InventoryEntry>, bool) {
        let identity = after.map(InventoryEntry::identity);
        let mut candidates = BTreeSet::new();
        let mut scan_truncated = false;
        let mut append = |scheme: &str, keys: Vec<ResourceIdentity>| {
            scan_truncated |= keys.len() >= limit.saturating_add(2);
            for (route_family, realm, area, resource) in keys {
                let entry = InventoryEntry {
                    route_family,
                    realm,
                    area,
                    resource,
                    scheme: scheme.into(),
                };
                if after.is_none_or(|after| entry > *after) {
                    candidates.insert(entry);
                }
            }
        };
        let scan_limit = limit.saturating_add(2);
        macro_rules! append_rows {
            ($scheme:literal, $field:ident) => {
                if scope.allows_domain($scheme) {
                    append(
                        $scheme,
                        self.$field
                            .read()
                            .resource_page(scope, identity.as_ref(), scan_limit),
                    );
                }
            };
        }
        append_rows!("kv", kv_transactions);
        append_rows!("stream", streams);
        append_rows!("notice", notice_subscriptions);
        append_rows!("notice", notice_routes);
        append_rows!("queue", queues);
        append_rows!("rpc", rpc_workers);
        append_rows!("rpc", rpc_pending);
        let start = identity.unwrap_or_else(|| scope.start());
        if scope.allows_domain("lease") {
            append(
                "lease",
                self.leases
                    .read()
                    .range(start.clone()..)
                    .take_while(|(key, _)| {
                        scope.family.is_none_or(|family| key.0 == family)
                            && scope.realm.is_none_or(|realm| key.1 == realm)
                            && scope.area.is_none_or(|area| key.2 == area)
                            && scope.resource.is_none_or(|resource| key.3 == resource)
                    })
                    .take(scan_limit)
                    .map(|(key, _)| key.clone())
                    .collect(),
            );
        }
        if scope.allows_domain("schedule") {
            // A resource can own many Schedule operations. Seek past the previous resource
            // when it is already emitted, so a large operation count cannot prevent progress.
            let mut schedule_start = start;
            if after.is_some_and(|after| after.scheme.as_str() >= "schedule") {
                schedule_start.3.push('\0');
            }
            let schedule_key = (
                schedule_start.0,
                schedule_start.1,
                schedule_start.2,
                schedule_start.3,
                String::new(),
            );
            append(
                "schedule",
                self.schedules
                    .read()
                    .range(schedule_key..)
                    .take_while(|(key, _)| {
                        scope.family.is_none_or(|family| key.0 == family)
                            && scope.realm.is_none_or(|realm| key.1 == realm)
                            && scope.area.is_none_or(|area| key.2 == area)
                            && scope.resource.is_none_or(|resource| key.3 == resource)
                    })
                    .take(scan_limit)
                    .map(|(key, _)| (key.0, key.1.clone(), key.2.clone(), key.3.clone()))
                    .collect(),
            );
        }
        let truncated = scan_truncated || candidates.len() > limit;
        (candidates.into_iter().take(limit).collect(), truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::admin::RpcWorker;

    #[test]
    fn should_page_resource_names_after_selecting_the_family() {
        // Arrange
        let model = AdminReadModel::new();
        let mut workers = (0..300)
            .map(|id| {
                RpcWorker::snapshot(
                    2,
                    id,
                    "other",
                    &format!("rpc://other/app/job-{id}/run"),
                    "2026-10-04T00:00:00Z",
                    0,
                    0.0,
                )
            })
            .collect::<Vec<_>>();
        workers.extend((0..3).map(|id| {
            RpcWorker::snapshot(
                1,
                id + 1000,
                "acme",
                &format!("rpc://acme/app/job-{id}/run"),
                "2026-10-04T00:00:00Z",
                0,
                0.0,
            )
        }));
        model.replace_rpc_workers(workers);

        // Act
        let (first, has_more) = model.inventory_page(
            InventoryScope {
                family: Some(1),
                ..InventoryScope::default()
            },
            None,
            2,
        );
        let (last, last_has_more) = model.inventory_page(
            InventoryScope {
                family: Some(1),
                ..InventoryScope::default()
            },
            first.last(),
            2,
        );

        // Assert
        assert_eq!(first.len(), 2);
        assert!(has_more);
        assert_eq!(last.len(), 1);
        assert!(!last_has_more);
        assert!(first
            .iter()
            .chain(&last)
            .all(|entry| entry.route_family == 1 && entry.realm == "acme"));
        assert_ne!(first[1], last[0]);
    }

    #[test]
    fn should_continue_past_many_schedule_operations_for_one_resource() {
        // Arrange
        let model = AdminReadModel::new();
        model.replace_schedules(
            (0..500)
                .map(|index| {
                    super::super::ScheduleInfo::enabled_snapshot(
                        1,
                        "acme".into(),
                        "jobs".into(),
                        "alpha".into(),
                        format!("operation-{index}"),
                        "* * * * *".into(),
                        "2099-01-01T00:00:00Z",
                    )
                })
                .chain(std::iter::once(
                    super::super::ScheduleInfo::enabled_snapshot(
                        1,
                        "acme".into(),
                        "jobs".into(),
                        "beta".into(),
                        "run".into(),
                        "* * * * *".into(),
                        "2099-01-01T00:00:00Z",
                    ),
                ))
                .collect(),
        );
        let scope = InventoryScope {
            family: Some(1),
            realm: Some("acme"),
            area: Some("jobs"),
            scheme: Some("schedule"),
            ..InventoryScope::default()
        };

        // Act
        let (first, more) = model.inventory_page(scope, None, 2);
        let (last, last_more) = model.inventory_page(scope, first.last(), 2);

        // Assert
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].resource, "alpha");
        assert!(more);
        assert_eq!(last.len(), 1);
        assert_eq!(last[0].resource, "beta");
        assert!(!last_more);
    }
}
