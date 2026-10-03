//! Storage-backed admin prefix and paginated row scans.

use super::super::locks::KvResourceLockKey;
use super::super::state::KvFamilyRuntime;
use super::{
    AdminKvCommittedPair, AdminKvPrefixScanResult, AdminKvRowsError, AdminKvRowsRequest,
    AdminKvRowsResult,
};
use crate::domains::kv::KvActor;

fn parse_kv_resource_route(route: &str) -> Option<(&str, &str, &str)> {
    let rest = route.strip_prefix("kv://")?;
    let (realm, rest) = rest.split_once('/')?;
    let (area, resource) = rest.split_once('/')?;
    if realm.is_empty() || area.is_empty() || resource.is_empty() || resource.contains('/') {
        return None;
    }
    Some((realm, area, resource))
}

impl KvFamilyRuntime<'_> {
    pub(crate) fn restore_kv_snapshot(
        &mut self,
        artifact: &crate::snapshot::SnapshotArtifact,
    ) -> Result<Vec<String>, String> {
        if artifact.domain() != crate::snapshot::SnapshotDomain::Kv {
            return Err("KV snapshot restore requires a KV artifact".to_string());
        }
        let family = crate::runtime::routing::RouteFamily::try_from(artifact.route_family())
            .map_err(|_| "snapshot route family is invalid".to_string())?;
        let selector = crate::snapshot::SnapshotSelector::new(
            crate::snapshot::SnapshotDomain::Kv,
            family,
            artifact.selector(),
        )?;
        let mut resources = std::collections::BTreeMap::new();
        let mut existing_keys = std::collections::BTreeMap::new();
        for resource in KvActor::snapshot_committed_resources(&self.core.store, &selector)? {
            existing_keys.insert(
                resource.route.clone(),
                resource
                    .entries
                    .iter()
                    .map(|entry| entry.key.clone())
                    .collect::<Vec<_>>(),
            );
            resources.insert(resource.route, Vec::new());
        }
        for resource in artifact.kv_resources() {
            resources.insert(resource.route.clone(), resource.entries.clone());
        }

        let column_family = KvActor::resolve_column_family(family)?;
        let mut completed = Vec::new();
        for (route, entries) in resources {
            let Some((realm, area, name)) = parse_kv_resource_route(&route) else {
                return Err(format!("invalid KV resource route in snapshot: {route}"));
            };
            let prefix = KvActor::realm_resource_prefix(realm, area, name);
            let record_count = u64::try_from(entries.len()).unwrap_or(u64::MAX);
            let keys_to_delete = existing_keys.get(&route).into_iter().flatten();
            let mut tx = self
                .core
                .store
                .begin(column_family, crate::domains::kv::TxMode::ReadWrite)
                .map_err(|error| {
                    format!("restore failed after resources {completed:?}: {error}")
                })?;
            for key in keys_to_delete {
                let scoped_key = KvActor::encode_scoped_key(&prefix, key);
                tx.delete(scoped_key).map_err(|error| {
                    format!("restore failed after resources {completed:?}: {error}")
                })?;
            }
            let mut storage_bytes = 0_u64;
            for entry in entries {
                storage_bytes = storage_bytes.saturating_add(
                    u64::try_from(entry.key.len().saturating_add(entry.value.len()))
                        .unwrap_or(u64::MAX),
                );
                tx.put(KvActor::encode_scoped_key(&prefix, &entry.key), entry.value)
                    .map_err(|error| {
                        format!("restore failed after resources {completed:?}: {error}")
                    })?;
            }
            let estimate = crate::domains::kv::inventory::KvInventoryEstimate {
                estimated_record_count: record_count,
                estimated_storage_bytes: storage_bytes,
                estimate_complete: true,
            };
            tx.put(
                KvActor::inventory_metadata_key(realm, area, name),
                crate::domains::kv::inventory::encode_estimate(estimate),
            )
            .map_err(|error| format!("restore failed after resources {completed:?}: {error}"))?;
            tx.commit(self.core.sync_write_policy).map_err(|error| {
                format!("restore failed after resources {completed:?}: {error}")
            })?;
            completed.push(route);
        }
        Ok(completed)
    }

    pub(super) fn admin_snapshot_committed_resources(
        &self,
        selector: &crate::snapshot::SnapshotSelector,
    ) -> Result<Vec<crate::snapshot::SnapshotKvResource>, String> {
        KvActor::snapshot_committed_resources(&self.core.store, selector)
    }

    pub(super) fn admin_scan_committed_prefix(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key_prefix: &[u8],
        limit: usize,
    ) -> Result<AdminKvPrefixScanResult, String> {
        let started_at = std::time::Instant::now();
        let column_family = KvActor::resolve_column_family(route_family)?;
        let tx = self
            .core
            .store
            .begin(column_family, crate::domains::kv::TxMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let scoped_prefix = KvActor::encode_scoped_key(&resource_prefix, key_prefix);
        let mut rows = Self::scan_scoped_prefix(
            &tx,
            &resource_prefix,
            &scoped_prefix,
            &scoped_prefix,
            limit.saturating_add(1),
        )?;

        let has_more = rows.len() > limit;
        rows.truncate(limit);
        self.record_read_latency(
            &KvResourceLockKey::new(route_family.as_u64(), realm, area, resource),
            started_at,
        );
        Ok(AdminKvPrefixScanResult {
            items: rows,
            has_more,
        })
    }

    pub(super) fn admin_scan_committed_rows(
        &self,
        request: &AdminKvRowsRequest<'_>,
    ) -> Result<AdminKvRowsResult, AdminKvRowsError> {
        let started_at = std::time::Instant::now();
        if let Some(cursor) = request.cursor {
            if !cursor.starts_with(request.starts_with) {
                return Err(AdminKvRowsError::InvalidCursorPrefix);
            }
        }

        let column_family = KvActor::resolve_column_family(request.route_family)
            .map_err(AdminKvRowsError::Backend)?;
        let tx = self
            .core
            .store
            .begin(column_family, crate::domains::kv::TxMode::ReadOnly)
            .map_err(|error| AdminKvRowsError::Backend(error.to_string()))?;
        let resource_prefix =
            KvActor::realm_resource_prefix(request.realm, request.area, request.resource);
        let scoped_prefix = KvActor::encode_scoped_key(&resource_prefix, request.starts_with);
        let scoped_start = request.cursor.map_or_else(
            || scoped_prefix.clone(),
            |cursor| KvActor::encode_scoped_key(&resource_prefix, cursor),
        );
        let mut rows = Self::scan_scoped_prefix(
            &tx,
            &resource_prefix,
            &scoped_prefix,
            &scoped_start,
            request.limit.saturating_add(1),
        )
        .map_err(AdminKvRowsError::Backend)?;
        rows.retain(|item| {
            request
                .cursor
                .is_none_or(|cursor| item.key.as_slice() > cursor)
        });

        let has_more = rows.len() > request.limit;
        rows.truncate(request.limit);
        let next_cursor = if has_more {
            rows.last().map(|item| item.key.clone())
        } else {
            None
        };
        self.record_read_latency(
            &KvResourceLockKey::new(
                request.route_family.as_u64(),
                request.realm,
                request.area,
                request.resource,
            ),
            started_at,
        );
        Ok(AdminKvRowsResult {
            items: rows,
            next_cursor,
            has_more,
        })
    }

    pub(super) fn scan_scoped_prefix(
        tx: &crate::domains::kv::store::KvTransaction,
        resource_prefix: &[u8],
        scoped_prefix: &[u8],
        scoped_start: &[u8],
        limit: usize,
    ) -> Result<Vec<AdminKvCommittedPair>, String> {
        let iterator = tx
            .scan(
                scoped_prefix,
                scoped_start.to_vec(),
                KvActor::prefix_range_end(scoped_prefix),
                limit,
                false,
            )
            .map_err(|error| error.to_string())?;
        let mut rows = Vec::new();
        for (scoped_key, value) in iterator {
            if let Some(user_key) = KvActor::strip_scoped_prefix(resource_prefix, &scoped_key) {
                rows.push(AdminKvCommittedPair {
                    key: user_key,
                    value: value.to_vec(),
                });
            }
        }
        Ok(rows)
    }
}
