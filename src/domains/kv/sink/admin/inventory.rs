//! Resource inventory enumeration and estimate refresh behavior.
//!
//! Persisted estimates use the shared codec in `domains::kv::inventory`; the
//! actor-side update policy lives in `actor/inventory_delta.rs`.

use super::super::locks::KvResourceLockKey;
use super::super::state::KvDomainRuntime;
use crate::domains::kv::inventory::{decode_estimate, encode_estimate, KvInventoryEstimate};
use crate::domains::kv::KvActor;

const ADMIN_INVENTORY_REFRESH_LIMIT: usize = 10_000;

impl KvDomainRuntime<'_> {
    /// Build an admin inventory snapshot for the requested route family scope.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying storage inventory scan fails.
    pub(super) fn admin_inventory(
        &self,
        family: Option<crate::runtime::routing::RouteFamily>,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let families = if let Some(family) = family {
            vec![family.id()]
        } else {
            self.core
                .store
                .list_column_families()
                .map_err(|error| error.to_string())?
                .into_iter()
                .map(|handle| handle.id())
                .filter(|family_id| *family_id != 0)
                .collect::<Vec<_>>()
        };

        let mut entries = Vec::new();
        for family_id in families {
            entries.extend(self.admin_inventory_for_family(u64::from(family_id))?);
        }
        entries.sort_by(|left, right| {
            (
                left.route_family,
                left.realm.as_str(),
                left.area.as_str(),
                left.resource.as_str(),
            )
                .cmp(&(
                    right.route_family,
                    right.realm.as_str(),
                    right.area.as_str(),
                    right.resource.as_str(),
                ))
        });
        Ok(entries)
    }

    /// Read one admin inventory entry for a specific KV resource.
    ///
    /// # Errors
    ///
    /// Returns an error when storage reads, estimate refreshes, or estimate
    /// decoding fails.
    pub(super) fn admin_inventory_resource(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
    ) -> Result<Option<crate::control::admin::KvResourceInventoryEntry>, String> {
        let family_id = route_family.as_u64();
        let column_family = KvActor::resolve_column_family(route_family)?;
        let key = KvActor::inventory_metadata_key(realm, area, resource);
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;

        let estimate = if let Some(value) = tx.get(&key).map_err(|error| error.to_string())? {
            decode_estimate(&value)?
        } else {
            let refreshed =
                self.refresh_inventory_estimate(family_id, realm, area, resource, false)?;
            if refreshed.estimated_record_count == 0 && refreshed.estimate_complete {
                return Ok(None);
            }
            refreshed
        };
        let estimate = if estimate.estimate_complete {
            estimate
        } else {
            self.refresh_inventory_estimate(family_id, realm, area, resource, true)?
        };

        Ok(Some(self.inventory_entry_from_estimate(
            family_id, realm, area, resource, estimate,
        )))
    }

    pub(super) fn admin_inventory_for_family(
        &self,
        family_id: u64,
    ) -> Result<Vec<crate::control::admin::KvResourceInventoryEntry>, String> {
        let route_family = crate::runtime::routing::RouteFamily::try_from(family_id)
            .map_err(|_| format!("invalid route family ID: {family_id}"))?;
        let column_family = KvActor::resolve_column_family(route_family)?;
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let mut iterator = tx
            .scan(&cntryl_midge::Query::new())
            .map_err(|error| error.to_string())?;
        let mut discovered = Vec::new();

        for entry in iterator.by_ref() {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let Some((realm, area, resource)) = KvActor::parse_inventory_metadata_key(&key) else {
                continue;
            };
            let estimate = decode_estimate(&value)?;
            discovered.push((realm, area, resource, estimate));
        }

        drop(iterator);
        drop(tx);

        discovered
            .into_iter()
            .map(|(realm, area, resource, estimate)| {
                let estimate = if estimate.estimate_complete {
                    estimate
                } else {
                    self.refresh_inventory_estimate(family_id, &realm, &area, &resource, true)?
                };
                Ok(self
                    .inventory_entry_from_estimate(family_id, &realm, &area, &resource, estimate))
            })
            .collect()
    }

    pub(super) fn refresh_inventory_estimate(
        &self,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        persist_empty: bool,
    ) -> Result<KvInventoryEstimate, String> {
        let route_family = crate::runtime::routing::RouteFamily::try_from(family_id)
            .map_err(|_| format!("invalid route family ID: {family_id}"))?;
        let column_family = KvActor::resolve_column_family(route_family)?;
        let read_tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let resource_prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let mut rows = Self::scan_scoped_prefix(
            &read_tx,
            &resource_prefix,
            &resource_prefix,
            &resource_prefix,
            ADMIN_INVENTORY_REFRESH_LIMIT.saturating_add(1),
        )?;
        let has_more = rows.len() > ADMIN_INVENTORY_REFRESH_LIMIT;
        rows.truncate(ADMIN_INVENTORY_REFRESH_LIMIT);
        let count = u64::try_from(rows.len()).unwrap_or(u64::MAX);
        let storage_bytes = rows.iter().fold(0u64, |total, item| {
            total
                .saturating_add(item.key.len() as u64)
                .saturating_add(item.value.len() as u64)
        });

        let estimate_complete = !has_more;
        let estimate = KvInventoryEstimate {
            estimated_record_count: count,
            estimated_storage_bytes: storage_bytes,
            estimate_complete,
        };

        drop(read_tx);

        if persist_empty || estimate.estimated_record_count > 0 || !estimate.estimate_complete {
            let mut write_tx = self
                .core
                .store
                .begin_tx(column_family, cntryl_midge::TransactionMode::ReadWrite)
                .map_err(|error| error.to_string())?;
            write_tx
                .put(
                    KvActor::inventory_metadata_key(realm, area, resource),
                    encode_estimate(estimate),
                    None,
                )
                .map_err(|error| error.to_string())?;
            write_tx
                .commit(self.core.sync_write_options)
                .map_err(|error| error.to_string())?;
        }

        Ok(estimate)
    }

    pub(super) fn inventory_entry_from_estimate(
        &self,
        family_id: u64,
        realm: &str,
        area: &str,
        resource: &str,
        estimate: KvInventoryEstimate,
    ) -> crate::control::admin::KvResourceInventoryEntry {
        let resource_key = KvResourceLockKey::new(family_id, realm, area, resource);
        let (read_latency, write_latency) = self.latency_snapshots(&resource_key);
        crate::control::admin::KvResourceInventoryEntry {
            route_family: family_id,
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            estimated_record_count: estimate.estimated_record_count,
            estimated_storage_bytes: estimate.estimated_storage_bytes,
            estimate_complete: estimate.estimate_complete,
            read_latency_avg_ms: read_latency.avg_ms,
            read_latency_p95_ms: read_latency.p95_ms,
            write_latency_avg_ms: write_latency.avg_ms,
            write_latency_p95_ms: write_latency.p95_ms,
            transactions_active: self.active_transactions_for_resource(&resource_key),
        }
    }
}
