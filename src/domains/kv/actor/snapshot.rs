//! Family-scoped committed-row reads for KV snapshot capture.

use super::KvActor;
use crate::domains::kv::TxMode;
use crate::snapshot::{SnapshotDomain, SnapshotKvEntry, SnapshotKvResource, SnapshotSelector};
use std::collections::BTreeMap;

impl KvActor {
    /// Read selected committed user rows from one family-scoped transaction.
    ///
    /// Inventory metadata is not used for discovery: it is best-effort admin
    /// state and can lag a successful user commit while repair is pending.
    ///
    /// # Errors
    ///
    /// Returns an error when the selector is not KV, the family is invalid, or
    /// the storage scan fails.
    pub(crate) fn snapshot_committed_resources(
        store: &super::super::store::KvStore,
        selector: &SnapshotSelector,
    ) -> Result<Vec<SnapshotKvResource>, String> {
        if selector.domain() != SnapshotDomain::Kv {
            return Err("KV snapshot requires a KV selector".to_string());
        }
        let column_family = Self::resolve_column_family(selector.route_family())?;
        let read = store
            .begin(column_family, TxMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let rows = read.scan_all().map_err(|error| error.to_string())?;
        let mut resources = BTreeMap::<String, Vec<SnapshotKvEntry>>::new();

        for (key, value) in rows {
            let Some((realm, area, resource)) = Self::parse_user_scope_key(&key) else {
                continue;
            };
            let route = format!("kv://{realm}/{area}/{resource}");
            if !selector.matches(SnapshotDomain::Kv, selector.route_family(), &route) {
                continue;
            }
            let prefix = Self::realm_resource_prefix(&realm, &area, &resource);
            let Some(key) = Self::strip_scoped_prefix(&prefix, &key) else {
                continue;
            };
            resources.entry(route).or_default().push(SnapshotKvEntry {
                key,
                value: value.to_vec(),
            });
        }

        Ok(resources
            .into_iter()
            .map(|(route, entries)| SnapshotKvResource { route, entries })
            .collect())
    }
}
