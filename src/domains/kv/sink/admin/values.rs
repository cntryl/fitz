//! Direct committed-value inspection for the admin façade.

use super::super::locks::KvResourceLockKey;
use super::super::state::KvDomainRuntime;
use crate::domains::kv::KvActor;

impl KvDomainRuntime<'_> {
    pub(super) fn admin_get_committed_value(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
        realm: &str,
        area: &str,
        resource: &str,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, String> {
        let started_at = std::time::Instant::now();
        let column_family = KvActor::resolve_column_family(route_family)?;
        let tx = self
            .core
            .store
            .begin_tx(column_family, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|error| error.to_string())?;
        let prefix = KvActor::realm_resource_prefix(realm, area, resource);
        let scoped_key = KvActor::encode_scoped_key(&prefix, key);
        let value = tx
            .get(&scoped_key)
            .map(|value| value.map(|value| value.as_ref().to_vec()))
            .map_err(|error| error.to_string())?;
        self.record_read_latency(
            &KvResourceLockKey::new(route_family.as_u64(), realm, area, resource),
            started_at,
        );
        Ok(value)
    }
}
