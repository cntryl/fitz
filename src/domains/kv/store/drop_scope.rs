//! Atomic prefix deletion, independent of the number of stored rows.

use super::KvStore;
use crate::domains::kv::{KvActor, KvDropScope, KvError, TxMode};
use crate::domains::WritePolicy;

impl KvStore {
    pub(crate) fn drop_scope(
        &self,
        scope: &KvDropScope,
        write_policy: WritePolicy,
    ) -> Result<(), KvError> {
        self.drop_scope_until(scope, write_policy, Self::write_admission_deadline())
    }

    pub(super) fn drop_scope_until(
        &self,
        scope: &KvDropScope,
        write_policy: WritePolicy,
        deadline: std::time::Instant,
    ) -> Result<(), KvError> {
        scope.validate().map_err(KvError::InvalidRoute)?;
        let column_family = KvActor::resolve_column_family(scope.route_family)
            .map_err(|_| KvError::InvalidRouteFamily)?;
        let inventory_gate = self.inventory_gate_for(column_family, deadline)?;
        let _inventory_guard = inventory_gate
            .try_lock_until(deadline)
            .ok_or_else(Self::inventory_coordination_timeout)?;
        let mut tx = self.begin(column_family, TxMode::ReadWrite)?;
        for prefix in KvActor::drop_prefixes(scope) {
            let end = KvActor::prefix_range_end(&prefix);
            tx.delete_range(prefix, end)?;
        }
        tx.commit_until(write_policy, deadline)
    }
}
