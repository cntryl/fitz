//! Family-serialized DROP checks live writers before atomically deleting storage ranges.

use super::locks::KvResourceLockKey;
use super::state::KvFamilyRuntime;
use crate::domains::kv::{KvDropScope, KvError, KvResponse};
use crate::domains::CommitPersistence;

impl KvResourceLockKey {
    fn is_in_drop_scope(&self, scope: &KvDropScope) -> bool {
        crate::runtime::routing::RouteFamily::try_from(self.family_id).is_ok_and(|family| {
            scope.matches_parts(family, &self.realm, &self.area, &self.resource)
        })
    }
}

impl KvFamilyRuntime<'_> {
    pub(super) fn handle_drop_scope(
        &mut self,
        scope: &KvDropScope,
        persistence: CommitPersistence,
    ) -> KvResponse {
        if let Err(error) = scope.validate() {
            return KvResponse::Error {
                error: KvError::InvalidRoute(error),
            };
        }
        // Include the caller's own transactions. A successful DROP must never
        // leave an older writable snapshot able to recreate the removed data.
        if self
            .core
            .resource_locks
            .keys()
            .any(|key| key.is_in_drop_scope(scope))
        {
            return KvResponse::Error {
                error: KvError::Conflict(
                    "DROP scope contains an open read-write transaction".to_string(),
                ),
            };
        }
        let cloud =
            self.core.sync_write_policy.is_cloud() || self.core.buffered_write_policy.is_cloud();
        match self
            .core
            .store
            .drop_scope(scope, persistence.storage_policy(cloud))
        {
            Ok(()) => {
                // An earlier failed companion estimate update must not recreate
                // inventory metadata after DROP has removed it.
                self.core
                    .pending_inventory_repairs
                    .retain(|key, _| !key.is_in_drop_scope(scope));
                KvResponse::DropOk
            }
            Err(error) => KvResponse::Error { error },
        }
    }
}
