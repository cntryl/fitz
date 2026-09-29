//! Retry of inventory estimate repairs after a failed post-commit update.
//!
//! A committed KV write whose estimate update failed leaves the persisted
//! estimate stale but marked complete. The family keeps the resource until it
//! persists the estimate as incomplete, which makes the admin path rescan.

use super::locks::KvResourceLockKey;
use super::state::KvFamilyRuntime;
use crate::domains::kv::KvActor;

pub(super) const METRIC_INVENTORY_UPDATE_FAILURES: &str =
    "fitz_kv_inventory_estimate_update_failures_total";

impl KvFamilyRuntime<'_> {
    /// Move a session actor's failed estimate updates into family state.
    pub(super) fn collect_inventory_repairs(&mut self, session_id: u64) {
        let repairs = self
            .core
            .actors
            .get_mut(&session_id)
            .map(KvActor::take_inventory_repairs)
            .unwrap_or_default();
        for (column_family, scope) in repairs {
            self.counter_inc(METRIC_INVENTORY_UPDATE_FAILURES);
            self.core.pending_inventory_repairs.insert(
                KvResourceLockKey::from_scope(&scope),
                (column_family, scope),
            );
        }
    }

    /// Persist pending repairs; keep any that still fail for the next frame.
    pub(super) fn retry_inventory_repairs(&mut self) {
        if self.core.pending_inventory_repairs.is_empty() {
            return;
        }
        let store = self.core.store.clone();
        let write_policy = self.core.sync_write_policy;
        self.core
            .pending_inventory_repairs
            .retain(|_, (column_family, scope)| {
                match KvActor::mark_inventory_incomplete(
                    &store,
                    *column_family,
                    scope,
                    write_policy,
                ) {
                    Ok(()) => false,
                    Err(error) => {
                        tracing::warn!(
                            ?error,
                            realm = %scope.realm,
                            resource = %scope.resource,
                            "KV inventory estimate repair failed; will retry"
                        );
                        true
                    }
                }
            });
    }
}
