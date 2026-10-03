//! Retry of inventory estimate repairs after a failed post-commit update.
//!
//! A committed KV write whose estimate update failed leaves the persisted
//! estimate stale but marked complete. The family keeps the resource until it
//! persists the estimate as incomplete, which makes the admin path rescan.

use super::locks::KvResourceLockKey;
use super::state::KvFamilyRuntime;
use crate::domains::kv::KvActor;

/// Pause between repair attempts after one fails, so a failing store does not
/// add a storage write to every KV frame on the family.
const INVENTORY_REPAIR_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);

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
            self.counter_inc(crate::domains::kv::metrics::METRIC_INVENTORY_UPDATE_FAILURES_TOTAL);
            self.core.pending_inventory_repairs.insert(
                KvResourceLockKey::from_scope(&scope),
                (column_family, scope),
            );
        }
    }

    /// Persist pending repairs; keep any that still fail and back off.
    pub(super) fn retry_inventory_repairs(&mut self) {
        if self.core.pending_inventory_repairs.is_empty() {
            return;
        }
        let now = std::time::Instant::now();
        if self
            .core
            .next_inventory_repair_at
            .is_some_and(|next| now < next)
        {
            return;
        }
        let store = self.core.store.clone();
        // Estimates are best-effort bookkeeping; use the same companion policy
        // as the post-commit update rather than a synchronous write.
        let write_policy = KvActor::inventory_write_policy(self.core.sync_write_policy);
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
        self.core.next_inventory_repair_at = (!self.core.pending_inventory_repairs.is_empty())
            .then(|| now + INVENTORY_REPAIR_RETRY_BACKOFF);
    }
}
