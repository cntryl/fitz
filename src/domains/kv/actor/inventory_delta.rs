//! Transient inventory bookkeeping and post-commit estimate persistence.
//!
//! A successful `INSERT` proves that a key was absent and can be counted
//! exactly. `PUT`, `DELETE`, and `DELETE_RANGE` deliberately avoid hot-path
//! reads and mark the estimate incomplete; the admin inventory path then
//! refreshes it from committed rows.

use super::KvActor;
use crate::domains::kv::inventory::encode_estimate;
use crate::domains::kv::{KvError, KvResourceScope, TxMode};
use std::collections::HashMap;

#[cfg(test)]
std::thread_local! {
    static FAIL_NEXT_INVENTORY_UPDATE: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Default)]
pub(super) struct KvInventoryDelta {
    inserted_key_bytes: HashMap<Vec<u8>, usize>,
    estimate_incomplete: bool,
}

impl KvInventoryDelta {
    pub(super) fn is_empty(&self) -> bool {
        self.inserted_key_bytes.is_empty() && !self.estimate_incomplete
    }

    pub(super) fn mark_incomplete(&mut self) {
        self.estimate_incomplete = true;
    }

    pub(super) fn record_insert(&mut self, user_key: &[u8], stored_bytes: usize) {
        self.inserted_key_bytes
            .entry(user_key.to_vec())
            .or_insert(stored_bytes);
    }
}

impl KvActor {
    pub(crate) fn inventory_write_policy(
        committed: crate::domains::WritePolicy,
    ) -> crate::domains::WritePolicy {
        // Inventory estimates are best-effort admin bookkeeping, so we avoid
        // imposing stronger durability than required for user data writes.
        committed.buffered_companion()
    }

    pub(super) fn apply_inventory_delta(
        store: &crate::domains::kv::store::KvStore,
        column_family: u32,
        scope: &KvResourceScope,
        inventory_delta: &KvInventoryDelta,
        write_policy: crate::domains::WritePolicy,
    ) -> Result<(), KvError> {
        if inventory_delta.is_empty() {
            return Ok(());
        }
        #[cfg(test)]
        if FAIL_NEXT_INVENTORY_UPDATE.with(|cell| cell.replace(false)) {
            return Err(KvError::BackendError(
                "Injected KV inventory update failure".to_string(),
            ));
        }

        let key = Self::inventory_metadata_key(&scope.realm, &scope.area, &scope.resource);
        let mut tx = store.begin(column_family, TxMode::ReadWrite)?;
        let mut estimate = tx
            .get(&key)?
            .as_deref()
            .map(crate::domains::kv::inventory::decode_estimate)
            .transpose()
            .map_err(KvError::BackendError)?
            .unwrap_or_default();

        for stored_bytes in inventory_delta.inserted_key_bytes.values() {
            estimate.estimated_record_count = estimate.estimated_record_count.saturating_add(1);
            estimate.estimated_storage_bytes = estimate
                .estimated_storage_bytes
                .saturating_add(*stored_bytes as u64);
        }
        if inventory_delta.estimate_incomplete {
            estimate.estimate_complete = false;
        }

        tx.put(key, encode_estimate(estimate))?;
        tx.commit(write_policy)
    }

    /// Persist the estimate as incomplete so the admin path rescans exact counts.
    ///
    /// # Errors
    ///
    /// Returns an error when the estimate cannot be read or written.
    pub(crate) fn mark_inventory_incomplete(
        store: &crate::domains::kv::store::KvStore,
        column_family: u32,
        scope: &KvResourceScope,
        write_policy: crate::domains::WritePolicy,
    ) -> Result<(), KvError> {
        let mut delta = KvInventoryDelta::default();
        delta.mark_incomplete();
        Self::apply_inventory_delta(store, column_family, scope, &delta, write_policy)
    }

    /// Hand over resources whose post-commit estimate update failed.
    pub(crate) fn take_inventory_repairs(&mut self) -> Vec<(u32, KvResourceScope)> {
        std::mem::take(&mut self.inventory_repairs)
    }

    #[cfg(test)]
    pub(crate) fn fail_next_inventory_update_for_tests() {
        FAIL_NEXT_INVENTORY_UPDATE.with(|cell| cell.set(true));
    }
}
