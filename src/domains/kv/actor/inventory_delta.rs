//! Transient inventory bookkeeping and post-commit estimate persistence.
//!
//! A successful `INSERT` proves that a key was absent and can be counted
//! exactly. `PUT`, `DELETE`, and `DELETE_RANGE` deliberately avoid hot-path
//! reads and mark the estimate incomplete; the admin inventory path then
//! refreshes it from committed rows.

use super::KvActor;
use crate::domains::kv::inventory::encode_estimate;
use crate::domains::kv::{KvError, KvResourceScope};
use cntryl_midge::{ColumnFamilyId, Engine as MidgeEngine, TransactionMode};
use std::collections::HashMap;

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
    pub(super) fn inventory_write_options(
        committed: cntryl_midge::WriteOptions,
    ) -> cntryl_midge::WriteOptions {
        // Inventory estimates are best-effort admin bookkeeping, so we avoid
        // imposing stronger durability than required for user data writes.
        if committed.is_cloud_async() || committed.is_cloud_strict() {
            cntryl_midge::WriteOptions::cloud_async()
        } else {
            cntryl_midge::WriteOptions::buffered()
        }
    }

    pub(super) fn apply_inventory_delta(
        store: &MidgeEngine,
        column_family: ColumnFamilyId,
        scope: &KvResourceScope,
        inventory_delta: &KvInventoryDelta,
        write_options: cntryl_midge::WriteOptions,
    ) -> Result<(), KvError> {
        if inventory_delta.is_empty() {
            return Ok(());
        }

        let key = Self::inventory_metadata_key(&scope.realm, &scope.area, &scope.resource);
        let mut tx = store
            .begin_tx(column_family, TransactionMode::ReadWrite)
            .map_err(|error| Self::map_midge_error(&error))?;
        let mut estimate = tx
            .get(&key)
            .map_err(|error| Self::map_midge_error(&error))?
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

        tx.put(key, encode_estimate(estimate), None)
            .map_err(|error| Self::map_midge_error(&error))?;
        tx.commit(write_options)
            .map_err(|error| Self::map_midge_error(&error))
    }
}
