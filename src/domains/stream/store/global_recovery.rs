use super::{
    encode_compact_global_page_key, encode_family_writer_epoch_key, encode_global_counter_key,
    encode_global_watermark_key, family_to_storage_partition, Bytes, CompactGlobalPageValue,
    RealmCounterValue, StreamStore, WatermarkValue,
};

impl StreamStore {
    pub(super) fn recover_global_ordering_once(&self, family: u64) -> Result<(), String> {
        if self.recovered_global_families.lock().contains(&family) {
            return Ok(());
        }

        let guard = self.family_sequence_guard(family);
        let _lock = guard.lock();
        if self.recovered_global_families.lock().contains(&family) {
            return Ok(());
        }

        self.recover_global_ordering_locked(family, None)
    }

    /// The caller holds this family's sequencing mutex.
    pub(super) fn recover_global_ordering_locked(
        &self,
        family: u64,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), String> {
        let mut txn = self
            .db
            .begin_tx(
                family_to_storage_partition(family),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("begin global recovery failed: {error:?}"))?;

        let epoch_key = encode_family_writer_epoch_key();
        let current_epoch = txn
            .get(&epoch_key)
            .map_err(|error| format!("read writer epoch during recovery failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        let next_epoch = current_epoch
            .checked_add(1)
            .ok_or_else(|| "ERR_STREAM_WRITER_EPOCH_EXHAUSTED".to_string())?;
        txn.put(
            epoch_key,
            RealmCounterValue {
                next_offset: next_epoch,
            }
            .encode(),
            None,
        )
        .map_err(|error| format!("write writer epoch during recovery failed: {error:?}"))?;

        let counter_key = encode_global_counter_key();
        let persisted_head = txn
            .get(&counter_key)
            .map_err(|error| format!("read global head during recovery failed: {error:?}"))?
            .map_or(Ok(0), |bytes| {
                RealmCounterValue::decode(&bytes).map(|value| value.next_offset)
            })?;
        let mut prefix = encode_compact_global_page_key(0);
        prefix.truncate(prefix.len().saturating_sub(24));
        let mut rows = txn
            .scan(
                &cntryl_midge::Query::new()
                    .prefix(Bytes::from(prefix))
                    .reverse()
                    .limit(1),
            )
            .map_err(|error| format!("scan global fragments during recovery failed: {error:?}"))?;
        let durable_head = if let Some(row) = rows.next() {
            let (key, value) =
                row.map_err(|error| format!("read last global fragment failed: {error:?}"))?;
            let first = super::decode_realm_offset_from_key(&key)?;
            let page = CompactGlobalPageValue::try_decode(&value)?;
            let count = u64::try_from(page.records.len())
                .map_err(|_| "ERR_STREAM_OFFSET_EXHAUSTED".to_string())?;
            first.saturating_add(count)
        } else {
            0
        };
        drop(rows);
        // The durable epoch fence above makes every missing reservation below
        // the allocation head terminal: an old writer either committed before
        // this transaction or conflicts on the epoch key afterward. Those
        // missing ranges are therefore resolved skips, so the repaired global
        // watermark intentionally advances to the allocation head rather than
        // stopping at the highest record-bearing fragment.
        let repaired_head = persisted_head.max(durable_head);
        txn.put(
            counter_key,
            RealmCounterValue {
                next_offset: repaired_head,
            }
            .encode(),
            None,
        )
        .map_err(|error| format!("repair global head failed: {error:?}"))?;
        txn.put(
            encode_global_watermark_key(),
            WatermarkValue {
                watermark: repaired_head,
            }
            .encode(),
            None,
        )
        .map_err(|error| format!("repair global watermark failed: {error:?}"))?;
        txn.commit(self.sync_write_options)
            .map_err(|error| format!("commit global recovery failed: {error:?}"))?;

        self.finish_recovery_cache(family, repaired_head, deadline)
    }

    fn finish_recovery_cache(
        &self,
        family: u64,
        repaired_head: u64,
        deadline: Option<std::time::Instant>,
    ) -> Result<(), String> {
        let completion = if let Some(deadline) = deadline {
            let mut states = self
                .global_completion_states
                .try_lock_until(deadline)
                .ok_or_else(super::drop_mutation::timeout)?;
            states.entry(family).or_default().clone()
        } else {
            self.global_completion_state(family)
        };
        let mut completion = if let Some(deadline) = deadline {
            completion
                .try_lock_until(deadline)
                .ok_or_else(super::drop_mutation::timeout)?
        } else {
            completion.lock()
        };
        completion.watermark = Some(repaired_head);
        completion.resolved.clear();
        let mut recovered = if let Some(deadline) = deadline {
            self.recovered_global_families
                .try_lock_until(deadline)
                .ok_or_else(super::drop_mutation::timeout)?
        } else {
            self.recovered_global_families.lock()
        };
        recovered.insert(family);
        Ok(())
    }
}
