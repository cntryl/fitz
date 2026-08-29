use super::model::{
    parse_concrete_schedule_route, BTreeMap, Bytes, PersistedPendingFireClaim, PersistedSchedule,
    SchedulePersistenceError, ScheduleStore, WriteOptions, DEFINITION_PREFIX, DUE_INDEX_VALUE,
    DUE_PREFIX, PENDING_FIRE_PREFIX,
};

impl ScheduleStore {
    /// Load authoritative durable schedule definitions and rebuild the full due index.
    ///
    /// # Errors
    ///
    /// Returns an error when stored schedule rows cannot be read, decoded,
    /// normalized, or durably rewritten for the current family. Only the
    /// current row generation is read; no other layout is migrated.
    pub fn load_all(
        &self,
        cf_id: u64,
        write_options: WriteOptions,
    ) -> Result<Vec<PersistedSchedule>, String> {
        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;
        let read_tx = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {e:?}"))?;

        let mut schedules = BTreeMap::<String, PersistedSchedule>::new();
        let mut metadata_rows = BTreeMap::<String, (u64, Option<u64>, u64)>::new();

        let definition_rows = Self::scan_schedule_rows(&read_tx, DEFINITION_PREFIX)?;
        let body_rows = Self::scan_schedule_rows(&read_tx, super::model::BODY_PREFIX)?;
        Self::decode_definition_rows(definition_rows, &mut metadata_rows)?;

        let mut body_definitions = BTreeMap::new();
        Self::decode_body_rows(body_rows, &mut body_definitions)?;

        Self::merge_metadata_rows(metadata_rows, &body_definitions, &mut schedules)?;

        let due_rows = Self::scan_schedule_rows(&read_tx, DUE_PREFIX)?;
        drop(read_tx);

        let mut write_tx = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin schedule load tx failed: {e:?}"))?;

        Self::rewrite_due_index(&mut write_tx, due_rows, schedules.values())?;

        self.commit_or_inject(write_tx, write_options)?;
        Ok(schedules.into_values().collect())
    }

    /// # Errors
    ///
    /// Returns an error when pending-claim rows cannot be loaded or decoded.
    pub fn load_pending_fire_claims(
        &self,
        cf_id: u64,
    ) -> Result<Vec<PersistedPendingFireClaim>, String> {
        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;
        let read_tx = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadOnly)
            .map_err(|e| format!("begin_tx failed: {e:?}"))?;

        let rows = Self::scan_schedule_rows(&read_tx, PENDING_FIRE_PREFIX)?;

        let mut pending = Vec::with_capacity(rows.len());
        for (key, value) in rows {
            match (
                Self::decode_pending_fire_key(&key),
                Self::decode_pending_fire_value(&value),
            ) {
                (Ok((fire_ms, route)), Ok((claimed_at_ms, delivery_mode, payload))) => {
                    pending.push(PersistedPendingFireClaim {
                        route,
                        payload,
                        delivery_mode,
                        claimed_at_ms,
                        fire_ms,
                    });
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode pending schedule fire failed: {error}"));
                }
            }
        }

        pending.sort_by(|left, right| {
            (left.fire_ms, left.route.as_str()).cmp(&(right.fire_ms, right.route.as_str()))
        });
        Ok(pending)
    }

    /// # Errors
    ///
    /// Returns an error when the family id is invalid, opening the family
    /// transaction fails, or committing with the caller-selected write options fails.
    pub(crate) fn sync_family(
        &self,
        cf_id: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;
        let txn = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin schedule commit tx failed: {e:?}"))?;
        txn.commit(write_options)
            .map_err(|e| format!("commit schedule column family failed: {e:?}"))
    }

    /// # Errors
    ///
    /// Returns an error when deleting any pending fire-claim row or committing
    /// the transaction fails.
    pub fn delete_pending_fire_claims(
        &self,
        cf_id: u64,
        items: &[(u64, String)],
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if items.is_empty() {
            return Ok(());
        }

        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;
        let mut txn = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {e:?}"))?;

        for (fire_ms, route) in items {
            txn.delete(Self::encode_pending_fire_key(*fire_ms, route))
                .map_err(|e| format!("delete pending fire failed: {e:?}"))?;
        }

        Ok(self.commit_or_inject(txn, write_options)?)
    }

    pub(in crate::domains::schedule::store) fn u64_to_u32_saturating(
        value: u64,
    ) -> Result<u32, String> {
        u32::try_from(value).map_err(|_| format!("column family id {value} exceeds u32"))
    }

    fn decode_definition_rows(
        definition_rows: Vec<(Vec<u8>, Vec<u8>)>,
        metadata_rows: &mut BTreeMap<String, (u64, Option<u64>, u64)>,
    ) -> Result<(), String> {
        for (key, value) in definition_rows {
            match (
                Self::decode_definition_key(&key),
                Self::decode_definition_value(&value),
            ) {
                (
                    Ok(route),
                    Ok(super::model::DecodedDefinitionRow {
                        next_fire_ms,
                        last_fire_ms,
                        executions_total,
                    }),
                ) => {
                    metadata_rows.insert(route, (next_fire_ms, last_fire_ms, executions_total));
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!(
                        "decode persisted schedule definition failed: {error}"
                    ));
                }
            }
        }

        Ok(())
    }

    fn decode_body_rows(
        body_rows: Vec<(Vec<u8>, Vec<u8>)>,
        body_definitions: &mut BTreeMap<
            String,
            (
                String,
                crate::domains::schedule::ScheduleDeliveryMode,
                Bytes,
            ),
        >,
    ) -> Result<(), String> {
        for (key, value) in body_rows {
            match (
                Self::decode_body_key(&key),
                Self::decode_definition_body_value(&value),
            ) {
                (Ok(route), Ok((cron, delivery_mode, payload))) => {
                    body_definitions.insert(route, (cron, delivery_mode, payload));
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode persisted schedule body failed: {error}"));
                }
            }
        }

        Ok(())
    }

    fn merge_metadata_rows(
        metadata_rows: BTreeMap<String, (u64, Option<u64>, u64)>,
        body_definitions: &BTreeMap<
            String,
            (
                String,
                crate::domains::schedule::ScheduleDeliveryMode,
                Bytes,
            ),
        >,
        schedules: &mut BTreeMap<String, PersistedSchedule>,
    ) -> Result<(), String> {
        for (route, (next_fire_ms, last_fire_ms, executions_total)) in metadata_rows {
            match body_definitions.get(&route) {
                Some((cron, delivery_mode, payload)) => {
                    schedules.insert(
                        route.clone(),
                        PersistedSchedule {
                            route,
                            cron: cron.clone(),
                            delivery_mode: *delivery_mode,
                            payload: payload.clone(),
                            next_fire_ms,
                            last_fire_ms,
                            executions_total,
                        },
                    );
                }
                None => {
                    return Err(format!(
                        "missing schedule body row for persisted definition: {route}"
                    ));
                }
            }
        }

        Ok(())
    }

    fn rewrite_due_index<'a>(
        write_tx: &mut cntryl_midge::Transaction,
        due_rows: Vec<(Vec<u8>, Vec<u8>)>,
        schedules: impl Iterator<Item = &'a PersistedSchedule>,
    ) -> Result<(), String> {
        Self::delete_rows(write_tx, due_rows, "delete stale due index failed")?;

        for schedule in schedules {
            let parsed = parse_concrete_schedule_route(&schedule.route)?;
            write_tx
                .put(
                    Self::encode_prefixed_timed_route_key_from_realm(
                        &parsed.realm,
                        schedule.next_fire_ms,
                        &schedule.route,
                        DUE_PREFIX,
                    ),
                    DUE_INDEX_VALUE.to_vec(),
                    None,
                )
                .map_err(|e| format!("rebuild due index failed: {e:?}"))?;
        }

        Ok(())
    }

    fn delete_rows(
        write_tx: &mut cntryl_midge::Transaction,
        rows: Vec<(Vec<u8>, Vec<u8>)>,
        error_prefix: &str,
    ) -> Result<(), String> {
        for (key, _) in rows {
            write_tx
                .delete(key)
                .map_err(|e| format!("{error_prefix}: {e:?}"))?;
        }

        Ok(())
    }

    #[cfg(test)]
    pub fn fail_next_commit_for_tests(&self) {
        self.fail_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn stall_next_commit_for_tests(&self) {
        self.stall_next_commit
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    #[cfg(test)]
    pub(in crate::domains::schedule::store) fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), SchedulePersistenceError> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected schedule store commit failure".to_string().into());
        }
        if self
            .stall_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err(SchedulePersistenceError::midge(
                "commit failed",
                cntryl_midge::MidgeError::WriteStall("runtime request queue is full".to_string()),
            ));
        }

        txn.commit(write_options)
            .map_err(|error| SchedulePersistenceError::midge("commit failed", error))
    }

    #[cfg(not(test))]
    // Keep the receiver so production and test builds share one call shape; test builds
    // use the store-local receiver to inject a commit failure deterministically.
    #[allow(clippy::unused_self)]
    pub(in crate::domains::schedule::store) fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), SchedulePersistenceError> {
        txn.commit(write_options)
            .map_err(|error| SchedulePersistenceError::midge("commit failed", error))
    }
}
