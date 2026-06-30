use super::model::{
    parse_concrete_schedule_route, BTreeMap, Bytes, PersistedPendingFireClaim, PersistedSchedule,
    ScheduleDefinitionData, ScheduleStore, WriteOptions, DEFINITION_PREFIX, DUE_INDEX_VALUE,
    DUE_PREFIX, LEGACY_INDEX_PREFIX, LEGACY_PREFIX, PENDING_FIRE_PREFIX,
};

impl ScheduleStore {
    /// Load authoritative durable schedule definitions, migrate any legacy TTL-backed
    /// rows that are still present, rebuild the full due index, and delete stale
    /// legacy rows and indexes for the current family.
    ///
    /// # Errors
    ///
    /// Returns an error when stored schedule rows cannot be read, decoded,
    /// normalized, or durably rewritten for the current family.
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
        let mut normalized_definitions = Vec::<PersistedSchedule>::new();
        let mut metadata_rows = BTreeMap::<String, (u64, Option<u64>, u64)>::new();

        let definition_rows = Self::scan_schedule_rows(&read_tx, DEFINITION_PREFIX)?;
        let body_rows = Self::scan_schedule_rows(&read_tx, super::model::BODY_PREFIX)?;
        let legacy_rows = Self::scan_schedule_rows(&read_tx, LEGACY_PREFIX)?;

        Self::decode_definition_rows(
            definition_rows,
            &mut schedules,
            &mut normalized_definitions,
            &mut metadata_rows,
        )?;

        let mut body_definitions = BTreeMap::<String, (String, Bytes)>::new();
        Self::decode_body_rows(body_rows, &mut body_definitions)?;

        Self::merge_metadata_rows(metadata_rows, &body_definitions, &mut schedules)?;

        Self::decode_legacy_rows(&legacy_rows, &mut schedules, &mut normalized_definitions)?;

        let due_rows = Self::scan_schedule_rows(&read_tx, DUE_PREFIX)?;
        let legacy_index_rows = Self::scan_schedule_rows(&read_tx, LEGACY_INDEX_PREFIX)?;
        drop(read_tx);

        let mut write_tx = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin migration tx failed: {e:?}"))?;

        Self::rewrite_normalized_definitions(&mut write_tx, &normalized_definitions)?;
        Self::rewrite_due_index(&mut write_tx, due_rows, schedules.values())?;
        Self::delete_rows(
            &mut write_tx,
            legacy_index_rows,
            "delete legacy schedule index failed",
        )?;
        Self::delete_rows(
            &mut write_tx,
            legacy_rows,
            "delete legacy schedule row failed",
        )?;

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
                (Ok((fire_ms, route)), Ok((claimed_at_ms, payload))) => {
                    pending.push(PersistedPendingFireClaim {
                        route,
                        payload,
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

        self.commit_or_inject(txn, write_options)
    }

    pub(in crate::domains::schedule::store) fn u64_to_u32_saturating(
        value: u64,
    ) -> Result<u32, String> {
        u32::try_from(value).map_err(|_| format!("column family id {value} exceeds u32"))
    }

    fn decode_definition_rows(
        definition_rows: Vec<(Vec<u8>, Vec<u8>)>,
        schedules: &mut BTreeMap<String, PersistedSchedule>,
        normalized_definitions: &mut Vec<PersistedSchedule>,
        metadata_rows: &mut BTreeMap<String, (u64, Option<u64>, u64)>,
    ) -> Result<(), String> {
        for (key, value) in definition_rows {
            match (
                Self::decode_definition_key(&key),
                Self::decode_definition_value(&value),
            ) {
                (
                    Ok(route),
                    Ok(super::model::DecodedDefinitionRow::Inline {
                        next_fire_ms,
                        cron,
                        payload,
                        last_fire_ms,
                        executions_total,
                    }),
                ) => {
                    let persisted = PersistedSchedule {
                        route: route.clone(),
                        cron,
                        payload,
                        next_fire_ms,
                        last_fire_ms,
                        executions_total,
                    };
                    normalized_definitions.push(persisted.clone());
                    schedules.insert(route, persisted);
                }
                (
                    Ok(route),
                    Ok(super::model::DecodedDefinitionRow::Metadata {
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
        body_definitions: &mut BTreeMap<String, (String, Bytes)>,
    ) -> Result<(), String> {
        for (key, value) in body_rows {
            match (
                Self::decode_body_key(&key),
                Self::decode_definition_body_value(&value),
            ) {
                (Ok(route), Ok((cron, payload))) => {
                    body_definitions.insert(route, (cron, payload));
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
        body_definitions: &BTreeMap<String, (String, Bytes)>,
        schedules: &mut BTreeMap<String, PersistedSchedule>,
    ) -> Result<(), String> {
        for (route, (next_fire_ms, last_fire_ms, executions_total)) in metadata_rows {
            match body_definitions.get(&route) {
                Some((cron, payload)) => {
                    schedules.insert(
                        route.clone(),
                        PersistedSchedule {
                            route,
                            cron: cron.clone(),
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

    fn decode_legacy_rows(
        legacy_rows: &[(Vec<u8>, Vec<u8>)],
        schedules: &mut BTreeMap<String, PersistedSchedule>,
        normalized_definitions: &mut Vec<PersistedSchedule>,
    ) -> Result<(), String> {
        for (key, value) in legacy_rows {
            match (
                Self::decode_legacy_key(key),
                Self::decode_legacy_value(value),
            ) {
                (Ok((next_fire_ms, route)), Ok((cron, payload))) => {
                    if schedules.contains_key(&route) {
                        continue;
                    }

                    let persisted = PersistedSchedule {
                        route: route.clone(),
                        cron,
                        payload,
                        next_fire_ms,
                        last_fire_ms: None,
                        executions_total: 0,
                    };
                    normalized_definitions.push(persisted.clone());
                    schedules.insert(route.clone(), persisted);
                }
                (Err(error), _) | (_, Err(error)) => {
                    return Err(format!("decode legacy schedule row failed: {error}"));
                }
            }
        }

        Ok(())
    }

    fn rewrite_normalized_definitions(
        write_tx: &mut cntryl_midge::Transaction,
        normalized_definitions: &[PersistedSchedule],
    ) -> Result<(), String> {
        for schedule in normalized_definitions {
            Self::put_schedule_definition(
                write_tx,
                &schedule.route,
                &ScheduleDefinitionData {
                    next_fire_ms: schedule.next_fire_ms,
                    last_fire_ms: schedule.last_fire_ms,
                    executions_total: schedule.executions_total,
                    cron: &schedule.cron,
                    payload: &schedule.payload,
                },
            )
            .map_err(|error| format!("import schedule definition failed: {error}"))?;
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
    pub(in crate::domains::schedule::store) fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        if self
            .fail_next_commit
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            return Err("injected schedule store commit failure".to_string());
        }

        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {e:?}"))
    }

    #[cfg(not(test))]
    #[allow(clippy::unused_self)]
    pub(in crate::domains::schedule::store) fn commit_or_inject(
        &self,
        txn: cntryl_midge::Transaction,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        txn.commit(write_options)
            .map_err(|e| format!("commit failed: {e:?}"))
    }
}
