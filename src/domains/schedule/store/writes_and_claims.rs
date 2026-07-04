use super::model::{
    parse_concrete_schedule_route, ScheduleBatchInsert, ScheduleDefinitionData, ScheduleFireClaim,
    ScheduleInsert, SchedulePendingFireClaimAck, ScheduleStore, WriteOptions, BODY_PREFIX,
    DEFINITION_PREFIX, DUE_PREFIX, PENDING_FIRE_PREFIX,
};

impl ScheduleStore {
    /// # Errors
    ///
    /// Returns an error when the route cannot be parsed, any write fails, or
    /// the transaction commit fails.
    pub fn insert(
        &self,
        cf_id: u64,
        schedule: ScheduleInsert<'_>,
        write_options: WriteOptions,
    ) -> Result<Vec<u8>, String> {
        let parsed = parse_concrete_schedule_route(schedule.route)?;
        let due_key = Self::encode_prefixed_timed_route_key_from_realm(
            &parsed.realm,
            schedule.next_fire_ms,
            schedule.route,
            DUE_PREFIX,
        );
        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;

        let mut txn = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {e:?}"))?;

        Self::put_schedule_definition(
            &mut txn,
            schedule.route,
            &ScheduleDefinitionData {
                next_fire_ms: schedule.next_fire_ms,
                last_fire_ms: schedule.last_fire_ms,
                executions_total: schedule.executions_total,
                cron: schedule.cron,
                payload: schedule.payload,
            },
        )?;

        if let Some(previous_fire_ms) = schedule.previous_fire_ms {
            if previous_fire_ms != schedule.next_fire_ms {
                txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                    &parsed.realm,
                    previous_fire_ms,
                    schedule.route,
                    DUE_PREFIX,
                ))
                .map_err(|e| format!("delete previous due key failed: {e:?}"))?;
            }
        }

        // The durable definition row is authoritative. Live actors use their in-memory
        // ready heap and `load_all()` rebuilds the due index on restart.

        self.commit_or_inject(txn, write_options)?;
        Ok(due_key)
    }

    /// # Errors
    ///
    /// Returns an error when any route cannot be parsed, any write fails, or
    /// the transaction commit fails.
    pub fn insert_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleBatchInsert],
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

        for item in items {
            let parsed = parse_concrete_schedule_route(&item.route)?;
            Self::put_schedule_definition(
                &mut txn,
                &item.route,
                &ScheduleDefinitionData {
                    next_fire_ms: item.next_fire_ms,
                    last_fire_ms: item.last_fire_ms,
                    executions_total: item.executions_total,
                    cron: &item.cron,
                    payload: &item.payload,
                },
            )?;

            if let Some(previous_fire_ms) = item.previous_fire_ms {
                if previous_fire_ms != item.next_fire_ms {
                    txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                        &parsed.realm,
                        previous_fire_ms,
                        &item.route,
                        DUE_PREFIX,
                    ))
                    .map_err(|e| format!("delete previous due key failed: {e:?}"))?;
                }
            }
        }

        self.commit_or_inject(txn, write_options)
    }

    /// # Errors
    ///
    /// Returns an error when any claimed route cannot be parsed, any write
    /// fails, or the transaction commit fails.
    pub fn claim_due_batch(
        &self,
        cf_id: u64,
        items: &[ScheduleFireClaim<'_>],
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

        for item in items {
            let parsed = item.route_parts;
            Self::put_definition_metadata_from_parts(
                &mut txn,
                parsed,
                item.next_fire_ms,
                item.last_fire_ms,
                item.executions_total,
            )?;
            let pending_fire_key = Self::encode_prefixed_timed_route_key_from_parts(
                &parsed.realm,
                item.previous_fire_ms,
                &parsed.area,
                &parsed.resource,
                &parsed.operation,
                PENDING_FIRE_PREFIX,
            );

            if item.previous_fire_ms != item.next_fire_ms {
                txn.delete(Self::encode_prefixed_timed_route_key_from_parts(
                    &parsed.realm,
                    item.previous_fire_ms,
                    &parsed.area,
                    &parsed.resource,
                    &parsed.operation,
                    DUE_PREFIX,
                ))
                .map_err(|e| format!("delete previous due key failed: {e:?}"))?;
            }
            txn.put(
                pending_fire_key,
                Self::encode_pending_fire_value(item.payload, item.claimed_at_ms),
                None,
            )
            .map_err(|e| format!("put pending fire failed: {e:?}"))?;
        }

        self.commit_or_inject(txn, write_options)
    }

    /// # Errors
    ///
    /// Returns an error when any pending-claim route cannot be parsed, any
    /// write fails, or the transaction commit fails.
    pub fn ack_pending_fire_claims(
        &self,
        cf_id: u64,
        items: &[SchedulePendingFireClaimAck<'_>],
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

        for item in items {
            let parsed = parse_concrete_schedule_route(item.route)?;
            txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
                &parsed.realm,
                item.fire_ms,
                item.route,
                PENDING_FIRE_PREFIX,
            ))
            .map_err(|e| format!("delete pending fire failed: {e:?}"))?;

            if let Some(definition) = &item.definition {
                Self::put_definition_metadata(
                    &mut txn,
                    item.route,
                    definition.next_fire_ms,
                    Some(item.acknowledged_at_ms),
                    definition.executions_total,
                )
                .map_err(|error| {
                    format!("update schedule acknowledgement state failed: {error}")
                })?;
            }
        }

        self.commit_or_inject(txn, write_options)
    }

    /// # Errors
    ///
    /// Returns an error when the route cannot be parsed or the delete/commit
    /// operation fails.
    pub fn delete_current(
        &self,
        cf_id: u64,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let parsed = parse_concrete_schedule_route(route)?;
        self.delete_current_with_realm(cf_id, &parsed.realm, route, next_fire_ms, write_options)
    }

    pub(crate) fn delete_current_with_realm(
        &self,
        cf_id: u64,
        realm: &str,
        route: &str,
        next_fire_ms: u64,
        write_options: WriteOptions,
    ) -> Result<(), String> {
        let cf_id_u32 = Self::u64_to_u32_saturating(cf_id)?;
        let mut txn = self
            .db
            .begin_tx(cf_id_u32, cntryl_midge::TransactionMode::ReadWrite)
            .map_err(|e| format!("begin_tx failed: {e:?}"))?;

        txn.delete(Self::encode_prefixed_route_key_from_realm(
            realm,
            route,
            DEFINITION_PREFIX,
        ))
        .map_err(|e| format!("delete schedule definition failed: {e:?}"))?;
        txn.delete(Self::encode_prefixed_route_key_from_realm(
            realm,
            route,
            BODY_PREFIX,
        ))
        .map_err(|e| format!("delete schedule body failed: {e:?}"))?;
        txn.delete(Self::encode_prefixed_timed_route_key_from_realm(
            realm,
            next_fire_ms,
            route,
            DUE_PREFIX,
        ))
        .map_err(|e| format!("delete schedule due index failed: {e:?}"))?;

        self.commit_or_inject(txn, write_options)
    }
}
