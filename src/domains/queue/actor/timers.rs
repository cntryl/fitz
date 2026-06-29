use super::*;

impl QueueActor {
    pub(super) fn handle_inflight_expired(&mut self, id: MessageId) {
        let inflight = match self.inflight.get(&id) {
            Some(inflight) => inflight.clone(),
            None => return,
        };

        let now = self.clock.now_instant();

        if inflight.expires_at > now {
            return;
        }
        let now_epoch_ms = self.clock.now_epoch_ms();

        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);

        let (mut record, record_layout) = if let Some(cached) = self.records.get(&id) {
            (
                cached.clone(),
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok((record, layout)) => (record, layout),
                Err(e) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error_reason = %e,
                        "Failed to load queue message during redelivery"
                    );
                    self.schedule_inflight_retry(id, &inflight);
                    return;
                }
            }
        };

        record.attempts += 1;

        let is_dlq = if let Some(max) = self.max_attempts {
            record.attempts >= max
        } else {
            false
        };

        match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => {
                let has_split_record = match txn.get(&header_key) {
                    Ok(Some(_)) => true,
                    Ok(None) => false,
                    Err(e) => {
                        tracing::warn!(
                            queue = ?self.queue_key,
                            route_family = self.queue_key.family.as_u64(),
                            message_id = id.as_u64(),
                            error = ?e,
                            "Failed to inspect queue storage layout during redelivery"
                        );
                        self.schedule_inflight_retry(id, &inflight);
                        return;
                    }
                };
                let has_body_key = if has_split_record {
                    match txn.get(&body_key) {
                        Ok(Some(_)) => true,
                        Ok(None) => false,
                        Err(e) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                error = ?e,
                                "Failed to inspect queue body storage layout during redelivery"
                            );
                            false
                        }
                    }
                } else {
                    false
                };

                if is_dlq {
                    let dead_lettered_at_ms = now_epoch_ms;
                    let index_plan = self.plan_index_mutation_for_unavailable_message(id);
                    record.state = QueueState::Dlq;
                    record.ready_seq = None;
                    record.visible_at_ms = 0;
                    record.inflight_token = None;
                    record.inflight_expires_at_ms = None;
                    record.dead_lettered_at_ms = Some(dead_lettered_at_ms);
                    record.dlq_reason = Some(DlqReason::MaxAttemptsExceeded);
                    let write_result =
                        if matches!(record_layout, StoredRecordLayout::SplitHeaderBody)
                            && has_body_key
                        {
                            txn.put(
                                header_key.clone(),
                                Self::encode_record_header(&record),
                                None,
                            )
                            .map_err(|e| format!("Failed to write DLQ header: {e:?}"))
                        } else {
                            if record.body.is_none() {
                                match self.load_body_from_store(id) {
                                    Ok(body) => record.body = Some(body),
                                    Err(e) => {
                                        tracing::warn!(
                                            queue = ?self.queue_key,
                                            route_family = self.queue_key.family.as_u64(),
                                            message_id = id.as_u64(),
                                            error_reason = %e,
                                            "Failed to load queue body for DLQ transition"
                                        );
                                        self.schedule_inflight_retry(id, &inflight);
                                        return;
                                    }
                                }
                            }

                            self.write_record_as_split(&mut txn, id, &record, Some(record_layout))
                        };

                    if let Err(e) = write_result {
                        tracing::warn!(
                            queue = ?self.queue_key,
                            route_family = self.queue_key.family.as_u64(),
                            message_id = id.as_u64(),
                            error = ?e,
                            "Failed to persist queue DLQ record"
                        );
                        self.schedule_inflight_retry(id, &inflight);
                        return;
                    }

                    if let Err(error) = self.write_index_mutation_plan(
                        &mut txn,
                        id,
                        index_plan,
                        Some(dead_lettered_at_ms),
                    ) {
                        tracing::warn!(
                            queue = ?self.queue_key,
                            route_family = self.queue_key.family.as_u64(),
                            message_id = id.as_u64(),
                            error_reason = %error,
                            "Failed to update queue indexes during DLQ transition"
                        );
                        self.schedule_inflight_retry(id, &inflight);
                        return;
                    }

                    let update_start = Instant::now();
                    if let Err(e) =
                        Self::commit_redelivery_transaction(txn, self.commit_write_options)
                    {
                        tracing::warn!(
                            queue = ?self.queue_key,
                            route_family = self.queue_key.family.as_u64(),
                            message_id = id.as_u64(),
                            error = ?e,
                            "Failed to commit queue DLQ transition"
                        );
                        self.schedule_inflight_retry(id, &inflight);
                        return;
                    }

                    Self::observe_elapsed_us(
                        obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY,
                        update_start,
                    );
                    self.inflight.remove(&id);
                    self.update_cached_inflight_metadata(
                        id,
                        inflight.inflight_epoch,
                        None,
                        None,
                        record.last_inflight_at_ms,
                    );
                    self.apply_index_mutation_plan(id, index_plan, Some(dead_lettered_at_ms));
                    self.cache_record(
                        id,
                        record.metadata_only_from(),
                        StoredRecordLayout::SplitHeaderBody,
                    );
                    self.evict_cached_body(id);

                    tracing::info!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        attempts = record.attempts,
                        "Message moved to queue dead letter state"
                    );

                    Self::increment_counter(obs::METRIC_QUEUE_DLQ_TRANSITIONS);
                    return;
                }

                let write_result = if has_split_record {
                    match txn.get(&header_key) {
                        Ok(Some(bytes)) if !has_body_key && bytes.len() >= 16 => {
                            match Self::decode_legacy_record(bytes) {
                                Ok(mut embedded_record) => {
                                    embedded_record.attempts = record.attempts;
                                    embedded_record.visible_at_ms = record.visible_at_ms;
                                    let value = Self::encode_legacy_record(&embedded_record);
                                    txn.put(header_key.clone(), value, None)
                                }
                                Err(e) => {
                                    tracing::warn!(
                                        queue = ?self.queue_key,
                                        route_family = self.queue_key.family.as_u64(),
                                        message_id = id.as_u64(),
                                        error_reason = %e,
                                        "Failed to decode embedded queue message during redelivery"
                                    );
                                    self.schedule_inflight_retry(id, &inflight);
                                    return;
                                }
                            }
                        }
                        Ok(Some(_)) => {
                            let value = Self::encode_record_header(&record);
                            txn.put(header_key.clone(), value, None)
                        }
                        Ok(None) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                "Queue message disappeared during redelivery"
                            );
                            self.schedule_inflight_retry(id, &inflight);
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                error = ?e,
                                "Failed to read queue message during redelivery"
                            );
                            self.schedule_inflight_retry(id, &inflight);
                            return;
                        }
                    }
                } else {
                    match txn.get(&legacy_key) {
                        Ok(Some(bytes)) => match Self::decode_legacy_record(bytes) {
                            Ok(mut legacy_record) => {
                                legacy_record.attempts = record.attempts;
                                legacy_record.visible_at_ms = record.visible_at_ms;
                                let value = Self::encode_legacy_record(&legacy_record);
                                txn.put(legacy_key.clone(), value, None)
                            }
                            Err(e) => {
                                tracing::warn!(
                                    queue = ?self.queue_key,
                                    route_family = self.queue_key.family.as_u64(),
                                    message_id = id.as_u64(),
                                    error_reason = %e,
                                    "Failed to decode legacy queue message during redelivery"
                                );
                                self.schedule_inflight_retry(id, &inflight);
                                return;
                            }
                        },
                        Ok(None) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                "Legacy queue message disappeared during redelivery"
                            );
                            self.schedule_inflight_retry(id, &inflight);
                            return;
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                error = ?e,
                                "Failed to read legacy queue message during redelivery"
                            );
                            self.schedule_inflight_retry(id, &inflight);
                            return;
                        }
                    }
                };

                if let Err(e) = write_result {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?e,
                        "Failed to persist queue redelivery attempt update"
                    );
                    self.schedule_inflight_retry(id, &inflight);
                    return;
                }
                let update_start = Instant::now();
                if let Err(e) = Self::commit_redelivery_transaction(txn, self.commit_write_options)
                {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?e,
                        "Failed to commit queue redelivery retry transaction"
                    );
                    self.schedule_inflight_retry(id, &inflight);
                    return;
                }
                Self::observe_elapsed_us(obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY, update_start);
                Self::increment_counter(obs::METRIC_QUEUE_REDELIVERIES);
            }
            Err(e) => {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    message_id = id.as_u64(),
                    error = ?e,
                    "Failed to begin queue redelivery transaction"
                );
                self.schedule_inflight_retry(id, &inflight);
                return;
            }
        }

        self.inflight.remove(&id);

        self.cache_record(
            id,
            QueueRecord::metadata_only(record.attempts, record.visible_at_ms),
            record_layout,
        );
        self.update_cached_inflight_metadata(
            id,
            inflight.inflight_epoch,
            None,
            None,
            record.last_inflight_at_ms,
        );

        self.push_ready(id);
    }

    pub fn process_expired_timers(&mut self) {
        let now = self.clock.now_instant();

        while let Some(Reverse(expiry)) = self.timers.peek() {
            if expiry.expires_at > now {
                self.next_expiration_deadline = expiry.expires_at;
                break;
            }

            let expiry = self.timers.pop().unwrap().0;

            if let Some(inflight) = self.inflight.get(&expiry.id) {
                if inflight.inflight_epoch != expiry.inflight_epoch {
                    continue;
                }
            }

            self.handle_inflight_expired(expiry.id);
        }

        if self.timers.is_empty() {
            self.next_expiration_deadline = now + Duration::from_secs(3600);
        }
    }

    pub(super) fn schedule_inflight_retry(&mut self, id: MessageId, inflight: &Inflight) {
        let retry_at = self.clock.now_instant() + Duration::from_secs(1);
        self.timers.push(Reverse(InflightExpiry {
            id,
            inflight_epoch: inflight.inflight_epoch,
            expires_at: retry_at,
            expires_at_ms: inflight.expires_at_epoch_ms,
        }));
        if retry_at < self.next_expiration_deadline {
            self.next_expiration_deadline = retry_at;
        }
    }
}
