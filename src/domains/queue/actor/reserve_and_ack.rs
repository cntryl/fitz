use super::*;

impl QueueActor {
    pub fn handle_receive_for_session(
        &mut self,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        self.handle_receive_internal(Some(session_id), inflight_seconds, batch_size)
    }

    fn handle_receive_internal(
        &mut self,
        owner_session_id: Option<u64>,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        let batch_size = batch_size.unwrap_or(1);
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let inflight_duration = Duration::from_secs(inflight_seconds);
        let Some(expires_at) = now.checked_add(inflight_duration) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };

        let mut messages = Vec::with_capacity(self.ready.len().min(batch_size));

        for _ in 0..batch_size {
            let id = match self.ready.front().map(|entry| entry.id) {
                Some(id) => id,
                None => break, // No more messages
            };

            let (body, attempts) = match self.hydrate_record_for_receive(id) {
                Ok(record) => record,
                Err(e) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error_reason = %e,
                        "Failed to hydrate queue record for receive"
                    );
                    break;
                }
            };

            let id = match self.pop_ready() {
                Some(id) => id,
                None => break,
            };
            self.evict_cached_body(id);

            // Generate inflight token
            let token = Self::generate_token();
            let inflight_epoch = self
                .records
                .get(&id)
                .map(|record| record.inflight_epoch.saturating_add(1))
                .unwrap_or(1);
            let expires_at_epoch_ms =
                now_epoch_ms.saturating_add(inflight_seconds.saturating_mul(1_000));

            // Create inflight entry
            self.inflight.insert(
                id,
                Inflight {
                    token,
                    expires_at,
                    expires_at_epoch_ms,
                    owner_session_id,
                    attempts: attempts + 1,
                    inflight_epoch,
                },
            );
            self.update_cached_inflight_metadata(
                id,
                inflight_epoch,
                Some(token),
                Some(expires_at_epoch_ms),
                Some(now_epoch_ms),
            );

            // Schedule expiration timer
            self.timers.push(Reverse(InflightExpiry {
                id,
                inflight_epoch,
                expires_at,
                expires_at_ms: expires_at_epoch_ms,
            }));

            // Update deadline cache if this expiration is sooner
            if expires_at < self.next_expiration_deadline {
                self.next_expiration_deadline = expires_at;
            }

            // Build response message
            messages.push(ReservedMessage {
                id,
                body,
                token,
                inflight_seconds,
                attempts: attempts + 1, // First attempt is 1 (not 0)
            });
        }

        // If no messages were reserved, return an empty response (avoid NotFound).
        // Clients expect an empty slice when the queue is empty rather than an error.
        if messages.is_empty() {
            return QueueResponse::Received { messages };
        }

        QueueResponse::Received { messages }
    }

    /// Handle session-bound extend operation
    pub fn handle_extend_for_session(
        &mut self,
        session_id: u64,
        id: MessageId,
        token: u64,
        inflight_seconds: u64,
    ) -> QueueResponse {
        match self.inflight.get(&id) {
            Some(inflight) if inflight.owner_session_id == Some(session_id) => {
                self.handle_extend_authorized(id, token, inflight_seconds)
            }
            Some(_) => QueueResponse::NotFound,
            None => QueueResponse::NotFound,
        }
    }

    fn handle_extend_authorized(
        &mut self,
        id: MessageId,
        token: u64,
        inflight_seconds: u64,
    ) -> QueueResponse {
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();

        // Check if message is inflight
        let inflight = match self.inflight.get_mut(&id) {
            Some(inflight) => inflight,
            None => return QueueResponse::NotFound,
        };

        // Validate token
        if inflight.token != token {
            return QueueResponse::InvalidToken;
        }

        // Check if already expired
        if inflight.expires_at <= now {
            self.handle_inflight_expired(id);
            return QueueResponse::InflightExpired;
        }

        // Extend expiration
        let Some(new_expires_at) = now.checked_add(Duration::from_secs(inflight_seconds)) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };
        inflight.inflight_epoch = inflight.inflight_epoch.saturating_add(1);
        inflight.expires_at = new_expires_at;
        inflight.expires_at_epoch_ms =
            now_epoch_ms.saturating_add(inflight_seconds.saturating_mul(1_000));
        let inflight_epoch = inflight.inflight_epoch;
        let inflight_expires_at_ms = inflight.expires_at_epoch_ms;

        // Schedule new timer (old timer will be ignored when it fires)
        self.timers.push(Reverse(InflightExpiry {
            id,
            inflight_epoch,
            expires_at: new_expires_at,
            expires_at_ms: inflight_expires_at_ms,
        }));
        self.update_cached_inflight_metadata(
            id,
            inflight_epoch,
            Some(token),
            Some(inflight_expires_at_ms),
            Some(now_epoch_ms),
        );

        // Update deadline cache if this expiration is sooner
        if new_expires_at < self.next_expiration_deadline {
            self.next_expiration_deadline = new_expires_at;
        }

        QueueResponse::Extended
    }

    /// Handle session-bound acknowledge operation
    pub fn handle_ack_for_session(
        &mut self,
        session_id: u64,
        id: MessageId,
        token: u64,
    ) -> QueueResponse {
        match self.inflight.get(&id) {
            Some(inflight) if inflight.owner_session_id == Some(session_id) => {
                self.handle_ack_authorized(session_id, id, token)
            }
            Some(_) => QueueResponse::NotFound,
            None => self.handle_ack_authorized(session_id, id, token),
        }
    }

    fn handle_ack_authorized(
        &mut self,
        owner_session_id: u64,
        id: MessageId,
        token: u64,
    ) -> QueueResponse {
        use crate::utils::idempotency::{DedupIdentifier, DedupKey, Domain};

        let now = self.clock.now_instant();

        // Check deduplication store first (prevents re-processing completed operations)
        let dedup_key = DedupKey {
            realm: self.queue_key.realm.clone(),
            domain: Domain::Queue,
            identifier: DedupIdentifier::QueueComplete {
                family: self.queue_key.family.as_u64(),
                area: self.queue_key.area.clone(),
                resource: self.queue_key.resource.clone(),
                owner_session_id,
                message_id: id.as_u64(),
                token,
            },
        };

        if let Some(cached_response) = self.dedup_store.get(&dedup_key) {
            tracing::debug!(
                realm = %self.queue_key.realm,
                area = %self.queue_key.area,
                resource = %self.queue_key.resource,
                message_id = id.as_u64(),
                token = token,
                "Queue COMPLETE deduplicated (returning cached response)"
            );
            // Deserialize cached response
            match decode_cached_response(&cached_response) {
                Ok(resp) => return resp,
                Err(e) => {
                    tracing::warn!(
                        message_id = id.as_u64(),
                        token = token,
                        error = ?e,
                        "Failed to deserialize cached COMPLETE response, processing normally"
                    );
                }
            }
        }

        // Check if message is inflight
        let inflight = match self.inflight.get(&id) {
            Some(inflight) => inflight.clone(),
            None => {
                let response = QueueResponse::NotFound;
                // Cache negative response (prevents retries from hitting storage)
                if let Some(bytes) = encode_cached_response(&response) {
                    self.dedup_store.record(dedup_key, bytes);
                }
                return response;
            }
        };

        // Validate token
        if inflight.token != token {
            Self::increment_counter(obs::METRIC_QUEUE_COMPLETE_REJECTED);
            let response = QueueResponse::InvalidToken;
            // Don't cache invalid token - security: wrong token should fail every time
            return response;
        }

        // Check if already expired
        if inflight.expires_at <= now {
            self.handle_inflight_expired(id);
            Self::increment_counter(obs::METRIC_QUEUE_COMPLETE_REJECTED);
            let response = QueueResponse::InflightExpired;
            return response;
        }

        let (stored_layout, _record_visible_at_ms) = if let Some(record) = self.records.get(&id) {
            (
                self.record_layouts
                    .get(&id)
                    .copied()
                    .unwrap_or(StoredRecordLayout::EmbeddedHeader),
                record.visible_at_ms,
            )
        } else {
            match self.load_record_metadata_from_store(id) {
                Ok((record, layout)) => (layout, record.visible_at_ms),
                Err(_) => (StoredRecordLayout::EmbeddedHeader, 0),
            }
        };
        let index_plan = self.plan_index_mutation_for_unavailable_message(id);

        let cf_id = self.queue_key.family.id();
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);
        let legacy_key = self.cached_legacy_message_key(id);

        let commit_result = match self
            .store
            .begin_tx(cf_id, cntryl_midge::TransactionMode::ReadWrite)
        {
            Ok(mut txn) => match Self::delete_record_for_layout(
                &mut txn,
                stored_layout,
                header_key,
                body_key,
                legacy_key,
            ) {
                Err(e) => {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?e,
                        "Failed to delete queue message in transaction"
                    );
                    Err(format!("Failed to delete message {} in txn: {:?}", id, e))
                }
                Ok(()) => {
                    if let Err(error) =
                        self.write_index_mutation_plan(&mut txn, id, index_plan, None)
                    {
                        return QueueResponse::Error { message: error };
                    }

                    match Self::commit_ack_transaction(txn, self.commit_write_options) {
                        Ok(()) => {
                            self.apply_index_mutation_plan(id, index_plan, None);
                            Ok(())
                        }
                        Err(e) => {
                            tracing::warn!(
                                queue = ?self.queue_key,
                                route_family = self.queue_key.family.as_u64(),
                                message_id = id.as_u64(),
                                error_reason = %e,
                                "Failed to commit queue delete transaction"
                            );
                            Err(format!(
                                "Failed to commit delete txn for message {}: {}",
                                id, e
                            ))
                        }
                    }
                }
            },
            Err(e) => Err(format!(
                "Failed to begin tx to delete message {}: {:?}",
                id, e
            )),
        };

        if let Err(message) = commit_result {
            return QueueResponse::Error { message };
        }

        self.inflight.remove(&id);
        self.evict_cached_record(id);
        self.evict_cached_body(id);
        self.complete_success_window
            .record(self.clock.now_epoch_ms(), 1);

        let response = QueueResponse::Acked;

        // Cache successful completion response
        if let Some(bytes) = encode_cached_response(&response) {
            self.dedup_store.record(dedup_key, bytes);
        }

        tracing::info!(
            realm = %self.queue_key.realm,
            area = %self.queue_key.area,
            resource = %self.queue_key.resource,
            message_id = id.as_u64(),
            "Queue COMPLETE processed successfully"
        );

        response
    }
}
