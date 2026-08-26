//! Queue receive, lease extension, and acknowledgement processing.

use super::{
    decode_cached_response, encode_cached_response, obs, DlqReason, Duration, FxBuildHasher,
    HashMap, HashSet, Inflight, InflightExpiry, Instant, MessageId, PersistedIndexMutationPlan,
    PersistedReadyMutation, QueueActor, QueueCommit, QueueRecord, QueueResponse, QueueState,
    ReadyRange, ReservedMessage, Reverse, VecDeque,
};
use crate::utils::idempotency::{DedupIdentifier, DedupKey, Domain};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AckAuthorizationError {
    InvalidToken,
    Expired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReserveWireBudgetDecision {
    Reserve(usize),
    Skip,
    Stop,
}

fn validate_ack_authorization(
    inflight: &Inflight,
    token: u64,
    now: Instant,
) -> Result<(), AckAuthorizationError> {
    if inflight.token != token {
        return Err(AckAuthorizationError::InvalidToken);
    }
    if inflight.expires_at <= now {
        return Err(AckAuthorizationError::Expired);
    }
    Ok(())
}

struct AckBatchDelete {
    id: MessageId,
    dedup_key: DedupKey,
    index_plan: PersistedIndexMutationPlan,
    header_key: Vec<u8>,
    body_key: Vec<u8>,
}

impl QueueActor {
    pub fn handle_receive_for_session(
        &mut self,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
    ) -> QueueResponse {
        let mut response_bytes_remaining = usize::MAX;
        self.handle_receive_for_session_with_wire_budget(
            session_id,
            inflight_seconds,
            batch_size,
            &mut response_bytes_remaining,
            0,
        )
        .0
    }

    pub(crate) fn handle_receive_for_session_with_wire_budget(
        &mut self,
        session_id: u64,
        inflight_seconds: u64,
        batch_size: Option<usize>,
        response_bytes_remaining: &mut usize,
        message_wire_overhead_bytes: usize,
    ) -> (QueueResponse, bool) {
        self.handle_receive_internal(
            Some(session_id),
            inflight_seconds,
            batch_size.unwrap_or(1),
            response_bytes_remaining,
            message_wire_overhead_bytes,
        )
    }

    fn handle_receive_internal(
        &mut self,
        owner_session_id: Option<u64>,
        inflight_seconds: u64,
        batch_size: usize,
        response_bytes_remaining: &mut usize,
        message_wire_overhead_bytes: usize,
    ) -> (QueueResponse, bool) {
        let now = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();
        let (expires_at, expires_at_epoch_ms) =
            match Self::inflight_expiration(now, now_epoch_ms, inflight_seconds) {
                Ok(expiration) => expiration,
                Err(response) => return (response, false),
            };

        let mut messages = Vec::with_capacity(self.ready.len().min(batch_size));

        while messages.len() < batch_size {
            let Some(id) = self.ready.front().map(|entry| entry.id) else {
                break;
            };

            // Skip hydration (real storage I/O) when this message's fixed
            // per-response overhead alone already exceeds what's left of the
            // wire budget: no body size, however small, changes that outcome.
            // This only short-circuits once a prior message has already been
            // reserved this call - an untouched budget failing here instead
            // means the message may be fundamentally too large for any
            // response, which the divert check below can only decide once
            // it knows the body size.
            if !messages.is_empty() && message_wire_overhead_bytes > *response_bytes_remaining {
                return (QueueResponse::Received { messages }, true);
            }

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
                    if self.divert_ready_or_log(id, DlqReason::HydrationFailed, now_epoch_ms) {
                        continue;
                    }
                    break;
                }
            };

            let Some(next_attempts) = attempts.checked_add(1) else {
                if self.divert_ready_or_log(id, DlqReason::DeliveryAttemptsExhausted, now_epoch_ms)
                {
                    continue;
                }
                break;
            };
            let inflight_epoch = self
                .records
                .get(&id)
                .map_or(Some(1), |record| record.inflight_epoch.checked_add(1));
            let Some(inflight_epoch) = inflight_epoch else {
                if self.divert_ready_or_log(id, DlqReason::InflightEpochExhausted, now_epoch_ms) {
                    continue;
                }
                break;
            };

            let message_wire_bytes = match self.reserve_wire_budget_decision(
                id,
                body.len(),
                message_wire_overhead_bytes,
                *response_bytes_remaining,
                now_epoch_ms,
            ) {
                ReserveWireBudgetDecision::Reserve(bytes) => bytes,
                ReserveWireBudgetDecision::Skip => continue,
                ReserveWireBudgetDecision::Stop => {
                    return (QueueResponse::Received { messages }, true);
                }
            };

            let Some(id) = self.pop_ready() else {
                break;
            };
            *response_bytes_remaining -= message_wire_bytes;
            self.evict_cached_body(id);

            let token = Self::generate_token();

            self.inflight.insert(
                id,
                Inflight {
                    token,
                    expires_at,
                    expires_at_epoch_ms,
                    owner_session_id,
                    attempts: next_attempts,
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

            self.schedule_inflight_expiration(id, inflight_epoch, expires_at, expires_at_epoch_ms);

            // Build response message
            messages.push(ReservedMessage {
                id,
                body,
                token,
                inflight_seconds,
                attempts: next_attempts, // First attempt is 1 (not 0)
            });
        }

        (QueueResponse::Received { messages }, false)
    }

    fn schedule_inflight_expiration(
        &mut self,
        id: MessageId,
        inflight_epoch: u64,
        expires_at: Instant,
        expires_at_epoch_ms: u64,
    ) {
        self.timers.push(Reverse(InflightExpiry {
            id,
            inflight_epoch,
            expires_at,
            expires_at_ms: expires_at_epoch_ms,
        }));
        if expires_at < self.next_expiration_deadline {
            self.next_expiration_deadline = expires_at;
        }
    }

    fn reserve_wire_budget_decision(
        &mut self,
        id: MessageId,
        body_bytes: usize,
        message_wire_overhead_bytes: usize,
        response_bytes_remaining: usize,
        now_epoch_ms: u64,
    ) -> ReserveWireBudgetDecision {
        let message_wire_bytes = message_wire_overhead_bytes.saturating_add(body_bytes);
        if message_wire_bytes <= response_bytes_remaining {
            return ReserveWireBudgetDecision::Reserve(message_wire_bytes);
        }
        let empty_response_message_budget =
            crate::domains::queue::protocol::MAX_QUEUE_RESPONSE_PAYLOAD_BYTES
                - crate::domains::queue::protocol::RECEIVED_RESPONSE_HEADER_BYTES;
        // Dead-lettering is permanent, so it must only fire when *no* reserve
        // shape could ever carry this body. A wildcard reserve pays extra
        // per-message overhead (routing envelope + route string) that a
        // concrete reserve does not, so judging by this caller's overhead
        // would discard messages a concrete `RESERVE` delivers fine. Charge
        // the smallest possible shape instead, and let a wildcard caller that
        // cannot fit the message simply `Stop` and leave it ready.
        let smallest_possible_message_wire_bytes =
            crate::domains::queue::protocol::RESERVED_MESSAGE_WIRE_OVERHEAD_BYTES
                .saturating_add(body_bytes);
        if smallest_possible_message_wire_bytes > empty_response_message_budget
            && self.divert_ready_or_log(id, DlqReason::ReserveResponseTooLarge, now_epoch_ms)
        {
            return ReserveWireBudgetDecision::Skip;
        }
        ReserveWireBudgetDecision::Stop
    }

    fn divert_ready_or_log(
        &mut self,
        id: MessageId,
        reason: DlqReason,
        dead_lettered_at_ms: u64,
    ) -> bool {
        if let Err(error) = self.divert_ready_to_dlq(id, reason, dead_lettered_at_ms) {
            tracing::error!(
                queue = ?self.queue_key,
                message_id = id.as_u64(),
                error_reason = %error,
                "Failed to divert undeliverable queue message"
            );
            return false;
        }
        true
    }

    fn divert_ready_to_dlq(
        &mut self,
        id: MessageId,
        reason: DlqReason,
        dead_lettered_at_ms: u64,
    ) -> Result<(), String> {
        let ready_entry = self
            .ready
            .front()
            .copied()
            .filter(|entry| entry.id == id)
            .ok_or_else(|| format!("Message {id} is no longer ready"))?;
        let mut record = self.records.get(&id).cloned().unwrap_or_else(|| {
            QueueRecord::enqueued(id, 0, true, ready_entry.ready_enqueued_at_ms)
        });
        record.state = QueueState::Dlq;
        record.ready_seq = None;
        record.visible_at_ms = 0;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.dead_lettered_at_ms = Some(dead_lettered_at_ms);
        record.dlq_reason = Some(reason);

        let index_plan = self.plan_index_mutation_for_unavailable_message(id);
        let mut txn = self
            .store
            .begin_tx(
                self.queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("Failed to begin ready diversion for {id}: {error:?}"))?;
        txn.put(
            self.cached_header_key(id),
            Self::encode_record_header(&record),
            None,
        )
        .map_err(|error| format!("Failed to write diverted queue header for {id}: {error:?}"))?;
        self.write_index_mutation_plan(&mut txn, id, index_plan, Some(dead_lettered_at_ms))?;
        txn.commit(self.commit_write_options)
            .map_err(|error| format!("Failed to commit ready diversion for {id}: {error:?}"))?;

        let popped = self.pop_ready();
        debug_assert_eq!(popped, Some(id));
        self.apply_index_mutation_plan(id, index_plan, Some(dead_lettered_at_ms));
        self.cache_record(id, record.metadata_only_from());
        self.evict_cached_body(id);
        Self::increment_counter(obs::METRIC_QUEUE_DLQ_TRANSITIONS);
        if reason == DlqReason::HydrationFailed {
            Self::increment_counter(obs::METRIC_QUEUE_HYDRATION_DLQ_TRANSITIONS);
        }
        Ok(())
    }

    fn inflight_expiration(
        now: Instant,
        now_epoch_ms: u64,
        inflight_seconds: u64,
    ) -> Result<(Instant, u64), QueueResponse> {
        let Some(expires_at) = now.checked_add(Duration::from_secs(inflight_seconds)) else {
            return Err(QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            });
        };
        let Some(inflight_duration_ms) = inflight_seconds.checked_mul(1_000) else {
            return Err(QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            });
        };
        let Some(expires_at_epoch_ms) = now_epoch_ms.checked_add(inflight_duration_ms) else {
            return Err(QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            });
        };

        Ok((expires_at, expires_at_epoch_ms))
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
            Some(_) | None => QueueResponse::NotFound,
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
        let Some(inflight) = self.inflight.get_mut(&id) else {
            return QueueResponse::NotFound;
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
        let Some(inflight_duration_ms) = inflight_seconds.checked_mul(1_000) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };
        let Some(inflight_expires_at_ms) = now_epoch_ms.checked_add(inflight_duration_ms) else {
            return QueueResponse::BadRequest {
                reason: "inflight_seconds is too large".to_string(),
            };
        };
        let Some(inflight_epoch) = inflight.inflight_epoch.checked_add(1) else {
            return QueueResponse::Error {
                message: "queue inflight epoch exhausted".to_string(),
            };
        };
        inflight.inflight_epoch = inflight_epoch;
        inflight.expires_at = new_expires_at;
        inflight.expires_at_epoch_ms = inflight_expires_at_ms;

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
            // A missing live entry may be a retry of an already-completed acknowledgement;
            // intentionally fall through so the dedup cache can return the original response.
            None => self.handle_ack_authorized(session_id, id, token),
        }
    }

    pub fn handle_ack_batch_for_session(
        &mut self,
        session_id: u64,
        acknowledgements: &[(MessageId, u64)],
    ) -> Vec<QueueResponse> {
        if acknowledgements.is_empty() {
            return Vec::new();
        }
        if acknowledgements.len() == 1 {
            // This fast path aliases the single-ack implementation and must remain behaviorally identical.
            let (id, token) = acknowledgements[0];
            return vec![self.handle_ack_for_session(session_id, id, token)];
        }

        let now = self.clock.now_instant();
        let Some(deletes) = self.plan_ack_batch(session_id, acknowledgements, now) else {
            return acknowledgements
                .iter()
                .map(|&(id, token)| self.handle_ack_for_session(session_id, id, token))
                .collect();
        };

        if let Err(message) = self.commit_ack_batch_delete(&deletes) {
            return vec![QueueResponse::Error { message }; acknowledgements.len()];
        }

        for delete in deletes {
            self.apply_index_mutation_plan(delete.id, delete.index_plan, None);
            let _ = self.finish_ack_success(delete.id, delete.dedup_key);
        }

        vec![QueueResponse::Acked; acknowledgements.len()]
    }

    fn handle_ack_authorized(
        &mut self,
        owner_session_id: u64,
        id: MessageId,
        token: u64,
    ) -> QueueResponse {
        // Check deduplication store first (prevents re-processing completed operations)
        let dedup_key = self.ack_response_dedup_key(owner_session_id, id, token);

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

        let now = self.clock.now_instant();
        let Some(inflight) = self.load_inflight_for_ack(id, &dedup_key) else {
            return QueueResponse::NotFound;
        };

        if let Err(rejection) = validate_ack_authorization(&inflight, token, now) {
            Self::increment_counter(obs::METRIC_QUEUE_COMPLETE_REJECTED);
            return match rejection {
                // Invalid tokens are deliberately not cached: every attempt must fail.
                AckAuthorizationError::InvalidToken => QueueResponse::InvalidToken,
                AckAuthorizationError::Expired => {
                    self.handle_inflight_expired(id);
                    QueueResponse::InflightExpired
                }
            };
        }

        let index_plan = self.plan_index_mutation_for_unavailable_message(id);
        if let Err(message) = self.commit_ack_delete(id, index_plan) {
            return QueueResponse::Error { message };
        }

        self.finish_ack_success(id, dedup_key)
    }

    fn plan_ack_batch(
        &self,
        owner_session_id: u64,
        acknowledgements: &[(MessageId, u64)],
        now: Instant,
    ) -> Option<Vec<AckBatchDelete>> {
        let mut seen = HashSet::with_capacity(acknowledgements.len());
        let mut staged_ready_shards = self.persisted_ready_shards.clone();
        let mut staged_ready_count = self.persisted_ready_count;
        let mut staged_delayed = self.persisted_delayed.clone();
        let mut staged_next_delayed_visibility = self.persisted_next_delayed_visibility_ms;
        let mut deletes = Vec::with_capacity(acknowledgements.len());

        for &(id, token) in acknowledgements {
            if !seen.insert(id) {
                return None;
            }

            let dedup_key = self.ack_response_dedup_key(owner_session_id, id, token);
            if self.dedup_store.get(&dedup_key).is_some() {
                return None;
            }

            let inflight = self.inflight.get(&id)?;
            if inflight.owner_session_id != Some(owner_session_id)
                || inflight.token != token
                || inflight.expires_at <= now
            {
                return None;
            }

            let ready_mutation = Self::plan_ready_index_mutation(&staged_ready_shards, id);
            if let Some((shard, mutation)) = ready_mutation {
                Self::apply_staged_ready_mutation(
                    &mut staged_ready_shards,
                    &mut staged_ready_count,
                    shard,
                    mutation,
                );
            }

            let delayed_index_delete = Self::stage_delayed_mutation(
                &mut staged_delayed,
                &mut staged_next_delayed_visibility,
                id,
            );

            deletes.push(AckBatchDelete {
                id,
                dedup_key,
                index_plan: PersistedIndexMutationPlan {
                    ready_mutation,
                    delayed_index_delete,
                    staged_ready_count,
                    staged_delayed_count: staged_delayed.len(),
                    staged_next_delayed_visibility,
                },
                header_key: self.cached_header_key(id),
                body_key: self.cached_body_key(id),
            });
        }

        Some(deletes)
    }

    fn apply_staged_ready_mutation(
        staged_ready_shards: &mut [VecDeque<ReadyRange>],
        staged_ready_count: &mut usize,
        shard: usize,
        mutation: PersistedReadyMutation,
    ) {
        let removed_len = match mutation {
            PersistedReadyMutation::Delete { removed }
            | PersistedReadyMutation::Replace { removed, .. }
            | PersistedReadyMutation::Split { removed, .. } => Self::range_len(removed),
        };
        let inserted_len = match mutation {
            PersistedReadyMutation::Delete { .. } => 0,
            PersistedReadyMutation::Replace { inserted, .. } => Self::range_len(inserted),
            PersistedReadyMutation::Split { left, right, .. } => {
                Self::range_len(left) + Self::range_len(right)
            }
        };

        Self::apply_ready_index_mutation_to_shards(staged_ready_shards, shard, mutation);
        *staged_ready_count = staged_ready_count
            .saturating_sub(removed_len)
            .saturating_add(inserted_len);
    }

    fn stage_delayed_mutation(
        staged_delayed: &mut HashMap<MessageId, u64, FxBuildHasher>,
        staged_next_delayed_visibility: &mut Option<u64>,
        id: MessageId,
    ) -> Option<u64> {
        let removed = staged_delayed.remove(&id);
        if removed.is_some() {
            *staged_next_delayed_visibility = staged_delayed.values().copied().min();
        }
        removed
    }

    fn ack_response_dedup_key(&self, owner_session_id: u64, id: MessageId, token: u64) -> DedupKey {
        DedupKey {
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
        }
    }

    fn load_inflight_for_ack(&mut self, id: MessageId, dedup_key: &DedupKey) -> Option<Inflight> {
        if let Some(inflight) = self.inflight.get(&id) {
            return Some(inflight.clone());
        }

        let response = QueueResponse::NotFound;
        if let Some(bytes) = encode_cached_response(&response) {
            self.dedup_store.record(dedup_key.clone(), bytes);
        }

        None
    }

    fn commit_ack_delete(
        &mut self,
        id: MessageId,
        index_plan: PersistedIndexMutationPlan,
    ) -> Result<(), String> {
        let header_key = self.cached_header_key(id);
        let body_key = self.cached_body_key(id);

        match self.store.begin_tx(
            self.queue_key.family.id(),
            cntryl_midge::TransactionMode::ReadWrite,
        ) {
            Ok(mut txn) => {
                if let Err(error) = Self::delete_record(&mut txn, header_key, body_key) {
                    tracing::warn!(
                        queue = ?self.queue_key,
                        route_family = self.queue_key.family.as_u64(),
                        message_id = id.as_u64(),
                        error = ?error,
                        "Failed to delete queue message in transaction"
                    );
                    return Err(format!("Failed to delete message {id} in txn: {error:?}"));
                }

                self.write_index_mutation_plan(&mut txn, id, index_plan, None)?;
                Self::commit_transaction(txn, self.commit_write_options, QueueCommit::Ack)
                    .map_err(|error| {
                        tracing::warn!(
                            queue = ?self.queue_key,
                            route_family = self.queue_key.family.as_u64(),
                            message_id = id.as_u64(),
                            error_reason = %error,
                            "Failed to commit queue delete transaction"
                        );
                        format!("Failed to commit delete txn for message {id}: {error}")
                    })?;
                self.apply_index_mutation_plan(id, index_plan, None);
                Ok(())
            }
            Err(error) => Err(format!(
                "Failed to begin tx to delete message {id}: {error:?}"
            )),
        }
    }

    fn commit_ack_batch_delete(&self, deletes: &[AckBatchDelete]) -> Result<(), String> {
        let mut txn = self
            .store
            .begin_tx(
                self.queue_key.family.id(),
                cntryl_midge::TransactionMode::ReadWrite,
            )
            .map_err(|error| format!("Failed to begin queue ack batch tx: {error:?}"))?;

        for delete in deletes {
            Self::delete_record(&mut txn, delete.header_key.clone(), delete.body_key.clone())
                .map_err(|error| {
                    format!(
                        "Failed to delete message {} in queue ack batch tx: {error:?}",
                        delete.id
                    )
                })?;
            self.write_index_mutation_plan(&mut txn, delete.id, delete.index_plan, None)?;
        }

        Self::commit_transaction(txn, self.commit_write_options, QueueCommit::Ack).map_err(
            |error| {
                tracing::warn!(
                    queue = ?self.queue_key,
                    route_family = self.queue_key.family.as_u64(),
                    error_reason = %error,
                    ack_count = deletes.len(),
                    "Failed to commit queue ack batch transaction"
                );
                format!("Failed to commit queue ack batch transaction: {error}")
            },
        )
    }

    fn finish_ack_success(&mut self, id: MessageId, dedup_key: DedupKey) -> QueueResponse {
        self.inflight.remove(&id);
        self.evict_cached_record(id);
        self.evict_cached_body(id);
        self.complete_success_window
            .record(self.clock.now_epoch_ms(), 1);

        let response = QueueResponse::Acked;
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

#[cfg(test)]
mod authorization_tests {
    use super::*;

    #[test]
    fn should_reject_wrong_ack_token_without_transaction() {
        // Arrange
        let now = Instant::now();
        let inflight = Inflight {
            token: 7,
            expires_at: now + Duration::from_secs(30),
            expires_at_epoch_ms: 30_000,
            owner_session_id: Some(1),
            attempts: 1,
            inflight_epoch: 1,
        };

        // Act
        let result = validate_ack_authorization(&inflight, 8, now);

        // Assert
        assert_eq!(result, Err(AckAuthorizationError::InvalidToken));
    }
}
