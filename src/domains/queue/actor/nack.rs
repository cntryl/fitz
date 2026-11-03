//! Atomically relinquish a live reservation without consuming another attempt.

use super::{
    decode_cached_response, encode_cached_response, DelayedMessage, Duration, MessageId,
    QueueActor, QueueCommit, QueueResponse, QueueState, Reverse,
};
use crate::utils::idempotency::{DedupIdentifier, DedupKey, Domain};

impl QueueActor {
    pub fn handle_nack_for_session(
        &mut self,
        session_id: u64,
        id: MessageId,
        token: u64,
        visibility_seconds: u64,
    ) -> QueueResponse {
        if self.storage_outcome_is_unknown() {
            return Self::unknown_outcome_response();
        }
        let dedup_key = self.nack_dedup_key(session_id, id, token);
        if self.dedup_store.get(&dedup_key).is_some_and(|bytes| {
            matches!(decode_cached_response(&bytes), Ok(QueueResponse::Nacked))
        }) {
            return QueueResponse::Nacked;
        }
        let Some(inflight) = self.inflight.get(&id).cloned() else {
            return QueueResponse::NotFound;
        };
        if inflight.owner_session_id != Some(session_id) {
            return QueueResponse::NotFound;
        }
        if inflight.token != token {
            return QueueResponse::InvalidToken;
        }
        match self.message_is_live(id) {
            Ok(true) => {}
            Ok(false) => return QueueResponse::NotFound,
            Err(error) => return error.into_response(),
        }
        let now = self.clock.now_instant();
        let now_ms = self.clock.now_epoch_ms();
        if inflight.expires_at <= now {
            self.handle_inflight_expired(id);
            return QueueResponse::InflightExpired;
        }
        let (visible_at, visible_ms) = match Self::nack_visibility(visibility_seconds, now, now_ms)
        {
            Ok(visibility) => visibility,
            Err(response) => return response,
        };
        let mut record = match self.current_record(id) {
            Ok(Some(record)) => record,
            Ok(None) => return QueueResponse::NotFound,
            Err(message) => return QueueResponse::Error { message },
        };
        let txn = match self.persistence.store.begin(
            self.queue_key.family.id(),
            super::recovery_store::QueueTransactionMode::ReadWrite,
        ) {
            Ok(txn) => txn,
            Err(error) => {
                return super::mutation_storage::QueueMutationError::from(error).into_response()
            }
        };
        if self
            .max_attempts
            .is_some_and(|limit| record.attempts >= limit)
        {
            if !self.persist_dlq_transition(id, &inflight, &mut record, txn, now_ms) {
                return QueueResponse::Error {
                    message: "Failed to dead-letter relinquished reservation".to_string(),
                };
            }
            return self.finish_nack(dedup_key);
        }

        let delayed = visibility_seconds > 0;
        let mut plan = self.plan_index_mutation_for_unavailable_message(id);
        record.state = if delayed {
            QueueState::Delayed
        } else {
            QueueState::Ready
        };
        record.visible_at_ms = if delayed { visible_ms } else { 0 };
        record.ready_seq = (!delayed).then_some(self.next_ready_seq);
        record.inflight_epoch = inflight.inflight_epoch;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        if delayed {
            plan.staged_delayed_count += 1;
            plan.staged_next_delayed_visibility = Some(
                plan.staged_next_delayed_visibility
                    .map_or(visible_ms, |current| current.min(visible_ms)),
            );
        }
        if let Err(error) = self.commit_nack(txn, id, &record, plan, delayed.then_some(visible_ms))
        {
            return error.into_response();
        }
        self.inflight.remove(&id);
        self.cache_record(id, record.metadata_only_from());
        if delayed {
            self.apply_index_mutation_plan(id, plan, None);
            self.insert_persisted_delayed(id, visible_ms);
            self.delayed.push(Reverse(DelayedMessage {
                id,
                enqueue_seq: record.enqueue_seq,
                visible_at,
                visible_at_ms: visible_ms,
            }));
            self.next_delayed_deadline = self.next_delayed_deadline.min(visible_at);
        } else {
            self.push_ready(id);
            self.needs_wake_waiters = true;
        }
        self.finish_nack(dedup_key)
    }

    fn commit_nack(
        &self,
        mut txn: super::recovery_store::QueueTransaction,
        id: MessageId,
        record: &super::QueueRecord,
        plan: super::PersistedIndexMutationPlan,
        visible_ms: Option<u64>,
    ) -> Result<(), super::mutation_storage::QueueMutationError> {
        let prepare = |txn: &mut super::recovery_store::QueueTransaction| -> Result<(), String> {
            let ttl = self.record_ttl_seconds(record);
            txn.put(
                self.cached_header_key(id),
                Self::encode_record_header(record),
                ttl,
            )
            .map_err(|error| format!("Failed to write relinquished queue header: {error:?}"))?;
            if let Some(visible_ms) = visible_ms {
                self.write_index_mutation_plan(txn, id, plan, None)?;
                txn.put(self.delayed_index_key(visible_ms, id), Vec::new(), ttl)
                    .map_err(|error| format!("Failed to delay relinquished message: {error:?}"))?;
            }
            Ok(())
        };
        if let Err(message) = prepare(&mut txn) {
            return Err(message.into());
        }
        self.commit_prepared_with_admission_retry(txn, QueueCommit::Redelivery, prepare)?;
        Ok(())
    }

    fn nack_dedup_key(&self, session_id: u64, id: MessageId, token: u64) -> DedupKey {
        DedupKey {
            realm: self.queue_key.realm.clone(),
            domain: Domain::Queue,
            identifier: DedupIdentifier::QueueNack {
                family: self.queue_key.family.as_u64(),
                area: self.queue_key.area.clone(),
                resource: self.queue_key.resource.clone(),
                owner_session_id: session_id,
                message_id: id.as_u64(),
                token,
            },
        }
    }

    fn nack_visibility(
        seconds: u64,
        now: super::Instant,
        now_ms: u64,
    ) -> Result<(super::Instant, u64), QueueResponse> {
        let invalid = || QueueResponse::BadRequest {
            reason: "visibility_seconds is too large".to_string(),
        };
        let delay_ms = seconds.checked_mul(1_000).ok_or_else(invalid)?;
        let visible_ms = now_ms.checked_add(delay_ms).ok_or_else(invalid)?;
        let visible_at = now
            .checked_add(Duration::from_millis(delay_ms))
            .ok_or_else(invalid)?;
        Ok((visible_at, visible_ms))
    }

    fn finish_nack(&self, dedup_key: DedupKey) -> QueueResponse {
        let response = QueueResponse::Nacked;
        if let Some(bytes) = encode_cached_response(&response) {
            self.dedup_store.record(dedup_key, bytes);
        }
        response
    }
}
