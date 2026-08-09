//! Dead-letter transition policy and persistence orchestration.

use super::{
    obs, DlqReason, Inflight, Instant, MessageId, QueueActor, QueueCommit, QueueRecord, QueueState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RedeliveryOutcome {
    Retry,
    DeadLetter,
}

pub(super) fn decide_redelivery(
    max_attempts: Option<u32>,
    record: &QueueRecord,
    _inflight: &Inflight,
) -> RedeliveryOutcome {
    if max_attempts.is_some_and(|limit| record.attempts >= limit) {
        RedeliveryOutcome::DeadLetter
    } else {
        RedeliveryOutcome::Retry
    }
}

impl QueueActor {
    pub(super) fn persist_dlq_transition(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &mut QueueRecord,
        mut txn: cntryl_midge::Transaction,
        now_epoch_ms: u64,
    ) -> bool {
        let index_plan = self.plan_index_mutation_for_unavailable_message(id);
        Self::transition_record_to_dlq(record, now_epoch_ms);

        if !self.persist_dlq_record(id, inflight, record, &mut txn) {
            return false;
        }

        if let Err(error) =
            self.write_index_mutation_plan(&mut txn, id, index_plan, Some(now_epoch_ms))
        {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error_reason = %error,
                "Failed to update queue indexes during DLQ transition"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        let update_start = Instant::now();
        if let Err(error) =
            Self::commit_transaction(txn, self.commit_write_options, QueueCommit::Redelivery)
        {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to commit queue DLQ transition"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        Self::observe_elapsed_us(obs::METRIC_QUEUE_REDELIVERY_UPDATE_LATENCY, update_start);
        self.inflight.remove(&id);
        self.update_cached_inflight_metadata(
            id,
            inflight.inflight_epoch,
            None,
            None,
            record.last_inflight_at_ms,
        );
        self.apply_index_mutation_plan(id, index_plan, Some(now_epoch_ms));
        self.cache_record(id, record.metadata_only_from());
        self.evict_cached_body(id);

        tracing::info!(
            queue = ?self.queue_key,
            route_family = self.queue_key.family.as_u64(),
            message_id = id.as_u64(),
            attempts = record.attempts,
            "Message moved to queue dead letter state"
        );
        Self::increment_counter(obs::METRIC_QUEUE_DLQ_TRANSITIONS);
        true
    }

    fn transition_record_to_dlq(record: &mut QueueRecord, dead_lettered_at_ms: u64) {
        record.state = QueueState::Dlq;
        record.ready_seq = None;
        record.visible_at_ms = 0;
        record.inflight_token = None;
        record.inflight_expires_at_ms = None;
        record.dead_lettered_at_ms = Some(dead_lettered_at_ms);
        record.dlq_reason = Some(DlqReason::MaxAttemptsExceeded);
    }

    fn persist_dlq_record(
        &mut self,
        id: MessageId,
        inflight: &Inflight,
        record: &QueueRecord,
        txn: &mut cntryl_midge::Transaction,
    ) -> bool {
        let write_result = txn
            .put(
                self.cached_header_key(id),
                Self::encode_record_header(record),
                None,
            )
            .map_err(|error| format!("Failed to write DLQ header: {error:?}"));

        if let Err(error) = write_result {
            tracing::warn!(
                queue = ?self.queue_key,
                route_family = self.queue_key.family.as_u64(),
                message_id = id.as_u64(),
                error = ?error,
                "Failed to persist queue DLQ record"
            );
            self.schedule_inflight_retry(id, inflight);
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::{Duration, Instant};

    #[test]
    fn should_decide_redelivery_without_storage_transaction() {
        // Arrange
        let mut record = QueueRecord::ready(Bytes::new(), 1, 1, 1);
        record.attempts = 2;
        let inflight = Inflight {
            token: 1,
            expires_at: Instant::now() + Duration::from_secs(1),
            expires_at_epoch_ms: 1,
            attempts: 1,
            owner_session_id: Some(1),
            inflight_epoch: 1,
        };

        // Act
        let outcome = decide_redelivery(Some(2), &record, &inflight);

        // Assert
        assert_eq!(outcome, RedeliveryOutcome::DeadLetter);
    }
}
