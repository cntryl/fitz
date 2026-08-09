use super::{
    DlqReason, MessageId, QueueActor, QueueActorLiveCounts, QueueAdminSnapshot,
    QueueDeadLetterSnapshot, QueueInflightSnapshot, QueueState,
};
use crate::control::admin::QueueAgeBuckets;

impl QueueActor {
    #[must_use]
    pub fn ready_len(&self) -> usize {
        self.ready.len()
    }

    #[must_use]
    pub(crate) fn live_counts(&self) -> QueueActorLiveCounts {
        QueueActorLiveCounts {
            ready: self.ready_count,
            delayed: self.persisted_delayed.len(),
            inflight: self.inflight.len(),
            dead_letters: self.persisted_dlq.len(),
        }
    }

    fn backlog_age_metrics(&self, now_epoch_ms: u64) -> (QueueAgeBuckets, u64) {
        let mut buckets = QueueAgeBuckets::default();
        let mut oldest_backlog_age_seconds = 0u64;

        for record in self.records.values() {
            if matches!(record.state, QueueState::Ready | QueueState::Delayed) {
                let age_seconds = now_epoch_ms.saturating_sub(record.first_enqueued_at_ms) / 1_000;
                buckets.record_age_seconds(age_seconds);
                oldest_backlog_age_seconds = oldest_backlog_age_seconds.max(age_seconds);
            }
        }

        (buckets, oldest_backlog_age_seconds)
    }

    fn delay_age_metrics(&self, now_epoch_ms: u64) -> QueueAgeBuckets {
        let mut buckets = QueueAgeBuckets::default();

        for record in self.records.values() {
            if matches!(record.state, QueueState::Delayed) {
                let age_seconds = now_epoch_ms.saturating_sub(record.first_enqueued_at_ms) / 1_000;
                buckets.record_age_seconds(age_seconds);
            }
        }

        buckets
    }

    #[must_use]
    pub fn admin_snapshot(&self) -> QueueAdminSnapshot {
        let now_epoch_ms = self.clock.now_epoch_ms();
        let (backlog_age_buckets, oldest_backlog_age_seconds) =
            self.backlog_age_metrics(now_epoch_ms);
        let delay_age_buckets = self.delay_age_metrics(now_epoch_ms);
        QueueAdminSnapshot {
            messages_ready: self.ready.len(),
            messages_delayed: self.persisted_delayed.len(),
            messages_inflight: self.inflight.len(),
            messages_dead_lettered: self.persisted_dlq.len(),
            messages_total: self.ready.len()
                + self.inflight.len()
                + self.persisted_delayed.len()
                + self.persisted_dlq.len(),
            oldest_message_age_seconds: self.oldest_ready_enqueued_at_ms.map_or(0, |timestamp| {
                now_epoch_ms.saturating_sub(timestamp) / 1_000
            }),
            oldest_backlog_age_seconds,
            backlog_age_buckets,
            delay_age_buckets,
            enqueue_success_total: self.enqueue_success_window.total(),
            complete_success_total: self.complete_success_window.total(),
            in_rate_per_second: self.enqueue_success_window.rate_per_second(now_epoch_ms),
            out_rate_per_second: self.complete_success_window.rate_per_second(now_epoch_ms),
        }
    }

    #[must_use]
    pub fn admin_inflight(&self) -> Vec<QueueInflightSnapshot> {
        let now_instant = self.clock.now_instant();
        let now_epoch_ms = self.clock.now_epoch_ms();

        self.inflight
            .iter()
            .map(|(id, inflight)| QueueInflightSnapshot {
                message_id: id.as_u64(),
                inflight_token: inflight.token,
                session_id: inflight.owner_session_id,
                expires_at_epoch_ms: now_epoch_ms.saturating_add(
                    u64::try_from(
                        inflight
                            .expires_at
                            .saturating_duration_since(now_instant)
                            .as_millis(),
                    )
                    .unwrap_or(u64::MAX),
                ),
                attempts: usize::try_from(inflight.attempts).unwrap_or(usize::MAX),
            })
            .collect()
    }

    #[must_use]
    pub fn admin_dead_letters(&self) -> Vec<QueueDeadLetterSnapshot> {
        let mut dead_letters: Vec<_> = self
            .persisted_dlq
            .iter()
            .map(|(id, &dead_lettered_at_epoch_ms)| {
                let record = self
                    .records
                    .get(id)
                    .cloned()
                    .filter(|record| matches!(record.state, QueueState::Dlq))
                    .or_else(|| {
                        self.load_record_metadata_from_store(*id)
                            .ok()
                            .filter(|record| matches!(record.state, QueueState::Dlq))
                    });

                let attempts = record
                    .as_ref()
                    .map(|record| record.attempts as usize)
                    .unwrap_or_default();
                let dead_lettered_at_epoch_ms = record
                    .as_ref()
                    .and_then(|record| record.dead_lettered_at_ms)
                    .unwrap_or(dead_lettered_at_epoch_ms);
                let reason = record
                    .as_ref()
                    .and_then(|record| record.dlq_reason)
                    .map_or("unknown", Self::dlq_reason_label);

                QueueDeadLetterSnapshot {
                    message_id: id.as_u64(),
                    dead_lettered_at_epoch_ms,
                    attempts,
                    reason,
                }
            })
            .collect();

        dead_letters.sort_by(|left, right| {
            (left.dead_lettered_at_epoch_ms, left.message_id)
                .cmp(&(right.dead_lettered_at_epoch_ms, right.message_id))
        });
        dead_letters
    }

    fn dlq_reason_label(reason: DlqReason) -> &'static str {
        match reason {
            DlqReason::MaxAttemptsExceeded => "max_attempts_exceeded",
            DlqReason::HydrationFailed => "hydration_failed",
            DlqReason::DeliveryAttemptsExhausted => "delivery_attempts_exhausted",
            DlqReason::InflightEpochExhausted => "inflight_epoch_exhausted",
        }
    }

    /// Drop any live inflight entries owned by a disconnected session and return the
    /// accepted messages to the ready queue. The inflight ownership itself is
    /// ephemeral and is not durably recovered.
    pub fn cleanup_session_inflight(&mut self, session_id: u64) -> usize {
        let released: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(id, inflight)| {
                (inflight.owner_session_id == Some(session_id)).then_some(*id)
            })
            .collect();

        for id in released.iter().copied() {
            self.inflight.remove(&id);
            if let Some(record) = self.records.get_mut(&id) {
                record.state = QueueState::Ready;
                record.visible_at_ms = 0;
                record.inflight_token = None;
                record.inflight_expires_at_ms = None;
            }
            self.push_ready(id);
        }

        released.len()
    }

    #[must_use]
    pub fn ready_contains(&self, id: MessageId) -> bool {
        self.ready.iter().any(|entry| entry.id == id)
    }

    /// Returns true if queue-local waiters should be re-checked.
    /// Clears the flag. Used by the domain sink after state-changing operations.
    pub fn take_needs_wake_waiters(&mut self) -> bool {
        std::mem::take(&mut self.needs_wake_waiters)
    }
}
