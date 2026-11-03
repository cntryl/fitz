//! Preserve publish deadlines and cap dead-letter retention.

use std::num::NonZeroU32;

use super::{QueueActor, QueueRecord};
use crate::domains::queue::{retention::QueueRetention, QueueResponse};

impl QueueActor {
    #[must_use]
    pub fn with_retention(mut self, retention: QueueRetention) -> Self {
        self.retention = retention;
        self
    }

    pub(super) fn set_publish_deadline(
        &self,
        record: &mut QueueRecord,
        ttl_hours: Option<NonZeroU32>,
    ) -> Result<(), QueueResponse> {
        let seconds = ttl_hours
            .map(|hours| u64::from(hours.get()) * 3_600)
            .or(self.retention.message_seconds);
        record.expires_at_ms = seconds
            .map(|seconds| {
                seconds
                    .checked_mul(1_000)
                    .and_then(|ms| record.first_enqueued_at_ms.checked_add(ms))
                    .ok_or_else(|| QueueResponse::BadRequest {
                        reason: "message TTL is too large".to_string(),
                    })
            })
            .transpose()?;
        record.retention_expires_at_ms = record.expires_at_ms;
        Ok(())
    }

    pub(super) fn record_ttl_seconds(&self, record: &QueueRecord) -> Option<u64> {
        Self::ttl_seconds_until(record.retention_expires_at_ms, self.clock.now_epoch_ms())
    }

    pub(super) fn index_expiry_value(deadline: Option<u64>) -> Vec<u8> {
        deadline.map_or_else(Vec::new, |deadline| deadline.to_le_bytes().to_vec())
    }

    pub(super) fn ttl_seconds_until(deadline: Option<u64>, now_ms: u64) -> Option<u64> {
        deadline.map(|deadline| {
            // Midge interprets zero as no expiry. Expired records must be removed,
            // never rewritten; one second keeps a late commit bounded as well.
            deadline.saturating_sub(now_ms).div_ceil(1_000).max(1)
        })
    }

    pub(super) fn record_has_expired(&self, record: &QueueRecord) -> bool {
        record
            .retention_expires_at_ms
            .is_some_and(|deadline| deadline <= self.clock.now_epoch_ms())
    }

    pub(super) fn cap_dead_letter_deadline(&self, record: &mut QueueRecord, entered_at_ms: u64) {
        let dlq_deadline = self
            .retention
            .dlq_seconds
            .map(|seconds| entered_at_ms.saturating_add(seconds.saturating_mul(1_000)));
        record.retention_expires_at_ms = match (record.expires_at_ms, dlq_deadline) {
            (Some(message), Some(dlq)) => Some(message.min(dlq)),
            (message, dlq) => message.or(dlq),
        };
    }

    pub(super) fn write_dead_letter_record(
        &self,
        txn: &mut super::recovery_store::QueueTransaction,
        id: super::MessageId,
        record: &QueueRecord,
    ) -> Result<(), String> {
        let ttl = self.record_ttl_seconds(record);
        txn.put(
            self.cached_header_key(id),
            Self::encode_record_header(record),
            ttl,
        )
        .map_err(|error| format!("Failed to write DLQ header: {error:?}"))?;
        if ttl.is_some() {
            let key = self.cached_body_key(id);
            if let Some(body) = txn
                .get(&key)
                .map_err(|error| format!("Failed to read DLQ body: {error:?}"))?
            {
                txn.put(key, body.to_vec(), ttl)
                    .map_err(|error| format!("Failed to set DLQ body TTL: {error:?}"))?;
            }
        }
        Ok(())
    }
}
