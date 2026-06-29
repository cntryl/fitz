use super::super::*;

impl QueueActor {
    /// Serialize QueueRecord header to bytes.
    pub(in crate::domains::queue::actor) fn encode_record_header(record: &QueueRecord) -> Vec<u8> {
        let mut buf = Vec::with_capacity(79);
        buf.push(Self::HEADER_VERSION_V2);
        buf.push(record.state as u8);
        buf.extend_from_slice(&record.enqueue_seq.to_le_bytes());
        buf.extend_from_slice(&record.ready_seq.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.attempts.to_le_bytes());
        buf.extend_from_slice(&record.visible_at_ms.to_le_bytes());
        buf.extend_from_slice(&record.first_enqueued_at_ms.to_le_bytes());
        buf.extend_from_slice(&record.last_inflight_at_ms.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.inflight_epoch.to_le_bytes());
        buf.extend_from_slice(&record.inflight_token.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.inflight_expires_at_ms.unwrap_or(0).to_le_bytes());
        buf.extend_from_slice(&record.dead_lettered_at_ms.unwrap_or(0).to_le_bytes());
        buf.push(record.dlq_reason.map(|value| value as u8).unwrap_or(0));
        buf
    }

    /// Serialize a legacy combined QueueRecord for compatibility writes.
    pub(in crate::domains::queue::actor) fn encode_legacy_record(record: &QueueRecord) -> Vec<u8> {
        let body = record
            .body
            .as_ref()
            .expect("legacy queue record must have a body before persistence");
        let mut buf = Vec::with_capacity(16 + body.len());
        buf.extend_from_slice(&record.attempts.to_le_bytes());
        buf.extend_from_slice(&record.visible_at_ms.to_le_bytes());
        buf.extend_from_slice(&(body.len() as u32).to_le_bytes());
        buf.extend_from_slice(body);
        buf
    }
}
