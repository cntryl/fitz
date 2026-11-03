use super::super::{DlqReason, QueueActor, QueueRecord};

impl QueueActor {
    /// Serialize `QueueRecord` header to bytes.
    pub(in crate::domains::queue::actor) fn encode_record_header(record: &QueueRecord) -> Vec<u8> {
        let has_ttl = record.expires_at_ms.is_some() || record.retention_expires_at_ms.is_some();
        let mut buf = Vec::with_capacity(if has_ttl { 95 } else { 79 });
        buf.push(if has_ttl {
            Self::HEADER_VERSION_V3
        } else {
            Self::HEADER_VERSION_V2
        });
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
        buf.push(record.dlq_reason.map_or(0, DlqReason::wire_code));
        if has_ttl {
            buf.extend_from_slice(&record.expires_at_ms.unwrap_or(0).to_le_bytes());
            buf.extend_from_slice(&record.retention_expires_at_ms.unwrap_or(0).to_le_bytes());
        }
        buf
    }
}
