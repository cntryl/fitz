use crate::domains::queue::core::{MessageId, QueueKey};

/// Semantic state transitions emitted by the queue domain.
///
/// Events are emitted after successful operations, outside the hot path.
/// They contain identifiers and timestamps only — no aggregates or metric summaries.
#[derive(Debug, Clone)]
pub enum QueueEvent {
    MessageEnqueued {
        queue_key: QueueKey,
        message_id: MessageId,
        delay_ms: Option<u64>,
    },
    MessageReserved {
        queue_key: QueueKey,
        message_id: MessageId,
        session_id: Option<u64>,
        attempts: u32,
    },
    MessageCompleted {
        queue_key: QueueKey,
        message_id: MessageId,
        attempts: u32,
    },
    MessageReleased {
        queue_key: QueueKey,
        message_id: MessageId,
    },
    InflightExpired {
        queue_key: QueueKey,
        message_id: MessageId,
        attempts: u32,
    },
    InflightExtended {
        queue_key: QueueKey,
        message_id: MessageId,
    },
    MessageDeadLettered {
        queue_key: QueueKey,
        message_id: MessageId,
        reason: &'static str,
    },
}
