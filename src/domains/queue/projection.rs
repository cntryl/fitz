/// Point-in-time warm-actor queue counts for admin diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueAdminSnapshot {
    pub messages_ready: usize,
    pub messages_delayed: usize,
    pub messages_leased: usize,
    pub messages_dead_lettered: usize,
    pub messages_total: usize,
    pub oldest_message_age_seconds: u64,
}

/// Point-in-time live lease snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueLeaseSnapshot {
    pub message_id: u64,
    pub lease_token: u64,
    pub session_id: Option<u64>,
    pub expires_at_epoch_ms: u64,
    pub attempts: usize,
}

/// Point-in-time dead-letter snapshot for admin diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueDeadLetterSnapshot {
    pub message_id: u64,
    pub dead_lettered_at_epoch_ms: u64,
    pub attempts: usize,
    pub reason: &'static str,
}