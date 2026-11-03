//! Queue lifetimes. Public configuration uses days; publish overrides use hours.

use std::num::NonZeroU32;

#[derive(Debug, Clone, Copy, Default)]
pub struct QueueRetention {
    pub message_seconds: Option<u64>,
    pub dlq_seconds: Option<u64>,
}

impl QueueRetention {
    #[must_use]
    pub fn from_days(message: Option<NonZeroU32>, dlq: Option<NonZeroU32>) -> Self {
        Self {
            message_seconds: message.map(|days| u64::from(days.get()) * 86_400),
            dlq_seconds: dlq.map(|days| u64::from(days.get()) * 86_400),
        }
    }
}
