use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

pub trait Clock: Send + Sync {
    fn now_instant(&self) -> Instant;
    fn now_epoch_ms(&self) -> u64;
}

pub struct SystemClock;

impl Clock for SystemClock {
    fn now_instant(&self) -> Instant {
        Instant::now()
    }

    fn now_epoch_ms(&self) -> u64 {
        u128_to_u64_saturating(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_else(|_| Duration::from_secs(0))
                .as_millis(),
        )
    }
}

#[must_use]
pub fn instant_to_epoch_ms_with_reference(
    instant: Instant,
    reference_instant: Instant,
    reference_epoch_ms: u64,
) -> u64 {
    if instant >= reference_instant {
        reference_epoch_ms.saturating_add(u128_to_u64_saturating(
            instant.duration_since(reference_instant).as_millis(),
        ))
    } else {
        reference_epoch_ms.saturating_sub(u128_to_u64_saturating(
            reference_instant.duration_since(instant).as_millis(),
        ))
    }
}

#[must_use]
pub fn epoch_ms_to_instant_with_reference(
    timestamp_ms: u64,
    reference_instant: Instant,
    reference_epoch_ms: u64,
) -> Instant {
    if timestamp_ms >= reference_epoch_ms {
        reference_instant
            .checked_add(Duration::from_millis(timestamp_ms - reference_epoch_ms))
            .unwrap_or(reference_instant)
    } else {
        reference_instant
            .checked_sub(Duration::from_millis(reference_epoch_ms - timestamp_ms))
            .unwrap_or(reference_instant)
    }
}
