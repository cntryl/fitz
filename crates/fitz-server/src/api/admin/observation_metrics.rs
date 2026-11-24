//! Low-cardinality process counters for bounded admin observations.

use std::sync::atomic::{AtomicU64, Ordering};

static OVERLOAD_REJECTIONS: AtomicU64 = AtomicU64::new(0);

pub(crate) fn record_overload_rejection() {
    OVERLOAD_REJECTIONS.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn overload_rejections() -> u64 {
    OVERLOAD_REJECTIONS.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_record_admin_observation_overload_rejections() {
        // Arrange
        let before = overload_rejections();

        // Act
        record_overload_rejection();

        // Assert
        assert_eq!(overload_rejections(), before + 1);
    }
}
