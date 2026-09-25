//! Shared retry state for area and realm watermark notifications.

use super::constants::{NOTICE_DEBOUNCE_MS, WATERMARK_NOTIFICATION_MAX_RETRY_SHIFT};
use crate::runtime::actor::{Actor, Context, SendError};
use crate::runtime::context::TimerId;
use crate::runtime::DomainPublishEvent;
use std::time::Duration;

/// Encode the watermark notification body published to
/// `stream://…/watermark` subscribers.
pub(super) fn watermark_payload(previous: u64, watermark: u64) -> bytes::Bytes {
    let payload_json = serde_json::json!({
        "previous": previous,
        "watermark": watermark,
        "ts": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    });
    bytes::Bytes::from(payload_json.to_string())
}

pub(super) struct WatermarkNotification {
    pending: Option<DomainPublishEvent>,
    timer: Option<TimerId>,
    retry_attempts: u8,
}

impl WatermarkNotification {
    pub(super) const fn new() -> Self {
        Self {
            pending: None,
            timer: None,
            retry_attempts: 0,
        }
    }

    pub(super) fn queue<A: Actor>(&mut self, event: DomainPublishEvent, ctx: &mut Context<A>) {
        self.pending = Some(event);
        self.retry_attempts = 0;
        if self.timer.is_none() {
            self.timer = Some(
                ctx.timer_manager()
                    .schedule_once(Duration::from_millis(NOTICE_DEBOUNCE_MS)),
            );
        }
    }

    pub(super) fn publish<A: Actor>(&mut self, ctx: &mut Context<A>) -> Result<(), SendError> {
        let Some(event) = self.pending.take() else {
            return Ok(());
        };

        if let Err(error) = ctx.publish_event(event.clone()) {
            self.pending = Some(event);
            self.retry_attempts = self.retry_attempts.saturating_add(1);
            let retry_shift = u32::from(self.retry_attempts.saturating_sub(1))
                .min(WATERMARK_NOTIFICATION_MAX_RETRY_SHIFT);
            self.timer = Some(
                ctx.timer_manager()
                    .schedule_once(Duration::from_millis(NOTICE_DEBOUNCE_MS << retry_shift)),
            );
            return Err(error);
        }

        self.retry_attempts = 0;
        Ok(())
    }

    pub(super) fn take_fired_timer(&mut self, timer_id: TimerId) -> bool {
        if self.timer != Some(timer_id) {
            return false;
        }
        self.timer = None;
        true
    }

    #[cfg(test)]
    pub(super) const fn timer(&self) -> Option<TimerId> {
        self.timer
    }

    #[cfg(test)]
    pub(super) const fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
}

#[cfg(test)]
mod payload_tests {
    use super::watermark_payload;

    #[test]
    fn should_encode_watermark_payload_with_stable_field_layout() {
        // Arrange
        let (previous, watermark) = (4, 9);

        // Act
        let payload = watermark_payload(previous, watermark);

        // Assert
        let text = std::str::from_utf8(&payload).expect("utf8 payload");
        assert_eq!(
            crate::testkit::golden::mask_json_number(text, "ts"),
            r#"{"previous":4,"ts":<n>,"watermark":9}"#
        );
    }
}
