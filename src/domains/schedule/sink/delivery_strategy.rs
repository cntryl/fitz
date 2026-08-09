//! Pure recipient selection for schedule delivery modes.

use crate::domains::schedule::ScheduleDeliveryMode;

pub(super) struct DeliveryStrategy {
    recipients: Vec<u64>,
    stop_after_first_success: bool,
}

impl DeliveryStrategy {
    pub(super) fn select_recipients(
        mode: ScheduleDeliveryMode,
        candidates: &[u64],
        cursor: usize,
    ) -> Self {
        match mode {
            ScheduleDeliveryMode::Broadcast => Self {
                recipients: candidates.to_vec(),
                stop_after_first_success: false,
            },
            ScheduleDeliveryMode::Single => {
                let start = cursor % candidates.len();
                let recipients = (0..candidates.len())
                    .map(|offset| candidates[(start + offset) % candidates.len()])
                    .collect();
                Self {
                    recipients,
                    stop_after_first_success: true,
                }
            }
        }
    }

    pub(super) fn recipients(&self) -> &[u64] {
        &self.recipients
    }

    pub(super) fn stops_after_success(&self) -> bool {
        self.stop_after_first_success
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_rotate_single_delivery_candidates_without_router() {
        // Arrange
        let candidates = [10, 20, 30];

        // Act
        let selected =
            DeliveryStrategy::select_recipients(ScheduleDeliveryMode::Single, &candidates, 1);

        // Assert
        assert_eq!(selected.recipients(), &[20, 30, 10]);
        assert!(selected.stops_after_success());
    }
}
