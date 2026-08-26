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

/// Starting cursor for a route that has not delivered before.
///
/// Defaulting an unseen route to 0 sends every first-time Single schedule to
/// the same subscriber: the cursor is per route, so a fleet of one-shot
/// schedules never rotates at all. Seeding from the route spreads them
/// deterministically while later fires still advance the stored cursor.
pub(super) fn initial_round_robin_cursor(route: &str) -> usize {
    use std::hash::{Hash, Hasher};

    let mut hasher = rustc_hash::FxHasher::default();
    route.hash(&mut hasher);
    usize::try_from(hasher.finish()).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_spread_first_delivery_of_distinct_routes_across_subscribers() {
        // Arrange
        // Every one of these routes is firing for the first time, so each has
        // no stored cursor. If unseen routes start at zero they all choose the
        // same subscriber and one client absorbs the entire fleet's load.
        let candidates = [10_u64, 20, 30, 40];
        let routes = (0..200)
            .map(|index| format!("schedule://acme/jobs/one-shot-{index:04}/run"))
            .collect::<Vec<_>>();

        // Act
        let chosen = routes
            .iter()
            .map(|route| {
                let cursor = initial_round_robin_cursor(route);
                DeliveryStrategy::select_recipients(
                    ScheduleDeliveryMode::Single,
                    &candidates,
                    cursor,
                )
                .recipients()[0]
            })
            .collect::<Vec<_>>();

        // The old behaviour, kept explicit so this test cannot silently stop
        // discriminating: an unseen route defaulting to cursor 0 puts every
        // first delivery on the same subscriber.
        let all_at_zero = routes
            .iter()
            .map(|_| {
                DeliveryStrategy::select_recipients(ScheduleDeliveryMode::Single, &candidates, 0)
                    .recipients()[0]
            })
            .collect::<std::collections::HashSet<_>>();

        // Assert
        assert_eq!(
            all_at_zero.len(),
            1,
            "a zero cursor should concentrate; if not, this test proves nothing"
        );
        for candidate in candidates {
            let share = chosen.iter().filter(|picked| **picked == candidate).count();
            assert!(
                share > 0,
                "subscriber {candidate} received none of {} first deliveries",
                chosen.len()
            );
        }
    }

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
