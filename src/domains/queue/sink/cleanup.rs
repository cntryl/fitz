//! Release of Queue session state on disconnect.
//!
//! The cleanup protocol (mark-before-release, stale-request rejection) is
//! owned by [`crate::runtime::SessionScoped`]; this file only releases state.
//! Cleanup bypasses client admission (see `deliver_to_actor` in `mailbox.rs`).

use super::model::QueueFamilyState;
use crate::runtime::{CleanedUpSessions, SessionScoped};
use std::time::Instant;

impl SessionScoped for QueueFamilyState {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.cleaned_up_sessions
    }

    /// Drop all live queue inflight entries owned by the disconnected session and return
    /// those accepted messages to the ready queue. Inflight ownership is
    /// broker-local runtime state only.
    fn release_session_resources(&mut self, session_id: u64) {
        self.reservation_book.remove_session(session_id);
        let mut released_any = false;
        let mut released_counts = Vec::new();
        for (key, warm_actor) in self.actor_registry.iter_mut() {
            if warm_actor.actor.cleanup_session_inflight(session_id) > 0 {
                released_any = true;
                released_counts.push((key.clone(), warm_actor.actor.live_counts()));
            }
        }
        let notifications = released_counts
            .into_iter()
            .filter_map(|(key, counts)| {
                self.record_ready_state(&key, counts)
                    .map(|notification| (key, notification))
            })
            .collect::<Vec<_>>();

        let families = &mut self.families;
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("queue family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        let _ = families;

        if released_any {
            self.mark_admin_snapshot_dirty();
        }

        for (key, notification) in notifications {
            self.notify_queue_ready_and_wake_reserves(&key, Some(notification), Instant::now());
        }

        tracing::debug!(
            domain = "queue",
            session = session_id,
            "Queue session cleanup completed"
        );
    }
}
