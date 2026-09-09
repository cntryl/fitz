//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the control-plane mailbox lane (see
//! `deliver_to_actor`'s `is_control_plane` check in `mailbox.rs`), so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead
//! of silently recreating a subscription or pending reserve for a session
//! that is already gone and will never be cleaned up again.

use super::model::QueueFamilyState;
use std::time::Instant;

impl QueueFamilyState {
    pub(super) fn is_cleaned_up_session(&mut self, session_id: u64) -> bool {
        self.cleaned_up_sessions.contains(session_id)
    }

    pub(super) fn mark_cleaned_up_session(&mut self, session_id: u64) {
        self.cleaned_up_sessions.mark(session_id);
    }

    /// Drop all live queue inflight entries owned by the disconnected session and return
    /// those accepted messages to the ready queue. Inflight ownership is
    /// broker-local runtime state only.
    pub(in crate::domains::queue::sink) fn cleanup_session(&mut self, session_id: u64) {
        self.pending_reserves
            .retain(|pending| pending.meta.session_id != session_id);
        let mut released_any = false;
        let mut released_counts = Vec::new();
        for (key, warm_actor) in &mut self.actors {
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
            self.route_queue_ready_notification(&key, notification);
            let route = Self::queue_ready_route(&key);
            self.wake_pending_reserves_for_route(key.family, &route, Instant::now());
        }

        tracing::debug!(
            domain = "queue",
            session = session_id,
            "Queue session cleanup completed"
        );
    }
}
