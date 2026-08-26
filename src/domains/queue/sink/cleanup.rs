//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the control-plane mailbox lane (see
//! `deliver_to_actor`'s `is_control_plane` check in `mailbox.rs`), so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead
//! of silently recreating a subscription or pending reserve for a session
//! that is already gone and will never be cleaned up again.

use super::model::{Instant, QueueDomainCore};
use std::collections::{HashSet, VecDeque};

/// Bounded record of sessions `cleanup_session` has already run for.
pub(super) struct CleanedUpSessions {
    order: VecDeque<u64>,
    seen: HashSet<u64>,
    capacity: usize,
}

impl CleanedUpSessions {
    #[must_use]
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            order: VecDeque::new(),
            seen: HashSet::new(),
            capacity: capacity.max(1),
        }
    }

    pub(super) fn mark(&mut self, session_id: u64) {
        if self.seen.insert(session_id) {
            self.order.push_back(session_id);
            if self.order.len() > self.capacity {
                if let Some(oldest) = self.order.pop_front() {
                    self.seen.remove(&oldest);
                }
            }
        }
    }

    pub(super) fn contains(&self, session_id: u64) -> bool {
        self.seen.contains(&session_id)
    }
}

impl QueueDomainCore {
    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.cleaned_up_sessions.lock().contains(session_id)
    }

    pub(super) fn mark_cleaned_up_session(&self, session_id: u64) {
        self.cleaned_up_sessions.lock().mark(session_id);
    }

    /// Drop all live queue inflight entries owned by the disconnected session and return
    /// those accepted messages to the ready queue. Inflight ownership is
    /// broker-local runtime state only.
    pub(in crate::domains::queue::sink) fn cleanup_session(&self, session_id: u64) {
        self.pending_reserves
            .lock()
            .retain(|pending| pending.meta.session_id != session_id);
        let mut released_any = false;
        let mut notifications = Vec::new();
        let mut actors = self.actors.lock();
        for (key, warm_actor) in actors.iter_mut() {
            let mut actor = warm_actor.actor.lock();
            if actor.cleanup_session_inflight(session_id) > 0 {
                released_any = true;
                if let Some(notification) = self.record_ready_state(key, actor.live_counts()) {
                    notifications.push((key.clone(), notification));
                }
            }
        }
        drop(actors);

        let mut families = self.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("queue family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);

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
