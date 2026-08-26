//! Stale queued-request rejection state for disconnect cleanup.
//!
//! `SessionCleanup` is delivered on the high-priority (control-plane) mailbox
//! lane - see `deliver_to_actor`'s `is_control_plane` check - so it can pass
//! an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead
//! of silently recreating a subscription or pending reserve for a session
//! that is already gone and will never be cleaned up again.

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
