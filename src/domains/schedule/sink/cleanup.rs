//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a subscription for a session that is already gone and
//! will never be cleaned up again.

use super::model::{Envelope, ScheduleDomainRuntime, ScheduleDomainSink};
use std::collections::{HashSet, VecDeque};

/// Bounded record of sessions `unsubscribe_all` has already run for as part
/// of disconnect cleanup.
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

impl ScheduleDomainSink {
    /// Remove every Schedule subscription owned by one disconnected session.
    ///
    /// This crosses the mailbox (high-priority lane); the work itself happens
    /// in `ScheduleDomainRuntime::unsubscribe_all`.
    pub fn unsubscribe_all(&self, session_id: u64) {
        if let Err(error) =
            self.actor
                .try_send_high_priority(super::model::ScheduleDomainCommand::CleanupSession(
                    session_id,
                ))
        {
            tracing::warn!(domain = "schedule", error = %error, "Schedule cleanup enqueue failed");
        }
    }
}

impl ScheduleDomainRuntime<'_> {
    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.core.cleaned_up_sessions.lock().contains(session_id)
    }

    pub(super) fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a subscription for this session below.
            self.core
                .cleaned_up_sessions
                .lock()
                .mark(cleanup.session_id);
            self.unsubscribe_all(cleanup.session_id);
            return true;
        }

        false
    }

    /// Remove every Schedule subscription owned by one session.
    pub(super) fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.core.sub_families.lock();
        for (family, state) in families.iter_mut() {
            state.remove_session(
                crate::runtime::routing::RouteFamily::new(
                    u32::try_from(*family).unwrap_or(u32::MAX),
                ),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }
}
