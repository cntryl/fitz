//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a lease/waiter/subscription for a session that is
//! already gone and will never be cleaned up again.

use super::model::{Instant, LeaseDomainRuntime};
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

impl LeaseDomainRuntime<'_> {
    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.core.cleaned_up_sessions.lock().contains(session_id)
    }

    pub(super) fn handle_cleanup_envelope(&self, envelope: &crate::runtime::Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a lease, waiter, or subscription for this
            // session below.
            self.core
                .cleaned_up_sessions
                .lock()
                .mark(cleanup.session_id);
            self.cleanup_session(cleanup.session_id);
            return true;
        }

        false
    }

    /// Drops session waiters before ownership and grants released keys in FIFO order.
    pub fn cleanup_session(&self, session_id: u64) {
        let now = Instant::now();
        let tracked_keys = self
            .core
            .session_leases
            .lock()
            .remove(&session_id)
            .map(|keys| keys.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let removed_waiters = self.remove_session_waiters(session_id);

        let mut removed_keys = Vec::with_capacity(tracked_keys.len());
        if !tracked_keys.is_empty() {
            let mut leases = self.core.leases.lock();
            for key in tracked_keys {
                if leases.remove(&key).is_some() {
                    removed_keys.push(key);
                }
            }
        }

        let removed_subscriptions = self.unsubscribe_all(session_id);
        for key in &removed_keys {
            self.remove_admin_lease(key);
            self.notify_lease_change(key);
        }
        for key in &removed_keys {
            let _ = self.advance_waiter_queue(key, now);
        }

        tracing::debug!(
            domain = "lease",
            session = session_id,
            count_removed = removed_keys.len(),
            waiters_removed = removed_waiters,
            subscriptions_removed = removed_subscriptions,
            "Lease: released all leases for disconnected session"
        );
        self.refresh_metrics_gauges();
    }

    /// Removes every queued waiter owned by the session before empty queues are dropped.
    pub(in crate::domains::lease::sink) fn remove_session_waiters(&self, session_id: u64) -> usize {
        let waiter_refs = self
            .core
            .session_waiters
            .lock()
            .remove(&session_id)
            .map(|waiters| waiters.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();

        if waiter_refs.is_empty() {
            return 0;
        }

        let mut removed = 0;
        let mut pending_acquires = self.core.pending_acquires.lock();
        let mut empty_keys = Vec::new();
        for waiter_ref in waiter_refs {
            if let Some(queue) = pending_acquires.get_mut(&waiter_ref.key) {
                if let Some(index) = queue
                    .iter()
                    .position(|waiter| waiter.queued_token == waiter_ref.queued_token)
                {
                    queue.remove(index);
                    removed += 1;
                }
                if queue.is_empty() {
                    empty_keys.push(waiter_ref.key.clone());
                }
            }
        }

        for key in empty_keys {
            pending_acquires.remove(&key);
        }

        removed
    }

    pub(super) fn unsubscribe_all(&self, session_id: u64) -> usize {
        let mut families = self.core.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(
                crate::runtime::routing::RouteFamily::try_from(*family_id)
                    .expect("lease family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        removed
    }
}
