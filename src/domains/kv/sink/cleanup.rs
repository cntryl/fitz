//! Disconnect cleanup and stale queued-session rejection state.

use super::state::KvDomainRuntime;
use crate::runtime::Envelope;
use std::collections::{HashSet, VecDeque};

/// Bounded record of sessions `cleanup_session` has already run for.
///
/// Cleanup uses the high-priority mailbox lane, so it can pass an older normal
/// request from the same session. Remembering the cleaned session makes that
/// stale request fail instead of recreating an actor, transaction, lock, watch,
/// or admin projection for a disconnected session.
///
/// The operation dispatch path also re-checks this guard before it can create
/// session state.
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

impl KvDomainRuntime<'_> {
    pub(super) fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return true;
        }
        false
    }

    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.core.cleaned_up_sessions.lock().contains(session_id)
    }

    /// Remove all live KV state owned by a disconnected session.
    pub(super) fn cleanup_session(&self, session_id: u64) {
        // Mark first so an older normal-lane request that cleanup jumped over
        // cannot recreate any of the state removed below.
        self.core.cleaned_up_sessions.lock().mark(session_id);
        self.core.actors.lock().remove(&session_id);
        self.core
            .resource_locks
            .lock()
            .retain(|_, owner| owner.session_id != session_id);

        {
            let mut watch_registries = self.core.watch_registries.lock();
            for registry in watch_registries.values_mut() {
                registry.remove_session(session_id);
            }
            watch_registries.retain(|_, registry| !registry.is_empty());
        }

        tracing::debug!(
            domain = "kv",
            session = session_id,
            "All KV transactions, resource locks, watches, and admin state released for session"
        );
        self.core.projection.remove_session_transactions(session_id);
        self.refresh_metrics_gauges();
    }
}
