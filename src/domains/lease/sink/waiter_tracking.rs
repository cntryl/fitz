//! Per-session index of owned leases and queued waiters, used by cleanup and
//! by acquire/expiry bookkeeping to keep both directions in sync.

use super::model::{LeaseFamilyRuntime, PendingAcquireRef};

impl LeaseFamilyRuntime<'_> {
    pub(super) fn track_session_lease(
        &mut self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        self.core
            .session_leases
            .entry(session_id)
            .or_default()
            .insert(key.clone());
    }

    pub(super) fn untrack_session_lease(
        &mut self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
    ) {
        let session_leases = &mut self.core.session_leases;
        let should_remove_session = if let Some(keys) = session_leases.get_mut(&session_id) {
            keys.remove(key);
            keys.is_empty()
        } else {
            false
        };

        if should_remove_session {
            session_leases.remove(&session_id);
        }
    }

    pub(super) fn track_session_waiter(
        &mut self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        self.core
            .session_waiters
            .entry(session_id)
            .or_default()
            .insert(PendingAcquireRef {
                key: key.clone(),
                queued_token,
            });
    }

    pub(super) fn untrack_session_waiter(
        &mut self,
        session_id: u64,
        key: &crate::domains::lease::protocol::LeaseKey,
        queued_token: u64,
    ) {
        let session_waiters = &mut self.core.session_waiters;
        let should_remove_session = if let Some(waiters) = session_waiters.get_mut(&session_id) {
            waiters.remove(&PendingAcquireRef {
                key: key.clone(),
                queued_token,
            });
            waiters.is_empty()
        } else {
            false
        };

        if should_remove_session {
            session_waiters.remove(&session_id);
        }
    }
}
