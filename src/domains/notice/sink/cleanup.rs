//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a subscription for a session that is already gone and
//! will never be cleaned up again.

use super::{model::usize_to_u64, DeliveryError, Envelope, NoticeDomainCore};

impl NoticeDomainCore {
    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.cleaned_up_sessions.lock().contains(session_id)
    }

    pub(super) fn enqueue_if_session_open(
        &self,
        session_id: u64,
        enqueue: impl FnOnce() -> Result<(), DeliveryError>,
    ) -> Result<(), DeliveryError> {
        let cleaned_up_sessions = self.cleaned_up_sessions.lock();
        if cleaned_up_sessions.contains(session_id) {
            return Err(DeliveryError::ActorStopped);
        }
        enqueue()
    }

    pub(super) fn handle_cleanup_envelope(&self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a subscription for this session below.
            self.cleaned_up_sessions.lock().mark(cleanup.session_id);
            self.unsubscribe_all_for_session(cleanup.session_id);
            return true;
        }

        false
    }

    /// Remove every Notice subscription owned by one session.
    ///
    /// Shared by disconnect cleanup (`handle_cleanup_envelope`, which marks
    /// the session cleaned-up first) and the client-initiated
    /// `UnsubscribeAll` request (which does not - a still-connected client is
    /// free to subscribe again afterward).
    pub(super) fn unsubscribe_all_for_session(&self, session_id: u64) -> usize {
        let mut families = self.families.lock();
        let mut removed = 0;
        for (family_id, state) in families.iter_mut() {
            removed += state.remove_session(*family_id, session_id);
        }
        families.retain(|_, state| !state.is_empty());
        tracing::debug!(
            domain = "notice",
            session = session_id,
            "All notice subscriptions removed for session (disconnect cleanup)"
        );
        drop(families);
        if removed > 0 {
            self.counter_add("fitz_notice_unsubscribes_total", usize_to_u64(removed));
            self.mark_admin_snapshot_dirty();
        }
        removed
    }
}
