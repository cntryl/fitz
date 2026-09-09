//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a subscription for a session that is already gone and
//! will never be cleaned up again.

use super::{model::usize_to_u64, Envelope, NoticeFamilyState};

impl NoticeFamilyState {
    pub(super) fn is_cleaned_up_session(&mut self, session_id: u64) -> bool {
        self.cleaned_up_sessions.contains(session_id)
    }

    pub(super) fn handle_cleanup_envelope(&mut self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a subscription for this session below.
            self.cleaned_up_sessions.mark(cleanup.session_id);
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
    pub(super) fn unsubscribe_all_for_session(&mut self, session_id: u64) -> usize {
        let removed = {
            let families = &mut self.families;
            let mut removed = 0;
            for (family_id, state) in families.iter_mut() {
                removed += state.remove_session(*family_id, session_id);
            }
            families.retain(|_, state| !state.is_empty());
            removed
        };
        tracing::debug!(
            domain = "notice",
            session = session_id,
            "All notice subscriptions removed for session (disconnect cleanup)"
        );
        if removed > 0 {
            self.counter_add("fitz_notice_unsubscribes_total", usize_to_u64(removed));
            self.mark_admin_snapshot_dirty();
        }
        removed
    }
}
