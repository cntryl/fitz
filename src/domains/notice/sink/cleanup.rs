//! Release of Notice session state on disconnect.
//!
//! The cleanup protocol (mark-before-release, stale-request rejection) is
//! owned by [`crate::runtime::SessionScoped`]; this file only releases state.

use super::{model::usize_to_u64, NoticeFamilyState};
use crate::runtime::{CleanedUpSessions, SessionScoped};

impl SessionScoped for NoticeFamilyState {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.cleaned_up_sessions
    }

    fn release_session_resources(&mut self, session_id: u64) {
        self.unsubscribe_all_for_session(session_id);
    }
}

impl NoticeFamilyState {
    /// Remove every Notice subscription owned by one session.
    ///
    /// Shared by disconnect cleanup (`release_session_resources`, which runs
    /// after the session is marked cleaned up) and the client-initiated
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
