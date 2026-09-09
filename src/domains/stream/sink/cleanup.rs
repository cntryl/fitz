//! Disconnect cleanup: removal of all Stream state owned by one session.
//!
//! `cleanup_session` marks the session in `cleaned_up_sessions` before doing
//! any mutation, and `mailbox_sink_impl`'s envelope dispatch rejects any
//! session-mutating frame for a session already in that set - see
//! `sink/mailbox_sink_impl/envelope_dispatch.rs`. That check-before-dispatch
//! guard is what actually prevents a stale queued request from recreating
//! state after cleanup; this file only owns the mutation itself.

use super::model::StreamFamilyState;
use crate::runtime::routing::RouteFamily;

impl StreamFamilyState {
    pub(in crate::domains::stream::sink) fn unsubscribe_all(&mut self, session_id: u64) {
        let families = &mut self.subscriptions.families;
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                RouteFamily::try_from(*family_id)
                    .expect("stream family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        let _ = families;
        self.remove_pending_notifications_for_session(session_id);
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::stream::sink) fn cleanup_session(&mut self, session_id: u64) {
        self.cleaned_up_sessions.mark(session_id);
        self.unsubscribe_all(session_id);

        let mut removed_sessions = Vec::new();
        let mut advanced_families = std::collections::BTreeSet::new();
        for (key, actor) in &mut self.actors {
            if let Some(stream_session_id) = actor.cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
                advanced_families.insert(key.family.as_u64());
            }
        }

        for family_id in advanced_families {
            self.handle_visibility_advance(
                RouteFamily::try_from(family_id)
                    .expect("stream family IDs originate from RouteFamily"),
            );
        }

        if !removed_sessions.is_empty() {
            let removed_count = super::model::usize_to_u64_saturating(removed_sessions.len());
            for stream_session_id in removed_sessions {
                self.session_owners.remove(&stream_session_id);
            }
            self.counter_add("fitz_stream_append_sessions_ended_total", removed_count);
            self.admin_snapshot.mark_dirty();
        }
    }
}
