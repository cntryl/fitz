//! Disconnect cleanup: removal of all Stream state owned by one session.
//!
//! `cleanup_session` marks the session in `cleaned_up_sessions` before doing
//! any mutation, and `mailbox_sink_impl`'s envelope dispatch rejects any
//! session-mutating frame for a session already in that set - see
//! `sink/mailbox_sink_impl/envelope_dispatch.rs`. That check-before-dispatch
//! guard is what actually prevents a stale queued request from recreating
//! state after cleanup; this file only owns the mutation itself.

use super::model::{RouteFamily, StreamDomainCore};

impl StreamDomainCore {
    pub(in crate::domains::stream::sink) fn unsubscribe_all(&self, session_id: u64) {
        let mut families = self.subscriptions.families.lock();
        for (family_id, state) in families.iter_mut() {
            state.remove_session(
                RouteFamily::try_from(*family_id)
                    .expect("stream family IDs originate from RouteFamily"),
                session_id,
            );
        }
        families.retain(|_, state| !state.is_empty());
        drop(families);
        self.remove_pending_notifications_for_session(session_id);
        self.refresh_metrics_gauges();
    }

    pub(in crate::domains::stream::sink) fn cleanup_session(&self, session_id: u64) {
        self.cleaned_up_sessions.lock().insert(session_id);
        self.unsubscribe_all(session_id);

        let actors = self
            .actors
            .lock()
            .iter()
            .map(|(key, actor)| (key.family.as_u64(), actor.clone()))
            .collect::<Vec<_>>();
        let mut removed_sessions = Vec::new();
        let mut advanced_families = std::collections::BTreeSet::new();
        for (family_id, actor) in actors {
            if let Some(stream_session_id) = actor.lock().cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
                advanced_families.insert(family_id);
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
            let mut session_owners = self.session_owners.lock();
            for stream_session_id in removed_sessions {
                session_owners.remove(&stream_session_id);
            }
            self.counter_add("fitz_stream_append_sessions_ended_total", removed_count);
            self.admin_snapshot.mark_dirty();
        }
    }
}
