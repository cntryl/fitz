//! Release of Stream session state on disconnect.
//!
//! The cleanup protocol (mark-before-release) is owned by
//! [`crate::runtime::SessionScoped`]. Envelope dispatch rejects
//! session-mutating frames for cleaned-up sessions (see
//! `sink/mailbox_sink_impl/envelope_dispatch.rs`); this file only releases
//! state.

use super::model::StreamFamilyState;
use crate::runtime::routing::RouteFamily;
use crate::runtime::{CleanedUpSessions, SessionScoped};

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
    }
}

impl SessionScoped for StreamFamilyState {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.cleaned_up_sessions
    }

    fn release_session_resources(&mut self, session_id: u64) {
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
                self.session_owners.remove(stream_session_id);
            }
            self.counter_add("fitz_stream_append_sessions_ended_total", removed_count);
            self.observability.mark_dirty();
        }
        self.refresh_metrics_gauges();
    }
}
