//! Release of Stream session state on disconnect.
//!
//! The cleanup protocol (mark-before-release) is owned by
//! [`crate::runtime::SessionScoped`]. Envelope dispatch rejects
//! session-mutating frames for cleaned-up sessions (see
//! `sink/mailbox_sink_impl/envelope_dispatch.rs`); this file only releases
//! state.

use super::model::StreamFamilyState;
use crate::runtime::{CleanedUpSessions, SessionScoped};

impl SessionScoped for StreamFamilyState {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.cleaned_up_sessions
    }

    fn release_session_resources(&mut self, session_id: u64) {
        let subscriptions = self.subscriptions.remove_session(session_id);
        let subscriptions_removed = !subscriptions.is_empty();
        for (id, entry) in subscriptions {
            self.delivery.retire(&entry.canonical, id);
        }

        let mut removed_sessions = Vec::new();
        let mut idle_keys = Vec::new();
        let mut advanced_scopes = std::collections::BTreeSet::new();
        for (key, actor) in &mut self.actors {
            if let Some(stream_session_id) = actor.cleanup_session(session_id) {
                removed_sessions.push(stream_session_id);
                idle_keys.push(key.clone());
                advanced_scopes.insert((key.realm.clone(), key.area.clone()));
            }
        }
        for key in idle_keys {
            self.actors.remove(&key);
        }
        let sessions_removed = !removed_sessions.is_empty();

        for (realm, area) in advanced_scopes {
            self.recheck_commit_scopes(&realm, &area);
        }

        if !removed_sessions.is_empty() {
            let removed_count = super::model::usize_to_u64_saturating(removed_sessions.len());
            for stream_session_id in removed_sessions {
                self.session_owners.remove(stream_session_id);
            }
            self.counter_add("fitz_stream_append_sessions_ended_total", removed_count);
        }
        if subscriptions_removed || sessions_removed {
            self.mark_admin_snapshot_dirty();
        } else {
            self.refresh_metrics_gauges();
        }
    }
}
