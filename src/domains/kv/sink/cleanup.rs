//! Release of KV session state on disconnect.
//!
//! The cleanup protocol (mark-before-release, stale-request rejection) is
//! owned by [`crate::runtime::SessionScoped`]; this file only releases state.

use super::state::KvFamilyRuntime;
use crate::runtime::{CleanedUpSessions, SessionScoped};

impl SessionScoped for KvFamilyRuntime<'_> {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.core.cleaned_up_sessions
    }

    /// Remove all live KV state owned by a disconnected session.
    fn release_session_resources(&mut self, session_id: u64) {
        self.core.actors.remove(&session_id);
        self.core
            .resource_locks
            .retain(|_, owner| owner.session_id != session_id);

        {
            let watch_registries = &mut self.core.watch_registries;
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
