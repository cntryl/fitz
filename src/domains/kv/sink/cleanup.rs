//! Disconnect cleanup and stale queued-session rejection state.

use super::state::KvDomainRuntime;
use crate::runtime::Envelope;

impl KvDomainRuntime<'_> {
    pub(super) fn handle_cleanup_envelope(&mut self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            self.cleanup_session(cleanup.session_id);
            return true;
        }
        false
    }

    pub(super) fn is_cleaned_up_session(&self, session_id: u64) -> bool {
        self.core.cleaned_up_sessions.contains(session_id)
    }

    /// Remove all live KV state owned by a disconnected session.
    pub(super) fn cleanup_session(&mut self, session_id: u64) {
        // Mark first so an older normal-lane request that cleanup jumped over
        // cannot recreate any of the state removed below.
        self.core.cleaned_up_sessions.mark(session_id);
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
