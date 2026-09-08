//! Disconnect cleanup and stale queued-request rejection state.
//!
//! `SessionCleanup` is delivered on the high-priority mailbox lane, so it can
//! pass an older, already-queued normal-lane request from the same session.
//! Remembering the cleaned-up session lets that stale request fail instead of
//! silently recreating a subscription for a session that is already gone and
//! will never be cleaned up again.

use super::model::{ScheduleDomainCommand, ScheduleDomainRuntime, ScheduleDomainSink};
use crate::runtime::Envelope;

impl ScheduleDomainSink {
    /// Remove every Schedule subscription owned by one disconnected session.
    ///
    /// This crosses the mailbox (high-priority lane); the work itself happens
    /// in `ScheduleDomainRuntime::unsubscribe_all`.
    ///
    /// # Errors
    ///
    /// Returns the actor enqueue failure or a bounded reply-wait failure when
    /// cleanup execution cannot be confirmed.
    pub fn cleanup_session(&self, session_id: u64) -> Result<(), crate::runtime::DeliveryError> {
        let mut replies = Vec::with_capacity(self.route_families.len());
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                ScheduleDomainCommand::CleanupSession(session_id, reply_tx),
            )?;
            replies.push(reply_rx);
        }
        for reply in replies {
            reply
                .recv_timeout(std::time::Duration::from_secs(1))
                .map_err(crate::runtime::reply_wait::map_reply_wait_error)?;
        }
        Ok(())
    }
}

impl ScheduleDomainRuntime<'_> {
    pub(super) fn is_cleaned_up_session(&mut self, session_id: u64) -> bool {
        self.core.cleaned_up_sessions.contains(session_id)
    }

    pub(super) fn handle_cleanup_envelope(&mut self, envelope: &Envelope) -> bool {
        if let Some(cleanup) = envelope.payload::<crate::runtime::SessionCleanup>() {
            // Mark first so an older normal-lane request that cleanup jumped
            // over cannot recreate a subscription for this session below.
            self.core.cleaned_up_sessions.mark(cleanup.session_id);
            self.unsubscribe_all(cleanup.session_id);
            return true;
        }

        false
    }

    /// Remove every Schedule subscription owned by one session.
    pub(super) fn unsubscribe_all(&mut self, session_id: u64) {
        self.core
            .subscriptions
            .remove_session(self.core.route_family, session_id);
        tracing::debug!(
            domain = "schedule",
            session = session_id,
            "All schedule subscriptions removed for session"
        );
    }
}
