//! Release of Schedule session state on disconnect.
//!
//! The cleanup protocol (mark-before-release, stale-request rejection) is
//! owned by [`crate::runtime::SessionScoped`]; this file only releases state.

use super::model::{ScheduleDomain, ScheduleDomainCommand, ScheduleDomainRuntime};
use crate::runtime::{CleanedUpSessions, SessionScoped};

impl ScheduleDomain {
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

impl SessionScoped for ScheduleDomainRuntime<'_> {
    fn cleaned_up_sessions(&mut self) -> &mut CleanedUpSessions {
        &mut self.core.cleaned_up_sessions
    }

    fn release_session_resources(&mut self, session_id: u64) {
        self.unsubscribe_all(session_id);
    }
}

impl ScheduleDomainRuntime<'_> {
    /// Remove every Schedule subscription owned by one session.
    ///
    /// Shared by disconnect cleanup and the client-initiated `UnsubscribeAll`
    /// request, which does not mark the session cleaned up.
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
