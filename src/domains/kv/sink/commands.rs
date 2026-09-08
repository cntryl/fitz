//! Managed-actor command protocol and synchronous public controls.

#[cfg(test)]
use super::locks::KvResourceLockKey;
use super::state::KvDomainSink;
use std::time::Duration;

pub(super) enum KvDomainCommand {
    Deliver(crate::runtime::Envelope),
    CleanupSession(u64, crossbeam_channel::Sender<()>),
    #[cfg(test)]
    SyncAdminSnapshot(crossbeam_channel::Sender<()>),
    #[cfg(test)]
    ReadLatencySnapshots(
        KvResourceLockKey,
        crossbeam_channel::Sender<(
            crate::control::admin::KvLatencySnapshot,
            crate::control::admin::KvLatencySnapshot,
        )>,
    ),
    #[cfg(test)]
    /// Ask the mailbox actor to apply its configured BEGIN write policy.
    ApplyWriteOptions(
        crate::domains::kv::KvMessage,
        crossbeam_channel::Sender<crate::domains::kv::KvMessage>,
    ),
    PanicForFailpoint,
    #[cfg(test)]
    BlockForTests(
        crossbeam_channel::Sender<()>,
        crossbeam_channel::Receiver<()>,
    ),
    #[cfg(test)]
    InspectForTests(
        Box<dyn FnOnce(&mut super::state::KvDomainCore) + Send>,
        crossbeam_channel::Sender<()>,
    ),
}

impl KvDomainSink {
    #[cfg(test)]
    pub(super) fn request_actor<T>(
        &self,
        family: crate::runtime::routing::RouteFamily,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<T>) -> KvDomainCommand,
    ) -> Option<T> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            build_command(reply_tx),
        ) {
            tracing::warn!(domain = "kv", operation, error = %error, "KV actor command enqueue failed");
            return None;
        }

        match reply_rx.recv_timeout(Duration::from_secs(1)) {
            Ok(reply) => Some(reply),
            Err(error) => {
                tracing::warn!(domain = "kv", operation, error = %error, "KV actor command reply failed");
                None
            }
        }
    }

    /// Remove all live state owned by a disconnected session.
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
                KvDomainCommand::CleanupSession(session_id, reply_tx),
            )?;
            replies.push(reply_rx);
        }
        for reply_rx in replies {
            reply_rx
                .recv_timeout(Duration::from_secs(1))
                .map_err(crate::runtime::reply_wait::map_reply_wait_error)?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.try_send(
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            KvDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue KV actor test block");
    }

    #[cfg(test)]
    pub(super) fn inspect_for_tests(
        &self,
        inspect: impl FnOnce(&mut super::state::KvDomainCore) + Send + 'static,
    ) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.try_send(
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            KvDomainCommand::InspectForTests(Box::new(inspect), reply_tx),
        )
        .expect("enqueue KV actor inspection");
        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("receive KV actor inspection reply");
    }

    /// Return the number of live KV transactions, or zero if the actor does not reply.
    #[must_use]
    pub fn active_transaction_count(&self) -> usize {
        self.config.projection.active_transaction_count()
    }
}
