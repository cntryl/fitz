//! Public `NoticeDomainSink` API and actor lifecycle management.

use super::{
    DeliveryError, Envelope, NoticeDomainActor, NoticeDomainCommand, NoticeDomainCore,
    NoticeDomainSink, NoticeMetrics,
};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl NoticeDomainSink {
    pub fn new(
        router: Arc<crate::runtime::Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        let core = Arc::new(NoticeDomainCore {
            families: parking_lot::Mutex::new(std::collections::HashMap::new()),
            route_stats: parking_lot::Mutex::new(std::collections::HashMap::with_capacity(64)),
            next_sub_id: std::sync::atomic::AtomicU64::new(1),
            router,
            admin_read_model,
            admin_snapshot_dirty: std::sync::atomic::AtomicBool::new(false),
            metrics: None,
            active: std::sync::atomic::AtomicBool::new(true),
            delivery_workers: parking_lot::Mutex::new(std::collections::HashMap::new()),
            cleaned_up_sessions: parking_lot::Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
        });
        let actor = Self::spawn_actor(core.clone());
        Self { core, actor }
    }

    fn spawn_actor(
        core: Arc<NoticeDomainCore>,
    ) -> crate::runtime::ManagedActor<NoticeDomainCommand> {
        let router = core.router.clone();
        crate::runtime::ManagedActor::spawn_fail_closed(
            router,
            NoticeDomainActor::route_address(),
            move || NoticeDomainActor::new(core.clone()),
            crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
        )
    }

    fn rebuild_actor(&mut self) {
        self.actor.stop();
        self.actor = Self::spawn_actor(self.core.clone());
    }

    fn core_for_builder(&mut self) -> &mut NoticeDomainCore {
        Arc::get_mut(&mut self.core).expect("Notice sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.actor.stop();
        self.core_for_builder().metrics = Some(NoticeMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        self.actor.stop();
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn subscription_family_count(&self) -> usize {
        self.core.families.lock().len()
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn route_stats_count(&self) -> usize {
        self.core.route_stats.lock().len()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.actor.is_running()
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.actor.health_snapshot()
    }

    #[cfg(test)]
    pub(crate) fn panic_actor_for_tests(&self) {
        let _ = self
            .actor
            .try_send_high_priority(NoticeDomainCommand::PanicForTests);
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.actor.stop();
    }

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.actor
            .try_send_high_priority(NoticeDomainCommand::BlockForTests(entered, release))
            .expect("enqueue Notice actor test block");
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send(NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply_tx))
        {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice admin snapshot refresh enqueue failed"
            );
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(Duration::from_secs(1)) {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice admin snapshot refresh reply failed"
            );
        }
    }

    /// Return the actor-owned live Notice subscription count.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// actor does not reply before the bounded query deadline.
    pub fn subscription_count(&self) -> Result<usize, DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self
            .actor
            .try_send_high_priority(NoticeDomainCommand::ReadSubscriptionCount(reply_tx))
        {
            tracing::warn!(domain = "notice", error = %error, "Notice subscription-count query enqueue failed");
            return Err(error);
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| DeliveryError::Timeout)
    }

    /// Remove every Notice registration owned by one ephemeral session.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// actor does not reply before the bounded cleanup deadline.
    pub fn unsubscribe_all_for_session(&self, session_id: u64) -> Result<usize, DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) =
            self.actor
                .try_send_high_priority(NoticeDomainCommand::UnsubscribeAllForSession(
                    session_id, reply_tx,
                ))
        {
            tracing::warn!(
                domain = "notice",
                error = %error,
                "Notice session cleanup command enqueue failed"
            );
            return Err(error);
        }

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .map_err(|_| DeliveryError::Timeout)
    }

    pub(super) fn deliver_to_actor(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        if Self::can_accept_without_reply(&envelope) {
            let session_id = envelope
                .payload::<crate::domains::notice::NoticeClientRequest>()
                .map(|request| request.meta.session_id);
            let command = NoticeDomainCommand::DeliverAccepted(envelope);
            let enqueue = || {
                if high_priority {
                    self.actor.try_send_high_priority(command)
                } else {
                    self.actor.try_send(command)
                }
            };
            return match session_id {
                Some(session_id) => self.core.enqueue_if_session_open(session_id, enqueue),
                None => enqueue(),
            };
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = NoticeDomainCommand::Deliver(envelope, reply_tx);
        let enqueue_result = if high_priority {
            self.actor.try_send_high_priority(command)
        } else {
            self.actor.try_send(command)
        };
        enqueue_result?;

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::Timeout))
    }

    fn can_accept_without_reply(envelope: &Envelope) -> bool {
        if envelope
            .payload::<crate::runtime::DomainPublishEvent>()
            .is_some()
        {
            return true;
        }

        envelope
            .payload::<crate::domains::notice::NoticeClientRequest>()
            .is_some_and(|request| {
                let Ok(crate::domains::notice::protocol::NotificationMessage::Publish(publish)) =
                    &request.message
                else {
                    return false;
                };
                request.meta.route_family == *envelope.destination().family()
                    && envelope
                        .source()
                        .is_none_or(|source| *source.family() == request.meta.route_family)
                    && publish.family_id == request.meta.route_family
            })
    }
}
