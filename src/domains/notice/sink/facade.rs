//! Public `NoticeDomain` API and family-runtime lifecycle management.

use super::{
    DeliveryError, Envelope, NoticeDomain, NoticeDomainCommand, NoticeDomainConfig,
    NoticeFamilyRuntime, NoticeFamilyState, NoticeMetrics,
};
use crate::runtime::routing::RouteFamily;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl NoticeDomain {
    pub fn new(
        router: Arc<crate::runtime::Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_families(
            router,
            admin_read_model,
            &[RouteFamily::new(1), RouteFamily::new(2)],
        )
    }

    pub(crate) fn new_with_families(
        router: Arc<crate::runtime::Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        families: &[RouteFamily],
    ) -> Self {
        let config = NoticeDomainConfig {
            next_sub_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            router,
            admin_read_model,
            metrics: None,
            active: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        };
        let family_families = families.to_vec();
        let family_runtime = Self::spawn_family_runtime(&config, &family_families);
        Self {
            config,
            family_runtime,
            family_families,
        }
    }

    fn spawn_family_runtime(
        config: &NoticeDomainConfig,
        families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<NoticeDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .expect("validated Notice family actor pool configuration");
        let factory_config = config.clone();
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            config.active.clone(),
            move |family| NoticeFamilyState::new(family, &factory_config),
            |state, _family, _lane, command| NoticeFamilyRuntime { core: state }.receive(command),
            crate::domains::notice::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn try_send(
        &self,
        family: RouteFamily,
        lane: crate::runtime::FamilyActorLane,
        command: NoticeDomainCommand,
    ) -> Result<(), DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.family_runtime = Self::spawn_family_runtime(&self.config, &self.family_families);
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(NoticeMetrics::new(collector));
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.config.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn is_active(&self) -> bool {
        self.config.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn subscription_family_count(&self) -> usize {
        self.state_counts().0
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn route_stats_count(&self) -> usize {
        self.state_counts().1
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(super) fn is_family_running(&self, family: RouteFamily) -> bool {
        self.family_runtime.is_family_running(family)
    }

    pub(crate) fn family_health_snapshot(
        &self,
    ) -> crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot {
        self.family_runtime.health_snapshot()
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.family_families {
            let _ = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                NoticeDomainCommand::PanicForFailpoint,
            );
        }
    }

    #[cfg(test)]
    pub(super) fn panic_family_for_tests(&self, family: RouteFamily) {
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            NoticeDomainCommand::PanicForFailpoint,
        )
        .expect("enqueue Notice family panic");
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(super) fn block_actor_for_tests(
        &self,
        entered: crossbeam_channel::Sender<()>,
        release: crossbeam_channel::Receiver<()>,
    ) {
        self.try_send(
            self.family_families[0],
            crate::runtime::FamilyActorLane::Control,
            NoticeDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue Notice family test block");
    }

    #[cfg(test)]
    fn state_counts(&self) -> (usize, usize) {
        let mut replies = Vec::with_capacity(self.family_families.len());
        for family in &self.family_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                NoticeDomainCommand::ReadStateCounts(reply_tx),
            )
            .expect("enqueue Notice state-count query");
            replies.push(reply_rx);
        }
        replies.into_iter().fold((0, 0), |totals, reply| {
            let counts = reply
                .recv_timeout(Duration::from_secs(1))
                .expect("receive Notice state-count query");
            (totals.0 + counts.0, totals.1 + counts.1)
        })
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        let _configured_families = self.family_families.len();
        NoticeDomainConfig::refresh_admin_snapshot_if_dirty();
    }

    /// Return the family-actor-owned live Notice subscription count.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// family runtime does not reply before the bounded query deadline.
    #[cfg(test)]
    pub fn subscription_count(&self) -> Result<usize, DeliveryError> {
        let mut replies = Vec::with_capacity(self.family_families.len());
        for family in &self.family_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                NoticeDomainCommand::ReadSubscriptionCount(reply_tx),
            ) {
                tracing::warn!(domain = "notice", error = %error, "Notice subscription-count query enqueue failed");
                return Err(error);
            }
            replies.push(reply_rx);
        }

        replies.into_iter().try_fold(0_usize, |total, reply| {
            reply
                .recv_timeout(Duration::from_secs(1))
                .map(|count| total.saturating_add(count))
                .map_err(|_| DeliveryError::Timeout)
        })
    }

    /// Remove every Notice registration owned by one ephemeral session.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// family runtime does not reply before the bounded cleanup deadline.
    #[cfg(test)]
    pub fn unsubscribe_all_for_session(&self, session_id: u64) -> Result<usize, DeliveryError> {
        let mut replies = Vec::with_capacity(self.family_families.len());
        for family in &self.family_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                NoticeDomainCommand::UnsubscribeAllForSession(session_id, reply_tx),
            ) {
                tracing::warn!(
                    domain = "notice",
                    error = %error,
                    "Notice session cleanup command enqueue failed"
                );
                return Err(error);
            }
            replies.push(reply_rx);
        }

        replies.into_iter().try_fold(0_usize, |total, reply| {
            reply
                .recv_timeout(Duration::from_secs(1))
                .map(|count| total.saturating_add(count))
                .map_err(|_| DeliveryError::Timeout)
        })
    }

    pub(super) fn deliver_to_family(
        &self,
        envelope: Envelope,
        high_priority: bool,
    ) -> Result<(), DeliveryError> {
        let family = *envelope.destination().family();
        let lane = if high_priority {
            crate::runtime::FamilyActorLane::Control
        } else {
            crate::runtime::FamilyActorLane::Normal
        };
        if Self::can_accept_without_reply(&envelope) {
            let command = NoticeDomainCommand::DeliverAccepted(envelope);
            return self.try_send(family, lane, command);
        }

        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let command = NoticeDomainCommand::Deliver(envelope, reply_tx);
        self.try_send(family, lane, command)?;

        reply_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap_or(Err(DeliveryError::Timeout))
    }

    #[cfg(test)]
    pub(super) fn enqueue_cleanup_for_tests(
        &self,
        envelope: Envelope,
    ) -> Result<(), DeliveryError> {
        let family = *envelope.destination().family();
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            NoticeDomainCommand::Deliver(envelope, crossbeam_channel::bounded(1).0),
        )
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
