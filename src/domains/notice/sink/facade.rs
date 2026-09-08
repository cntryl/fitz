//! Public `NoticeDomainSink` API and family-runtime lifecycle management.

use super::{
    DeliveryError, Envelope, NoticeDomainCommand, NoticeDomainCore, NoticeDomainRuntime,
    NoticeDomainSink, NoticeMetrics,
};
use crate::runtime::routing::RouteFamily;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

impl NoticeDomainSink {
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
        let core = Arc::new(NoticeDomainCore {
            families: parking_lot::Mutex::new(std::collections::HashMap::new()),
            route_stats: parking_lot::Mutex::new(std::collections::HashMap::with_capacity(64)),
            next_sub_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
            router,
            admin_read_model,
            admin_snapshot_dirty: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            metrics: None,
            active: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            family_cores: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
            delivery_workers: parking_lot::Mutex::new(std::collections::HashMap::new()),
            cleaned_up_sessions: parking_lot::Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
        });
        let family_families = families.to_vec();
        let family_runtime = Self::spawn_family_runtime(&core, &family_families);
        let core = Self::primary_family_core(&core, &family_families);
        Self {
            core,
            family_runtime,
            family_families,
        }
    }

    fn spawn_family_runtime(
        core: &Arc<NoticeDomainCore>,
        families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<NoticeDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(families)
            .expect("validated Notice family actor pool configuration");
        let factory_core = core.clone();
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            core.active.clone(),
            move |family| Self::family_core_for(&factory_core, family),
            |core, _family, _lane, command| NoticeDomainRuntime { core }.receive(command),
            crate::domains::notice::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn family_core_for(
        shared: &Arc<NoticeDomainCore>,
        family: RouteFamily,
    ) -> Arc<NoticeDomainCore> {
        let core = Arc::new(NoticeDomainCore {
            families: parking_lot::Mutex::new(std::collections::HashMap::new()),
            route_stats: parking_lot::Mutex::new(std::collections::HashMap::with_capacity(64)),
            next_sub_id: shared.next_sub_id.clone(),
            router: shared.router.clone(),
            admin_read_model: shared.admin_read_model.clone(),
            admin_snapshot_dirty: shared.admin_snapshot_dirty.clone(),
            metrics: shared.metrics.clone(),
            active: shared.active.clone(),
            family_cores: shared.family_cores.clone(),
            cleaned_up_sessions: parking_lot::Mutex::new(crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            )),
            delivery_workers: parking_lot::Mutex::new(std::collections::HashMap::new()),
        });
        shared
            .family_cores
            .lock()
            .insert(family.id(), Arc::downgrade(&core));
        core
    }

    fn primary_family_core(
        shared: &Arc<NoticeDomainCore>,
        families: &[RouteFamily],
    ) -> Arc<NoticeDomainCore> {
        let primary = families
            .first()
            .expect("Notice route families must not be empty");
        shared
            .family_cores
            .lock()
            .get(&primary.id())
            .and_then(std::sync::Weak::upgrade)
            .expect("Notice primary family core was created with its runtime")
    }

    fn try_send(
        &self,
        family: RouteFamily,
        lane: crate::runtime::FamilyActorLane,
        command: NoticeDomainCommand,
    ) -> Result<(), DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(Self::family_enqueue_error)
    }

    fn family_enqueue_error(error: crate::runtime::FamilyActorEnqueueError) -> DeliveryError {
        match error {
            crate::runtime::FamilyActorEnqueueError::NormalLaneFull => DeliveryError::MailboxFull {
                capacity: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
                current_len: crate::runtime::FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            },
            crate::runtime::FamilyActorEnqueueError::ControlLaneFull => {
                DeliveryError::HighLaneFull {
                    capacity: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                    current_len: crate::runtime::FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
                }
            }
            crate::runtime::FamilyActorEnqueueError::UnknownFamily
            | crate::runtime::FamilyActorEnqueueError::ActorStopped => DeliveryError::ActorStopped,
        }
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.family_runtime = Self::spawn_family_runtime(&self.core, &self.family_families);
        self.core = Self::primary_family_core(&self.core, &self.family_families);
    }

    fn core_for_builder(&mut self) -> &mut NoticeDomainCore {
        self.family_runtime.stop();
        if let Some(primary) = self.family_families.first() {
            self.core.family_cores.lock().remove(&primary.id());
        }
        Arc::get_mut(&mut self.core).expect("Notice sink builders must run before sharing the sink")
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.core_for_builder().metrics = Some(NoticeMetrics::new(collector));
        self.core.refresh_metrics_gauges();
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.core.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn is_active(&self) -> bool {
        self.core.active.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn subscription_family_count(&self) -> usize {
        self.live_family_cores()
            .iter()
            .map(|core| core.families.lock().len())
            .sum()
    }

    #[cfg(test)]
    #[must_use]
    pub(super) fn route_stats_count(&self) -> usize {
        self.live_family_cores()
            .iter()
            .map(|core| core.route_stats.lock().len())
            .sum()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(super) fn is_family_running(&self, family: RouteFamily) -> bool {
        self.family_runtime.is_family_running(family)
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        self.family_runtime.managed_actor_health_snapshot()
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
    fn live_family_cores(&self) -> Vec<Arc<NoticeDomainCore>> {
        self.core
            .family_cores
            .lock()
            .values()
            .filter_map(std::sync::Weak::upgrade)
            .collect()
    }

    fn family_core(&self, family: RouteFamily) -> Option<Arc<NoticeDomainCore>> {
        self.core
            .family_cores
            .lock()
            .get(&family.id())
            .and_then(std::sync::Weak::upgrade)
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        let family = self.family_families[0];
        if let Err(error) = self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            NoticeDomainCommand::RefreshAdminSnapshotIfDirty(reply_tx),
        ) {
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

    /// Return the family-actor-owned live Notice subscription count.
    ///
    /// # Errors
    ///
    /// Returns the enqueue failure or `DeliveryError::Timeout` when the live
    /// family runtime does not reply before the bounded query deadline.
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
            let session_id = envelope
                .payload::<crate::domains::notice::NoticeClientRequest>()
                .map(|request| request.meta.session_id);
            let command = NoticeDomainCommand::DeliverAccepted(envelope);
            let enqueue = || self.try_send(family, lane, command);
            return match session_id {
                Some(session_id) => self
                    .family_core(family)
                    .ok_or(DeliveryError::ActorStopped)?
                    .enqueue_if_session_open(session_id, enqueue),
                None => enqueue(),
            };
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
