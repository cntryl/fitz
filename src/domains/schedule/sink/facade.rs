//! Public `ScheduleDomainSink` API and actor lifecycle management.

use super::model::{
    duration_millis, ScheduleDomainCommand, ScheduleDomainConfig, ScheduleDomainCore,
    ScheduleDomainRuntime, ScheduleDomainSink, ScheduleDomainState, ScheduleLiveCounts,
};
use crate::domains::schedule::ScheduleMetrics;
use crate::runtime::routing::RouteFamily;
use crate::runtime::Router;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

pub(crate) const DEFAULT_SCHEDULE_PRELOAD_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(120);

impl ScheduleDomainState {
    fn new_family(config: &ScheduleDomainConfig, route_family: RouteFamily) -> Self {
        Self {
            core: ScheduleDomainCore {
                route_family,
                store: config.store.clone(),
                actor: None,
                subscriptions: super::model::ScheduleSubscriptionSet::new(),
                cleaned_up_sessions: crate::runtime::CleanedUpSessions::new(
                    crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
                ),
                next_sub_id: config.next_sub_id.clone(),
                router: config.router.clone(),
                admin_read_model: config.admin_read_model.clone(),
                snapshot_dirty: config.snapshot_dirty.clone(),
                snapshot_syncing: config.snapshot_syncing.clone(),
                last_snapshot_elapsed_us: config.last_snapshot_elapsed_us.clone(),
                snapshot_epoch: config.snapshot_epoch.clone(),
                family_snapshots: config.family_snapshots.clone(),
                live_publish_failures: 0,
                ack_failures: 0,
                pending_ack_retries: HashMap::new(),
                recent_acknowledgement_ms: VecDeque::new(),
                write_policy: config.write_policy,
                metrics: config.metrics.clone(),
            },
        }
    }

    pub(super) fn runtime(&mut self) -> ScheduleDomainRuntime<'_> {
        ScheduleDomainRuntime {
            core: &mut self.core,
        }
    }
}

impl ScheduleDomainSink {
    pub fn new(
        store: crate::domains::schedule::ScheduleStore,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
    ) -> Self {
        Self::new_with_storage_and_families(
            store.into_storage(),
            router,
            admin_read_model,
            &[RouteFamily::new(1), RouteFamily::new(2)],
        )
    }

    pub(crate) fn new_with_storage_and_families(
        store: crate::storage::FitzStorageEngine,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        route_families: &[RouteFamily],
    ) -> Self {
        assert!(
            !route_families.is_empty(),
            "Schedule route families must not be empty"
        );
        let config = ScheduleDomainConfig {
            store,
            router,
            admin_read_model,
            next_sub_id: Arc::new(AtomicU64::new(1)),
            family_snapshots: Arc::new(parking_lot::Mutex::new(std::collections::BTreeMap::new())),
            snapshot_dirty: Arc::new(AtomicBool::new(false)),
            snapshot_syncing: Arc::new(AtomicBool::new(false)),
            last_snapshot_elapsed_us: Arc::new(AtomicU64::new(0)),
            snapshot_epoch: Arc::new(Instant::now()),
            write_policy: crate::domains::WritePolicy::Buffered,
            metrics: None,
        };
        let active = Arc::new(AtomicBool::new(true));
        let family_runtime =
            Self::spawn_family_runtime(config.clone(), active.clone(), route_families);
        Self {
            family_runtime,
            route_families: route_families.to_vec(),
            active,
            config,
        }
    }

    fn spawn_family_runtime(
        config: ScheduleDomainConfig,
        active: Arc<AtomicBool>,
        route_families: &[RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<ScheduleDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(route_families)
            .expect("validated Schedule family actor pool configuration");
        crate::runtime::FamilyActorPoolRuntime::spawn_with_family_failed_metric(
            pool,
            active,
            move |family| ScheduleDomainState::new_family(&config, family),
            |state, _, _, command| state.runtime().receive(command),
            crate::domains::schedule::metrics::METRIC_FAMILY_FAILED_CLOSED_TOTAL,
        )
    }

    fn rebuild_family_runtime(&mut self) {
        self.family_runtime.stop();
        self.active = Arc::new(AtomicBool::new(true));
        self.family_runtime = Self::spawn_family_runtime(
            self.config.clone(),
            self.active.clone(),
            &self.route_families,
        );
    }

    pub(super) fn try_send(
        &self,
        family: RouteFamily,
        lane: crate::runtime::FamilyActorLane,
        command: ScheduleDomainCommand,
    ) -> Result<(), crate::runtime::DeliveryError> {
        self.family_runtime
            .try_enqueue(family, lane, command)
            .map_err(crate::runtime::family_actor_enqueue_error_to_delivery_error)
    }

    #[must_use]
    pub fn with_write_policy(mut self, write_policy: crate::domains::WritePolicy) -> Self {
        self.family_runtime.stop();
        self.config.write_policy = write_policy;
        self.rebuild_family_runtime();
        self
    }

    #[must_use]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(ScheduleMetrics::new(collector));
        self.rebuild_family_runtime();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(super) fn is_family_running(&self, family: RouteFamily) -> bool {
        self.family_runtime.is_family_running(family)
    }

    #[cfg(test)]
    pub(super) fn panic_family_for_tests(&self, family: RouteFamily) {
        self.try_send(
            family,
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::PanicForFailpoint,
        )
        .expect("enqueue Schedule family panic");
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ActorHealthSnapshot {
        self.family_runtime.actor_health_snapshot()
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.route_families {
            let _ = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                ScheduleDomainCommand::PanicForFailpoint,
            );
        }
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
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::BlockForTests(entered, release),
        )
        .expect("enqueue Schedule actor test block");
    }

    /// # Errors
    ///
    /// Returns an error when listing column families or preloading a persisted
    /// schedule actor fails.
    pub fn preload_persisted_families(&self) -> Result<(), String> {
        self.preload_persisted_families_with_timeout(DEFAULT_SCHEDULE_PRELOAD_TIMEOUT)
    }

    /// # Errors
    ///
    /// Returns an error when the actor cannot be reached, preload fails, or the
    /// actor does not reply before `timeout`.
    pub(crate) fn preload_persisted_families_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<(), String> {
        let started_at = std::time::Instant::now();
        let timeout_ms = duration_millis(timeout);
        tracing::info!(domain = "schedule", timeout_ms, "Schedule preload started");
        let mut replies = Vec::with_capacity(self.route_families.len());
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                ScheduleDomainCommand::PreloadPersistedFamilies(reply_tx),
            ) {
                return Err(format!("schedule preload enqueue failed: {error}"));
            }
            replies.push(reply_rx);
        }
        for reply_rx in replies {
            match reply_rx.recv_timeout(timeout) {
                Ok(result) => result?,
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    tracing::error!(
                        domain = "schedule",
                        timeout_ms,
                        "Schedule preload timed out"
                    );
                    return Err(format!(
                        "schedule preload reply timed out after {timeout_ms}ms"
                    ));
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    return Err(
                        "schedule preload reply failed: actor reply channel disconnected"
                            .to_string(),
                    );
                }
            }
        }
        tracing::info!(
            domain = "schedule",
            elapsed_ms = duration_millis(started_at.elapsed()),
            "Schedule preload completed"
        );
        Ok(())
    }

    pub(crate) fn is_active(&self) -> bool {
        debug_assert!(!self.route_families.is_empty());
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn scan_due_schedules(&self) {
        for family in &self.route_families {
            if let Err(error) = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                ScheduleDomainCommand::ScanDueSchedules,
            ) {
                tracing::warn!(domain = "schedule", route_family = family.id(), error = %error, "Schedule due scan enqueue failed");
            }
        }
    }

    pub(crate) fn force_due_scan_for_tests(&self, ready_count: usize) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send(
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::ForceDueScanForTests(ready_count, reply_tx),
        ) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule forced due scan enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule forced due scan reply failed");
        }
    }

    pub fn admin_pending_claims(
        &self,
        route_family: crate::runtime::routing::RouteFamily,
    ) -> Vec<crate::control::admin::SchedulePendingClaimInfo> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send(
            route_family,
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::ReadPendingClaims(route_family, reply_tx),
        ) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule pending claim read enqueue failed");
            return Vec::new();
        }

        reply_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .unwrap_or_default()
    }

    fn live_counts(&self) -> ScheduleLiveCounts {
        let mut replies = Vec::with_capacity(self.route_families.len());
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if let Err(error) = self.try_send(
                *family,
                crate::runtime::FamilyActorLane::Control,
                ScheduleDomainCommand::ReadLiveCounts(reply_tx),
            ) {
                tracing::warn!(domain = "schedule", route_family = family.id(), error = %error, "Schedule live-count query enqueue failed");
                continue;
            }
            replies.push(reply_rx);
        }
        replies
            .into_iter()
            .filter_map(|reply| reply.recv_timeout(std::time::Duration::from_secs(1)).ok())
            .fold(ScheduleLiveCounts::default(), |counts, family_counts| {
                counts.merge(&family_counts)
            })
    }

    pub(crate) fn refresh_admin_snapshot_if_dirty(&self) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send(
            self.route_families[0],
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::RefreshAdminSnapshotIfDirty(reply_tx),
        ) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule admin snapshot refresh enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule admin snapshot refresh reply failed");
        }
    }

    #[doc(hidden)]
    pub fn bench_publish_event(&self, event: &crate::runtime::DomainPublishEvent) {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.try_send(
            event.family_id,
            crate::runtime::FamilyActorLane::Control,
            ScheduleDomainCommand::BenchPublishEvent(event.clone(), reply_tx),
        ) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule bench publish enqueue failed");
            return;
        }

        if let Err(error) = reply_rx.recv_timeout(std::time::Duration::from_secs(1)) {
            tracing::warn!(domain = "schedule", error = %error, "Schedule bench publish reply failed");
        }
    }
}

/// Narrow read-only surface used by metrics and administration code.
impl ScheduleDomainSink {
    pub fn subscription_count(&self) -> usize {
        self.live_counts().subscriptions
    }

    pub fn schedule_count(&self) -> usize {
        self.live_counts().schedules
    }

    pub fn pending_fire_count(&self) -> usize {
        self.live_counts().pending_fires
    }

    pub fn executions_per_minute(&self) -> f64 {
        self.live_counts().executions_per_minute
    }

    pub fn notify_failure_count(&self) -> u64 {
        self.live_counts().notify_failures
    }

    pub fn ack_failure_count(&self) -> u64 {
        self.live_counts().ack_failures
    }

    pub fn pending_ack_retry_count(&self) -> usize {
        self.live_counts().pending_ack_retries
    }

    pub fn oldest_pending_claim_age_seconds(&self) -> u64 {
        self.live_counts().oldest_pending_claim_age_seconds
    }

    pub fn overdue_normalization_count(&self) -> u64 {
        self.live_counts().overdue_normalizations
    }
}
