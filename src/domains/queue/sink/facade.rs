use super::model::{
    QueueDomain, QueueDomainCommand, QueueDomainConfig, QueueFamilyRuntime, QueueFamilyState,
    QueueLiveCounts,
};
#[cfg(test)]
use super::model::{WarmQueueActor, QUEUE_ACTOR_IDLE_TTL};
use crate::domains::queue::actor::QUEUE_ACTOR_REPLY_TIMEOUT;
use crate::domains::queue::{projection::QueueAdminProjection, QueueMetrics};
use crate::runtime::Router;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueueCounts {
    pub pending: usize,
    pub ready: usize,
    pub delayed: usize,
    pub inflight: usize,
    pub dead_letters: usize,
}

impl QueueFamilyState {
    fn new(config: &QueueDomainConfig, family: crate::runtime::routing::RouteFamily) -> Self {
        Self {
            route_family: family,
            delivery_service_us: config.delivery_service_us[&family.id()].clone(),
            store: config.store.clone(),
            queue_write_policy: config.queue_write_policy,
            dedup_store: config.dedup_store.clone(),
            actors: HashMap::new(),
            idle_sweep_keys: VecDeque::new(),
            known_queue_keys: config
                .known_queue_keys
                .iter()
                .filter(|key| key.family == family)
                .cloned()
                .collect(),
            inventory_error: config.inventory_error.clone(),
            wildcard_reserve_sequence: AtomicU64::new(0),
            families: HashMap::new(),
            cleaned_up_sessions: crate::runtime::CleanedUpSessions::new(
                crate::domains::DOMAIN_ACTOR_MAILBOX_CAPACITY,
            ),
            next_sub_id: AtomicU64::new(1),
            ready_states: HashMap::new(),
            pending_reserves: VecDeque::new(),
            router: config.router.clone(),
            projection: config.projection.clone(),
            metrics: config.metrics.clone(),
            active: config.active.clone(),
            runtime_sweep_pending: config.runtime_sweep_pending[&family.id()].clone(),
            #[cfg(test)]
            panic_next_runtime_sweep: AtomicBool::new(false),
            next_idle_sweep_at: Instant::now(),
            next_dedup_sweep_at: Instant::now(),
            dirty_fast_flush_families: HashSet::new(),
            fast_flush_interval: config.fast_flush_interval,
            next_fast_flush_at: config
                .fast_flush_interval
                .map_or_else(Instant::now, |interval| Instant::now() + interval),
        }
    }
}

impl QueueDomain {
    /// Constructs a queue sink over a raw Midge engine after preparing persisted queue state.
    ///
    /// The recovery write policy is explicit because startup reconciliation can write before the
    /// sink starts handling queue traffic. Cloud-backed engines must receive a cloud-compatible
    /// recovery policy such as [`crate::domains::WritePolicy::CloudAsync`] or
    /// [`crate::domains::WritePolicy::CloudStrict`].
    ///
    /// # Errors
    ///
    /// Returns an error when persisted queue state is invalid or cannot be reconciled.
    #[allow(private_bounds)]
    #[cfg(test)]
    pub fn try_new(
        store: impl Into<crate::domains::queue::actor::recovery_store::QueueStore>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_policy: crate::domains::WritePolicy,
        recovery_write_policy: crate::domains::WritePolicy,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        Self::try_new_with_storage(
            store,
            router,
            admin_read_model,
            queue_write_policy,
            recovery_write_policy,
            dedup_store,
        )
    }

    pub(crate) fn try_new_with_storage(
        store: impl Into<crate::domains::queue::actor::recovery_store::QueueStore>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_policy: crate::domains::WritePolicy,
        recovery_write_policy: crate::domains::WritePolicy,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Result<Self, String> {
        let store = store.into();
        crate::domains::queue::QueueActor::prepare_persisted_state_for_existing_families(
            store.clone(),
            queue_write_policy,
            recovery_write_policy,
        )?;
        let known_queue_keys = QueueFamilyState::inventory_existing_queue_keys(&store)?;
        Ok(Self::new_with_storage_and_inventory(
            store,
            router,
            admin_read_model,
            queue_write_policy,
            dedup_store,
            known_queue_keys,
            None,
        ))
    }

    #[allow(private_bounds)]
    pub fn new(
        store: impl Into<crate::domains::queue::actor::recovery_store::QueueStore>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_policy: crate::domains::WritePolicy,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        Self::new_with_storage(
            store,
            router,
            admin_read_model,
            queue_write_policy,
            dedup_store,
        )
    }

    pub(crate) fn new_with_storage(
        store: impl Into<crate::domains::queue::actor::recovery_store::QueueStore>,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_policy: crate::domains::WritePolicy,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
    ) -> Self {
        let store = store.into();
        let (known_queue_keys, inventory_error) =
            match QueueFamilyState::inventory_existing_queue_keys(&store) {
                Ok(keys) => (keys, None),
                Err(error) => {
                    tracing::warn!(
                        domain = "queue",
                        error = %error,
                        "Queue inventory unavailable during infallible sink construction"
                    );
                    (HashSet::new(), Some(error))
                }
            };
        Self::new_with_storage_and_inventory(
            store,
            router,
            admin_read_model,
            queue_write_policy,
            dedup_store,
            known_queue_keys,
            inventory_error,
        )
    }

    #[allow(clippy::needless_pass_by_value)]
    fn new_with_storage_and_inventory(
        store: crate::domains::queue::actor::recovery_store::QueueStore,
        router: Arc<Router>,
        admin_read_model: Arc<crate::control::admin::read_model::AdminReadModel>,
        queue_write_policy: crate::domains::WritePolicy,
        dedup_store: Arc<crate::utils::idempotency::DedupStore>,
        known_queue_keys: HashSet<crate::domains::queue::QueueKey>,
        inventory_error: Option<String>,
    ) -> Self {
        let route_families = store
            .family_ids()
            .expect("Queue column families were inventoried during construction")
            .into_iter()
            .filter(|family| *family != 0)
            .map(crate::runtime::routing::RouteFamily::new)
            .collect::<Vec<_>>();
        let active = Arc::new(AtomicBool::new(true));
        let projection = Arc::new(QueueAdminProjection::new(admin_read_model));
        let delivery_service_us = Arc::new(
            route_families
                .iter()
                .map(|family| {
                    (
                        family.id(),
                        Arc::new(std::sync::atomic::AtomicU64::new(
                            super::model::assumed_service_us(),
                        )),
                    )
                })
                .collect(),
        );
        let runtime_sweep_pending = Arc::new(
            route_families
                .iter()
                .map(|family| (family.id(), Arc::new(AtomicBool::new(false))))
                .collect(),
        );
        let config = QueueDomainConfig {
            store,
            queue_write_policy,
            dedup_store,
            router,
            projection,
            metrics: None,
            active: active.clone(),
            fast_flush_interval: None,
            known_queue_keys: Arc::new(known_queue_keys),
            inventory_error,
            delivery_service_us,
            runtime_sweep_pending,
        };
        let family_runtime = Self::spawn_family_runtime(&config, active.clone(), &route_families);
        Self {
            config,
            active,
            family_runtime,
            route_families,
            inflight_client_deliveries: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    fn spawn_family_runtime(
        config: &QueueDomainConfig,
        active: Arc<AtomicBool>,
        route_families: &[crate::runtime::routing::RouteFamily],
    ) -> crate::runtime::FamilyActorPoolRuntime<QueueDomainCommand> {
        let pool = crate::runtime::FamilyActorPool::new(route_families)
            .expect("validated Queue family actor pool configuration");
        let family_config = config.clone();
        crate::runtime::FamilyActorPoolRuntime::spawn(
            pool,
            active,
            move |family| QueueFamilyRuntime {
                core: QueueFamilyState::new(&family_config, family),
            },
            |actor, _, _, command| actor.receive_command(command),
        )
    }

    fn rebuild_actor(&mut self) {
        self.family_runtime.stop();
        self.family_runtime =
            Self::spawn_family_runtime(&self.config, self.active.clone(), &self.route_families);
    }

    #[must_use]
    /// Adds Queue metrics to every configured family runtime.
    ///
    /// # Panics
    ///
    /// Panics if a family runtime retains its core after the runtime has stopped.
    #[allow(clippy::needless_pass_by_value)]
    pub fn with_metrics(
        mut self,
        collector: crate::observability::metrics::MetricsCollector,
    ) -> Self {
        self.family_runtime.stop();
        self.config.metrics = Some(QueueMetrics::new(collector));
        self.rebuild_actor();
        self
    }

    #[must_use]
    /// Configures the fast-policy flush interval for every Queue family.
    ///
    /// # Panics
    ///
    /// Panics if a family runtime retains its core after the runtime has stopped.
    pub fn with_fast_flush_interval(mut self, interval: Option<Duration>) -> Self {
        self.family_runtime.stop();
        self.config.fast_flush_interval = interval;
        self.rebuild_actor();
        self
    }

    pub fn stop(&self) {
        self.active.store(false, Ordering::Relaxed);
        self.family_runtime.stop();
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    pub(crate) fn family_health_snapshot(
        &self,
    ) -> crate::runtime::family_actor_pool::FamilyActorPoolHealthSnapshot {
        self.family_runtime.health_snapshot()
    }

    #[cfg(test)]
    pub(super) fn is_actor_running(&self) -> bool {
        self.family_runtime.is_running()
    }

    #[cfg(test)]
    pub(super) fn set_inventory_error_for_tests(&self, error: impl Into<String>) {
        let error = error.into();
        for family in &self.route_families {
            let error = error.clone();
            self.inspect_family_for_tests(*family, move |state| {
                state.inventory_error = Some(error);
            });
        }
    }

    pub(crate) fn panic_actor_for_failpoint(&self) {
        for family in &self.route_families {
            let _ = self.family_runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                QueueDomainCommand::PanicForFailpoint,
            );
        }
    }

    #[cfg(test)]
    pub(super) fn stop_actor_for_tests(&self) {
        self.family_runtime.stop();
    }

    #[cfg(test)]
    pub(super) fn actor_count_for_tests(&self) -> usize {
        self.route_families
            .iter()
            .map(|family| self.inspect_family_for_tests(*family, |state| state.actors.len()))
            .sum()
    }

    #[cfg(test)]
    pub(super) fn actors_are_empty_for_tests(&self) -> bool {
        self.route_families
            .iter()
            .all(|family| self.inspect_family_for_tests(*family, |state| state.actors.is_empty()))
    }

    #[cfg(test)]
    pub(super) fn known_queue_count_for_tests(&self) -> usize {
        self.route_families
            .iter()
            .map(|family| {
                self.inspect_family_for_tests(*family, |state| state.known_queue_keys.len())
            })
            .sum()
    }

    #[cfg(test)]
    pub(super) fn known_queue_contains_for_tests(
        &self,
        key: &crate::domains::queue::QueueKey,
    ) -> bool {
        let key = key.clone();
        self.inspect_family_for_tests(key.family, move |state| {
            state.known_queue_keys.contains(&key)
        })
    }

    #[cfg(test)]
    pub(super) fn install_actor_for_tests(
        &self,
        key: crate::domains::queue::QueueKey,
        actor: crate::domains::queue::QueueActor,
    ) {
        self.inspect_family_for_tests(key.family, move |state| {
            state.known_queue_keys.insert(key.clone());
            state.actors.insert(
                key.clone(),
                WarmQueueActor {
                    actor,
                    last_used: Instant::now(),
                },
            );
            state.idle_sweep_keys.push_back(key);
        });
    }

    #[cfg(test)]
    pub(super) fn queue_snapshot_for_tests(
        &self,
        family: crate::runtime::routing::RouteFamily,
        queue_route: &str,
    ) -> crate::domains::queue::QueueAdminSnapshot {
        let key = crate::domains::queue::QueueKey::from_route(
            family,
            &crate::runtime::routing::Route::new(queue_route),
        )
        .expect("queue key");
        self.inspect_family_for_tests(family, move |state| {
            state.actors[&key].actor.admin_snapshot()
        })
    }

    #[cfg(test)]
    pub(super) fn force_actor_idle_for_tests(
        &self,
        family: crate::runtime::routing::RouteFamily,
        queue_route: &str,
    ) {
        let key = crate::domains::queue::QueueKey::from_route(
            family,
            &crate::runtime::routing::Route::new(queue_route),
        )
        .expect("queue key");
        self.inspect_family_for_tests(family, move |state| {
            let warm_actor = state.actors.get_mut(&key).expect("warm queue actor");
            warm_actor.last_used = Instant::now()
                .checked_sub(QUEUE_ACTOR_IDLE_TTL + Duration::from_secs(1))
                .expect("idle deadline should remain representable");
        });
    }

    #[cfg(test)]
    pub(super) fn dirty_fast_flush_contains_family_for_tests(&self, family_id: u32) -> bool {
        self.route_families.iter().any(|family| {
            self.inspect_family_for_tests(*family, move |state| {
                state.dirty_fast_flush_families.contains(&family_id)
            })
        })
    }

    #[cfg(test)]
    pub(super) fn dirty_fast_flush_is_empty_for_tests(&self) -> bool {
        self.route_families.iter().all(|family| {
            self.inspect_family_for_tests(*family, |state| {
                state.dirty_fast_flush_families.is_empty()
            })
        })
    }

    #[cfg(test)]
    pub(super) fn clear_dirty_fast_flush_for_tests(&self) {
        for family in &self.route_families {
            self.inspect_family_for_tests(*family, |state| {
                state.dirty_fast_flush_families.clear();
            });
        }
    }

    #[cfg(test)]
    pub(super) fn insert_dirty_fast_flush_family_for_tests(&self, family_id: u32) {
        self.inspect_family_for_tests(self.route_families[0], move |state| {
            state.dirty_fast_flush_families.insert(family_id);
        });
    }

    #[cfg(test)]
    pub(super) fn watch_families_are_empty_for_tests(&self) -> bool {
        self.route_families
            .iter()
            .all(|family| self.inspect_family_for_tests(*family, |state| state.families.is_empty()))
    }

    #[cfg(test)]
    pub(super) fn set_next_dedup_sweep_at_for_tests(&self, now: Instant) {
        for family in &self.route_families {
            self.inspect_family_for_tests(*family, move |state| state.next_dedup_sweep_at = now);
        }
    }

    #[cfg(test)]
    pub(super) fn panic_next_runtime_sweep_for_tests(&self) {
        self.inspect_family_for_tests(self.route_families[0], |state| {
            state
                .panic_next_runtime_sweep
                .store(true, Ordering::Release);
        });
    }

    #[cfg(test)]
    pub(super) fn runtime_sweep_pending_for_tests(&self) -> bool {
        self.config
            .runtime_sweep_pending
            .values()
            .any(|pending| pending.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(super) fn inspect_family_for_tests<T: Send + 'static>(
        &self,
        family: crate::runtime::routing::RouteFamily,
        inspect: impl FnOnce(&mut QueueFamilyState) -> T + Send + 'static,
    ) -> T {
        let (result_tx, result_rx) = crossbeam_channel::bounded(1);
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        self.family_runtime
            .try_enqueue(
                family,
                crate::runtime::FamilyActorLane::Control,
                QueueDomainCommand::InspectForTests(
                    Box::new(move |state| {
                        let _ = result_tx.send(inspect(state));
                    }),
                    reply_tx,
                ),
            )
            .expect("enqueue Queue family inspection");
        reply_rx
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .expect("complete Queue family inspection");
        result_rx
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .expect("receive Queue family inspection")
    }

    fn send_unit_actor_command(
        &self,
        family: crate::runtime::routing::RouteFamily,
        operation: &'static str,
        build_command: impl FnOnce(crossbeam_channel::Sender<()>) -> QueueDomainCommand,
    ) -> Result<(), crate::runtime::DeliveryError> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.family_runtime.try_enqueue(
            family,
            crate::runtime::FamilyActorLane::Control,
            build_command(reply_tx),
        ) {
            let error = crate::runtime::family_actor_enqueue_error_to_delivery_error(error);
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command enqueue failed");
            return Err(error);
        }

        // Returning the outcome rather than swallowing it: callers previously
        // could not tell a completed command from one that timed out, so a
        // silently dropped session cleanup looked identical to a successful
        // one.
        reply_rx
            .recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT)
            .map_err(|error| {
                tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
                crate::runtime::reply_wait::map_reply_wait_error(error)
            })
    }

    fn send_bool_actor_command(
        &self,
        family: crate::runtime::routing::RouteFamily,
        operation: &'static str,
        build_command: impl FnOnce(
            crossbeam_channel::Sender<Result<bool, String>>,
        ) -> QueueDomainCommand,
    ) -> Result<bool, String> {
        let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
        if let Err(error) = self.family_runtime.try_enqueue(
            family,
            crate::runtime::FamilyActorLane::Control,
            build_command(reply_tx),
        ) {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command enqueue failed");
            return Err(format!(
                "Queue actor command enqueue failed for {operation}: {error}"
            ));
        }

        reply_rx.recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT).map_err(|error| {
            tracing::warn!(domain = "queue", operation, error = %error, "Queue actor command reply failed");
            format!("Queue actor command reply failed for {operation}: {error}")
        })?
    }

    pub fn refresh_admin_snapshot_if_dirty(&self) {
        // Best effort: the snapshot refreshes again on the next tick, so a
        // missed one is not worth surfacing.
        for family in &self.route_families {
            let _ = self.send_unit_actor_command(
                *family,
                "refresh_admin_snapshot_if_dirty",
                QueueDomainCommand::RefreshAdminSnapshotIfDirty,
            );
        }
    }

    fn live_counts(&self) -> QueueLiveCounts {
        let mut total = QueueLiveCounts::default();
        for family in &self.route_families {
            let (reply_tx, reply_rx) = crossbeam_channel::bounded(1);
            if self
                .family_runtime
                .try_enqueue(
                    *family,
                    crate::runtime::FamilyActorLane::Control,
                    QueueDomainCommand::ReadLiveCounts(reply_tx),
                )
                .is_err()
            {
                continue;
            }
            if let Ok(counts) = reply_rx.recv_timeout(QUEUE_ACTOR_REPLY_TIMEOUT) {
                total.pending = total.pending.saturating_add(counts.pending);
                total.ready = total.ready.saturating_add(counts.ready);
                total.delayed = total.delayed.saturating_add(counts.delayed);
                total.inflight = total.inflight.saturating_add(counts.inflight);
                total.dead_letters = total.dead_letters.saturating_add(counts.dead_letters);
            }
        }
        total
    }

    pub fn counts(&self) -> QueueCounts {
        let counts = self.live_counts();
        QueueCounts {
            pending: counts.pending,
            ready: counts.ready,
            delayed: counts.delayed,
            inflight: counts.inflight,
            dead_letters: counts.dead_letters,
        }
    }

    /// Run session cleanup on the actor, reporting whether it completed.
    ///
    /// The outcome must reach the caller: swallowing it made a cleanup that
    /// never ran indistinguishable from one that succeeded, so the ingress
    /// retry-ticket machinery never saw a queue cleanup failure at all.
    ///
    /// # Errors
    ///
    /// Returns the delivery failure when the command could not be enqueued, or
    /// when the actor did not reply before its deadline.
    #[must_use = "a dropped cleanup failure is indistinguishable from a cleanup that succeeded"]
    #[cfg(test)]
    pub fn cleanup_session(&self, session_id: u64) -> Result<(), crate::runtime::DeliveryError> {
        for family in &self.route_families {
            self.send_unit_actor_command(*family, "cleanup_session", |reply| {
                QueueDomainCommand::CleanupSession(session_id, reply)
            })?;
        }
        Ok(())
    }

    pub(crate) fn sweep_runtime_state(&self) {
        self.request_runtime_sweep_at(Instant::now());
    }

    #[cfg(test)]
    pub(super) fn sweep_runtime_state_at(&self, now: Instant) {
        for family in &self.route_families {
            let _ = self.send_unit_actor_command(*family, "sweep_runtime_state", |reply| {
                QueueDomainCommand::SweepRuntimeStateAt(now, Some(reply))
            });
        }
    }

    pub(super) fn request_runtime_sweep_at(&self, now: Instant) -> bool {
        let mut enqueued = false;
        for family in &self.route_families {
            let pending = &self.config.runtime_sweep_pending[&family.id()];
            if pending.swap(true, Ordering::AcqRel) {
                continue;
            }
            if let Err(error) = self.family_runtime.try_enqueue(
                *family,
                crate::runtime::FamilyActorLane::Control,
                QueueDomainCommand::SweepRuntimeStateAt(now, None),
            ) {
                pending.store(false, Ordering::Release);
                tracing::warn!(domain = "queue", family = family.id(), operation = "sweep_runtime_state", error = %error, "Queue actor command enqueue failed");
            } else {
                enqueued = true;
            }
        }
        enqueued
    }

    /// Replays a dead-lettered message back into its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue domain actor cannot process the command or
    /// the replay fails.
    pub fn replay_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.send_bool_actor_command(key.family, "replay_dead_letter", |reply| {
            QueueDomainCommand::ReplayDeadLetter(key.clone(), id, reply)
        })
    }

    /// Permanently removes a dead-lettered message from its queue.
    ///
    /// # Errors
    ///
    /// Returns an error when the queue domain actor cannot process the command or
    /// the purge fails.
    pub fn purge_dead_letter(
        &self,
        key: &crate::domains::queue::QueueKey,
        id: crate::domains::queue::MessageId,
    ) -> Result<bool, String> {
        self.send_bool_actor_command(key.family, "purge_dead_letter", |reply| {
            QueueDomainCommand::PurgeDeadLetter(key.clone(), id, reply)
        })
    }
}
