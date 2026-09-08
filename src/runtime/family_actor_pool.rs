//! Synchronous, family-affine actor mailboxes.
//!
//! Transport code is responsible for choosing a family and enqueueing work.
//! A shard owns the receivers for its families and is the only code that
//! drains them.  This keeps family state on one worker without introducing a
//! mutex around the domain core or an async scheduler into the runtime.

use crate::runtime::routing::RouteFamily;
use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// The normal data-plane capacity required by the domain contract.
pub const FAMILY_ACTOR_NORMAL_LANE_CAPACITY: usize = 16_384;

/// A separate bounded lane for control-plane work.
pub const FAMILY_ACTOR_CONTROL_LANE_CAPACITY: usize = 256;

/// The lane used when enqueueing family work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyActorLane {
    Normal,
    Control,
}

/// One item returned by an owning family shard.
#[derive(Debug, PartialEq, Eq)]
pub struct FamilyActorWork<M> {
    pub family: RouteFamily,
    pub lane: FamilyActorLane,
    pub message: M,
}

/// Failure to enqueue work at the transport/router edge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyActorEnqueueError {
    UnknownFamily,
    NormalLaneFull,
    ControlLaneFull,
    ActorStopped,
}

impl fmt::Display for FamilyActorEnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownFamily => f.write_str("route family is not provisioned"),
            Self::NormalLaneFull => f.write_str("family actor normal lane is full"),
            Self::ControlLaneFull => f.write_str("family actor control lane is full"),
            Self::ActorStopped => f.write_str("family actor shard is stopped"),
        }
    }
}

impl std::error::Error for FamilyActorEnqueueError {}

pub(crate) fn family_actor_enqueue_error_to_delivery_error(
    error: FamilyActorEnqueueError,
) -> crate::runtime::DeliveryError {
    match error {
        FamilyActorEnqueueError::NormalLaneFull => crate::runtime::DeliveryError::MailboxFull {
            capacity: FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
            current_len: FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
        },
        FamilyActorEnqueueError::ControlLaneFull => crate::runtime::DeliveryError::HighLaneFull {
            capacity: FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
            current_len: FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
        },
        FamilyActorEnqueueError::UnknownFamily | FamilyActorEnqueueError::ActorStopped => {
            crate::runtime::DeliveryError::ActorStopped
        }
    }
}

struct FamilyActorSender<M> {
    normal: Sender<M>,
    control: Sender<M>,
    wake: Sender<()>,
}

struct FamilyActorReceivers<M> {
    family: RouteFamily,
    normal: Receiver<M>,
    control: Receiver<M>,
}

/// The transport/router-facing half of a family actor pool.
pub struct FamilyActorIngress<M> {
    senders: std::sync::Arc<BTreeMap<u32, FamilyActorSender<M>>>,
    shard_wakes: Arc<Vec<Sender<()>>>,
    shard_count: usize,
}

impl<M> Clone for FamilyActorIngress<M> {
    fn clone(&self) -> Self {
        Self {
            senders: self.senders.clone(),
            shard_wakes: self.shard_wakes.clone(),
            shard_count: self.shard_count,
        }
    }
}

impl<M: Send + 'static> FamilyActorIngress<M> {
    /// Enqueue work to the family selected by the caller.
    ///
    /// # Errors
    ///
    /// Returns a bounded-lane or stopped-actor error when the work cannot be
    /// accepted at the transport/router edge.
    pub fn try_enqueue(
        &self,
        family: RouteFamily,
        lane: FamilyActorLane,
        message: M,
    ) -> Result<(), FamilyActorEnqueueError> {
        let Some(sender) = self.senders.get(&family.id()) else {
            return Err(FamilyActorEnqueueError::UnknownFamily);
        };
        let result = match lane {
            FamilyActorLane::Normal => sender.normal.try_send(message),
            FamilyActorLane::Control => sender.control.try_send(message),
        };
        result.map_err(|error| match error {
            TrySendError::Full(_) => match lane {
                FamilyActorLane::Normal => FamilyActorEnqueueError::NormalLaneFull,
                FamilyActorLane::Control => FamilyActorEnqueueError::ControlLaneFull,
            },
            TrySendError::Disconnected(_) => FamilyActorEnqueueError::ActorStopped,
        })?;
        match sender.wake.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) => Ok(()),
            Err(TrySendError::Disconnected(())) => Err(FamilyActorEnqueueError::ActorStopped),
        }
    }

    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.shard_count
    }

    /// Return the permanent shard affinity for a provisioned family.
    #[must_use]
    pub fn shard_for_family(&self, family: RouteFamily) -> Option<usize> {
        self.senders
            .contains_key(&family.id())
            .then(|| family_shard_affinity(family, self.shard_count))
    }

    #[must_use]
    pub fn family_count(&self) -> usize {
        self.senders.len()
    }

    #[must_use]
    pub fn families(&self) -> Vec<RouteFamily> {
        self.senders.keys().copied().map(RouteFamily::new).collect()
    }

    fn wake_all(&self) {
        for wake in self.shard_wakes.iter() {
            let _ = wake.try_send(());
        }
    }
}

/// A family actor pool. Each shard can be moved to one owning worker.
pub struct FamilyActorPool<M> {
    ingress: FamilyActorIngress<M>,
    shards: Vec<Option<FamilyActorShard<M>>>,
}

impl<M: Send + 'static> FamilyActorPool<M> {
    /// Create a pool for the provisioned route families.
    ///
    /// The shard count is `available_parallelism`, capped at the number of
    /// provisioned families. Family affinity is stable for the lifetime of
    /// the pool and uses `(family_id - 1) % shard_count`.
    ///
    /// # Errors
    ///
    /// Returns an error for an empty, invalid, or duplicate family set.
    pub fn new(families: &[RouteFamily]) -> Result<Self, FamilyActorPoolError> {
        if families.is_empty() {
            return Err(FamilyActorPoolError::NoProvisionedFamilies);
        }

        let mut family_ids = BTreeMap::new();
        for family in families {
            if family.id() == 0 {
                return Err(FamilyActorPoolError::InvalidFamily(*family));
            }
            if family_ids.insert(family.id(), *family).is_some() {
                return Err(FamilyActorPoolError::DuplicateFamily(*family));
            }
        }

        let shard_count = shard_count_for_family_count(families.len());
        let shard_wakes = (0..shard_count).map(|_| bounded(1)).collect::<Vec<_>>();
        let mut sender_map = BTreeMap::new();
        let mut shard_receivers = (0..shard_count)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<FamilyActorReceivers<M>>>>();

        for family in family_ids.values().copied() {
            let (normal, normal_receiver) = bounded(FAMILY_ACTOR_NORMAL_LANE_CAPACITY);
            let (control, control_receiver) = bounded(FAMILY_ACTOR_CONTROL_LANE_CAPACITY);
            let shard = family_shard_affinity(family, shard_count);
            sender_map.insert(
                family.id(),
                FamilyActorSender {
                    normal,
                    control,
                    wake: shard_wakes[shard].0.clone(),
                },
            );
            shard_receivers[shard].push(FamilyActorReceivers {
                family,
                normal: normal_receiver,
                control: control_receiver,
            });
        }

        let shards = shard_receivers
            .into_iter()
            .zip(shard_wakes.iter())
            .map(|(receivers, (_, wake))| Some(FamilyActorShard::new(receivers, wake.clone())))
            .collect();

        Ok(Self {
            ingress: FamilyActorIngress {
                senders: std::sync::Arc::new(sender_map),
                shard_wakes: Arc::new(shard_wakes.into_iter().map(|(wake, _)| wake).collect()),
                shard_count,
            },
            shards,
        })
    }

    #[must_use]
    pub fn ingress(&self) -> FamilyActorIngress<M> {
        self.ingress.clone()
    }

    #[must_use]
    pub fn shard_count(&self) -> usize {
        self.ingress.shard_count()
    }

    /// Move one shard to its owning worker.
    pub fn take_shard(&mut self, shard: usize) -> Option<FamilyActorShard<M>> {
        self.shards.get_mut(shard).and_then(Option::take)
    }
}

/// Error raised while building a family actor pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FamilyActorPoolError {
    NoProvisionedFamilies,
    InvalidFamily(RouteFamily),
    DuplicateFamily(RouteFamily),
}

impl fmt::Display for FamilyActorPoolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoProvisionedFamilies => f.write_str("at least one route family is required"),
            Self::InvalidFamily(family) => write!(f, "route family {family} is invalid"),
            Self::DuplicateFamily(family) => write!(f, "route family {family} is duplicated"),
        }
    }
}

impl std::error::Error for FamilyActorPoolError {}

/// The worker-owned half of one shard.
pub struct FamilyActorShard<M> {
    receivers: Vec<FamilyActorReceivers<M>>,
    wake: Receiver<()>,
    cursor: usize,
}

/// A running set of family-affine workers.
///
/// The pool itself only owns bounded channels. This wrapper owns one worker
/// thread per shard and creates one state value per provisioned family on that
/// worker. A handler panic fails *that family* closed and drops the message
/// that triggered it; callers observe `ActorStopped` through the bounded edge
/// for that family only. Route families are a hard isolation boundary
/// (`RouteFamily`, `docs/development/domain-boundaries-spec.md`), so a panic
/// scoped to one family must never make the pool unusable for the others it
/// multiplexes (see `should_keep_sibling_family_running_after_a_family_actor_panics`).
/// The one exception is a shard receiving work for a family it was never
/// constructed with, which cannot happen under correct routing; that remains
/// a pool-fatal condition since it indicates a routing/config bug rather than
/// a per-family runtime fault.
///
/// A single family's failure never flips pool-wide health (`is_running`),
/// but once *every* provisioned family has failed closed the pool has zero
/// remaining capacity -- that is an honest aggregate fact about the pool,
/// not a blast-radius cascade from any one family, so it does flip
/// `is_running` to `false` (see
/// `should_fail_pool_closed_after_every_family_panics`).
pub struct FamilyActorPoolRuntime<M: Send + 'static> {
    ingress: FamilyActorIngress<M>,
    active: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    panic_count: Arc<AtomicU64>,
    family_failed: Arc<HashMap<u32, AtomicBool>>,
    join_handles: parking_lot::Mutex<Vec<thread::JoinHandle<()>>>,
}

type FamilyIdleHandler<S> = dyn Fn(&mut S, RouteFamily) + Send + Sync;

fn service_idle_family_states<S>(
    states: &mut HashMap<u32, S>,
    family_failed: &HashMap<u32, AtomicBool>,
    pool_failed: &AtomicBool,
    panic_count: &AtomicU64,
    metric: Option<&'static str>,
    handler: &FamilyIdleHandler<S>,
) {
    for (&family_id, state) in states {
        if !family_failed
            .get(&family_id)
            .is_some_and(|flag| flag.load(Ordering::Acquire))
        {
            let family = RouteFamily::new(family_id);
            if std::panic::catch_unwind(AssertUnwindSafe(|| handler(state, family))).is_err() {
                record_family_handler_panic(
                    family,
                    family_failed,
                    pool_failed,
                    panic_count,
                    metric,
                );
            }
        }
    }
}

fn create_family_states<M, S, F>(shard: &FamilyActorShard<M>, factory: &F) -> HashMap<u32, S>
where
    M: Send + 'static,
    F: Fn(RouteFamily) -> S,
{
    shard
        .families()
        .into_iter()
        .map(|family| (family.id(), factory(family)))
        .collect()
}

fn await_family_state_initialization(initialized: &Receiver<()>) {
    initialized
        .recv()
        .expect("family actor worker stopped during state initialization");
}

fn create_family_failure_flags<M: Send + 'static>(
    ingress: &FamilyActorIngress<M>,
) -> HashMap<u32, AtomicBool> {
    ingress
        .families()
        .into_iter()
        .map(|family| (family.id(), AtomicBool::new(false)))
        .collect()
}

fn record_family_handler_panic(
    family: RouteFamily,
    family_failed: &HashMap<u32, AtomicBool>,
    pool_failed: &AtomicBool,
    panic_count: &AtomicU64,
    metric: Option<&'static str>,
) {
    panic_count.fetch_add(1, Ordering::Relaxed);
    if let Some(flag) = family_failed.get(&family.id()) {
        flag.store(true, Ordering::Release);
    }
    if let Some(metric) = metric {
        crate::observability::counter_inc(metric);
    }
    tracing::error!(
        family = family.id(),
        "family actor failed closed for this family after handler panic"
    );
    if family_failed
        .values()
        .all(|flag| flag.load(Ordering::Acquire))
    {
        pool_failed.store(true, Ordering::Release);
    }
}

/// Health of a fail-closed family pool, which never attempts actor restarts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FamilyActorPoolHealthSnapshot {
    pub running: bool,
    pub panic_count: u64,
    pub failed_closed: bool,
    /// Families accepting normal and control work.
    pub healthy_families: Vec<RouteFamily>,
    /// Families without a permanent family fault that are unavailable because
    /// the pool is stopping or has failed globally.
    ///
    pub degraded_families: Vec<RouteFamily>,
    /// Families permanently isolated after their handler panicked.
    pub failed_families: Vec<RouteFamily>,
}

impl<M: Send + 'static> FamilyActorPoolRuntime<M> {
    /// Start family workers for a pool.
    ///
    /// `state_factory` runs once for every family on its owning shard. The
    /// returned state is then accessed only by that shard's handler closure.
    /// This keeps mutable family state out of a shared per-domain core while
    /// retaining a small, synchronous dispatch surface.
    #[must_use]
    pub fn spawn<S, F, H>(
        pool: FamilyActorPool<M>,
        active: Arc<AtomicBool>,
        state_factory: F,
        handler: H,
    ) -> Self
    where
        S: 'static,
        F: Fn(RouteFamily) -> S + Send + Sync + 'static,
        H: Fn(&mut S, RouteFamily, FamilyActorLane, M) + Send + Sync + 'static,
    {
        Self::spawn_inner(pool, active, state_factory, handler, None, None)
    }

    /// Like [`Self::spawn`], but increments `family_failed_metric` once per
    /// route family whose handler panics and fails closed.
    ///
    /// A single family's failure deliberately does not flip domain-wide
    /// health/liveness (that would reintroduce the very blast-radius bug this
    /// isolation exists to prevent) -- until every provisioned family has
    /// failed, in which case the pool legitimately has no remaining capacity
    /// and `is_running` does flip. Short of full exhaustion, this counter is
    /// the only operator-visible signal for a permanently degraded
    /// family/realm — without it, such a failure is observable only via a
    /// log line.
    #[must_use]
    pub fn spawn_with_family_failed_metric<S, F, H>(
        pool: FamilyActorPool<M>,
        active: Arc<AtomicBool>,
        state_factory: F,
        handler: H,
        family_failed_metric: &'static str,
    ) -> Self
    where
        S: 'static,
        F: Fn(RouteFamily) -> S + Send + Sync + 'static,
        H: Fn(&mut S, RouteFamily, FamilyActorLane, M) + Send + Sync + 'static,
    {
        Self::spawn_inner(
            pool,
            active,
            state_factory,
            handler,
            Some(family_failed_metric),
            None,
        )
    }

    #[must_use]
    pub fn spawn_with_family_failed_metric_and_idle<S, F, H, I>(
        pool: FamilyActorPool<M>,
        active: Arc<AtomicBool>,
        state_factory: F,
        handler: H,
        idle_handler: I,
        family_failed_metric: &'static str,
    ) -> Self
    where
        S: 'static,
        F: Fn(RouteFamily) -> S + Send + Sync + 'static,
        H: Fn(&mut S, RouteFamily, FamilyActorLane, M) + Send + Sync + 'static,
        I: Fn(&mut S, RouteFamily) + Send + Sync + 'static,
    {
        let idle_handler: Arc<FamilyIdleHandler<S>> = Arc::new(idle_handler);
        Self::spawn_inner(
            pool,
            active,
            state_factory,
            handler,
            Some(family_failed_metric),
            Some(&idle_handler),
        )
    }

    fn spawn_inner<S, F, H>(
        mut pool: FamilyActorPool<M>,
        active: Arc<AtomicBool>,
        state_factory: F,
        handler: H,
        family_failed_metric: Option<&'static str>,
        idle_handler: Option<&Arc<FamilyIdleHandler<S>>>,
    ) -> Self
    where
        S: 'static,
        F: Fn(RouteFamily) -> S + Send + Sync + 'static,
        H: Fn(&mut S, RouteFamily, FamilyActorLane, M) + Send + Sync + 'static,
    {
        let idle_handler = idle_handler.cloned();
        let ingress = pool.ingress();
        let running = Arc::new(AtomicBool::new(true));
        let failed = Arc::new(AtomicBool::new(false));
        let panic_count = Arc::new(AtomicU64::new(0));
        let family_failed = Arc::new(create_family_failure_flags(&ingress));
        let state_factory = Arc::new(state_factory);
        let handler = Arc::new(handler);
        let mut join_handles = Vec::with_capacity(pool.shard_count());

        for shard_index in 0..pool.shard_count() {
            let Some(mut shard) = pool.take_shard(shard_index) else {
                continue;
            };
            let worker_active = active.clone();
            let worker_running = running.clone();
            let worker_failed = failed.clone();
            let worker_panic_count = panic_count.clone();
            let worker_family_failed = family_failed.clone();
            let worker_family_failed_metric = family_failed_metric;
            let worker_handler = handler.clone();
            let worker_idle_handler = idle_handler.clone();
            let worker_state_factory = state_factory.clone();
            let worker_ingress = ingress.clone();
            let (initialized_tx, initialized_rx) = bounded(1);
            join_handles.push(thread::spawn(move || {
                let mut family_states = create_family_states(&shard, worker_state_factory.as_ref());
                let _ = initialized_tx.send(());
                while worker_active.load(Ordering::Acquire)
                    && worker_running.load(Ordering::Acquire)
                    && !worker_failed.load(Ordering::Acquire)
                {
                    let work = match shard.recv_timeout(Duration::from_millis(50)) {
                        Ok(work) => work,
                        Err(RecvTimeoutError::Timeout) => {
                            if let Some(idle_handler) = &worker_idle_handler {
                                service_idle_family_states(
                                    &mut family_states,
                                    &worker_family_failed,
                                    &worker_failed,
                                    &worker_panic_count,
                                    worker_family_failed_metric,
                                    idle_handler.as_ref(),
                                );
                            }
                            continue;
                        }
                        Err(RecvTimeoutError::Disconnected) => break,
                    };

                    let Some(state) = family_states.get_mut(&work.family.id()) else {
                        // Unowned-family work is a pool-fatal routing/config bug.
                        worker_panic_count.fetch_add(1, Ordering::Relaxed);
                        worker_failed.store(true, Ordering::Release);
                        worker_active.store(false, Ordering::Release);
                        worker_ingress.wake_all();
                        tracing::error!(
                            family = work.family.id(),
                            "family actor received work for an unowned route family"
                        );
                        break;
                    };

                    if worker_family_failed
                        .get(&work.family.id())
                        .is_some_and(|flag| flag.load(Ordering::Acquire))
                    {
                        // Drop messages for a previously failed family without invoking
                        // the handler again -- the reply sender embedded in
                        // `work.message` is dropped here, which callers
                        // observe as `ActorStopped` via `reply_wait`, not a
                        // hang. Sibling families keep being drained below.
                        tracing::warn!(
                            family = work.family.id(),
                            "dropping work for a family that already failed closed"
                        );
                        continue;
                    }

                    if std::panic::catch_unwind(AssertUnwindSafe(|| {
                        worker_handler(state, work.family, work.lane, work.message);
                    }))
                    .is_err()
                    {
                        record_family_handler_panic(
                            work.family,
                            &worker_family_failed,
                            &worker_failed,
                            &worker_panic_count,
                            worker_family_failed_metric,
                        );
                    }
                }
                worker_running.store(false, Ordering::Release);
            }));
            await_family_state_initialization(&initialized_rx);
        }

        Self {
            ingress,
            active,
            running,
            failed,
            panic_count,
            family_failed,
            join_handles: parking_lot::Mutex::new(join_handles),
        }
    }

    #[must_use]
    pub fn ingress(&self) -> FamilyActorIngress<M> {
        self.ingress.clone()
    }

    /// Enqueue work through the running family actor pool.
    ///
    /// # Errors
    ///
    /// Returns an error when the family is unknown, the selected bounded lane
    /// is full, or the pool has already stopped.
    pub fn try_enqueue(
        &self,
        family: RouteFamily,
        lane: FamilyActorLane,
        message: M,
    ) -> Result<(), FamilyActorEnqueueError> {
        if !self.is_family_running(family) {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        self.ingress.try_enqueue(family, lane, message)
    }

    /// Pool-wide liveness. A single family's handler panic never flips this
    /// (see the type-level docs) -- only an explicit [`Self::fail_closed`]
    /// call, [`Self::stop`], or every provisioned family having failed
    /// closed does.
    #[must_use]
    pub fn is_running(&self) -> bool {
        self.active.load(Ordering::Acquire)
            && self.running.load(Ordering::Acquire)
            && !self.failed.load(Ordering::Acquire)
    }

    /// Whether `family` is still accepting work.
    ///
    /// This is `false` for a family whose handler has panicked (fail-closed
    /// for that family only) or when the whole pool has stopped/failed. An
    /// unprovisioned family is not tracked here; `try_enqueue` rejects it
    /// separately as `UnknownFamily` via the ingress family lookup.
    #[must_use]
    pub fn is_family_running(&self, family: RouteFamily) -> bool {
        self.is_running()
            && !self
                .family_failed
                .get(&family.id())
                .is_some_and(|flag| flag.load(Ordering::Acquire))
    }

    /// Count of provisioned families whose handler has panicked and failed
    /// closed.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn failed_family_count(&self) -> usize {
        self.family_failed
            .values()
            .filter(|flag| flag.load(Ordering::Acquire))
            .count()
    }

    #[must_use]
    pub fn health_snapshot(&self) -> FamilyActorPoolHealthSnapshot {
        let mut healthy_families = Vec::with_capacity(self.family_failed.len());
        let mut degraded_families = Vec::new();
        let mut failed_families = Vec::new();
        let running = self.is_running();
        for (&family_id, failed) in self.family_failed.iter() {
            let family = RouteFamily::new(family_id);
            if failed.load(Ordering::Acquire) {
                failed_families.push(family);
            } else if running {
                healthy_families.push(family);
            } else {
                degraded_families.push(family);
            }
        }
        healthy_families.sort_by_key(RouteFamily::id);
        degraded_families.sort_by_key(RouteFamily::id);
        failed_families.sort_by_key(RouteFamily::id);
        FamilyActorPoolHealthSnapshot {
            running,
            panic_count: self.panic_count.load(Ordering::Relaxed),
            failed_closed: self.failed.load(Ordering::Acquire),
            healthy_families,
            degraded_families,
            failed_families,
        }
    }

    pub(crate) fn actor_health_snapshot(&self) -> crate::runtime::ActorHealthSnapshot {
        let health = self.health_snapshot();
        crate::runtime::ActorHealthSnapshot {
            running: health.running,
            restart_count: 0,
            panic_count: health.panic_count,
            restart_exhausted: health.failed_closed,
        }
    }

    /// Fail the pool closed without waiting for a worker panic.
    pub fn fail_closed(&self) {
        self.failed.store(true, Ordering::Release);
        self.active.store(false, Ordering::Release);
        self.running.store(false, Ordering::Release);
        self.ingress.wake_all();
    }

    /// Stop all shard workers and join them.
    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        self.ingress.wake_all();
        let handles = std::mem::take(&mut *self.join_handles.lock());
        for handle in handles {
            if let Err(error) = handle.join() {
                tracing::error!(error = ?error, "family actor worker panicked before join");
            }
        }
    }
}

impl<M: Send + 'static> Drop for FamilyActorPoolRuntime<M> {
    fn drop(&mut self) {
        self.stop();
    }
}

impl<M: Send + 'static> FamilyActorShard<M> {
    fn new(receivers: Vec<FamilyActorReceivers<M>>, wake: Receiver<()>) -> Self {
        Self {
            receivers,
            wake,
            cursor: 0,
        }
    }

    #[must_use]
    pub fn families(&self) -> Vec<RouteFamily> {
        self.receivers.iter().map(|item| item.family).collect()
    }

    /// Drain one ready message, using a round-robin family cursor.
    pub fn try_next(&mut self) -> Option<FamilyActorWork<M>> {
        let len = self.receivers.len();
        if len == 0 {
            return None;
        }

        for control in [true, false] {
            for offset in 0..len {
                let index = (self.cursor + offset) % len;
                let receiver = &self.receivers[index];
                let result = if control {
                    receiver
                        .control
                        .try_recv()
                        .map(|message| (FamilyActorLane::Control, message))
                } else {
                    receiver
                        .normal
                        .try_recv()
                        .map(|message| (FamilyActorLane::Normal, message))
                };
                if let Ok((lane, message)) = result {
                    self.cursor = (index + 1) % len;
                    return Some(FamilyActorWork {
                        family: receiver.family,
                        lane,
                        message,
                    });
                }
            }
        }
        None
    }

    /// Wait for one message while preserving the same fair drain order.
    ///
    /// # Errors
    ///
    /// Returns a timeout or disconnected error when no family mailbox produces
    /// work within the requested interval.
    pub fn recv_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<FamilyActorWork<M>, RecvTimeoutError> {
        if let Some(work) = self.try_next() {
            return Ok(work);
        }
        if self.receivers.is_empty() {
            return Err(RecvTimeoutError::Disconnected);
        }
        self.wake.recv_timeout(timeout)?;
        self.try_next().ok_or(RecvTimeoutError::Timeout)
    }
}

/// Calculate the fixed shard count for a number of provisioned families.
#[must_use]
pub fn shard_count_for_family_count(family_count: usize) -> usize {
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    available.max(1).min(family_count.max(1))
}

/// Calculate the permanent affinity for a valid non-zero route family.
#[must_use]
pub fn family_shard_affinity(family: RouteFamily, shard_count: usize) -> usize {
    debug_assert!(family.id() > 0);
    debug_assert!(shard_count > 0);
    (family.id() as usize - 1) % shard_count.max(1)
}

#[cfg(test)]
mod tests;
