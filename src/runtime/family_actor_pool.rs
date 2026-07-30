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
/// worker. A handler panic fails the pool closed and drops the message that
/// triggered it; callers observe `ActorStopped` through the bounded edge.
pub struct FamilyActorPoolRuntime<M: Send + 'static> {
    ingress: FamilyActorIngress<M>,
    active: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    panic_count: Arc<AtomicU64>,
    join_handles: parking_lot::Mutex<Vec<thread::JoinHandle<()>>>,
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
        mut pool: FamilyActorPool<M>,
        active: Arc<AtomicBool>,
        state_factory: F,
        handler: H,
    ) -> Self
    where
        S: Send + 'static,
        F: Fn(RouteFamily) -> S + Send + Sync + 'static,
        H: Fn(&mut S, RouteFamily, FamilyActorLane, M) + Send + Sync + 'static,
    {
        let ingress = pool.ingress();
        let running = Arc::new(AtomicBool::new(true));
        let failed = Arc::new(AtomicBool::new(false));
        let panic_count = Arc::new(AtomicU64::new(0));
        let state_factory = Arc::new(state_factory);
        let handler = Arc::new(handler);
        let mut join_handles = Vec::with_capacity(pool.shard_count());

        for shard_index in 0..pool.shard_count() {
            let Some(mut shard) = pool.take_shard(shard_index) else {
                continue;
            };
            let mut family_states = HashMap::with_capacity(shard.families().len());
            for family in shard.families() {
                family_states.insert(family.id(), state_factory(family));
            }

            let worker_active = active.clone();
            let worker_running = running.clone();
            let worker_failed = failed.clone();
            let worker_panic_count = panic_count.clone();
            let worker_handler = handler.clone();
            let worker_ingress = ingress.clone();
            join_handles.push(thread::spawn(move || {
                while worker_active.load(Ordering::Acquire)
                    && worker_running.load(Ordering::Acquire)
                    && !worker_failed.load(Ordering::Acquire)
                {
                    let work = match shard.recv_timeout(Duration::from_millis(50)) {
                        Ok(work) => work,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };

                    let Some(state) = family_states.get_mut(&work.family.id()) else {
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

                    if std::panic::catch_unwind(AssertUnwindSafe(|| {
                        worker_handler(state, work.family, work.lane, work.message);
                    }))
                    .is_err()
                    {
                        worker_panic_count.fetch_add(1, Ordering::Relaxed);
                        worker_failed.store(true, Ordering::Release);
                        worker_active.store(false, Ordering::Release);
                        worker_ingress.wake_all();
                        tracing::error!(
                            family = work.family.id(),
                            "family actor failed closed after handler panic"
                        );
                        break;
                    }
                }
                worker_running.store(false, Ordering::Release);
            }));
        }

        Self {
            ingress,
            active,
            running,
            failed,
            panic_count,
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
        if !self.is_running() {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        self.ingress.try_enqueue(family, lane, message)
    }

    #[must_use]
    pub fn is_running(&self) -> bool {
        self.active.load(Ordering::Acquire)
            && self.running.load(Ordering::Acquire)
            && !self.failed.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn health_snapshot(&self) -> crate::runtime::ManagedActorHealthSnapshot {
        crate::runtime::ManagedActorHealthSnapshot {
            running: self.is_running(),
            restart_count: 0,
            panic_count: self.panic_count.load(Ordering::Relaxed),
            restart_exhausted: self.failed.load(Ordering::Acquire),
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
mod tests {
    use super::*;

    fn family(id: u32) -> RouteFamily {
        RouteFamily::new(id)
    }

    #[test]
    fn should_cap_shards_at_provisioned_family_count() {
        // Arrange
        let families = [family(1), family(2)];

        // Act
        let pool = FamilyActorPool::<u64>::new(&families).expect("pool");

        // Assert
        assert_eq!(pool.shard_count(), shard_count_for_family_count(2));
        assert!(pool.shard_count() <= families.len());
    }

    #[test]
    fn should_keep_family_affinity_stable() {
        // Arrange
        let families = [family(1), family(2), family(3), family(4)];
        let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
        let ingress = pool.ingress();

        // Act
        let affinities = families
            .iter()
            .map(|family| ingress.shard_for_family(*family).expect("family"))
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(
            affinities[0],
            family_shard_affinity(family(1), pool.shard_count())
        );
        assert_eq!(
            affinities[1],
            family_shard_affinity(family(2), pool.shard_count())
        );
        assert_eq!(
            affinities[2],
            family_shard_affinity(family(3), pool.shard_count())
        );
        assert_eq!(
            affinities[3],
            family_shard_affinity(family(4), pool.shard_count())
        );
    }

    #[test]
    fn should_isolate_family_capacity_and_route_work_to_owning_shard() {
        // Arrange
        let families = [family(1), family(2)];
        let mut pool = FamilyActorPool::<u64>::new(&families).expect("pool");
        let ingress = pool.ingress();
        let family_one_shard = ingress.shard_for_family(family(1)).expect("family one");
        let family_two_shard = ingress.shard_for_family(family(2)).expect("family two");
        let mut first = pool.take_shard(family_one_shard).expect("first shard");
        let mut second = if family_two_shard == family_one_shard {
            None
        } else {
            pool.take_shard(family_two_shard)
        };

        // Act
        ingress
            .try_enqueue(family(1), FamilyActorLane::Control, 11)
            .expect("family one enqueue");
        ingress
            .try_enqueue(family(2), FamilyActorLane::Normal, 22)
            .expect("family two enqueue");
        let first_work = first.try_next().expect("first work");
        let second_work = second
            .as_mut()
            .and_then(FamilyActorShard::try_next)
            .or_else(|| first.try_next());

        // Assert
        assert_eq!(first_work.family, family(1));
        assert_eq!(first_work.lane, FamilyActorLane::Control);
        assert_eq!(first_work.message, 11);
        let second_work = second_work.expect("second work");
        assert_eq!(second_work.family, family(2));
        assert_eq!(second_work.message, 22);
    }

    #[test]
    fn should_schedule_ready_families_fairly_given_continuous_load() {
        // Arrange
        let shard_count = shard_count_for_family_count(usize::MAX);
        let family_count = shard_count.saturating_add(1);
        let families = (1..=family_count)
            .map(|id| family(u32::try_from(id).expect("test family fits")))
            .collect::<Vec<_>>();
        let mut pool = FamilyActorPool::<u64>::new(&families).expect("pool");
        let ingress = pool.ingress();
        let shard = ingress.shard_for_family(family(1)).expect("shard");
        let second_family =
            family(u32::try_from(pool.shard_count().saturating_add(1)).expect("test family fits"));
        for value in 0..3 {
            ingress
                .try_enqueue(family(1), FamilyActorLane::Normal, value)
                .expect("family one enqueue");
            ingress
                .try_enqueue(second_family, FamilyActorLane::Normal, value + 10)
                .expect("family two enqueue");
        }
        let mut worker = pool.take_shard(shard).expect("shard");

        // Act
        let order = (0..6)
            .map(|_| worker.try_next().expect("work").family.id())
            .collect::<Vec<_>>();

        // Assert
        assert_eq!(
            order,
            [
                1,
                second_family.id(),
                1,
                second_family.id(),
                1,
                second_family.id()
            ]
        );
    }

    #[test]
    fn should_reject_unknown_and_duplicate_families() {
        // Arrange
        let duplicate = [family(1), family(1)];
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");

        // Act
        let duplicate_result = FamilyActorPool::<u64>::new(&duplicate);
        let unknown_result = pool
            .ingress()
            .try_enqueue(family(2), FamilyActorLane::Normal, 1);

        // Assert
        assert!(matches!(
            duplicate_result,
            Err(FamilyActorPoolError::DuplicateFamily(_))
        ));
        assert_eq!(unknown_result, Err(FamilyActorEnqueueError::UnknownFamily));
    }

    #[test]
    fn should_dispatch_runtime_work_to_a_family_worker() {
        // Arrange
        let families = [family(1)];
        let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
        let active = Arc::new(AtomicBool::new(true));
        let (observed_tx, observed_rx) = crossbeam_channel::bounded(1);
        let runtime = FamilyActorPoolRuntime::spawn(
            pool,
            active,
            {
                let observed_tx = observed_tx.clone();
                move |_family| observed_tx.clone()
            },
            |observed_tx, family, lane, message| {
                observed_tx
                    .send((family, lane, message))
                    .expect("worker observer");
            },
        );

        // Act
        runtime
            .try_enqueue(family(1), FamilyActorLane::Normal, 42)
            .expect("enqueue");
        let observed = observed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("worker should receive work");

        // Assert
        assert_eq!(observed.0, family(1));
        assert_eq!(observed.1, FamilyActorLane::Normal);
        assert_eq!(observed.2, 42);
    }

    #[test]
    fn should_drain_control_lane_given_normal_lane_saturation() {
        // Arrange
        let mut pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let ingress = pool.ingress();
        let mut shard = pool.take_shard(0).expect("shard");
        ingress
            .try_enqueue(family(1), FamilyActorLane::Normal, 10)
            .expect("normal enqueue");
        ingress
            .try_enqueue(family(1), FamilyActorLane::Control, 20)
            .expect("control enqueue");

        // Act
        let first = shard.try_next().expect("first work");

        // Assert
        assert_eq!(first.lane, FamilyActorLane::Control);
        assert_eq!(first.message, 20);
    }

    #[test]
    fn should_preserve_control_lane_progress_given_normal_lane_flood() {
        // Arrange
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let ingress = pool.ingress();
        for value in 0..FAMILY_ACTOR_NORMAL_LANE_CAPACITY {
            ingress
                .try_enqueue(family(1), FamilyActorLane::Normal, value as u64)
                .expect("normal capacity");
        }
        for value in 0..FAMILY_ACTOR_CONTROL_LANE_CAPACITY {
            ingress
                .try_enqueue(family(1), FamilyActorLane::Control, value as u64)
                .expect("control capacity");
        }

        // Act
        let normal = ingress.try_enqueue(family(1), FamilyActorLane::Normal, 1);
        let control = ingress.try_enqueue(family(1), FamilyActorLane::Control, 1);

        // Assert
        assert_eq!(normal, Err(FamilyActorEnqueueError::NormalLaneFull));
        assert_eq!(control, Err(FamilyActorEnqueueError::ControlLaneFull));
    }

    #[test]
    fn should_drain_coalesced_burst_without_additional_wakes() {
        // Arrange
        const MESSAGE_COUNT: usize = 1_024;
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let active = Arc::new(AtomicBool::new(true));
        let (completed_tx, completed_rx) = bounded(1);
        let runtime = FamilyActorPoolRuntime::spawn(
            pool,
            active,
            |_| Vec::with_capacity(MESSAGE_COUNT),
            move |observed, _family, _lane, message| {
                observed.push(message);
                if observed.len() == MESSAGE_COUNT {
                    completed_tx
                        .send(observed.clone())
                        .expect("completion observer");
                }
            },
        );

        // Act
        for value in 0..MESSAGE_COUNT {
            runtime
                .try_enqueue(family(1), FamilyActorLane::Normal, value as u64)
                .expect("burst enqueue");
        }
        let observed = completed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("burst completion");

        // Assert
        assert_eq!(observed, (0..MESSAGE_COUNT as u64).collect::<Vec<_>>());
    }

    #[test]
    fn should_not_lose_wake_between_dispatch_and_next_wait() {
        // Arrange
        const MESSAGE_COUNT: u64 = 256;
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let active = Arc::new(AtomicBool::new(true));
        let (observed_tx, observed_rx) = bounded(1);
        let runtime = FamilyActorPoolRuntime::spawn(
            pool,
            active,
            |_| (),
            move |(), _family, _lane, message| {
                observed_tx.send(message).expect("dispatch observer");
            },
        );

        // Act
        for value in 0..MESSAGE_COUNT {
            runtime
                .try_enqueue(family(1), FamilyActorLane::Normal, value)
                .expect("enqueue");
            assert_eq!(
                observed_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("dispatch"),
                value
            );
        }

        // Assert
        assert!(runtime.is_running());
    }

    #[test]
    fn should_stop_idle_worker_promptly() {
        // Arrange
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let active = Arc::new(AtomicBool::new(true));
        let runtime = FamilyActorPoolRuntime::spawn(pool, active, |_| (), |(), _, _, _u64| {});
        let started = std::time::Instant::now();

        // Act
        runtime.stop();

        // Assert
        assert!(started.elapsed() < Duration::from_millis(100));
        assert!(!runtime.is_running());
    }

    #[test]
    fn should_reject_new_work_given_failed_family_actor() {
        // Arrange
        let pool = FamilyActorPool::<u64>::new(&[family(1)]).expect("pool");
        let active = Arc::new(AtomicBool::new(true));
        let (started_tx, started_rx) = bounded(1);
        let runtime = FamilyActorPoolRuntime::spawn(
            pool,
            active,
            |_| (),
            move |(), _family, _lane, _message: u64| {
                started_tx.send(()).expect("panic observer");
                panic!("injected handler panic");
            },
        );

        // Act
        runtime
            .try_enqueue(family(1), FamilyActorLane::Normal, 1)
            .expect("panic command enqueue");
        started_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("handler started");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while runtime.is_running() && std::time::Instant::now() < deadline {
            thread::yield_now();
        }

        // Assert
        assert!(!runtime.is_running());
        assert_eq!(
            runtime.try_enqueue(family(1), FamilyActorLane::Normal, 2),
            Err(FamilyActorEnqueueError::ActorStopped)
        );
        assert_eq!(runtime.health_snapshot().panic_count, 1);
    }
}
