//! Bounded synchronous execution with family isolation and per-key ordering.

use crate::runtime::family_actor_pool::{
    FamilyActorEnqueueError, FamilyActorLane, FAMILY_ACTOR_CONTROL_LANE_CAPACITY,
    FAMILY_ACTOR_NORMAL_LANE_CAPACITY,
};
use crate::runtime::routing::RouteFamily;
use parking_lot::{Condvar, Mutex};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

struct FamilyState<K, M, S> {
    state: Arc<S>,
    normal: HashMap<K, VecDeque<M>>,
    ready: VecDeque<K>,
    active_keys: HashSet<K>,
    normal_len: usize,
    control: VecDeque<M>,
    control_active: bool,
    failed: bool,
}

impl<K, M, S> FamilyState<K, M, S> {
    fn new(state: S) -> Self {
        Self {
            state: Arc::new(state),
            normal: HashMap::new(),
            ready: VecDeque::new(),
            active_keys: HashSet::new(),
            normal_len: 0,
            control: VecDeque::new(),
            control_active: false,
            failed: false,
        }
    }
}

struct Scheduler<K, M, S> {
    families: BTreeMap<u32, FamilyState<K, M, S>>,
    stopped: bool,
    next_family: usize,
}

enum Work<K, M, S> {
    Normal {
        family: RouteFamily,
        key: K,
        message: M,
        state: Arc<S>,
    },
    Control {
        family: RouteFamily,
        message: M,
        state: Arc<S>,
    },
}

struct Shared<K, M, S> {
    scheduler: Mutex<Scheduler<K, M, S>>,
    ready: Condvar,
}

/// Fixed-thread executor that serializes one key while allowing sibling keys
/// in the same route family to overlap.
pub(crate) struct KeyedFamilyExecutor<K, M, S> {
    shared: Arc<Shared<K, M, S>>,
    workers: Mutex<Vec<JoinHandle<()>>>,
}

impl<K, M, S> KeyedFamilyExecutor<K, M, S>
where
    K: Clone + Eq + Hash + Send + 'static,
    M: Send + 'static,
    S: Send + Sync + 'static,
{
    pub(crate) fn new<StateFactory, Handler, Failure>(
        families: &[RouteFamily],
        worker_count: usize,
        state_factory: StateFactory,
        handler: Handler,
        family_failed: Failure,
    ) -> Result<Self, String>
    where
        StateFactory: Fn(RouteFamily) -> S,
        Handler: Fn(&S, RouteFamily, FamilyActorLane, Option<&K>, M) + Send + Sync + 'static,
        Failure: Fn(RouteFamily) + Send + Sync + 'static,
    {
        Self::new_with_spawner(
            families,
            worker_count,
            state_factory,
            handler,
            family_failed,
            |index, task| {
                thread::Builder::new()
                    .name(format!("fitz-keyed-family-{index}"))
                    .spawn(task)
            },
        )
    }

    fn new_with_spawner<StateFactory, Handler, Failure, Spawn>(
        families: &[RouteFamily],
        worker_count: usize,
        state_factory: StateFactory,
        handler: Handler,
        family_failed: Failure,
        mut spawn: Spawn,
    ) -> Result<Self, String>
    where
        StateFactory: Fn(RouteFamily) -> S,
        Handler: Fn(&S, RouteFamily, FamilyActorLane, Option<&K>, M) + Send + Sync + 'static,
        Failure: Fn(RouteFamily) + Send + Sync + 'static,
        Spawn: FnMut(usize, Box<dyn FnOnce() + Send + 'static>) -> std::io::Result<JoinHandle<()>>,
    {
        if families.is_empty() {
            return Err("no route families were provisioned".to_owned());
        }
        if worker_count == 0 {
            return Err("worker count must be greater than zero".to_owned());
        }
        let mut provisioned = BTreeMap::new();
        for family in families {
            if family.id() == 0 {
                return Err("route family zero cannot be provisioned".to_owned());
            }
            if provisioned
                .insert(family.id(), FamilyState::new(state_factory(*family)))
                .is_some()
            {
                return Err(format!("duplicate route family {}", family.id()));
            }
        }
        let shared = Arc::new(Shared {
            scheduler: Mutex::new(Scheduler {
                families: provisioned,
                stopped: false,
                next_family: 0,
            }),
            ready: Condvar::new(),
        });
        let handler = Arc::new(handler);
        let family_failed = Arc::new(family_failed);
        let mut workers = Vec::with_capacity(worker_count.min(32));
        for index in 0..worker_count.min(32) {
            let worker_shared = shared.clone();
            let worker_handler = handler.clone();
            let worker_family_failed = family_failed.clone();
            match spawn(
                index,
                Box::new(move || {
                    worker_loop(
                        &worker_shared,
                        worker_handler.as_ref(),
                        worker_family_failed.as_ref(),
                    );
                }),
            ) {
                Ok(worker) => workers.push(worker),
                Err(error) => {
                    shared.scheduler.lock().stopped = true;
                    shared.ready.notify_all();
                    for worker in workers {
                        let _ = worker.join();
                    }
                    return Err(format!("spawn keyed family worker: {error}"));
                }
            }
        }
        Ok(Self {
            shared,
            workers: Mutex::new(workers),
        })
    }

    pub(crate) fn production_worker_count() -> usize {
        thread::available_parallelism()
            .map_or(1, std::num::NonZeroUsize::get)
            .min(32)
    }

    pub(crate) fn try_enqueue(
        &self,
        family: RouteFamily,
        key: K,
        message: M,
    ) -> Result<(), FamilyActorEnqueueError> {
        let mut scheduler = self.shared.scheduler.lock();
        if scheduler.stopped {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        let Some(state) = scheduler.families.get_mut(&family.id()) else {
            return Err(FamilyActorEnqueueError::UnknownFamily);
        };
        if state.failed {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        if state.normal_len == FAMILY_ACTOR_NORMAL_LANE_CAPACITY {
            return Err(FamilyActorEnqueueError::NormalLaneFull);
        }
        let queue = state.normal.entry(key.clone()).or_default();
        let was_empty = queue.is_empty();
        queue.push_back(message);
        state.normal_len += 1;
        if was_empty && !state.active_keys.contains(&key) {
            state.ready.push_back(key);
        }
        drop(scheduler);
        self.shared.ready.notify_one();
        Ok(())
    }

    pub(crate) fn try_enqueue_control(
        &self,
        family: RouteFamily,
        message: M,
    ) -> Result<(), FamilyActorEnqueueError> {
        let mut scheduler = self.shared.scheduler.lock();
        if scheduler.stopped {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        let Some(state) = scheduler.families.get_mut(&family.id()) else {
            return Err(FamilyActorEnqueueError::UnknownFamily);
        };
        if state.failed {
            return Err(FamilyActorEnqueueError::ActorStopped);
        }
        if state.control.len() == FAMILY_ACTOR_CONTROL_LANE_CAPACITY {
            return Err(FamilyActorEnqueueError::ControlLaneFull);
        }
        state.control.push_back(message);
        drop(scheduler);
        self.shared.ready.notify_all();
        Ok(())
    }

    pub(crate) fn is_family_running(&self, family: RouteFamily) -> bool {
        let scheduler = self.shared.scheduler.lock();
        !scheduler.stopped
            && scheduler
                .families
                .get(&family.id())
                .is_some_and(|state| !state.failed)
    }

    pub(crate) fn is_running(&self) -> bool {
        let scheduler = self.shared.scheduler.lock();
        !scheduler.stopped && scheduler.families.values().any(|state| !state.failed)
    }

    pub(crate) fn failed_family_count(&self) -> usize {
        self.shared
            .scheduler
            .lock()
            .families
            .values()
            .filter(|state| state.failed)
            .count()
    }

    pub(crate) fn stop(&self) {
        let mut scheduler = self.shared.scheduler.lock();
        scheduler.stopped = true;
        for state in scheduler.families.values_mut() {
            state.normal.clear();
            state.ready.clear();
            state.normal_len = 0;
            state.control.clear();
        }
        drop(scheduler);
        self.shared.ready.notify_all();
    }

    pub(crate) fn join(&self) {
        self.stop();
        for worker in self.workers.lock().drain(..) {
            let _ = worker.join();
        }
    }
}

impl<K, M, S> Drop for KeyedFamilyExecutor<K, M, S> {
    fn drop(&mut self) {
        let mut scheduler = self.shared.scheduler.lock();
        scheduler.stopped = true;
        drop(scheduler);
        self.shared.ready.notify_all();
        for worker in self.workers.get_mut().drain(..) {
            let _ = worker.join();
        }
    }
}

fn worker_loop<K, M, S, Handler, Failure>(
    shared: &Shared<K, M, S>,
    handler: &Handler,
    family_failed: &Failure,
) where
    K: Clone + Eq + Hash,
    Handler: Fn(&S, RouteFamily, FamilyActorLane, Option<&K>, M),
    Failure: Fn(RouteFamily),
{
    loop {
        let work = {
            let mut scheduler = shared.scheduler.lock();
            loop {
                if scheduler.stopped {
                    return;
                }
                if let Some(work) = take_work(&mut scheduler) {
                    break work;
                }
                shared.ready.wait(&mut scheduler);
            }
        };
        let family = match &work {
            Work::Normal { family, .. } | Work::Control { family, .. } => *family,
        };
        let completed_key = match &work {
            Work::Normal { key, .. } => Some(key.clone()),
            Work::Control { .. } => None,
        };
        let result = catch_unwind(AssertUnwindSafe(|| match work {
            Work::Normal {
                family,
                ref key,
                message,
                ref state,
            } => handler(state, family, FamilyActorLane::Normal, Some(key), message),
            Work::Control {
                family,
                message,
                ref state,
            } => handler(state, family, FamilyActorLane::Control, None, message),
        }));
        let mut scheduler = shared.scheduler.lock();
        let Some(state) = scheduler.families.get_mut(&family.id()) else {
            continue;
        };
        if result.is_ok() {
            finish_work(state, completed_key);
        } else {
            let first_failure = !state.failed;
            state.failed = true;
            state.normal.clear();
            state.ready.clear();
            state.normal_len = 0;
            state.control.clear();
            state.active_keys.clear();
            state.control_active = false;
            drop(scheduler);
            if first_failure && catch_unwind(AssertUnwindSafe(|| family_failed(family))).is_err() {
                tracing::error!(
                    family = family.id(),
                    "Keyed family failure callback panicked"
                );
            }
            shared.ready.notify_all();
            continue;
        }
        drop(scheduler);
        shared.ready.notify_all();
    }
}

fn take_work<K, M, S>(scheduler: &mut Scheduler<K, M, S>) -> Option<Work<K, M, S>>
where
    K: Clone + Eq + Hash,
{
    let ids = scheduler.families.keys().copied().collect::<Vec<_>>();
    for delta in 0..ids.len() {
        let index = (scheduler.next_family + delta) % ids.len();
        let id = ids[index];
        let state = scheduler.families.get_mut(&id)?;
        if state.failed || state.control_active {
            continue;
        }
        let family = RouteFamily::new(id);
        if !state.control.is_empty() {
            if state.active_keys.is_empty() {
                state.control_active = true;
                scheduler.next_family = (index + 1) % ids.len();
                return Some(Work::Control {
                    family,
                    message: state.control.pop_front()?,
                    state: state.state.clone(),
                });
            }
            continue;
        }
        while let Some(key) = state.ready.pop_front() {
            if state.active_keys.contains(&key) {
                continue;
            }
            let Some(message) = state.normal.get_mut(&key).and_then(VecDeque::pop_front) else {
                state.normal.remove(&key);
                continue;
            };
            state.normal_len -= 1;
            state.active_keys.insert(key.clone());
            scheduler.next_family = (index + 1) % ids.len();
            return Some(Work::Normal {
                family,
                key,
                message,
                state: state.state.clone(),
            });
        }
    }
    None
}

fn finish_work<K, M, S>(state: &mut FamilyState<K, M, S>, completed_key: Option<K>)
where
    K: Clone + Eq + Hash,
{
    match completed_key {
        Some(key) => {
            state.active_keys.remove(&key);
            if state
                .normal
                .get(&key)
                .is_some_and(|queue| !queue.is_empty())
            {
                state.ready.push_back(key);
            } else {
                state.normal.remove(&key);
            }
        }
        None => state.control_active = false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    #[test]
    fn should_overlap_different_keys_without_overlapping_one_key() {
        // Arrange
        let active = Arc::new(Mutex::new(HashSet::new()));
        let overlapped = Arc::new(AtomicBool::new(false));
        let same_key_overlap = Arc::new(AtomicBool::new(false));
        let (release_tx, release_rx) = crossbeam_channel::bounded::<()>(0);
        let (entered_tx, entered_rx) = crossbeam_channel::bounded(2);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            2,
            |_| (),
            {
                let active = active.clone();
                let overlapped = overlapped.clone();
                let same_key_overlap = same_key_overlap.clone();
                move |(), _, _, key: Option<&u64>, ()| {
                    let key = *key.expect("normal key");
                    let mut keys = active.lock();
                    if !keys.insert(key) {
                        same_key_overlap.store(true, Ordering::SeqCst);
                    }
                    if keys.len() > 1 {
                        overlapped.store(true, Ordering::SeqCst);
                    }
                    drop(keys);
                    entered_tx.send(()).expect("record entry");
                    release_rx.recv().expect("release handler");
                    active.lock().remove(&key);
                }
            },
            |_| {},
        )
        .expect("create executor");

        // Act
        executor.try_enqueue(RouteFamily::new(1), 1, ()).unwrap();
        executor.try_enqueue(RouteFamily::new(1), 1, ()).unwrap();
        executor.try_enqueue(RouteFamily::new(1), 2, ()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Assert
        assert!(overlapped.load(Ordering::SeqCst));
        assert!(!same_key_overlap.load(Ordering::SeqCst));
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_tx.send(()).unwrap();
        executor.join();
    }

    #[test]
    fn should_preserve_same_key_fifo() {
        // Arrange
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = crossbeam_channel::bounded(4);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            4,
            |_| (),
            {
                let seen = seen.clone();
                move |(), _, _, _: Option<&u64>, message| {
                    seen.lock().push(message);
                    done_tx.send(()).unwrap();
                }
            },
            |_| {},
        )
        .unwrap();

        // Act
        for message in 0..4 {
            executor
                .try_enqueue(RouteFamily::new(1), 7, message)
                .unwrap();
        }
        for _ in 0..4 {
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        // Assert
        assert_eq!(*seen.lock(), vec![0, 1, 2, 3]);
        executor.join();
    }

    #[test]
    fn should_prioritize_exclusive_control_over_queued_normal_work() {
        // Arrange
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (first_entered_tx, first_entered_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(3);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            2,
            |_| (),
            {
                let seen = seen.clone();
                move |(), _, lane, _, message| {
                    if message == 1 {
                        first_entered_tx.send(()).unwrap();
                        release_rx.recv().unwrap();
                    }
                    seen.lock().push((lane, message));
                    done_tx.send(()).unwrap();
                }
            },
            |_| {},
        )
        .unwrap();
        executor.try_enqueue(RouteFamily::new(1), 1, 1).unwrap();
        first_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .unwrap();

        // Act: enqueue the control message *before* the sibling-key normal
        // message. With 2 shards and only key 1 active, a free shard is
        // otherwise entitled to grab a ready sibling key immediately --
        // dispatch only skips a family once control is non-empty and a key
        // is still active (see `take_work`). Enqueuing normal-then-control
        // leaves a real window where control is still empty when the free
        // shard looks for work, so it can race ahead: an actual production
        // race, not a bug, but not what this test means to exercise.
        executor
            .try_enqueue_control(RouteFamily::new(1), 3)
            .unwrap();
        executor.try_enqueue(RouteFamily::new(1), 2, 2).unwrap();
        release_tx.send(()).unwrap();
        for _ in 0..3 {
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        // Assert
        assert_eq!(
            *seen.lock(),
            vec![
                (FamilyActorLane::Normal, 1),
                (FamilyActorLane::Control, 3),
                (FamilyActorLane::Normal, 2)
            ]
        );
        executor.join();
    }

    #[test]
    fn should_fail_only_panicking_family_and_keep_sibling_progressing() {
        // Arrange
        let (failure_tx, failure_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1), RouteFamily::new(2)],
            2,
            |_| (),
            move |(), family, _, _, message| {
                assert_ne!(family, RouteFamily::new(1), "injected panic");
                done_tx.send(message).unwrap();
            },
            move |_| failure_tx.send(()).expect("record family failure"),
        )
        .unwrap();

        // Act
        executor.try_enqueue(RouteFamily::new(1), 1, 1).unwrap();
        executor.try_enqueue(RouteFamily::new(2), 1, 2).unwrap();

        // Assert
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while executor.is_family_running(RouteFamily::new(1))
            && std::time::Instant::now() < deadline
        {
            std::thread::yield_now();
        }
        assert!(!executor.is_family_running(RouteFamily::new(1)));
        assert!(executor.is_family_running(RouteFamily::new(2)));
        failure_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("family failure callback");
        assert!(failure_rx.try_recv().is_err());
        assert_eq!(
            executor.try_enqueue(RouteFamily::new(1), 1, 3),
            Err(FamilyActorEnqueueError::ActorStopped)
        );
        executor.join();
    }

    #[test]
    fn should_keep_sibling_progressing_when_failure_callback_panics() {
        // Arrange
        let (failure_tx, failure_rx) = crossbeam_channel::bounded(1);
        let (done_tx, done_rx) = crossbeam_channel::bounded(1);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1), RouteFamily::new(2)],
            1,
            |_| (),
            move |(), family, _, _, message| {
                assert_ne!(family, RouteFamily::new(1), "injected handler panic");
                done_tx.send(message).unwrap();
            },
            move |_| {
                failure_tx.send(()).unwrap();
                panic!("injected failure callback panic");
            },
        )
        .unwrap();

        // Act
        executor.try_enqueue(RouteFamily::new(1), 1, 1).unwrap();
        failure_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        executor.try_enqueue(RouteFamily::new(2), 1, 2).unwrap();

        // Assert
        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        assert!(!executor.is_family_running(RouteFamily::new(1)));
        assert!(executor.is_family_running(RouteFamily::new(2)));
        executor.join();
    }

    #[test]
    fn should_rotate_ready_keys_fairly() {
        // Arrange
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = crossbeam_channel::bounded(4);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            1,
            |_| (),
            {
                let seen = seen.clone();
                move |(), _, _, key: Option<&u64>, ()| {
                    seen.lock().push(*key.unwrap());
                    done_tx.send(()).unwrap();
                }
            },
            |_| {},
        )
        .unwrap();

        // Act
        executor.try_enqueue(RouteFamily::new(1), 1, ()).unwrap();
        executor.try_enqueue(RouteFamily::new(1), 1, ()).unwrap();
        executor.try_enqueue(RouteFamily::new(1), 1, ()).unwrap();
        executor.try_enqueue(RouteFamily::new(1), 2, ()).unwrap();
        for _ in 0..4 {
            done_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        }

        // Assert
        assert_eq!(*seen.lock(), vec![1, 2, 1, 1]);
        executor.join();
    }

    #[test]
    fn should_enforce_both_lane_capacities() {
        // Arrange
        let (entered_tx, entered_rx) = crossbeam_channel::bounded(1);
        let (release_tx, release_rx) = crossbeam_channel::bounded(1);
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            1,
            |_| (),
            move |(), _, _, _, message: usize| {
                if message == usize::MAX {
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }
            },
            |_| {},
        )
        .unwrap();
        executor
            .try_enqueue(RouteFamily::new(1), 0, usize::MAX)
            .unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        // Act
        for message in 0..FAMILY_ACTOR_NORMAL_LANE_CAPACITY {
            executor
                .try_enqueue(RouteFamily::new(1), message + 1, message)
                .unwrap();
        }
        for message in 0..FAMILY_ACTOR_CONTROL_LANE_CAPACITY {
            executor
                .try_enqueue_control(RouteFamily::new(1), message)
                .unwrap();
        }

        // Assert
        assert_eq!(
            executor.try_enqueue(RouteFamily::new(1), 99_999, 1),
            Err(FamilyActorEnqueueError::NormalLaneFull)
        );
        assert_eq!(
            executor.try_enqueue_control(RouteFamily::new(1), 1),
            Err(FamilyActorEnqueueError::ControlLaneFull)
        );
        release_tx.send(()).unwrap();
        executor.join();
    }

    #[test]
    fn should_reject_work_after_stop_and_join_workers() {
        // Arrange
        let executor = KeyedFamilyExecutor::new(
            &[RouteFamily::new(1)],
            2,
            |_| (),
            |(), _, _, _: Option<&u64>, ()| {},
            |_| {},
        )
        .unwrap();

        // Act
        executor.stop();
        executor.join();

        // Assert
        assert!(!executor.is_running());
        assert_eq!(
            executor.try_enqueue(RouteFamily::new(1), 1, ()),
            Err(FamilyActorEnqueueError::ActorStopped)
        );
        assert_eq!(
            executor.try_enqueue_control(RouteFamily::new(1), ()),
            Err(FamilyActorEnqueueError::ActorStopped)
        );
    }

    #[test]
    fn should_join_started_workers_when_later_worker_spawn_fails() {
        // Arrange
        let exited = Arc::new(AtomicUsize::new(0));
        let mut spawn_count = 0;

        // Act
        let result = KeyedFamilyExecutor::new_with_spawner(
            &[RouteFamily::new(1)],
            2,
            |_| (),
            |(), _, _, _: Option<&u64>, ()| {},
            |_| {},
            {
                let exited = exited.clone();
                move |_, task| {
                    spawn_count += 1;
                    if spawn_count == 2 {
                        return Err(std::io::Error::other("injected spawn failure"));
                    }
                    let exited = exited.clone();
                    std::thread::Builder::new().spawn(move || {
                        task();
                        exited.fetch_add(1, Ordering::SeqCst);
                    })
                }
            },
        );

        // Assert
        assert!(matches!(result, Err(ref error) if error.contains("injected spawn failure")));
        assert_eq!(exited.load(Ordering::SeqCst), 1);
    }
}
