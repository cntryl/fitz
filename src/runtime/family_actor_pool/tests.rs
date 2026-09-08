use super::*;

fn family(id: u32) -> RouteFamily {
    RouteFamily::new(id)
}

#[test]
fn should_construct_non_send_family_state_on_owning_worker() {
    // Arrange
    let pool = FamilyActorPool::<crossbeam_channel::Sender<u64>>::new(&[family(1)]).expect("pool");
    let active = Arc::new(AtomicBool::new(true));
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| std::rc::Rc::new(std::cell::Cell::new(40_u64)),
        |state, _family, _lane, reply| {
            state.set(state.get() + 2);
            reply.send(state.get()).expect("state reply");
        },
    );
    let (reply_tx, reply_rx) = bounded(1);

    // Act
    runtime
        .try_enqueue(family(1), FamilyActorLane::Normal, reply_tx)
        .expect("enqueue");

    // Assert
    assert_eq!(reply_rx.recv_timeout(Duration::from_secs(1)), Ok(42));
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
fn should_isolate_family_capacity_plus_route_work_to_owning_shard() {
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
fn should_reject_unknown_plus_duplicate_families() {
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
fn should_not_lose_wake_between_dispatch_plus_next_wait() {
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
fn should_keep_sibling_family_running_after_a_family_actor_panics() {
    // Arrange: enough families that at least two are forced onto the
    // *same* shard thread by pigeonhole (shard count is capped at
    // available parallelism, `should_cap_shards_at_provisioned_family_count`).
    // This proves in-shard isolation -- that the shard's drain loop
    // `continue`s past a panicking family rather than starving or
    // breaking for its thread-mates -- not just cross-shard isolation,
    // which two families alone could pass trivially on any multi-core
    // host. This mirrors production, where one
    // `RpcDomainSink`/`StreamDomainSink` multiplexes every provisioned
    // realm/route family onto one `FamilyActorPoolRuntime`
    // (see `src/boot/domains.rs`).
    let shard_count = shard_count_for_family_count(usize::MAX);
    let family_count = shard_count + 1;
    let families = (1..=family_count)
        .map(|id| family(u32::try_from(id).expect("test family fits")))
        .collect::<Vec<_>>();
    let (panicking, sibling) = families
        .iter()
        .copied()
        .enumerate()
        .find_map(|(index, candidate)| {
            families[index + 1..]
                .iter()
                .copied()
                .find(|&other| {
                    family_shard_affinity(candidate, shard_count)
                        == family_shard_affinity(other, shard_count)
                })
                .map(|other| (candidate, other))
        })
        .expect("pigeonhole guarantees a same-shard pair");

    let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
    let active = Arc::new(AtomicBool::new(true));
    let (observed_tx, observed_rx) = bounded::<u64>(4);
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| (),
        move |(), target_family, _lane, message: u64| {
            assert!(
                target_family != panicking,
                "injected handler panic for a same-shard family"
            );
            if target_family == sibling {
                observed_tx.send(message).expect("sibling observer");
            }
        },
    );

    // Act: trigger the panic and wait for that family to fail closed.
    runtime
        .try_enqueue(panicking, FamilyActorLane::Normal, 1)
        .expect("panicking family enqueue");
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while runtime
        .try_enqueue(panicking, FamilyActorLane::Normal, 99)
        .is_ok()
        && std::time::Instant::now() < deadline
    {
        thread::yield_now();
    }

    // Assert: a family sharing the same shard thread as the panicking
    // one must keep accepting and processing work. A fault confined to
    // one route family/realm must not make the domain unusable for
    // every other family multiplexed onto the same worker thread.
    runtime
        .try_enqueue(sibling, FamilyActorLane::Normal, 42)
        .expect("a same-shard sibling family must keep accepting work after a panic");
    assert_eq!(
        observed_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("a same-shard sibling family must keep making progress after a panic"),
        42
    );
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

    // Assert: this pool has exactly one provisioned family, so that
    // family failing closed *is* full exhaustion -- the pool-wide
    // `is_running()` correctly follows suit here, same as it would for
    // any non-sharded domain's single actor. (A multi-family pool keeps
    // `is_running()` true after one sibling's panic --
    // see `should_keep_pool_running_given_one_of_several_families_panics`.)
    assert!(!runtime.is_family_running(family(1)));
    assert!(!runtime.is_running());
    assert_eq!(
        runtime.try_enqueue(family(1), FamilyActorLane::Normal, 2),
        Err(FamilyActorEnqueueError::ActorStopped)
    );
    assert_eq!(runtime.health_snapshot().panic_count, 1);
    let health = runtime.health_snapshot();
    assert!(health.healthy_families.is_empty());
    assert!(health.degraded_families.is_empty());
    assert_eq!(health.failed_families, vec![family(1)]);
}

#[test]
fn should_keep_pool_running_given_one_of_several_families_panics() {
    // Arrange: multiple provisioned families so a single panic must be
    // isolation, not exhaustion.
    let families = [family(1), family(2), family(3)];
    let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
    let active = Arc::new(AtomicBool::new(true));
    let (started_tx, started_rx) = bounded(1);
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| (),
        move |(), target_family, _lane, _message: u64| {
            if target_family == family(1) {
                started_tx.send(()).expect("panic observer");
                panic!("injected handler panic");
            }
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
    while runtime.is_family_running(family(1)) && std::time::Instant::now() < deadline {
        thread::yield_now();
    }

    // Assert: a single family's panic isolates that family only -- pool
    // health/liveness and restart-exhaustion must both stay unaffected.
    assert!(!runtime.is_family_running(family(1)));
    assert!(runtime.is_running());
    assert_eq!(runtime.failed_family_count(), 1);
    let family_health = runtime.health_snapshot();
    assert_eq!(family_health.healthy_families, vec![family(2), family(3)]);
    assert!(family_health.degraded_families.is_empty());
    assert_eq!(family_health.failed_families, vec![family(1)]);
    let health = runtime.actor_health_snapshot();
    assert!(health.running);
    assert!(!health.restart_exhausted);
}

#[test]
fn should_fail_only_panicking_family_closed_during_idle_work() {
    // Arrange
    let families = [family(1), family(2)];
    let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
    let active = Arc::new(AtomicBool::new(true));
    let (observed_tx, observed_rx) = bounded(1);
    let runtime = FamilyActorPoolRuntime::spawn_with_family_failed_metric_and_idle(
        pool,
        active,
        |_| (),
        move |(), target_family, _lane, message| {
            if target_family == family(2) {
                observed_tx.send(message).expect("sibling observer");
            }
        },
        |(), target_family| assert_ne!(target_family, family(1), "injected idle panic"),
        "test.family_idle_failed",
    );

    // Act
    let deadline = std::time::Instant::now() + Duration::from_secs(1);
    while runtime.is_family_running(family(1)) && std::time::Instant::now() < deadline {
        thread::yield_now();
    }
    runtime
        .try_enqueue(family(2), FamilyActorLane::Normal, 42)
        .expect("healthy sibling enqueue");

    // Assert
    assert!(!runtime.is_family_running(family(1)));
    assert!(runtime.is_family_running(family(2)));
    assert!(runtime.is_running());
    assert_eq!(runtime.health_snapshot().panic_count, 1);
    assert_eq!(observed_rx.recv_timeout(Duration::from_secs(1)), Ok(42));
}

#[test]
fn should_fail_pool_closed_after_every_family_panics() {
    // Arrange
    let families = [family(1), family(2), family(3)];
    let pool = FamilyActorPool::<u64>::new(&families).expect("pool");
    let active = Arc::new(AtomicBool::new(true));
    let runtime = FamilyActorPoolRuntime::spawn(
        pool,
        active,
        |_| (),
        |(), _family, _lane, _message: u64| {
            panic!("injected handler panic");
        },
    );

    // Act: panic every provisioned family.
    for target in families {
        let _ = runtime.try_enqueue(target, FamilyActorLane::Normal, 1);
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while runtime.is_running() && std::time::Instant::now() < deadline {
        thread::yield_now();
    }

    // Assert: full exhaustion is an honest pool-wide fact, unlike a
    // single family's failure, so `running`/`restart_exhausted` must
    // both flip.
    assert!(!runtime.is_running());
    assert_eq!(runtime.failed_family_count(), families.len());
    let family_health = runtime.health_snapshot();
    assert!(family_health.healthy_families.is_empty());
    assert!(family_health.degraded_families.is_empty());
    assert_eq!(family_health.failed_families, families);
    let health = runtime.actor_health_snapshot();
    assert!(!health.running);
    assert!(health.restart_exhausted);
}
