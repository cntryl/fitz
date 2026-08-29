use super::*;
use crate::testkit::create_test_engine_with_cfs;
use chrono::TimeZone;
use serial_test::serial;
use std::sync::Mutex;

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[derive(Clone)]
struct MockClock {
    state: Arc<Mutex<MockClockState>>,
}

#[derive(Clone, Copy)]
struct MockClockState {
    instant: Instant,
    epoch_ms: u64,
}

impl MockClock {
    fn new(epoch_ms: u64) -> Self {
        Self {
            state: Arc::new(Mutex::new(MockClockState {
                instant: Instant::now(),
                epoch_ms,
            })),
        }
    }

    fn advance(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.instant += duration;
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u128_to_u64_saturating(duration.as_millis()));
    }
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        self.state.lock().expect("lock mock clock").instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.state.lock().expect("lock mock clock").epoch_ms
    }
}

fn epoch_ms(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
    let millis = chrono::Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid datetime")
        .timestamp_millis();
    u64::try_from(millis).expect("test datetime should be after unix epoch")
}

fn make_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

fn make_actor_with_clock(clock: Arc<dyn Clock>) -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1]);
    ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock,
    )
}

fn metric_counter(name: &str) -> u64 {
    crate::observability::metrics().counter_get(name)
}

#[test]
fn should_normalize_overdue_persisted_schedule_forward_on_preload() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = ScheduleStore::new(db.clone());
    let route = "schedule://acme/jobs/normalize/run";
    let payload = Bytes::from_static(b"payload");
    let overdue_ms = ScheduleActor::current_epoch_ms().saturating_sub(4 * 60 * 60 * 1000);

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms: overdue_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert overdue schedule");

    // Act
    let actor = ScheduleActor::try_new(
        RouteFamily::new(1),
        db,
        cntryl_midge::WriteOptions::buffered(),
    )
    .expect("load actor");

    // Assert
    let schedule = actor
        .schedules
        .get(route)
        .expect("persisted schedule should be loaded");
    assert!(schedule.next_fire_ms > overdue_ms);
}

#[test]
fn should_fail_closed_given_impossible_persisted_cron() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = ScheduleStore::new(db.clone());
    let route = "schedule://acme/jobs/impossible/run";
    let cron = "0 0 31 2 *";
    let payload = Bytes::from_static(b"payload");
    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron,
                delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms: ScheduleActor::current_epoch_ms(),
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("seed invalid persisted schedule");

    // Act
    let result = ScheduleActor::try_new(
        RouteFamily::new(1),
        db,
        cntryl_midge::WriteOptions::buffered(),
    );
    let Err(error) = result else {
        panic!("invalid persisted cron must stop recovery");
    };

    // Assert
    assert!(error.contains(route));
    assert!(error.contains(cron));
    assert!(error.contains("no possible fire time"));
}

#[test]
fn should_skip_missed_execution_given_overdue_schedule_on_preload() {
    // Arrange
    let db = create_test_engine_with_cfs(vec![1]);
    let store = ScheduleStore::new(db.clone());
    let route = "schedule://acme/jobs/skip/run";
    let payload = Bytes::from_static(b"payload");
    let now = Instant::now();
    let overdue_at = now.checked_sub(Duration::from_mins(2)).unwrap();
    let overdue_ms = ScheduleActor::instant_to_ms_at(overdue_at, now);

    store
        .insert(
            1,
            ScheduleInsert {
                route,
                cron: "* * * * *",
                delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
                payload: &payload,
                next_fire_ms: overdue_ms,
                previous_fire_ms: None,
                last_fire_ms: None,
                executions_total: 0,
            },
            cntryl_midge::WriteOptions::buffered(),
        )
        .expect("insert overdue schedule");

    let mut actor = ScheduleActor::try_new_at(
        RouteFamily::new(1),
        db,
        cntryl_midge::WriteOptions::buffered(),
        now,
    )
    .expect("load actor");
    actor.last_scan_time = now
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let fired = actor.collect_due_occurrences_for_publish_at(now);

    // Assert
    assert!(
        fired.is_empty(),
        "overdue schedules should normalize forward instead of replaying missed executions"
    );
    assert!(actor.schedules.get(route).expect("schedule").next_fire_time > now);
}

#[test]
fn should_preserve_pending_due_occurrence_given_identical_create_retry_at_due_boundary() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/retry/run".to_string();
    let cron = "* * * * *".to_string();
    let payload = Bytes::from_static(b"payload");
    let created_at = Instant::now();

    actor
        .create_schedule_at(route.clone(), cron.clone(), payload.clone(), created_at)
        .expect("create schedule");

    let original = actor.schedules.get(&route).expect("schedule").clone();

    // Act
    let changed = actor
        .create_schedule_at(route.clone(), cron, payload, original.next_fire_time)
        .expect("retry identical create");

    // Assert
    assert!(
        !changed,
        "identical retry should remain idempotent at due time"
    );
    let schedule = actor.schedules.get(&route).expect("schedule");
    assert_eq!(schedule.next_fire_ms, original.next_fire_ms);
    assert_eq!(schedule.next_fire_time, original.next_fire_time);
}

#[test]
fn should_preserve_pending_due_occurrence_given_identical_batch_retry_at_due_boundary() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/batch-retry/run".to_string();
    let cron = "* * * * *".to_string();
    let payload = Bytes::from_static(b"payload");
    let created_at = Instant::now();

    actor
        .create_schedule_at(route.clone(), cron.clone(), payload.clone(), created_at)
        .expect("create schedule");

    let original = actor.schedules.get(&route).expect("schedule").clone();
    let entries = vec![ScheduleCreateEntry {
        route: route.clone(),
        cron,
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload,
    }];

    // Act
    let changed = actor
        .create_schedules_at(entries, original.next_fire_time)
        .expect("retry identical batch create");

    // Assert
    assert_eq!(
        changed, 0,
        "identical batch retry should not rewrite the schedule"
    );
    let schedule = actor.schedules.get(&route).expect("schedule");
    assert_eq!(schedule.next_fire_ms, original.next_fire_ms);
    assert_eq!(schedule.next_fire_time, original.next_fire_time);
}

#[test]
fn should_not_advance_in_memory_or_fire_when_reschedule_persist_fails() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/failure/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);

    let before = actor.schedules.get(route).expect("schedule").next_fire_ms;

    // Act
    actor.store.fail_next_commit_for_tests();
    let fired = actor.collect_due_occurrences_for_publish();

    // Assert
    assert!(fired.is_empty(), "scan should not emit on persist failure");
    assert_eq!(
        actor.schedules.get(route).expect("schedule").next_fire_ms,
        before,
        "in-memory schedule should not advance when persistence fails"
    );
}

#[test]
fn should_report_execution_state_given_acknowledged_pending_fire() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/execution-state/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    let claimed_fire_ms = actor.schedules.get(route).expect("schedule").next_fire_ms;
    let now = Instant::now();
    actor.last_scan_time = now
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let fired = actor.collect_due_occurrences_for_publish_at(now);
    actor
        .ack_pending_fire_claims(&[(claimed_fire_ms, route.to_string())])
        .expect("ack pending fire");
    let snapshot = actor.admin_snapshot();

    // Assert
    assert_eq!(fired.len(), 1, "scan should claim one due occurrence");
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].executions_total, 1);
    assert!(snapshot[0].last_run.is_some());
}

#[test]
fn should_report_oldest_pending_claim_age_given_pending_fire() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
    let mut actor = make_actor_with_clock(clock.clone());
    let route = "schedule://acme/jobs/pending/run".to_string();

    actor
        .create_schedule_at(
            route.clone(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
            clock.now_instant(),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);

    // Act
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(claimed.len(), 1);
    clock.advance(Duration::from_secs(45));

    // Assert
    assert_eq!(
        actor.oldest_pending_claim_age_seconds(clock.now_epoch_ms()),
        45
    );
}

#[test]
fn should_bound_due_claims_per_scan_and_leave_remaining_work_ready() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 8, 28, 19, 13, 0)));
    let mut actor = make_actor_with_clock(clock.clone());
    for sequence in 0..33 {
        actor
            .create_schedule_at(
                format!("schedule://acme/storm/jobs/job-{sequence:02}"),
                "* * * * *".to_string(),
                Bytes::from_static(b"payload"),
                clock.now_instant(),
            )
            .expect("create storm schedule");
    }
    actor.bench_prepare_scan(33);

    // Act
    let first = actor.bench_claim_due_fires();
    clock.advance(actor.scan_dedup_window + Duration::from_millis(1));
    let second = actor.bench_claim_due_fires();

    // Assert
    assert_eq!(first.len(), 32, "one scan must claim a bounded batch");
    assert_eq!(
        second.len(),
        1,
        "the next scan must claim remaining due work"
    );
    let routes: std::collections::HashSet<_> = first
        .iter()
        .chain(&second)
        .map(|claim| claim.route.as_str())
        .collect();
    assert_eq!(routes.len(), 33, "every due route must be claimed once");
}

#[test]
fn should_compact_ready_heap_given_repeated_schedule_upserts() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/compact/run".to_string();
    let now = Instant::now();
    actor
        .create_schedule_at(
            route.clone(),
            "0 2 * * *".to_string(),
            Bytes::from_static(b"one"),
            now,
        )
        .expect("create schedule");
    actor
        .create_schedule_at(
            route.clone(),
            "0 3 * * *".to_string(),
            Bytes::from_static(b"two"),
            now,
        )
        .expect("first upsert");

    // Act
    actor
        .create_schedule_at(
            route.clone(),
            "0 4 * * *".to_string(),
            Bytes::from_static(b"three"),
            now,
        )
        .expect("second upsert");

    // Assert
    assert_eq!(actor.schedule_count(), 1);
    assert_eq!(actor.ready_heap.len(), 1);
    assert_eq!(
        actor.schedules.get(&route).expect("schedule").cron,
        "0 4 * * *"
    );
}

#[test]
fn should_clear_ready_heap_given_last_schedule_delete() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/delete-compact/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");

    // Act
    let deleted = actor.delete_schedule(route).expect("delete schedule");

    // Assert
    assert!(deleted);
    assert!(actor.ready_heap.is_empty());
    assert_eq!(actor.schedule_count(), 0);
}

#[test]
fn should_invalidate_shared_full_list_cache_given_schedule_delete() {
    // Arrange
    let mut actor = make_actor();
    let first_route = "schedule://acme/jobs/cache-first/run";
    let second_route = "schedule://acme/jobs/cache-second/run";
    actor
        .create_schedule(
            first_route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"first"),
        )
        .expect("create first schedule");
    actor
        .create_schedule(
            second_route.to_string(),
            "0 * * * *".to_string(),
            Bytes::from_static(b"second"),
        )
        .expect("create second schedule");
    let (cached, _) = actor.list_entries(0, 0).expect("list entries");

    // Act
    actor.delete_schedule(first_route).expect("delete schedule");
    let (refreshed, total_count) = actor.list_entries(0, 0).expect("list entries");

    // Assert
    assert_eq!(cached.len(), 2);
    assert_eq!(refreshed.len(), 1);
    assert_eq!(total_count, 1);
    assert!(refreshed.iter().all(|entry| entry.route != first_route));
    assert!(refreshed.iter().any(|entry| entry.route == second_route));
}

#[test]
fn should_ignore_stale_heap_entries_left_by_upsert() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/stale/run";
    actor
        .create_schedule(
            route.to_string(),
            "0 2 * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");

    let now = Instant::now();
    let stale_fire_ms = ScheduleActor::instant_to_ms_at(now, now).saturating_sub(1);
    let current_fire_ms =
        ScheduleActor::instant_to_ms_at(now.checked_add(Duration::from_mins(1)).unwrap(), now);

    {
        let schedule = actor.schedules.get_mut(route).expect("schedule");
        schedule.next_fire_time = now.checked_add(Duration::from_mins(1)).unwrap();
        schedule.next_fire_ms = current_fire_ms;
    }
    actor.ready_heap.clear();
    actor
        .ready_heap
        .push((Reverse(stale_fire_ms), route.to_string()));
    actor.last_scan_time = now
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let fired = actor.collect_due_occurrences_for_publish_at(now);

    // Assert
    assert!(
        fired.is_empty(),
        "stale heap entry should not fire when definition has moved forward"
    );
    assert_eq!(
        actor.schedules.get(route).expect("schedule").next_fire_ms,
        current_fire_ms
    );
}

#[test]
fn should_not_fire_deleted_schedule_given_stale_ready_heap_entry() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/cancel/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    actor.delete_schedule(route).expect("delete schedule");

    let now = Instant::now();
    actor.last_scan_time = now
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let fired = actor.collect_due_occurrences_for_publish_at(now);

    // Assert
    assert!(
        fired.is_empty(),
        "deleted schedules should not ghost-fire from stale heap entries"
    );
    assert_eq!(actor.schedule_count(), 0);
}

#[test]
fn should_not_fire_future_occurrence_given_cancel_after_due_scan() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/cancel-after-scan/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);

    let first_scan_at = Instant::now();
    actor.last_scan_time = first_scan_at
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let first_fired = actor.collect_due_occurrences_for_publish_at(first_scan_at);
    let next_fire_time = actor.schedules.get(route).expect("schedule").next_fire_time;
    actor.delete_schedule(route).expect("delete schedule");
    actor.last_scan_time = next_fire_time
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();
    let second_fired = actor.collect_due_occurrences_for_publish_at(next_fire_time);

    // Assert
    assert_eq!(
        first_fired.len(),
        1,
        "scan should claim the due occurrence once"
    );
    assert!(
        second_fired.is_empty(),
        "cancel after the due scan should suppress all future occurrences"
    );
    assert_eq!(actor.schedule_count(), 0);
}

#[test]
fn should_delete_pending_occurrence_given_cancel_after_due_claim() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/cancel-pending/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    let scan_at = Instant::now();
    actor.last_scan_time = scan_at
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .expect("scan time");
    let claimed = actor.collect_due_occurrences_for_publish_at(scan_at);
    assert_eq!(claimed.len(), 1);
    assert_eq!(actor.pending_fire_count(), 1);
    assert_eq!(actor.pending_fire_times_for_route(route).len(), 1);

    // Act
    actor.delete_schedule(route).expect("cancel schedule");

    // Assert
    assert_eq!(actor.pending_fire_count(), 0);
    assert!(actor.pending_fire_times_for_route(route).is_empty());
    assert!(actor
        .store
        .load_pending_fire_claims(actor.family.as_u64())
        .expect("load pending claims")
        .is_empty());
}

#[test]
fn should_not_fire_original_due_occurrence_given_batch_reschedule_before_due_scan() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/batch-reschedule/run".to_string();
    let original_payload = Bytes::from_static(b"original");
    let replacement_payload = Bytes::from_static(b"replacement");
    let created_at = Instant::now();

    actor
        .create_schedule_at(
            route.clone(),
            "* * * * *".to_string(),
            original_payload,
            created_at,
        )
        .expect("create schedule");

    let original = actor.schedules.get(&route).expect("schedule").clone();
    let entries = vec![ScheduleCreateEntry {
        route: route.clone(),
        cron: "0 2 * * *".to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: replacement_payload.clone(),
    }];

    // Act
    let changed = actor
        .create_schedules_at(entries, original.next_fire_time)
        .expect("batch reschedule");
    actor.last_scan_time = original
        .next_fire_time
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();
    let fired = actor.collect_due_occurrences_for_publish_at(original.next_fire_time);

    // Assert
    assert_eq!(changed, 1, "batch reschedule should rewrite the schedule");
    assert!(
        fired.is_empty(),
        "reschedule before the due scan should suppress the original occurrence"
    );
    let schedule = actor.schedules.get(&route).expect("schedule");
    assert_eq!(schedule.cron, "0 2 * * *");
    assert_eq!(schedule.payload, replacement_payload);
    assert!(schedule.next_fire_time > original.next_fire_time);
}

#[test]
fn should_not_fire_original_due_occurrence_given_single_reschedule_before_due_scan() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/reschedule/run".to_string();
    let original_payload = Bytes::from_static(b"original");
    let replacement_payload = Bytes::from_static(b"replacement");
    let created_at = Instant::now();

    actor
        .create_schedule_at(
            route.clone(),
            "* * * * *".to_string(),
            original_payload,
            created_at,
        )
        .expect("create schedule");

    let original = actor.schedules.get(&route).expect("schedule").clone();

    // Act
    let changed = actor
        .create_schedule_at(
            route.clone(),
            "0 2 * * *".to_string(),
            replacement_payload.clone(),
            original.next_fire_time,
        )
        .expect("reschedule before due scan");
    actor.last_scan_time = original
        .next_fire_time
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();
    let fired = actor.collect_due_occurrences_for_publish_at(original.next_fire_time);

    // Assert
    assert!(changed, "single reschedule should rewrite the schedule");
    assert!(
        fired.is_empty(),
        "reschedule before the due scan should suppress the original occurrence"
    );
    let schedule = actor.schedules.get(&route).expect("schedule");
    assert_eq!(schedule.cron, "0 2 * * *");
    assert_eq!(schedule.payload, replacement_payload);
    assert!(schedule.next_fire_time > original.next_fire_time);
}

#[test]
fn should_allow_claimed_due_occurrence_given_single_reschedule_after_due_scan() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
    let mut actor = make_actor_with_clock(clock.clone());
    let route = "schedule://acme/jobs/reschedule-after-scan/run".to_string();
    let original_payload = Bytes::from_static(b"original");
    let replacement_payload = Bytes::from_static(b"replacement");

    actor
        .create_schedule_at(
            route.clone(),
            "* * * * *".to_string(),
            original_payload.clone(),
            clock.now_instant(),
        )
        .expect("create schedule");

    let original = actor.schedules.get(&route).expect("schedule").clone();
    clock.advance(original.next_fire_time.duration_since(clock.now_instant()));
    actor.last_scan_time = clock
        .now_instant()
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let first_fired = actor.collect_due_occurrences_for_publish();
    let changed = actor
        .create_schedule_at(
            route.clone(),
            "0 2 * * *".to_string(),
            replacement_payload.clone(),
            clock.now_instant(),
        )
        .expect("reschedule after due scan");
    clock.advance(Duration::from_millis(1));
    actor.last_scan_time = clock
        .now_instant()
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();
    let second_fired = actor.collect_due_occurrences_for_publish();

    // Assert
    assert_eq!(
        first_fired.len(),
        1,
        "scan should claim the original due occurrence"
    );
    assert_eq!(first_fired[0].0, route);
    assert_eq!(first_fired[0].1, original_payload);
    assert!(
        changed,
        "reschedule after the due scan should update future occurrences"
    );
    assert!(
            second_fired.is_empty(),
            "rescheduling after the due scan should not create a duplicate fire at the old due boundary"
        );
    let schedule = actor
        .schedules
        .get("schedule://acme/jobs/reschedule-after-scan/run")
        .expect("schedule");
    assert_eq!(schedule.cron, "0 2 * * *");
    assert_eq!(schedule.payload, replacement_payload);
    assert!(schedule.next_fire_time > original.next_fire_time);
}

#[test]
fn should_not_fire_twice_given_repeated_scan_within_dedup_window() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/dedup/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);

    let now = Instant::now();
    actor.last_scan_time = now
        .checked_sub(actor.scan_dedup_window + Duration::from_millis(1))
        .unwrap();

    // Act
    let first = actor.collect_due_occurrences_for_publish_at(now);
    let second = actor
        .collect_due_occurrences_for_publish_at(now.checked_add(Duration::from_millis(1)).unwrap());

    // Assert
    assert_eq!(first.len(), 1, "first due scan should emit one fire");
    assert!(
        second.is_empty(),
        "repeated scans inside the dedup window should not emit a duplicate fire"
    );
}

#[test]
#[serial]
fn should_not_advance_schedule_state_given_persistence_failure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/create-fail/run";
    let create_before = metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL);
    let upsert_before = metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL);
    let cancel_before = metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL);

    // Act
    actor.store.fail_next_commit_for_tests();
    let result = actor.create_schedule(
        route.to_string(),
        "* * * * *".to_string(),
        Bytes::from_static(b"payload"),
    );

    // Assert
    assert!(result.is_err(), "create should propagate the store error");
    assert_eq!(
        metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL),
        create_before + 1,
        "create persistence failures should increment"
    );
    assert_eq!(
        metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL),
        upsert_before,
        "upsert persistence failures must not increment on create failure"
    );
    assert_eq!(
        metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL),
        cancel_before,
        "cancel persistence failures must not increment on create failure"
    );
    assert!(
        !actor.schedules.contains_key(route),
        "schedule must not be inserted into in-memory map on persist failure"
    );
    assert!(
        !actor.ready_heap.iter().any(|(_, r)| r == route),
        "ready heap must not contain the route on persist failure"
    );
    assert!(
        actor.list_entries.iter().all(|e| e.route != route),
        "list index must not contain the route on persist failure"
    );
}

#[test]
fn should_retry_schedule_create_given_runtime_queue_backpressure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/create-stall/run";
    actor.store.stall_next_commit_for_tests();

    // Act
    let result = actor.create_schedule(
        route.to_string(),
        "* * * * *".to_string(),
        Bytes::from_static(b"payload"),
    );

    // Assert
    assert_eq!(result, Ok(true));
    assert!(actor.schedules.contains_key(route));
}

#[test]
fn should_classify_schedule_write_stall_without_matching_debug_text() {
    // Arrange
    let mut attempts = 0;

    // Act
    let result = retry_persistence(|| {
        attempts += 1;
        if attempts == 1 {
            return Err(SchedulePersistenceError::midge(
                "commit failed",
                cntryl_midge::MidgeError::WriteStall("different detail".to_string()),
            ));
        }
        Ok(())
    });

    // Assert
    assert_eq!(result, Ok(()));
    assert_eq!(attempts, 2);
}

#[test]
fn should_retry_schedule_delete_given_runtime_queue_backpressure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/delete-stall/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.store.stall_next_commit_for_tests();

    // Act
    let result = actor.delete_schedule(route);

    // Assert
    assert_eq!(result, Ok(true));
    assert!(!actor.schedules.contains_key(route));
}

#[test]
fn should_retry_schedule_claim_given_runtime_queue_backpressure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/claim-stall/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    actor.store.stall_next_commit_for_tests();

    // Act
    let claimed = actor.bench_claim_due_fires();

    // Assert
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].route, route);
}

#[test]
fn should_retry_schedule_ack_given_runtime_queue_backpressure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/ack-stall/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    actor.store.stall_next_commit_for_tests();

    // Act
    let result = actor.ack_pending_fire_claims(&[(claimed[0].fire_ms, route.to_string())]);

    // Assert
    assert_eq!(result.map(|(count, _)| count), Ok(1));
    assert_eq!(actor.pending_fire_count(), 0);
}

#[test]
#[serial]
fn should_not_update_schedule_given_upsert_persistence_failure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/upsert-fail/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"original"),
        )
        .expect("create schedule");
    let original_cron = actor.schedules.get(route).expect("schedule").cron.clone();
    let original_next_fire_ms = actor.schedules.get(route).expect("schedule").next_fire_ms;
    let create_before = metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL);
    let upsert_before = metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL);
    let cancel_before = metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL);

    // Act
    actor.store.fail_next_commit_for_tests();
    let result = actor.create_schedule(
        route.to_string(),
        "0 2 * * *".to_string(),
        Bytes::from_static(b"updated"),
    );

    // Assert
    assert!(result.is_err(), "upsert should propagate the store error");
    assert_eq!(
        metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL),
        create_before,
        "create persistence failures must not increment on upsert failure"
    );
    assert_eq!(
        metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL),
        upsert_before + 1,
        "upsert persistence failures should increment"
    );
    assert_eq!(
        metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL),
        cancel_before,
        "cancel persistence failures must not increment on upsert failure"
    );
    let schedule = actor.schedules.get(route).expect("schedule still present");
    assert_eq!(
        schedule.cron, original_cron,
        "cron must not change on upsert persist failure"
    );
    assert_eq!(
        schedule.payload,
        Bytes::from_static(b"original"),
        "payload must not change on upsert persist failure"
    );
    assert_eq!(
        schedule.next_fire_ms, original_next_fire_ms,
        "next_fire_ms must not change on upsert persist failure"
    );
}

#[test]
#[serial]
fn should_not_remove_schedule_given_cancel_persistence_failure() {
    // Arrange
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/cancel-fail/run";
    actor
        .create_schedule(
            route.to_string(),
            "* * * * *".to_string(),
            Bytes::from_static(b"payload"),
        )
        .expect("create schedule");
    let create_before = metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL);
    let upsert_before = metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL);
    let cancel_before = metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL);

    // Act
    actor.store.fail_next_commit_for_tests();
    let result = actor.delete_schedule(route);

    // Assert
    assert!(result.is_err(), "cancel should propagate the store error");
    assert_eq!(
        metric_counter(METRIC_CREATE_PERSISTENCE_FAILURES_TOTAL),
        create_before,
        "create persistence failures must not increment on cancel failure"
    );
    assert_eq!(
        metric_counter(METRIC_UPSERT_PERSISTENCE_FAILURES_TOTAL),
        upsert_before,
        "upsert persistence failures must not increment on cancel failure"
    );
    assert_eq!(
        metric_counter(METRIC_CANCEL_PERSISTENCE_FAILURES_TOTAL),
        cancel_before + 1,
        "cancel persistence failures should increment"
    );
    assert!(
        actor.schedules.contains_key(route),
        "schedule must not be removed from in-memory map on cancel persist failure"
    );
    assert!(
        actor.ready_heap.iter().any(|(_, r)| r == route),
        "ready heap must still contain the route on cancel persist failure"
    );
    assert!(
        actor.list_entries.iter().any(|e| e.route == route),
        "list index must still contain the route on cancel persist failure"
    );
}

#[test]
fn should_page_a_byte_bounded_list_to_completion_via_offset() {
    // Arrange
    // `limit=0` means "all remaining", but the response is still bounded by
    // the wire frame and can therefore return fewer entries than exist. The
    // wire format carries no `has_more` flag for canonical LIST, only
    // `total_count` - so a client must detect truncation by comparing the
    // returned entry count to `total_count` and continue with
    // `offset += entries.len()`. This proves that procedure actually
    // recovers every entry rather than stopping at the first partial page.
    let mut actor = make_actor();
    let payload = Bytes::from(vec![b'p'; 1024]);
    let total = 300;
    for index in 0..total {
        actor
            .create_schedule(
                format!("schedule://acme/jobs/list-page-{index:04}/run"),
                "* * * * *".to_string(),
                payload.clone(),
            )
            .expect("create schedule");
    }

    // Act
    let mut seen = std::collections::HashSet::new();
    let mut offset = 0u64;
    let mut pages = 0;
    loop {
        let (entries, total_count) = actor.list_entries(offset, 0).expect("list entries");
        pages += 1;
        assert!(pages < 50, "pagination must converge");
        assert!(!entries.is_empty(), "each page must make forward progress");
        for entry in entries.iter() {
            seen.insert(entry.route.clone());
        }
        offset += u64::try_from(entries.len()).unwrap();
        if offset >= total_count {
            break;
        }
    }

    // Assert
    assert!(pages > 1, "1 KiB payloads must not fit in a single page");
    assert_eq!(
        seen.len(),
        total,
        "offset continuation must recover every entry"
    );
}

#[test]
fn should_bound_list_response_to_one_wire_frame() {
    // Arrange
    // A schedule LIST response is encoded into a single TLV value, whose
    // length prefix is a u16. `limit = 0` means "all remaining", so a few
    // hundred ordinary schedules with 1 KiB payloads produce a response far
    // past 65_535 bytes - which panicked the outbound sink rather than
    // paginating. Every entry here is individually small and legal; only the
    // aggregate is oversized.
    let mut actor = make_actor();
    let payload = Bytes::from(vec![b'p'; 1024]);
    for index in 0..250 {
        actor
            .create_schedule(
                format!("schedule://acme/jobs/bulk-{index:04}/run"),
                "* * * * *".to_string(),
                payload.clone(),
            )
            .expect("create schedule");
    }

    // Act
    let (entries, total_count) = actor.list_entries(0, 0).expect("list entries");
    let payload_bytes = crate::dispatch::protocol::schedule_codec::encode_response(
        701,
        &crate::domains::schedule::ScheduleResponse::ListDefs {
            entries: entries.clone(),
            total_count,
        },
    );

    // Assert
    assert_eq!(total_count, 250, "every schedule must still be counted");
    assert!(
        u16::try_from(payload_bytes.len()).is_ok(),
        "list response is {} bytes, past the {}-byte TLV value limit",
        payload_bytes.len(),
        u16::MAX
    );
    assert!(
        !entries.is_empty(),
        "the page must still make forward progress"
    );
    assert!(
        entries.len() < 250,
        "an oversized listing must be truncated into a page, not returned whole"
    );
}

#[test]
fn should_keep_list_v2_continuation_inside_one_wire_frame() {
    // Arrange
    // ListPage's continuation cursor duplicates the last returned route as a
    // separate wire field (`family_prefix + route`), on top of that same
    // route already being encoded once inside the entry itself. A page filled
    // right up to the byte ceiling using only the entry's own cost therefore
    // produces a response the continuation field pushes past u16::MAX - a
    // legal definition can silently become unencodable purely because of
    // where the page boundary happened to land.
    let mut actor = make_actor();
    // A payload sized so entries divide the ceiling with a small remainder,
    // guaranteeing the last admitted entry sits close enough to the edge that
    // only the (missing) continuation reserve decides whether it still fits.
    // A long resource name so the duplicated continuation route (family
    // prefix + this same route) meaningfully exceeds the fixed envelope
    // margin - a short route would fit inside that margin's slack and the
    // bug would not reproduce.
    let long_resource = "x".repeat(400);
    let route_for = |index: usize| format!("schedule://acme/jobs/{long_resource}-{index:04}/run");
    let listable_ceiling =
        crate::domains::schedule::list_wire_budget::schedule_list_response_byte_ceiling();
    let probe_route = route_for(0);
    let entry_fixed = crate::domains::schedule::list_wire_budget::schedule_list_entry_wire_bytes(
        &crate::domains::schedule::ScheduleListEntry {
            route: probe_route.clone(),
            cron: "* * * * *".to_string(),
            delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: Bytes::new(),
        },
    );
    let entries_per_page = 4;
    let payload_len = (listable_ceiling / entries_per_page).saturating_sub(entry_fixed);
    let payload = Bytes::from(vec![b'p'; payload_len]);
    for index in 0..(entries_per_page * 2) {
        actor
            .create_schedule(route_for(index), "* * * * *".to_string(), payload.clone())
            .expect("create schedule");
    }

    // Act
    let (entries, has_more, continuation) = actor
        .list_entries_v2(None, u64::try_from(entries_per_page * 2).unwrap())
        .expect("list entries v2");

    // Assert
    let response_bytes = crate::dispatch::protocol::schedule_codec::encode_response(
        720,
        &crate::domains::schedule::ScheduleResponse::ListPage {
            entries: entries.clone(),
            has_more,
            continuation,
        },
    );
    assert!(
        u16::try_from(response_bytes.len()).is_ok(),
        "list_v2 page is {} bytes, past the {}-byte TLV value limit",
        response_bytes.len(),
        u16::MAX
    );
}

#[test]
fn should_reject_schedule_whose_definition_could_never_be_listed() {
    // Arrange
    // A CREATE arrives as one TLV value, so a payload can be ~140 bytes larger
    // than a LIST entry for the same route can encode. Accepting one leaves a
    // definition that is stored but permanently unlistable: the page cannot be
    // framed, so the response is dropped rather than answered.
    let mut actor = make_actor();
    let route = "schedule://acme/jobs/unlistable/run";
    let cron = "* * * * *";
    let listable_ceiling =
        crate::domains::schedule::list_wire_budget::schedule_list_response_byte_ceiling();
    let probe = crate::domains::schedule::ScheduleListEntry {
        route: route.to_string(),
        cron: cron.to_string(),
        delivery_mode: crate::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::new(),
    };
    let entry_overhead =
        crate::domains::schedule::list_wire_budget::schedule_list_entry_wire_bytes(&probe);
    let largest_listable = listable_ceiling - entry_overhead;

    // Act
    let accepted = actor.create_schedule(
        route.to_string(),
        cron.to_string(),
        Bytes::from(vec![b'p'; largest_listable]),
    );
    let rejected = actor.create_schedule(
        "schedule://acme/jobs/unlistable-two/run".to_string(),
        cron.to_string(),
        Bytes::from(vec![b'p'; largest_listable + 1]),
    );

    // Assert
    assert!(accepted.is_ok(), "a listable definition must be accepted");
    assert!(
        rejected.is_err(),
        "a definition that could never be listed must be refused at create"
    );
}
