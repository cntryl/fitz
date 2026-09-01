//! Schedule Advanced Tests - Tier 2
//!
//! Advanced patterns and edge cases covering:
//! - Cron next fire time calculations
//! - Complex scheduling scenarios
//! - Multi-schedule coordination
//! - Timing and scheduling edge cases

use bytes::Bytes;
use chrono::TimeZone;
use fitz::domains::schedule::protocol::{Clock, CronSchedule};
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleStore};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

// ========== Helper ==========

fn u128_to_u64_saturating(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

fn make_schedule_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
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

    fn advance_epoch(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.epoch_ms = state
            .epoch_ms
            .saturating_add(u128_to_u64_saturating(duration.as_millis()));
    }

    fn rewind_epoch(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.epoch_ms = state
            .epoch_ms
            .saturating_sub(u128_to_u64_saturating(duration.as_millis()));
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

fn make_schedule_actor_with_clock(clock: Arc<dyn Clock>) -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
        clock,
    )
}

fn make_schedule_actor_and_store_with_clock(
    clock: Arc<dyn Clock>,
) -> (Arc<cntryl_midge::Engine>, ScheduleActor) {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    let actor = ScheduleActor::new_with_clock(
        RouteFamily::new(1),
        store.clone(),
        cntryl_midge::WriteOptions::buffered(),
        clock,
    );
    (store, actor)
}

// ========== Next Fire Time Calculation Tests ==========

#[test]
fn should_calculate_next_fire_time_for_every_minute() {
    // Arrange
    let cron = CronSchedule::parse("* * * * *").unwrap();
    let now = std::time::Instant::now();

    // Act
    let next_fire = cron.next_fire_time(now);

    // Assert
    // Should be in the future
    assert!(next_fire > now);

    // Should be less than 2 minutes away (next minute boundary)
    let elapsed = next_fire.duration_since(now);
    assert!(
        elapsed.as_secs() <= 120,
        "Next fire should be within 2 minutes, got {} seconds",
        elapsed.as_secs()
    );
}

#[test]
fn should_calculate_next_fire_time_for_specific_hour() {
    // Arrange
    let cron = CronSchedule::parse("0 3 * * *").unwrap(); // Daily at 3 AM
    let now = std::time::Instant::now();

    // Act
    let next_fire = cron.next_fire_time(now);

    // Assert
    // Should be in the future (basic sanity check)
    assert!(next_fire > now);

    // Should be less than 25 hours away (next occurrence of 3 AM)
    let elapsed = next_fire.duration_since(now);
    assert!(elapsed.as_secs() < 25 * 3600);
}

// ========== Complex Scheduling Scenarios ==========

#[test]
fn should_handle_multiple_schedules_with_different_frequencies() {
    // Arrange
    let mut actor = make_schedule_actor();

    // Create schedules with different frequencies
    let routes = vec![
        (
            "schedule://acme/system/every-minute/run",
            "* * * * *",
            "freq-1",
        ),
        (
            "schedule://acme/system/every-hour/run",
            "0 * * * *",
            "freq-2",
        ),
        ("schedule://acme/system/daily/run", "0 0 * * *", "freq-3"),
        ("schedule://acme/system/weekly/run", "0 0 * * 1", "freq-4"),
    ];

    // Act
    for (route, cron, label) in routes {
        let response = actor.handle(ScheduleMessage::Create {
            route: route.to_string(),
            cron: cron.to_string(),
            delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
            payload: Bytes::from(label),
        });

        // Assert each create succeeds
        assert!(matches!(
            response,
            fitz::domains::schedule::ScheduleResponse::Ok
        ));
    }

    // Verify all created
    let list_response = actor.handle(ScheduleMessage::List {
        offset: 0,
        limit: 0,
    });
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs { entries, .. } => {
            assert_eq!(entries.len(), 4);
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_allow_creating_schedule_with_complex_cron() {
    // Arrange
    let mut actor = make_schedule_actor();

    // Act - Create schedule with complex cron (every 15 min on weekdays 9-17)
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/ops/business-hours-frequent/run".to_string(),
        cron: "*/15 9-17 * * 1-5".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("complex-cron"),
    });

    // Assert
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
}

/// Resolve a `next_fire_time` result computed against a fixed [`MockClock`] back
/// into a real calendar date/time, so tests can assert the *specific* day the
/// scheduler landed on rather than just "some time in the future".
fn resolve_fire_time(clock: &MockClock, next_fire: Instant) -> chrono::DateTime<chrono::Utc> {
    let next_ms = fitz::runtime::instant_to_epoch_ms_with_reference(
        next_fire,
        clock.now_instant(),
        clock.now_epoch_ms(),
    );
    chrono::Utc
        .timestamp_millis_opt(i64::try_from(next_ms).expect("epoch ms fits in i64"))
        .single()
        .expect("valid datetime")
}

#[test]
fn should_find_next_fire_for_business_hours_schedule() {
    use chrono::{Datelike, Timelike};

    // Arrange: Monday 2025-01-06 08:00 UTC, before the 9am window opens.
    let clock = MockClock::new(epoch_ms(2025, 1, 6, 8, 0, 0));
    let cron = CronSchedule::parse("0 9-17 * * 1-5").unwrap(); // 9-5 weekdays

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);
    let resolved = resolve_fire_time(&clock, next_fire);

    // Assert: fires the same day at 9am, still within the Mon-Fri window.
    assert_eq!(resolved.year(), 2025);
    assert_eq!(resolved.month(), 1);
    assert_eq!(resolved.day(), 6);
    assert_eq!(resolved.hour(), 9);
    let weekday = resolved.weekday().number_from_monday(); // 1=Mon .. 5=Fri
    assert!(
        (1..=5).contains(&weekday),
        "expected a weekday, got {weekday}"
    );
}

// ========== Edge Case Tests ==========

#[test]
fn should_handle_leap_year_february() {
    use chrono::Datelike;

    // Arrange: start from a non-leap year so the schedule must skip forward.
    let clock = MockClock::new(epoch_ms(2025, 1, 1, 0, 0, 0));
    let cron = CronSchedule::parse("0 0 29 2 *").unwrap(); // Feb 29

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);
    let resolved = resolve_fire_time(&clock, next_fire);

    // Assert: lands on the next actual leap day, not just "later".
    assert_eq!(resolved.month(), 2);
    assert_eq!(resolved.day(), 29);
    assert!(
        chrono::NaiveDate::from_ymd_opt(resolved.year(), 1, 1).is_some()
            && chrono::NaiveDate::from_ymd_opt(resolved.year(), 2, 29).is_some(),
        "resolved year {} must actually be a leap year",
        resolved.year()
    );
}

#[test]
fn should_handle_month_end_schedule() {
    use chrono::Datelike;

    // Arrange: January has 31 days, so this should fire later in the same month.
    let clock = MockClock::new(epoch_ms(2025, 1, 1, 0, 0, 0));
    let cron = CronSchedule::parse("0 0 31 * *").unwrap(); // 31st of month

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);
    let resolved = resolve_fire_time(&clock, next_fire);

    // Assert: lands specifically on the 31st, skipping any shorter months.
    assert_eq!(resolved.day(), 31);
    assert_eq!(resolved.year(), 2025);
    assert_eq!(resolved.month(), 1);
}

#[test]
fn should_handle_weekend_only_schedule() {
    use chrono::{Datelike, Weekday};

    // Arrange: Monday 2025-01-06, so the next weekend is a few days out.
    let clock = MockClock::new(epoch_ms(2025, 1, 6, 0, 0, 0));
    let cron = CronSchedule::parse("0 0 * * 0,6").unwrap(); // Saturday and Sunday

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);
    let resolved = resolve_fire_time(&clock, next_fire);

    // Assert: actually lands on a Saturday or Sunday, not just "later".
    assert!(
        matches!(resolved.weekday(), Weekday::Sat | Weekday::Sun),
        "expected a weekend day, got {:?}",
        resolved.weekday()
    );
    // The very next weekend from a Monday is that Saturday (5 days later).
    assert_eq!(resolved.day(), 11);
}

#[test]
fn should_handle_year_end_schedule() {
    use chrono::Datelike;

    // Arrange
    let clock = MockClock::new(epoch_ms(2025, 1, 1, 0, 0, 0));
    let cron = CronSchedule::parse("0 0 31 12 *").unwrap(); // New Year's Eve

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);
    let resolved = resolve_fire_time(&clock, next_fire);

    // Assert: lands specifically on December 31st of the same year.
    assert_eq!(resolved.year(), 2025);
    assert_eq!(resolved.month(), 12);
    assert_eq!(resolved.day(), 31);
}

#[test]
fn should_skip_missed_occurrences_given_forward_epoch_jump() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 58, 30)));
    let mut actor = make_schedule_actor_with_clock(clock.clone());
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/system/forward-jump/run".to_string(),
        cron: "* * * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("jump"),
    });
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
    clock.advance(Duration::from_millis(20));
    clock.advance_epoch(Duration::from_mins(3));

    // Act
    let first_fire = actor.collect_due_occurrences_for_publish();
    clock.advance(Duration::from_secs(1));
    let second_fire = actor.collect_due_occurrences_for_publish();

    // Assert
    assert_eq!(first_fire.len(), 1);
    assert!(second_fire.is_empty());
}

#[test]
fn should_not_fire_given_backward_epoch_jump_before_due_time() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
    let mut actor = make_schedule_actor_with_clock(clock.clone());
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/system/backward-jump/run".to_string(),
        cron: "* * * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("jump"),
    });
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
    clock.advance(Duration::from_millis(20));
    clock.rewind_epoch(Duration::from_secs(90));

    // Act
    let before_due = actor.collect_due_occurrences_for_publish();
    clock.advance(Duration::from_secs(121));
    let after_due = actor.collect_due_occurrences_for_publish();

    // Assert
    assert!(before_due.is_empty());
    assert_eq!(after_due.len(), 1);
}

#[test]
fn should_retain_pending_fire_claim_after_elapsed_time() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
    let (store, mut actor) = make_schedule_actor_and_store_with_clock(clock.clone());
    let create_response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/system/cleanup/run".to_string(),
        cron: "* * * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("cleanup"),
    });
    assert!(matches!(
        create_response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(claimed.len(), 1);
    clock.advance(Duration::from_hours(24));
    let schedule_store = ScheduleStore::new(store);

    // Act
    let persisted = schedule_store
        .load_pending_fire_claims(1)
        .expect("load pending fire claims");

    // Assert
    assert_eq!(
        actor.bench_pending_claimed_occurrences_for_publish().len(),
        1
    );
    assert_eq!(persisted.len(), 1);
}

// ========== Schedule Replacement Tests ==========

#[test]
fn should_replace_schedule_preserving_ordering() {
    // Arrange
    let mut actor = make_schedule_actor();

    // Create initial schedule
    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job/run".to_string(),
        cron: "0 2 * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("v1"),
    });

    // Act - Replace with different cron
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job/run".to_string(),
        cron: "0 3 * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("v2"),
    });

    // Assert
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));

    // Verify updated
    let list_response = actor.handle(ScheduleMessage::List {
        offset: 0,
        limit: 0,
    });
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs { entries, .. } => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].cron, "0 3 * * *");
        }
        _ => panic!("Expected ListDefs"),
    }
}

#[test]
fn should_maintain_independence_between_schedules() {
    // Arrange
    let mut actor = make_schedule_actor();

    // Create multiple schedules
    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job1/run".to_string(),
        cron: "0 2 * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("job1"),
    });

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job2/run".to_string(),
        cron: "0 3 * * *".to_string(),
        delivery_mode: fitz::domains::schedule::ScheduleDeliveryMode::Broadcast,
        payload: Bytes::from("job2"),
    });

    // Act - Cancel one schedule
    actor.handle(ScheduleMessage::Cancel {
        route: "schedule://acme/jobs/job1/run".to_string(),
    });

    // Assert - only job2 remains
    let list_response = actor.handle(ScheduleMessage::List {
        offset: 0,
        limit: 0,
    });
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs { entries, .. } => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].route, "schedule://acme/jobs/job2/run");
        }
        _ => panic!("Expected ListDefs"),
    }
}
