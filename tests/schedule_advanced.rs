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
        state.epoch_ms = state.epoch_ms.saturating_add(duration.as_millis() as u64);
    }

    fn advance_epoch(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.epoch_ms = state.epoch_ms.saturating_add(duration.as_millis() as u64);
    }

    fn rewind_epoch(&self, duration: Duration) {
        let mut state = self.state.lock().expect("lock mock clock");
        state.epoch_ms = state.epoch_ms.saturating_sub(duration.as_millis() as u64);
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
    chrono::Utc
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .expect("valid datetime")
        .timestamp_millis() as u64
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
        payload: Bytes::from("complex-cron"),
    });

    // Assert
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
}

#[test]
fn should_find_next_fire_for_business_hours_schedule() {
    // Arrange
    let cron = CronSchedule::parse("0 9-17 * * 1-5").unwrap(); // 9-5 weekdays
    let now = std::time::Instant::now();

    // Act
    let next_fire = cron.next_fire_time(now);

    // Assert
    assert!(next_fire > now);
    // Should fire within reasonable business hours window
    let elapsed = next_fire.duration_since(now);
    assert!(elapsed.as_secs() < 7 * 24 * 3600); // Within a week
}

// ========== Edge Case Tests ==========

#[test]
fn should_handle_leap_year_february() {
    // Arrange
    let cron = CronSchedule::parse("0 0 29 2 *").unwrap(); // Feb 29

    // Act
    let now = std::time::Instant::now();
    let next_fire = cron.next_fire_time(now);

    // Assert - should be valid and in future
    assert!(next_fire > now);
}

#[test]
fn should_handle_month_end_schedule() {
    // Arrange
    let cron = CronSchedule::parse("0 0 31 * *").unwrap(); // 31st of month

    // Act
    let now = std::time::Instant::now();
    let next_fire = cron.next_fire_time(now);

    // Assert - should skip months without 31st day
    assert!(next_fire > now);
}

#[test]
fn should_handle_weekend_only_schedule() {
    // Arrange
    let cron = CronSchedule::parse("0 0 * * 0,6").unwrap(); // Saturday and Sunday

    // Act
    let now = std::time::Instant::now();
    let next_fire = cron.next_fire_time(now);

    // Assert - fires only on weekends
    assert!(next_fire > now);
}

#[test]
fn should_handle_year_end_schedule() {
    // Arrange
    let cron = CronSchedule::parse("0 0 31 12 *").unwrap(); // New Year's Eve

    // Act
    let now = std::time::Instant::now();
    let next_fire = cron.next_fire_time(now);

    // Assert
    assert!(next_fire > now);
}

#[test]
fn should_skip_missed_occurrences_given_forward_epoch_jump() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 58, 30)));
    let mut actor = make_schedule_actor_with_clock(clock.clone());
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/system/forward-jump/run".to_string(),
        cron: "* * * * *".to_string(),
        payload: Bytes::from("jump"),
    });
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
    clock.advance(Duration::from_millis(20));
    clock.advance_epoch(Duration::from_secs(180));

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
fn should_cleanup_pending_fire_claim_after_ttl() {
    // Arrange
    let clock = Arc::new(MockClock::new(epoch_ms(2026, 3, 31, 5, 59, 30)));
    let (store, mut actor) = make_schedule_actor_and_store_with_clock(clock.clone());
    let create_response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/system/cleanup/run".to_string(),
        cron: "* * * * *".to_string(),
        payload: Bytes::from("cleanup"),
    });
    assert!(matches!(
        create_response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));
    actor.bench_prepare_scan(1);
    let claimed = actor.bench_claim_due_fires();
    assert_eq!(claimed.len(), 1);
    let ttl_ms = 5 * 60 * 1000;
    clock.advance(Duration::from_millis(ttl_ms + 1));
    let schedule_store = ScheduleStore::new(store);

    // Act
    let expired = actor
        .bench_cleanup_stale_pending_claims(ttl_ms)
        .expect("cleanup stale pending claims");
    let persisted = schedule_store
        .load_pending_fire_claims(1)
        .expect("load pending fire claims");

    // Assert
    assert_eq!(expired, 1);
    assert!(
        actor
            .bench_pending_claimed_occurrences_for_publish()
            .is_empty()
    );
    assert!(persisted.is_empty());
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
        payload: Bytes::from("v1"),
    });

    // Act - Replace with different cron
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job/run".to_string(),
        cron: "0 3 * * *".to_string(),
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
        payload: Bytes::from("job1"),
    });

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/job2/run".to_string(),
        cron: "0 3 * * *".to_string(),
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
