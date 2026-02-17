//! Schedule Advanced Tests - Tier 2
//!
//! Advanced patterns and edge cases covering:
//! - Cron next fire time calculations
//! - Complex scheduling scenarios
//! - Multi-schedule coordination
//! - Timing and scheduling edge cases

use fitz::domains::schedule::protocol::CronSchedule;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage};
use fitz::testkit::create_test_engine_with_cfs;
use fitz::runtime::routing::RouteFamily;
use bytes::Bytes;

// ========== Helper ==========

fn make_schedule_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
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
        ("schedule://acme/every-minute", "* * * * *", "freq-1"),
        ("schedule://acme/every-hour", "0 * * * *", "freq-2"),
        ("schedule://acme/daily", "0 0 * * *", "freq-3"),
        ("schedule://acme/weekly", "0 0 * * 1", "freq-4"),
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
    let list_response = actor.handle(ScheduleMessage::List);
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs(entries) => {
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
        route: "schedule://acme/business-hours-frequent".to_string(),
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

// ========== Schedule Replacement Tests ==========

#[test]
fn should_replace_schedule_preserving_ordering() {
    // Arrange
    let mut actor = make_schedule_actor();

    // Create initial schedule
    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/job".to_string(),
        cron: "0 2 * * *".to_string(),
        payload: Bytes::from("v1"),
    });

    // Act - Replace with different cron
    let response = actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/job".to_string(),
        cron: "0 3 * * *".to_string(),
        payload: Bytes::from("v2"),
    });

    // Assert
    assert!(matches!(
        response,
        fitz::domains::schedule::ScheduleResponse::Ok
    ));

    // Verify updated
    let list_response = actor.handle(ScheduleMessage::List);
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs(entries) => {
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
        route: "schedule://acme/job1".to_string(),
        cron: "0 2 * * *".to_string(),
        payload: Bytes::from("job1"),
    });

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/job2".to_string(),
        cron: "0 3 * * *".to_string(),
        payload: Bytes::from("job2"),
    });

    // Act - Cancel one schedule
    actor.handle(ScheduleMessage::Cancel {
        route: "schedule://acme/job1".to_string(),
    });

    // Assert - only job2 remains
    let list_response = actor.handle(ScheduleMessage::List);
    match list_response {
        fitz::domains::schedule::ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].route, "schedule://acme/job2");
        }
        _ => panic!("Expected ListDefs"),
    }
}
