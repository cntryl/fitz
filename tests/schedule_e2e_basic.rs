//! Schedule domain E2E integration tests
//!
//! Tests the new route-based schedule model where:
//! - Routes are unique schedule identifiers
//! - Schedules fire to subscribers matching route patterns
//! - Cron expressions determine when schedules fire

use bytes::Bytes;
use fitz::domains::schedule::protocol::CronSchedule;
use fitz::domains::schedule::{ScheduleActor, ScheduleMessage, ScheduleResponse};
use fitz::runtime::routing::RouteFamily;
use fitz::testkit::create_test_engine_with_cfs;

/// Helper to create a test Schedule actor
fn test_actor() -> ScheduleActor {
    let store = create_test_engine_with_cfs(vec![1, 2, 3]);
    ScheduleActor::new(
        RouteFamily::new(1),
        store,
        cntryl_midge::WriteOptions::buffered(),
    )
}

// ========== Cron Parsing Tests ==========

#[test]
fn should_parse_valid_cron_every_minute() {
    // Arrange
    let cron_str = "* * * * *";

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_workday_9am() {
    // Arrange
    let cron_str = "0 9 * * 1-5"; // Mon-Fri at 9 AM

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_with_step_syntax() {
    // Arrange
    let cron_str = "*/15 */6 * * *"; // Every 15 min, every 6 hours

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_with_list_syntax() {
    // Arrange
    let cron_str = "0 9,12,18 * * *"; // At 9 AM, 12 PM, 6 PM

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_with_range_syntax() {
    // Arrange
    let cron_str = "0 9-17 * * 1-5"; // 9 AM to 5 PM, Mon-Fri

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_max_values() {
    // Arrange
    let cron_str = "59 23 31 12 6";

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_parse_valid_cron_min_values() {
    // Arrange
    let cron_str = "0 0 1 1 0";

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

#[test]
fn should_reject_invalid_cron_minute_too_high() {
    // Arrange
    let cron_str = "60 0 * * *"; // minute must be 0-59

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_reject_invalid_cron_hour_too_high() {
    // Arrange
    let cron_str = "0 24 * * *"; // hour must be 0-23

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_reject_invalid_cron_day_too_high() {
    // Arrange
    let cron_str = "0 0 32 * *"; // day must be 1-31

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_reject_invalid_cron_month_too_high() {
    // Arrange
    let cron_str = "0 0 * 13 *"; // month must be 1-12

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_reject_invalid_cron_weekday_too_high() {
    // Arrange
    let cron_str = "0 0 * * 7"; // weekday must be 0-6

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_reject_invalid_cron_range() {
    // Arrange
    let cron_str = "0 0 * * 5-1"; // Invalid range (start > end)

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_parse_cron_with_all_wildcards() {
    // Arrange
    let cron_str = "* * * * *";

    // Act
    let cron = CronSchedule::parse(cron_str);

    // Assert
    assert!(cron.is_ok());
}

// ========== CREATE/CANCEL/LIST Operation Tests ==========

#[test]
fn should_create_schedule_successfully() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/backup".to_string();
    let cron = "0 2 * * *".to_string(); // Daily at 2 AM
    let payload = Bytes::from("backup-config");

    // Act
    let response = actor.handle(ScheduleMessage::Create {
        route: route.clone(),
        cron,
        payload,
    });

    // Assert
    match response {
        ScheduleResponse::Ok => {}
        ScheduleResponse::Error(e) => panic!("Expected Ok, got Error: {}", e),
        _ => panic!("Expected Ok response"),
    }
}

#[test]
fn should_reject_invalid_cron_on_create() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/backup".to_string();
    let cron = "60 0 * * *".to_string(); // Invalid: minute=60
    let payload = Bytes::from("data");

    // Act
    let response = actor.handle(ScheduleMessage::Create {
        route,
        cron,
        payload,
    });

    // Assert
    match response {
        ScheduleResponse::Error(e) => {
            assert!(e.contains("out of range") || e.contains("Value 60"));
        }
        _ => panic!("Expected Error response for invalid cron"),
    }
}

#[test]
fn should_upsert_schedule_by_route() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/backup".to_string();

    // Act - Create initial schedule
    let response1 = actor.handle(ScheduleMessage::Create {
        route: route.clone(),
        cron: "0 2 * * *".to_string(),
        payload: Bytes::from("config-v1"),
    });

    // Act - Update same route with different cron
    let response2 = actor.handle(ScheduleMessage::Create {
        route: route.clone(),
        cron: "0 3 * * *".to_string(), // Changed from 2 AM to 3 AM
        payload: Bytes::from("config-v2"),
    });

    // Assert
    assert!(matches!(response1, ScheduleResponse::Ok));
    assert!(matches!(response2, ScheduleResponse::Ok));

    // Verify only one schedule exists
    let list_response = actor.handle(ScheduleMessage::List);
    match list_response {
        ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].route, route);
            assert_eq!(entries[0].cron, "0 3 * * *");
            assert_eq!(entries[0].payload, Bytes::from("config-v2"));
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_cancel_schedule_by_route() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/backup".to_string();

    actor.handle(ScheduleMessage::Create {
        route: route.clone(),
        cron: "0 2 * * *".to_string(),
        payload: Bytes::from("data"),
    });

    // Act
    let response = actor.handle(ScheduleMessage::Cancel {
        route: route.clone(),
    });

    // Assert
    assert!(matches!(response, ScheduleResponse::Ok));

    // Verify schedule is gone
    let list_response = actor.handle(ScheduleMessage::List);
    match list_response {
        ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 0);
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_return_ok_when_canceling_nonexistent_schedule() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/nonexistent".to_string();

    // Act - Cancel schedule that doesn't exist
    let response = actor.handle(ScheduleMessage::Cancel { route });

    // Assert
    assert!(matches!(response, ScheduleResponse::Ok)); // Idempotent
}

#[test]
fn should_list_all_schedules() {
    // Arrange
    let mut actor = test_actor();

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/backup".to_string(),
        cron: "0 2 * * *".to_string(),
        payload: Bytes::from("backup"),
    });

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/cleanup".to_string(),
        cron: "0 3 * * *".to_string(),
        payload: Bytes::from("cleanup"),
    });

    actor.handle(ScheduleMessage::Create {
        route: "schedule://acme/jobs/report".to_string(),
        cron: "0 9 * * 1".to_string(), // Monday at 9 AM
        payload: Bytes::from("report"),
    });

    // Act
    let response = actor.handle(ScheduleMessage::List);

    // Assert
    match response {
        ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 3);

            // Verify all routes present
            let routes: Vec<String> = entries.iter().map(|e| e.route.clone()).collect();
            assert!(routes.contains(&"schedule://acme/jobs/backup".to_string()));
            assert!(routes.contains(&"schedule://acme/jobs/cleanup".to_string()));
            assert!(routes.contains(&"schedule://acme/jobs/report".to_string()));
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_list_empty_when_no_schedules() {
    // Arrange
    let mut actor = test_actor();

    // Act
    let response = actor.handle(ScheduleMessage::List);

    // Assert
    match response {
        ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 0);
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_preserve_payload_in_schedule() {
    // Arrange
    let mut actor = test_actor();
    let route = "schedule://acme/jobs/task".to_string();
    let payload = Bytes::from(vec![1, 2, 3, 4, 5, 0xFF, 0xAB, 0xCD]); // Binary data

    actor.handle(ScheduleMessage::Create {
        route: route.clone(),
        cron: "* * * * *".to_string(),
        payload: payload.clone(),
    });

    // Act
    let response = actor.handle(ScheduleMessage::List);

    // Assert
    match response {
        ScheduleResponse::ListDefs(entries) => {
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].payload, payload);
        }
        _ => panic!("Expected ListDefs response"),
    }
}

#[test]
fn should_handle_subscribe_operation() {
    use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

    // Arrange
    let mut actor = test_actor();
    let pattern = Route::new("schedule://acme/jobs/*");
    let family = RouteFamily::new(1);
    let subscriber = RouteAddress::new(family, Route::new("session1"));

    // Act
    let response = actor.handle(ScheduleMessage::Subscribe {
        family_id: family,
        pattern,
        session_id: 1,
        subscriber,
    });

    // Assert
    assert!(matches!(response, ScheduleResponse::Ok));
}

#[test]
fn should_handle_unsubscribe_operation() {
    use fitz::runtime::routing::{Route, RouteAddress, RouteFamily};

    // Arrange
    let mut actor = test_actor();
    let pattern = Route::new("schedule://acme/jobs/*");
    let family = RouteFamily::new(1);
    let subscriber = RouteAddress::new(family, Route::new("session1"));

    // Act
    let response = actor.handle(ScheduleMessage::Unsubscribe {
        family_id: family,
        pattern,
        session_id: 1,
        subscriber,
    });

    // Assert
    assert!(matches!(response, ScheduleResponse::Ok));
}

// ========== Cron Next Fire Time Tests ==========

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
