//! Schedule domain semantics tests
//!
//! Tests specific Schedule operation semantics: cron parsing, field validation,
//! schedule lifecycle, error conditions, and boundary cases.

use bytes::Bytes;
use fitz::domains::schedule::actor::ScheduleActor;
use fitz::domains::schedule::protocol::SchedulePayload;
use fitz::runtime::routing::RouteFamily;
use std::sync::Arc;

fn make_store() -> Arc<cntryl_midge::Engine> {
    Arc::new(
        cntryl_midge::Engine::open_with_options(cntryl_midge::MidgeOptions::default())
            .expect("Failed to open Midge"),
    )
}

fn make_cron_payload(cron: &str) -> Bytes {
    let sp = SchedulePayload {
        cron: cron.to_string(),
    };
    Bytes::from(sp.encode())
}

#[test]
fn should_parse_wildcard_cron() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Every minute
    let payload = make_cron_payload("* * * * *");

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/every_minute".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_parse_step_expression() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Every 5 minutes
    let payload = make_cron_payload("*/5 * * * *");

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/every5min".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_parse_range_expression() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // 9 to 17 (9 AM to 5 PM)
    let payload = make_cron_payload("0 9-17 * * *");

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/business_hours".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_parse_csv_expression() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Specific hours: 8, 12, 16, 20
    let payload = make_cron_payload("0 8,12,16,20 * * *");

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/specific_hours".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_missing_cron_field() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Only 4 fields instead of 5
    let sp = SchedulePayload {
        cron: "0 9 * *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_reject_too_many_cron_fields() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // 6 fields instead of 5 (includes seconds)
    let sp = SchedulePayload {
        cron: "0 0 9 * * *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_validate_minute_range_0_to_59() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Invalid: minute 60
    let sp = SchedulePayload {
        cron: "60 * * * *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_validate_hour_range_0_to_23() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Invalid: hour 24
    let sp = SchedulePayload {
        cron: "0 24 * * *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_validate_day_range_1_to_31() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Invalid: day 0 (must be 1-31)
    let sp = SchedulePayload {
        cron: "0 * 0 * *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_validate_month_range_1_to_12() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Invalid: month 13
    let sp = SchedulePayload {
        cron: "0 * * 13 *".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_validate_weekday_range_0_to_6() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Invalid: weekday 7 (0-6 only)
    let sp = SchedulePayload {
        cron: "0 * * * 7".to_string(),
    };
    let payload = Bytes::from(sp.encode());

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/invalid".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_handle_special_day_31_correctly() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    // Last day of month
    let payload = make_cron_payload("0 9 31 * *");

    // Act
    let result = actor.create_schedule(
        fitz::runtime::routing::Route::new("notice://test/schedule/month_end".to_string()),
        payload,
    );

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_assign_sequential_ids() {
    // Arrange
    let family = RouteFamily::new(0);
    let store = make_store();
    let mut actor = ScheduleActor::new(family, store, cntryl_midge::WriteOptions::sync());

    let payload = make_cron_payload("0 9 * * *");

    // Act: Create multiple schedules
    let id1 = actor
        .create_schedule(
            fitz::runtime::routing::Route::new("notice://test/schedule/1".to_string()),
            payload.clone(),
        )
        .unwrap();

    let id2 = actor
        .create_schedule(
            fitz::runtime::routing::Route::new("notice://test/schedule/2".to_string()),
            payload.clone(),
        )
        .unwrap();

    let id3 = actor
        .create_schedule(
            fitz::runtime::routing::Route::new("notice://test/schedule/3".to_string()),
            payload,
        )
        .unwrap();

    // Assert: Sequential IDs
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);
    assert_eq!(id3, 3);
}
