//! Schedule domain E2E integration tests
//!
//! Tests the new route-based schedule model where:
//! - Routes are unique schedule identifiers
//! - Schedules fire to subscribers matching route patterns
//! - Cron expressions determine when schedules fire

use bytes::Bytes;
use fitz::domains::schedule::protocol::CronSchedule;

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
