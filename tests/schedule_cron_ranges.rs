//! Schedule domain range syntax validation tests
//!
//! Tests cron parsing with range syntax support
//! Ensures ranges are properly parsed, bounded, and merged with CSV syntax

use chrono::TimeZone;
use fitz::domains::schedule::CronSchedule;

#[test]
fn should_support_hour_ranges() {
    // Arrange
    let expr = "0 9-17 * * *"; // 9 AM to 5 PM

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(
        cron.hour,
        vec![9, 10, 11, 12, 13, 14, 15, 16, 17],
        "Should parse hour range 9-17"
    );
}

#[test]
fn should_support_weekday_ranges() {
    // Arrange
    let expr = "0 0 * * 1-5"; // Monday to Friday

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(
        cron.weekday,
        vec![1, 2, 3, 4, 5],
        "Should parse weekday range 1-5"
    );
}

#[test]
fn should_support_minute_ranges() {
    // Arrange
    let expr = "15-45 * * * *"; // Minutes 15 to 45

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(
        cron.minute.len(),
        31,
        "Should have 31 values (15-45 inclusive)"
    );
    assert_eq!(cron.minute[0], 15);
    assert_eq!(cron.minute[30], 45);
}

#[test]
fn should_support_day_ranges() {
    // Arrange
    let expr = "0 0 1-15 * *"; // 1st to 15th of month

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.day.len(), 15, "Should have 15 values (1-15 inclusive)");
    assert_eq!(cron.day[0], 1);
    assert_eq!(cron.day[14], 15);
}

#[test]
fn should_support_month_ranges() {
    // Arrange
    let expr = "0 0 * 3-6 *"; // March to June

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.month, vec![3, 4, 5, 6], "Should parse month range 3-6");
}

#[test]
fn should_clamp_range_to_field_bounds() {
    // Arrange
    let expr = "0 20-30 * * *"; // Hour range extends beyond max (23)

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(
        cron.hour,
        vec![20, 21, 22, 23],
        "Should clamp to valid range 20-23"
    );
}

#[test]
fn should_ignore_reversed_ranges() {
    // Arrange
    let expr = "0 17-9 * * *"; // Reversed: end < start

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert!(
        cron.hour.is_empty(),
        "Should result in empty field for reversed range"
    );
}

#[test]
fn should_merge_ranges_with_csv() {
    // Arrange
    let expr = "0 9-12,15,18-20 * * *"; // Multiple ranges and single values

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    let expected = vec![9, 10, 11, 12, 15, 18, 19, 20];
    assert_eq!(cron.hour, expected, "Should merge ranges and CSV values");
}

#[test]
fn should_deduplicate_overlapping_ranges() {
    // Arrange
    let expr = "0 9-15,12-18 * * *"; // Overlapping ranges

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    let expected = vec![9, 10, 11, 12, 13, 14, 15, 16, 17, 18];
    assert_eq!(
        cron.hour, expected,
        "Should merge and deduplicate overlapping ranges"
    );
}

#[test]
fn should_handle_single_value_range() {
    // Arrange
    let expr = "0 14-14 * * *"; // Single value as range

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.hour, vec![14], "Should parse single-value range");
}

#[test]
fn should_maintain_sorted_output() {
    // Arrange
    let expr = "0 18-20,9-12,15 * * *"; // Ranges in non-sorted order

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    let expected = vec![9, 10, 11, 12, 15, 18, 19, 20];
    assert_eq!(cron.hour, expected, "Output should be sorted");
}

#[test]
fn should_work_with_step_syntax() {
    // Arrange
    let expr = "*/15 */4 * * *";

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.minute, vec![0, 15, 30, 45]);
    assert_eq!(cron.hour, vec![0, 4, 8, 12, 16, 20]);
}

#[test]
fn should_not_support_range_with_step() {
    // Arrange
    let expr = "0 9-17/2 * * *";

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    // The range 9-17/2 won't parse correctly (no support for step within range)
    // The parser will fail on "-17/2" which is not a valid number range
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    // The hour field will be empty because "-17/2" doesn't parse as a range
    assert!(cron.hour.is_empty());
}

#[test]
fn should_match_times_in_range() {
    // Arrange
    let cron = CronSchedule::parse("0 9-17 * * 1-5").unwrap();
    let dt_in_range = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap(); // Wed 12:00, in range
    let dt_out_range = chrono::Utc.with_ymd_and_hms(2025, 1, 15, 8, 0, 0).unwrap(); // Wed 8:00, out of range

    // Act
    let in_range = cron.matches_dt(&dt_in_range);
    let out_range = cron.matches_dt(&dt_out_range);

    // Assert
    assert!(in_range, "Should match times within range");
    assert!(!out_range, "Should not match times outside range");
}

#[test]
fn should_handle_all_field_ranges_simultaneously() {
    // Arrange
    let expr = "15-45 9-17 1-15 3-6 1-5";

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.minute.len(), 31); // 15-45
    assert_eq!(cron.hour.len(), 9); // 9-17
    assert_eq!(cron.day.len(), 15); // 1-15
    assert_eq!(cron.month.len(), 4); // 3-6
    assert_eq!(cron.weekday.len(), 5); // 1-5
}

#[test]
fn should_reject_malformed_range_without_dash() {
    // Arrange
    let expr = "0 9 17 * * *"; // No dash, just separate numbers

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_err());
}

#[test]
fn should_handle_range_at_boundary_min() {
    // Arrange
    let expr = "0-0 0-0 1-1 1-1 0-0"; // All minimums as ranges

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.minute, vec![0]);
    assert_eq!(cron.hour, vec![0]);
    assert_eq!(cron.day, vec![1]);
    assert_eq!(cron.month, vec![1]);
    assert_eq!(cron.weekday, vec![0]);
}

#[test]
fn should_handle_range_at_boundary_max() {
    // Arrange
    let expr = "59-59 23-23 31-31 12-12 6-6"; // All maximums as ranges

    // Act
    let cron = CronSchedule::parse(expr);

    // Assert
    assert!(cron.is_ok());
    let cron = cron.unwrap();
    assert_eq!(cron.minute, vec![59]);
    assert_eq!(cron.hour, vec![23]);
    assert_eq!(cron.day, vec![31]);
    assert_eq!(cron.month, vec![12]);
    assert_eq!(cron.weekday, vec![6]);
}
