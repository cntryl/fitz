use fitz::domains::schedule::protocol::{Clock, CronField, CronSchedule};
use std::time::{Duration, Instant};

struct FixedClock(Instant);

impl Clock for FixedClock {
    fn now_instant(&self) -> Instant {
        self.0
    }

    fn now_epoch_ms(&self) -> u64 {
        0
    }
}

#[test]
fn should_preserve_compiled_weekdays_when_changing_an_inspected_field_copy() {
    // Arrange
    let clock = FixedClock(Instant::now());
    let schedule: fitz::domains::schedule::CronSchedule =
        CronSchedule::parse("0 0 * * 1,3").expect("valid weekday expression");
    let mut inspected = schedule.day_of_week().clone();

    // Act
    if let CronField::List(days) = &mut inspected {
        days.clear();
    } else {
        panic!("expected a parsed weekday list");
    }
    let next = schedule
        .try_next_fire_time_with_clock(clock.0, &clock)
        .expect("validated schedule has a next occurrence");

    // Assert
    assert!(matches!(inspected, CronField::List(days) if days.is_empty()));
    assert_eq!(next.duration_since(clock.0), Duration::from_hours(96));
}

#[test]
fn should_apply_a_new_weekday_expression_after_replacing_a_parsed_schedule() {
    // Arrange
    let clock = FixedClock(Instant::now());
    let original = CronSchedule::parse("0 0 * * *").expect("valid daily expression");
    let mut schedules = [original];

    // Act
    schedules[0] = CronSchedule::parse("0 0 * * 1").expect("valid Monday expression");
    let next = schedules[0]
        .try_next_fire_time_with_clock(clock.0, &clock)
        .expect("validated schedule has a next occurrence");

    // Assert
    assert_eq!(next.duration_since(clock.0), Duration::from_hours(96));
}
