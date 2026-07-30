use super::*;
use chrono::TimeZone;

struct MockClock {
    instant: Instant,
    epoch_ms: u64,
}

impl Clock for MockClock {
    fn now_instant(&self) -> Instant {
        self.instant
    }

    fn now_epoch_ms(&self) -> u64 {
        self.epoch_ms
    }
}

#[test]
fn should_parse_simple_cron_expression() {
    // Arrange
    let cron = "0 12 * * *";

    // Act
    let result = CronSchedule::parse(cron);

    // Assert
    assert!(result.is_ok());
}

#[test]
fn should_reject_invalid_cron_field_count() {
    // Arrange
    let cron = "0 12 *";

    // Act
    let result = CronSchedule::parse(cron);

    // Assert
    assert!(result.is_err());
}

#[test]
fn should_parse_documented_range_step_expression() {
    // Arrange
    let expression = "0 9-17/2 * * *";

    // Act
    let schedule = CronSchedule::parse(expression).expect("valid range step");

    // Assert
    assert_eq!(schedule.hour_matcher.values, [9, 11, 13, 15, 17]);
}

#[test]
fn should_reject_invalid_composite_cron_syntax() {
    // Arrange
    let expressions = [
        "0 9-17/2/3 * * *",
        "0 1,,2 * * *",
        "0 */0 * * *",
        "0 1-* * * *",
        "0 *,1 * * *",
    ];

    // Act
    let results = expressions.map(CronSchedule::parse);

    // Assert
    assert!(results.iter().all(Result::is_err));
}

#[test]
fn should_apply_standard_or_semantics_when_both_day_fields_are_restricted() {
    // Arrange
    let schedule = CronSchedule::parse("0 0 13 * 1").expect("valid cron");
    let monday_not_thirteenth = (1..=31)
        .find(|day| *day != 13 && day_of_week_for_date(2026, 3, *day) == 1)
        .expect("March 2026 has a Monday other than the thirteenth");
    let thirteenth_day_of_week = day_of_week_for_date(2026, 3, 13);

    // Act
    let monday_matches = schedule.matches_day(monday_not_thirteenth, 1);
    let thirteenth_matches = schedule.matches_day(13, thirteenth_day_of_week);

    // Assert
    assert!(monday_matches);
    assert!(thirteenth_matches);
}

#[test]
fn should_reject_cron_with_impossible_calendar_date() {
    // Arrange
    let expression = "0 0 31 2 *";

    // Act
    let result = CronSchedule::parse(expression);

    // Assert
    assert_eq!(
        result.expect_err("February 31 is impossible"),
        "Cron expression has no possible fire time"
    );
}

#[test]
fn should_find_leap_day_after_non_leap_century_year() {
    // Arrange
    let clock = MockClock {
        instant: Instant::now(),
        epoch_ms: datetime_to_seconds(2096, 3, 1, 0, 0) * 1_000,
    };
    let schedule = CronSchedule::parse("0 0 29 2 *").expect("valid leap-day cron");

    // Act
    let next = schedule
        .try_next_fire_time_with_clock(clock.now_instant(), &clock)
        .expect("next leap day");
    let next_ms =
        instant_to_epoch_ms_with_reference(next, clock.now_instant(), clock.now_epoch_ms());

    // Assert
    assert_eq!(
        seconds_to_datetime(next_ms / 1_000),
        (2104, 2, 29, 0, 0, day_of_week_for_date(2104, 2, 29))
    );
}

#[test]
fn should_create_schedule_def() {
    // Arrange
    let route = "schedule://acme/jobs/backup/run".to_string();
    let cron = "0 */6 * * *".to_string();
    let payload = Bytes::from("backup data");

    // Act
    let parsed_cron = CronSchedule::parse(&cron).expect("Valid cron");
    let route_parts = parse_concrete_schedule_route(&route).expect("valid schedule route");
    let def = ScheduleDef {
        route,
        route_parts,
        cron,
        delivery_mode: ScheduleDeliveryMode::Broadcast,
        parsed_cron,
        payload,
        next_fire_time: Instant::now(),
        next_fire_ms: 0,
        last_fire_ms: None,
        executions_total: 0,
        list_index: 0,
    };

    // Assert
    assert_eq!(def.route, "schedule://acme/jobs/backup/run");
}

#[test]
fn should_parse_concrete_schedule_route_given_valid_route() {
    // Arrange
    let input = "schedule://acme/billing/invoice/send";

    // Act
    let route = parse_concrete_schedule_route(input).unwrap();

    // Assert
    assert_eq!(route.realm, "acme");
    assert_eq!(route.area, "billing");
    assert_eq!(route.resource, "invoice");
    assert_eq!(route.operation, "send");
}

#[test]
fn should_reject_concrete_schedule_route_given_missing_operation() {
    // Arrange
    let input = "schedule://acme/billing/invoice";

    // Act
    let err = parse_concrete_schedule_route(input).unwrap_err();

    // Assert
    assert_eq!(
        err,
        "schedule route must be schedule://{realm}/{area}/{resource}/{operation}"
    );
}

#[test]
fn should_reject_concrete_schedule_route_given_wildcard() {
    let err = parse_concrete_schedule_route("schedule://acme/billing/*/send").unwrap_err();

    assert_eq!(err, "schedule route must not contain wildcards");
}

#[test]
fn should_fast_path_hourly_schedule_given_single_minute() {
    // Arrange
    let cron = CronSchedule::parse("0 * * * *").unwrap();

    // Act
    let candidate = cron.simple_candidate_seconds((10 * 3_600) + (14 * 60) + 30);

    // Assert
    assert_eq!(candidate, Some(11 * 3_600));
}

#[test]
fn should_fast_path_daily_schedule_given_single_hour_and_minute() {
    // Arrange
    let cron = CronSchedule::parse("15 6 * * *").unwrap();

    // Act
    let candidate = cron.simple_candidate_seconds((6 * 3_600) + (20 * 60));

    // Assert
    assert_eq!(candidate, Some(86_400 + (6 * 3_600) + (15 * 60)));
}

#[test]
fn should_fast_path_every_minute_schedule() {
    // Arrange
    let cron = CronSchedule::parse("* * * * *").unwrap();

    // Act
    let candidate = cron.simple_candidate_seconds((10 * 3_600) + (14 * 60) + 30);

    // Assert
    assert_eq!(candidate, Some((10 * 3_600) + (15 * 60)));
}

#[test]
fn should_fast_path_monthly_schedule_given_fixed_day_hour_and_minute() {
    // Arrange
    let cron = CronSchedule::parse("0 2 1 * *").unwrap();
    let current_secs = datetime_to_seconds(2026, 3, 31, 5, 30);
    let expected_secs = datetime_to_seconds(2026, 4, 1, 2, 0);

    // Act
    let candidate = cron.simple_candidate_seconds(current_secs);

    // Assert
    assert_eq!(candidate, Some(expected_secs));
}

#[test]
fn should_calculate_next_fire_time_given_fixed_utc_clock_reference() {
    // Arrange
    let clock = MockClock {
        instant: Instant::now(),
        epoch_ms: u64::try_from(
            chrono::Utc
                .with_ymd_and_hms(2026, 3, 31, 5, 30, 0)
                .single()
                .expect("valid datetime")
                .timestamp_millis(),
        )
        .expect("test datetime should be after unix epoch"),
    };
    let cron = CronSchedule::parse("0 6 * * *").expect("valid cron");

    // Act
    let next_fire = cron.next_fire_time_with_clock(clock.now_instant(), &clock);

    // Assert
    assert_eq!(
        next_fire.duration_since(clock.now_instant()),
        Duration::from_mins(30)
    );
}
