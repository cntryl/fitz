use crate::runtime::routing::{route_exact_quad, route_scheme, Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub use crate::runtime::clock::{
    epoch_ms_to_instant_with_reference, instant_to_epoch_ms_with_reference, Clock, SystemClock,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcreteScheduleRoute {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
}

/// # Errors
///
/// Returns an error when `route` is not a concrete `schedule://` route with
/// non-empty realm, area, resource, and operation segments.
pub fn parse_concrete_schedule_route(route: &str) -> Result<ConcreteScheduleRoute, String> {
    let Some(scheme) = route_scheme(route) else {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    };
    if scheme != "schedule" {
        return Err("schedule route scheme must be schedule".to_string());
    }

    let Some(parts) = route_exact_quad(route) else {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    };
    if [parts.realm, parts.area, parts.resource, parts.operation]
        .iter()
        .any(|part| part.is_empty())
    {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    }
    if [parts.realm, parts.area, parts.resource, parts.operation]
        .iter()
        .any(|part| *part == "*" || *part == "**")
    {
        return Err("schedule route must not contain wildcards".to_string());
    }

    Ok(ConcreteScheduleRoute {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
        operation: parts.operation.to_string(),
    })
}

/// # Errors
///
/// Returns an error when `route` is not a valid concrete `schedule://` route.
pub fn validate_concrete_schedule_route(route: &str) -> Result<(), String> {
    parse_concrete_schedule_route(route).map(|_| ())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ScheduleDeliveryMode {
    Broadcast = 0,
    Single = 1,
}

impl ScheduleDeliveryMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Broadcast => "broadcast",
            Self::Single => "single",
        }
    }
}

impl TryFrom<u8> for ScheduleDeliveryMode {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Broadcast),
            1 => Ok(Self::Single),
            _ => Err("invalid schedule delivery mode".to_string()),
        }
    }
}

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create or update a schedule (route is identity, upsert)
    Create {
        route: String,
        cron: String,
        delivery_mode: ScheduleDeliveryMode,
        payload: Bytes,
    },
    /// Create or update multiple schedules atomically.
    CreateBatch { entries: Vec<ScheduleCreateEntry> },
    /// Cancel an existing schedule by route
    Cancel { route: String },
    /// List all schedules (supports pagination)
    List {
        /// Starting offset (0-based). Default: 0
        offset: u64,
        /// Maximum number of entries to return (0 = all remaining). Default: 100
        limit: u64,
    },
    /// Subscribe to live notifications for one exact schedule route.
    Subscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Remove one live notification subscription for one exact schedule route.
    Unsubscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe all schedule subscriptions for a session (called on disconnect)
    UnsubscribeAll {
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Parsed client request delivered to the Schedule domain sink.
#[derive(Debug, Clone)]
pub struct ScheduleClientRequest {
    pub meta: ClientFrameMeta,
    pub message: Result<ScheduleMessage, String>,
}

impl ScheduleClientRequest {
    pub fn new(meta: ClientFrameMeta, message: Result<ScheduleMessage, String>) -> Self {
        Self { meta, message }
    }
}

/// Typed Schedule response to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct ScheduleClientResponse {
    pub meta: ClientFrameMeta,
    pub response: ScheduleResponse,
}

impl ScheduleClientResponse {
    #[must_use]
    pub fn new(meta: ClientFrameMeta, response: ScheduleResponse) -> Self {
        Self { meta, response }
    }
}

/// Typed live Schedule notification to be encoded at the transport edge.
#[derive(Debug, Clone)]
pub struct ScheduleClientNotification {
    pub session_id: u64,
    pub route_family: RouteFamily,
    pub subscription_id: u64,
    pub payload: Bytes,
}

impl ScheduleClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleCreateEntry {
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Bytes,
}

/// Response from schedule operations
#[derive(Debug, Clone)]
pub enum ScheduleResponse {
    /// Operation succeeded (no `schedule_id` returned; route is identity).
    Ok,
    /// SUBSCRIBE succeeded with the logical `subscription_id` used for NOTIFY fanout.
    SubscribeOk { subscription_id: u64 },
    /// LIST operation: returns paginated schedules with total count
    ListDefs {
        entries: Arc<Vec<Arc<ScheduleListEntry>>>,
        total_count: u64,
    },
    /// Operation failed with error message
    Error(String),
}

/// Single schedule entry in LIST response
#[derive(Debug, Clone)]
pub struct ScheduleListEntry {
    pub route: String,
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    pub payload: Bytes,
}

/// Schedule definition plus actor-local timing accelerators.
#[derive(Debug, Clone)]
pub struct ScheduleDef {
    /// Route string (unique identity for this schedule)
    pub route: String,
    /// Parsed concrete route parts reused by hot storage-key paths.
    pub route_parts: ConcreteScheduleRoute,
    /// Cron expression (when to fire)
    pub cron: String,
    pub delivery_mode: ScheduleDeliveryMode,
    /// Parsed cron schedule (cached to avoid reparsing)
    pub parsed_cron: CronSchedule,
    /// Payload bytes handed to the live publish path when an occurrence is claimed.
    pub payload: Bytes,
    /// Next fire time calculated from cron + current time
    pub next_fire_time: Instant,
    /// Exact next-fire timestamp stored in the durable definition row
    pub next_fire_ms: u64,
    /// Last acknowledged live handoff timestamp in UNIX epoch milliseconds.
    pub last_fire_ms: Option<u64>,
    /// Total acknowledged live handoffs recorded for this definition.
    pub executions_total: u64,
    /// Internal index into the actor's mutable LIST backing store.
    pub list_index: usize,
}

/// Parses and validates cron expressions
#[derive(Debug, Clone)]
pub struct CronSchedule {
    pub minute: CronField,
    pub hour: CronField,
    pub day_of_month: CronField,
    pub month: CronField,
    pub day_of_week: CronField,
    minute_matcher: FieldMatcher,
    hour_matcher: FieldMatcher,
    day_of_month_matcher: FieldMatcher,
    month_matcher: FieldMatcher,
    day_of_week_matcher: FieldMatcher,
}

/// Represents a single field in a cron expression (e.g., hour, minute)
#[derive(Debug, Clone)]
pub enum CronField {
    Any,
    Single(u32),
    Range(u32, u32),
    List(Vec<u32>),
    Step(Box<CronField>, u32),
}

impl CronField {
    fn is_any(&self) -> bool {
        matches!(self, CronField::Any)
    }

    fn as_single(&self) -> Option<u32> {
        match self {
            CronField::Single(value) => Some(*value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
struct FieldMatcher {
    values: Vec<u32>,
}

impl FieldMatcher {
    fn from_field(field: &CronField, min: u32, max: u32) -> Self {
        let mut values = Vec::with_capacity((max - min + 1) as usize);
        for value in min..=max {
            if matches_field(field, value) {
                values.push(value);
            }
        }
        Self { values }
    }

    fn matches(&self, value: u32) -> bool {
        self.values.binary_search(&value).is_ok()
    }

    fn next_at_or_after(&self, current: u32) -> Option<(u32, bool)> {
        if self.values.is_empty() {
            return None;
        }

        match self.values.binary_search(&current) {
            Ok(index) => Some((self.values[index], false)),
            Err(index) if index < self.values.len() => Some((self.values[index], false)),
            Err(_) => Some((self.values[0], true)),
        }
    }
}

impl CronSchedule {
    /// Parse a 5-field cron expression (minute hour day month `day_of_week`).
    ///
    /// # Errors
    ///
    /// Returns an error when the expression does not contain exactly five
    /// fields or any field is invalid for its allowed range.
    pub fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err("Cron expression must have exactly 5 fields".to_string());
        }

        let minute = parse_cron_field(fields[0], 0, 59)?;
        let hour = parse_cron_field(fields[1], 0, 23)?;
        let day_of_month = parse_cron_field(fields[2], 1, 31)?;
        let month = parse_cron_field(fields[3], 1, 12)?;
        let day_of_week = parse_cron_field(fields[4], 0, 6)?;

        Ok(CronSchedule {
            minute_matcher: FieldMatcher::from_field(&minute, 0, 59),
            hour_matcher: FieldMatcher::from_field(&hour, 0, 23),
            day_of_month_matcher: FieldMatcher::from_field(&day_of_month, 1, 31),
            month_matcher: FieldMatcher::from_field(&month, 1, 12),
            day_of_week_matcher: FieldMatcher::from_field(&day_of_week, 0, 6),
            minute,
            hour,
            day_of_month,
            month,
            day_of_week,
        })
    }

    /// Calculate next fire time from current time
    #[must_use]
    pub fn next_fire_time(&self, from: Instant) -> Instant {
        self.next_fire_time_with_clock(from, &SystemClock)
    }

    pub fn next_fire_time_with_clock(&self, from: Instant, clock: &dyn Clock) -> Instant {
        let reference_instant = clock.now_instant();
        let reference_epoch_ms = clock.now_epoch_ms();
        let from_epoch_ms =
            instant_to_epoch_ms_with_reference(from, reference_instant, reference_epoch_ms);
        let seconds = from_epoch_ms / 1_000;

        if let Some(candidate_secs) = self.simple_candidate_seconds(seconds) {
            return instant_from_epoch_seconds(candidate_secs, from, from_epoch_ms);
        }

        // Start from next minute (round up). We then jump between matching
        // month/day/hour/minute candidates instead of scanning minute-by-minute.
        let mut candidate_secs = ((seconds / 60) + 1) * 60;

        // Try up to 4 years worth of month/day transitions to handle edge cases
        // like "Feb 31" without falling back to per-minute scans.
        for _ in 0..(4 * 366 * 12) {
            let (year, month, day, hour, minute, _) = seconds_to_datetime(candidate_secs);

            let Some((target_month, wrapped_month)) = self.month_matcher.next_at_or_after(month)
            else {
                break;
            };
            if wrapped_month || target_month != month {
                let target_year = if wrapped_month {
                    year.saturating_add(1)
                } else {
                    year
                };
                candidate_secs = datetime_to_seconds(target_year, target_month, 1, 0, 0);
                continue;
            }

            let Some(target_day) = self.next_matching_day(year, month, day) else {
                let (next_year, next_month) = increment_month(year, month);
                candidate_secs = datetime_to_seconds(next_year, next_month, 1, 0, 0);
                continue;
            };
            if target_day != day {
                candidate_secs = datetime_to_seconds(year, month, target_day, 0, 0);
                continue;
            }

            let Some((target_hour, wrapped_hour)) = self.hour_matcher.next_at_or_after(hour) else {
                break;
            };
            if wrapped_hour {
                candidate_secs = datetime_to_seconds(year, month, day, 0, 0) + 86_400;
                continue;
            }
            if target_hour != hour {
                candidate_secs = datetime_to_seconds(year, month, day, target_hour, 0);
                continue;
            }

            let Some((target_minute, wrapped_minute)) =
                self.minute_matcher.next_at_or_after(minute)
            else {
                break;
            };
            if wrapped_minute {
                candidate_secs = datetime_to_seconds(year, month, day, hour, 0) + 3_600;
                continue;
            }
            if target_minute != minute {
                candidate_secs = datetime_to_seconds(year, month, day, hour, target_minute);
                continue;
            }

            return instant_from_epoch_seconds(candidate_secs, from, from_epoch_ms);
        }

        // Fallback: if no match found in 4 years, return 1 hour from now
        // This should never happen with valid cron expressions
        from + Duration::from_hours(1)
    }

    fn simple_candidate_seconds(&self, current_secs: u64) -> Option<u64> {
        if !self.month.is_any() || !self.day_of_week.is_any() {
            return None;
        }

        if !self.day_of_month.is_any() {
            return self.monthly_candidate_seconds(current_secs);
        }

        if self.hour.is_any() && self.minute.is_any() {
            return Some(next_minute_start_seconds(current_secs));
        }

        if self.hour.is_any() {
            let target_minute = u64::from(self.minute.as_single()?);
            let hour_start = current_secs - (current_secs % 3_600);
            let target = hour_start + (target_minute * 60);
            return Some(if target > current_secs {
                target
            } else {
                target + 3_600
            });
        }

        let target_hour = u64::from(self.hour.as_single()?);
        let target_minute = u64::from(self.minute.as_single()?);
        let day_start = current_secs - (current_secs % 86_400);
        let target = day_start + (target_hour * 3_600) + (target_minute * 60);

        Some(if target > current_secs {
            target
        } else {
            target + 86_400
        })
    }

    fn monthly_candidate_seconds(&self, current_secs: u64) -> Option<u64> {
        const MONTH_SEARCH_LIMIT: usize = 48;

        let target_day = self.day_of_month.as_single()?;
        let target_hour = self.hour.as_single()?;
        let target_minute = self.minute.as_single()?;
        let (mut year, mut month, current_day, _, _, _) = seconds_to_datetime(current_secs);
        let seconds_into_day = current_secs % 86_400;
        let current_day_start = current_secs - seconds_into_day;
        let mut month_start = current_day_start
            .saturating_sub(u64::from(current_day.saturating_sub(1)).saturating_mul(86_400));

        if let Some(candidate) = candidate_in_month(
            month_start,
            year,
            month,
            target_day,
            target_hour,
            target_minute,
        ) {
            if candidate > current_secs {
                return Some(candidate);
            }
        }

        for _ in 0..MONTH_SEARCH_LIMIT {
            month_start = month_start
                .saturating_add(u64::from(days_in_month(year, month)).saturating_mul(86_400));
            (year, month) = increment_month(year, month);
            if let Some(candidate) = candidate_in_month(
                month_start,
                year,
                month,
                target_day,
                target_hour,
                target_minute,
            ) {
                return Some(candidate);
            }
        }

        None
    }

    fn matches_day_of_month(&self, day: u32) -> bool {
        self.day_of_month_matcher.matches(day)
    }

    fn matches_day_of_week(&self, day_of_week: u32) -> bool {
        self.day_of_week_matcher.matches(day_of_week)
    }

    fn next_matching_day(&self, year: u32, month: u32, start_day: u32) -> Option<u32> {
        let end_day = days_in_month(year, month);
        for day in start_day..=end_day {
            let day_of_week = day_of_week_for_date(year, month, day);
            if self.matches_day_of_month(day) && self.matches_day_of_week(day_of_week) {
                return Some(day);
            }
        }
        None
    }
}

fn next_minute_start_seconds(current_secs: u64) -> u64 {
    current_secs
        .checked_div(60)
        .and_then(|minute| minute.checked_add(1))
        .and_then(|minute| minute.checked_mul(60))
        .unwrap_or(u64::MAX)
}

fn candidate_in_month(
    month_start: u64,
    year: u32,
    month: u32,
    target_day: u32,
    target_hour: u32,
    target_minute: u32,
) -> Option<u64> {
    if target_day > days_in_month(year, month) {
        return None;
    }

    Some(
        month_start
            .saturating_add(u64::from(target_day.saturating_sub(1)).saturating_mul(86_400))
            .saturating_add(u64::from(target_hour).saturating_mul(3_600))
            .saturating_add(u64::from(target_minute).saturating_mul(60)),
    )
}

fn instant_from_epoch_seconds(
    candidate_secs: u64,
    anchor_instant: Instant,
    anchor_epoch_ms: u64,
) -> Instant {
    epoch_ms_to_instant_with_reference(
        candidate_secs.saturating_mul(1_000),
        anchor_instant,
        anchor_epoch_ms,
    )
}

/// Check if a value matches a cron field
fn matches_field(field: &CronField, value: u32) -> bool {
    match field {
        CronField::Any => true,
        CronField::Single(n) => value == *n,
        CronField::Range(start, end) => value >= *start && value <= *end,
        CronField::List(values) => values.contains(&value),
        CronField::Step(base, step) => {
            if matches_field(base, value) {
                // Check if value is on a step boundary
                match base.as_ref() {
                    CronField::Any => value.is_multiple_of(*step),
                    CronField::Range(start, _) => {
                        (value >= *start) && value.saturating_sub(*start).is_multiple_of(*step)
                    }
                    _ => true,
                }
            } else {
                false
            }
        }
    }
}

/// Convert Unix timestamp (seconds) to calendar components
/// Returns (year, month (1-12), day (1-31), hour (0-23), minute (0-59), `day_of_week` (0-6, 0=Sunday))
fn seconds_to_datetime(seconds: u64) -> (u32, u32, u32, u32, u32, u32) {
    // Days since Unix epoch
    let mut days = i32::try_from(seconds / 86_400).unwrap_or(i32::MAX);

    // Calculate time of day
    let seconds_in_day = seconds % 86_400;
    let hour = u32::try_from(seconds_in_day / 3_600).unwrap_or(u32::MAX);
    let minute = u32::try_from((seconds_in_day % 3_600) / 60).unwrap_or(u32::MAX);

    // Day of week (1970-01-01 was Thursday, so add 4)
    let day_of_week = ((days + 4) % 7).cast_unsigned();

    // Calculate year
    let mut year = 1970;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if days < days_in_year {
            break;
        }
        days -= days_in_year;
        year += 1;
    }

    // Calculate month and day
    let days_in_month = [
        31,                                       // Jan
        if is_leap_year(year) { 29 } else { 28 }, // Feb
        31,                                       // Mar
        30,                                       // Apr
        31,                                       // May
        30,                                       // Jun
        31,                                       // Jul
        31,                                       // Aug
        30,                                       // Sep
        31,                                       // Oct
        30,                                       // Nov
        31,                                       // Dec
    ];

    let mut month = 1;
    for &days_in_this_month in &days_in_month {
        if days < days_in_this_month {
            break;
        }
        days -= days_in_this_month;
        month += 1;
    }

    let day = (days + 1).cast_unsigned();

    (year, month, day, hour, minute, day_of_week)
}

fn datetime_to_seconds(year: u32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
    let mut days = 0_u64;

    for current_year in 1970..year {
        days += u64::from(if is_leap_year(current_year) {
            366_u32
        } else {
            365_u32
        });
    }

    for current_month in 1..month {
        days += u64::from(days_in_month(year, current_month));
    }

    days += u64::from(day.saturating_sub(1));

    (days * 86_400) + (u64::from(hour) * 3_600) + (u64::from(minute) * 60)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        4 | 6 | 9 | 11 => 30,
        _ => unreachable!("month should be in 1..=12"),
    }
}

fn day_of_week_for_date(year: u32, month: u32, day: u32) -> u32 {
    let (_, _, _, _, _, day_of_week) =
        seconds_to_datetime(datetime_to_seconds(year, month, day, 0, 0));
    day_of_week
}

fn increment_month(year: u32, month: u32) -> (u32, u32) {
    if month == 12 {
        (year.saturating_add(1), 1)
    } else {
        (year, month + 1)
    }
}

fn is_leap_year(year: u32) -> bool {
    // Use `is_multiple_of` for clarity and to satisfy clippy
    (year.is_multiple_of(4) && !year.is_multiple_of(100)) || year.is_multiple_of(400)
}

fn parse_cron_field(field: &str, min: u32, max: u32) -> Result<CronField, String> {
    if field == "*" {
        return Ok(CronField::Any);
    }

    if let Ok(n) = field.parse::<u32>() {
        if n >= min && n <= max {
            return Ok(CronField::Single(n));
        }
        return Err(format!("Value {n} out of range [{min}, {max}]"));
    }

    if field.contains('-') {
        let parts: Vec<&str> = field.split('-').collect();
        if parts.len() == 2 {
            let start = parts[0].parse::<u32>().map_err(|e| e.to_string())?;
            let end = parts[1].parse::<u32>().map_err(|e| e.to_string())?;
            if start <= end && start >= min && end <= max {
                return Ok(CronField::Range(start, end));
            }
        }
        return Err("Invalid range format".to_string());
    }

    if field.contains(',') {
        let mut values = Vec::new();
        for part in field.split(',') {
            let v = part.parse::<u32>().map_err(|e| e.to_string())?;
            if v >= min && v <= max {
                values.push(v);
            } else {
                return Err(format!("Value {v} out of range [{min}, {max}]"));
            }
        }
        return Ok(CronField::List(values));
    }

    if field.contains('/') {
        let parts: Vec<&str> = field.split('/').collect();
        if parts.len() == 2 {
            let base = parse_cron_field(parts[0], min, max)?;
            let step = parts[1].parse::<u32>().map_err(|e| e.to_string())?;
            if step > 0 {
                return Ok(CronField::Step(Box::new(base), step));
            }
        }
        return Err("Invalid step format".to_string());
    }

    Err(format!("Unparseable cron field: {field}"))
}

#[cfg(test)]
mod tests {
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
}

/// Schedule errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// Invalid cron expression (7002)
    InvalidCron,
    /// Schedule not found (7001)
    NotFound,
}

impl ScheduleError {
    #[must_use]
    pub fn code(&self) -> u16 {
        match self {
            ScheduleError::InvalidCron => 7002,
            ScheduleError::NotFound => 7001,
        }
    }
}
