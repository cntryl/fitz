use crate::runtime::routing::{route_exact_quad, route_scheme, Route, RouteAddress, RouteFamily};
use crate::runtime::ClientFrameMeta;
use bytes::Bytes;
use std::sync::Arc;
#[cfg(test)]
use std::time::Duration;
use std::time::Instant;

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
    /// Subscribe to live notifications for a schedule route pattern.
    Subscribe {
        family_id: RouteFamily,
        route: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Remove one live notification subscription for a schedule route pattern.
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
    pub message: Result<ScheduleMessage, ScheduleFailure>,
}

impl ScheduleClientRequest {
    pub fn new(meta: ClientFrameMeta, message: Result<ScheduleMessage, ScheduleFailure>) -> Self {
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
    pub route: String,
    pub payload: Bytes,
}

impl ScheduleClientNotification {
    pub fn new(
        session_id: u64,
        route_family: RouteFamily,
        subscription_id: u64,
        route: String,
        payload: Bytes,
    ) -> Self {
        Self {
            session_id,
            route_family,
            subscription_id,
            route,
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
    /// Operation failed with a stable wire category and actionable message.
    Error(ScheduleFailure),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleFailureCategory {
    NotFound,
    InvalidCron,
    Limit,
    Parse,
    InvalidTarget,
    InvalidSubscriptionPattern,
    SubscriptionLimit,
    InvalidDeliveryMode,
    Unauthorized,
}

impl ScheduleFailureCategory {
    #[must_use]
    pub const fn code(self) -> u16 {
        match self {
            Self::NotFound => 7001,
            Self::InvalidCron => 7002,
            Self::Limit => 7003,
            Self::Parse => 7004,
            Self::InvalidTarget => 7005,
            Self::InvalidSubscriptionPattern => 7006,
            Self::SubscriptionLimit => 7007,
            Self::InvalidDeliveryMode => 7008,
            Self::Unauthorized => 7009,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleFailure {
    pub category: ScheduleFailureCategory,
    pub message: String,
}

impl ScheduleFailure {
    #[must_use]
    pub fn new(category: ScheduleFailureCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn parse(message: impl Into<String>) -> Self {
        Self::new(ScheduleFailureCategory::Parse, message)
    }
}

impl std::fmt::Display for ScheduleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl From<String> for ScheduleFailure {
    fn from(message: String) -> Self {
        Self::parse(message)
    }
}

impl From<&str> for ScheduleFailure {
    fn from(message: &str) -> Self {
        Self::parse(message)
    }
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

        let schedule = CronSchedule {
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
        };
        if !schedule.is_satisfiable() {
            return Err("Cron expression has no possible fire time".to_string());
        }
        Ok(schedule)
    }

    /// Calculate next fire time from current time.
    ///
    /// # Panics
    ///
    /// Panics if a schedule not produced by [`Self::parse`] violates the
    /// validated-schedule invariant or the next occurrence exceeds the clock's
    /// representable range.
    #[must_use]
    pub fn next_fire_time(&self, from: Instant) -> Instant {
        self.try_next_fire_time(from)
            .expect("validated cron schedule must have a next fire time")
    }

    /// Calculate the next fire time, returning an error rather than fabricating
    /// an occurrence if the validated schedule invariant cannot be satisfied.
    ///
    /// # Errors
    ///
    /// Returns an error if no occurrence can be found across a full Gregorian
    /// calendar cycle.
    pub fn try_next_fire_time(&self, from: Instant) -> Result<Instant, String> {
        self.try_next_fire_time_with_clock(from, &SystemClock)
    }

    #[must_use]
    /// Calculate the next fire time using an injected clock.
    ///
    /// # Panics
    ///
    /// Panics if a schedule not produced by [`Self::parse`] violates the
    /// validated-schedule invariant or the next occurrence exceeds the clock's
    /// representable range.
    pub fn next_fire_time_with_clock(&self, from: Instant, clock: &dyn Clock) -> Instant {
        self.try_next_fire_time_with_clock(from, clock)
            .expect("validated cron schedule must have a next fire time")
    }

    /// Calculate the next fire time using an injected clock.
    ///
    /// # Errors
    ///
    /// Returns an error if no occurrence can be found across a full Gregorian
    /// calendar cycle.
    pub fn try_next_fire_time_with_clock(
        &self,
        from: Instant,
        clock: &dyn Clock,
    ) -> Result<Instant, String> {
        let reference_instant = clock.now_instant();
        let reference_epoch_ms = clock.now_epoch_ms();
        let from_epoch_ms =
            instant_to_epoch_ms_with_reference(from, reference_instant, reference_epoch_ms);
        let seconds = from_epoch_ms / 1_000;

        if let Some(candidate_secs) = self.simple_candidate_seconds(seconds) {
            if candidate_secs > seconds && candidate_secs != u64::MAX {
                return Ok(instant_from_epoch_seconds(
                    candidate_secs,
                    from,
                    from_epoch_ms,
                ));
            }
        }

        // Start from next minute (round up). We then jump between matching
        // month/day/hour/minute candidates instead of scanning minute-by-minute.
        let mut candidate_secs = seconds
            .checked_div(60)
            .and_then(|minute| minute.checked_add(1))
            .and_then(|minute| minute.checked_mul(60))
            .ok_or_else(|| "Cron next-fire calculation overflowed".to_string())?;
        let (start_year, _, _, _, _, _) = seconds_to_datetime(candidate_secs);
        let end_year = start_year.saturating_add(400);

        // A Gregorian calendar repeats every 400 years. The iteration cap is
        // defensive; normal paths jump by month, day, hour, or minute.
        for _ in 0..(401 * 366 * 4) {
            let (year, month, day, hour, minute, _) = seconds_to_datetime(candidate_secs);
            if year > end_year {
                break;
            }

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

            return Ok(instant_from_epoch_seconds(
                candidate_secs,
                from,
                from_epoch_ms,
            ));
        }

        Err("Cron schedule has no next fire time within a Gregorian cycle".to_string())
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
            if self.matches_day(day, day_of_week) {
                return Some(day);
            }
        }
        None
    }

    fn matches_day(&self, day: u32, day_of_week: u32) -> bool {
        match (self.day_of_month.is_any(), self.day_of_week.is_any()) {
            (true, true) => true,
            (true, false) => self.matches_day_of_week(day_of_week),
            (false, true) => self.matches_day_of_month(day),
            (false, false) => {
                self.matches_day_of_month(day) || self.matches_day_of_week(day_of_week)
            }
        }
    }

    fn is_satisfiable(&self) -> bool {
        (2000..2400).any(|year| {
            (1..=12).any(|month| {
                self.month_matcher.matches(month)
                    && (1..=days_in_month(year, month))
                        .any(|day| self.matches_day(day, day_of_week_for_date(year, month, day)))
            })
        })
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

    if field.contains(',') {
        let mut values = Vec::new();
        for component in field.split(',') {
            if component.is_empty() || component == "*" {
                return Err(format!("Invalid cron list: {field}"));
            }
            let parsed = parse_cron_component(component, min, max)?;
            for value in min..=max {
                if matches_field(&parsed, value) && !values.contains(&value) {
                    values.push(value);
                }
            }
        }
        values.sort_unstable();
        if values.is_empty() {
            return Err(format!("Cron list has no values: {field}"));
        }
        return Ok(CronField::List(values));
    }

    parse_cron_component(field, min, max)
}

fn parse_cron_component(component: &str, min: u32, max: u32) -> Result<CronField, String> {
    if let Some((base, step_text)) = component.split_once('/') {
        if base.is_empty() || step_text.is_empty() || step_text.contains('/') {
            return Err("Invalid step format".to_string());
        }
        let step = step_text
            .parse::<u32>()
            .map_err(|_| "Invalid step format".to_string())?;
        if step == 0 {
            return Err("Cron step must be greater than zero".to_string());
        }
        let base = if base == "*" {
            CronField::Range(min, max)
        } else if base.contains('-') {
            parse_cron_range(base, min, max)?
        } else {
            return Err("Cron steps require a wildcard or range base".to_string());
        };
        return Ok(CronField::Step(Box::new(base), step));
    }

    if component.contains('-') {
        return parse_cron_range(component, min, max);
    }

    let value = component
        .parse::<u32>()
        .map_err(|_| format!("Unparseable cron field: {component}"))?;
    if !(min..=max).contains(&value) {
        return Err(format!("Value {value} out of range [{min}, {max}]"));
    }
    Ok(CronField::Single(value))
}

fn parse_cron_range(range: &str, min: u32, max: u32) -> Result<CronField, String> {
    let Some((start, end)) = range.split_once('-') else {
        return Err("Invalid range format".to_string());
    };
    if start.is_empty() || end.is_empty() || end.contains('-') {
        return Err("Invalid range format".to_string());
    }
    let start = start
        .parse::<u32>()
        .map_err(|_| "Invalid range format".to_string())?;
    let end = end
        .parse::<u32>()
        .map_err(|_| "Invalid range format".to_string())?;
    if start > end || start < min || end > max {
        return Err(format!(
            "Cron range {start}-{end} is outside [{min}, {max}]"
        ));
    }
    Ok(CronField::Range(start, end))
}

#[cfg(test)]
#[path = "protocol_tests.rs"]
mod tests;
