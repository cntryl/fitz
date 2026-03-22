use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use std::sync::Arc;
use std::time::Instant;

/// Schedule operation messages
#[derive(Debug, Clone)]
pub enum ScheduleMessage {
    /// Create or update a schedule (route is identity, upsert)
    Create {
        route: String,
        cron: String,
        payload: Bytes,
    },
    /// Cancel an existing schedule by route
    Cancel { route: String },
    /// List all schedules (supports pagination)
    List {
        /// Starting offset (0-based). Default: 0
        offset: u64,
        /// Maximum number of entries to return (0 = all remaining). Default: 100
        limit: u64,
    },
    /// Subscribe to schedule fire notifications by pattern (client -> server)
    Subscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe from schedule fire notifications by pattern (client -> server)
    Unsubscribe {
        family_id: RouteFamily,
        pattern: Route,
        session_id: u64,
        subscriber: RouteAddress,
    },
    /// Unsubscribe all schedule subscriptions for a session (called on disconnect)
    UnsubscribeAll {
        session_id: u64,
        subscriber: RouteAddress,
    },
}

/// Response from schedule operations
#[derive(Debug, Clone)]
pub enum ScheduleResponse {
    /// Operation succeeded (no schedule_id returned - route is identity)
    Ok,
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
    pub payload: Bytes,
}

/// Schedule definition: cron timing, route identity, payload to fanout
#[derive(Debug, Clone)]
pub struct ScheduleDef {
    /// Route string (unique identity for this schedule)
    pub route: String,
    /// Cron expression (when to fire)
    pub cron: String,
    /// Parsed cron schedule (cached to avoid reparsing)
    pub parsed_cron: CronSchedule,
    /// Payload bytes to fanout to subscribers (what to send)
    pub payload: Bytes,
    /// Next fire time calculated from cron + current time
    pub next_fire_time: Instant,
    /// Exact next-fire timestamp used in the persisted storage key
    pub next_fire_ms: u64,
    /// Cached main storage key for the current next-fire timestamp.
    pub storage_key: Vec<u8>,
    /// Cached route index key for O(1) schedule lookups.
    pub index_key: Vec<u8>,
    /// Current index in the actor's mutable LIST backing store.
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
    /// Parse a 5-field cron expression (minute hour day month day_of_week)
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
    pub fn next_fire_time(&self, from: Instant) -> Instant {
        use std::time::{SystemTime, UNIX_EPOCH};

        // Get current system time as a reference point
        let now_instant = Instant::now();
        let now_sys = SystemTime::now();

        // Calculate system time corresponding to 'from' Instant
        // If 'from' is in the past relative to now_instant, subtract the difference
        // If 'from' is in the future, add the difference
        let from_sys = if from <= now_instant {
            let elapsed = now_instant - from;
            now_sys - elapsed
        } else {
            let ahead = from - now_instant;
            now_sys + ahead
        };

        // Get seconds since epoch for from_sys
        let seconds = from_sys
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

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

            let candidate_sys = UNIX_EPOCH + std::time::Duration::from_secs(candidate_secs);
            let duration_from_now_sys = candidate_sys
                .duration_since(now_sys)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0));
            return now_instant + duration_from_now_sys;
        }

        // Fallback: if no match found in 4 years, return 1 hour from now
        // This should never happen with valid cron expressions
        from + std::time::Duration::from_secs(3600)
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
/// Returns (year, month (1-12), day (1-31), hour (0-23), minute (0-59), day_of_week (0-6, 0=Sunday))
fn seconds_to_datetime(seconds: u64) -> (u32, u32, u32, u32, u32, u32) {
    // Days since Unix epoch
    let mut days = (seconds / 86400) as i32;

    // Calculate time of day
    let seconds_in_day = seconds % 86400;
    let hour = (seconds_in_day / 3600) as u32;
    let minute = ((seconds_in_day % 3600) / 60) as u32;

    // Day of week (1970-01-01 was Thursday, so add 4)
    let day_of_week = ((days + 4) % 7) as u32;

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

    let day = (days + 1) as u32;

    (year, month, day, hour, minute, day_of_week)
}

fn datetime_to_seconds(year: u32, month: u32, day: u32, hour: u32, minute: u32) -> u64 {
    let mut days = 0_u64;

    for current_year in 1970..year {
        days += if is_leap_year(current_year) { 366 } else { 365 } as u64;
    }

    for current_month in 1..month {
        days += days_in_month(year, current_month) as u64;
    }

    days += day.saturating_sub(1) as u64;

    (days * 86_400) + (hour as u64 * 3_600) + (minute as u64 * 60)
}

fn days_in_month(year: u32, month: u32) -> u32 {
    match month {
        1 => 31,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        3 => 31,
        4 => 30,
        5 => 31,
        6 => 30,
        7 => 31,
        8 => 31,
        9 => 30,
        10 => 31,
        11 => 30,
        12 => 31,
        _ => 31,
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
        return Err(format!("Value {} out of range [{}, {}]", n, min, max));
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
                return Err(format!("Value {} out of range [{}, {}]", v, min, max));
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

    Err(format!("Unparseable cron field: {}", field))
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let route = "schedule://acme/jobs/backup".to_string();
        let cron = "0 */6 * * *".to_string();
        let payload = Bytes::from("backup data");

        // Act
        let parsed_cron = CronSchedule::parse(&cron).expect("Valid cron");
        let def = ScheduleDef {
            route,
            cron,
            parsed_cron,
            payload,
            next_fire_time: Instant::now(),
            next_fire_ms: 0,
            storage_key: Vec::new(),
            index_key: Vec::new(),
            list_index: 0,
        };

        // Assert
        assert_eq!(def.route, "schedule://acme/jobs/backup");
    }
}

/// Schedule errors
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleError {
    /// Invalid cron expression (3040)
    InvalidCron,
    /// Schedule not found (3041)
    NotFound,
}

impl ScheduleError {
    pub fn code(&self) -> u16 {
        match self {
            ScheduleError::InvalidCron => 3040,
            ScheduleError::NotFound => 3041,
        }
    }
}
