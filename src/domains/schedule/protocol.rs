use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
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
    /// List all schedules
    List,
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
    /// LIST operation: returns all schedules as (route, cron, payload) tuples
    ListDefs(Vec<ScheduleListEntry>),
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
    /// Payload bytes to fanout to subscribers (what to send)
    pub payload: Bytes,
    /// Next fire time calculated from cron + current time
    pub next_fire_time: Instant,
}

/// Parses and validates cron expressions
#[derive(Debug, Clone)]
pub struct CronSchedule {
    pub minute: CronField,
    pub hour: CronField,
    pub day_of_month: CronField,
    pub month: CronField,
    pub day_of_week: CronField,
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

impl CronSchedule {
    /// Parse a 5-field cron expression (minute hour day month day_of_week)
    pub fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err("Cron expression must have exactly 5 fields".to_string());
        }

        Ok(CronSchedule {
            minute: parse_cron_field(fields[0], 0, 59)?,
            hour: parse_cron_field(fields[1], 0, 23)?,
            day_of_month: parse_cron_field(fields[2], 1, 31)?,
            month: parse_cron_field(fields[3], 1, 12)?,
            day_of_week: parse_cron_field(fields[4], 0, 6)?,
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

        // Start from next minute (round up)
        let mut candidate_secs = ((seconds / 60) + 1) * 60;

        // Try up to 4 years worth of minutes (to handle edge cases like "Feb 31")
        for _ in 0..(4 * 365 * 24 * 60) {
            let candidate_sys = UNIX_EPOCH + std::time::Duration::from_secs(candidate_secs);

            // Convert to calendar time (naive UTC)
            let (_year, month, day, hour, minute, day_of_week) =
                seconds_to_datetime(candidate_secs);

            // Check if this time matches all cron fields
            if self.matches_minute(minute)
                && self.matches_hour(hour)
                && self.matches_day_of_month(day)
                && self.matches_month(month)
                && self.matches_day_of_week(day_of_week)
            {
                // Found matching time - convert back to Instant
                // Calculate offset from now_sys to candidate_sys
                let duration_from_now_sys =
                    candidate_sys.duration_since(now_sys).unwrap_or_else(|_| {
                        // If candidate is in the past, treat as 0 (shouldn't happen)
                        std::time::Duration::from_secs(0)
                    });
                return now_instant + duration_from_now_sys;
            }

            // Try next minute
            candidate_secs += 60;
        }

        // Fallback: if no match found in 4 years, return 1 hour from now
        // This should never happen with valid cron expressions
        from + std::time::Duration::from_secs(3600)
    }

    fn matches_minute(&self, minute: u32) -> bool {
        matches_field(&self.minute, minute)
    }

    fn matches_hour(&self, hour: u32) -> bool {
        matches_field(&self.hour, hour)
    }

    fn matches_day_of_month(&self, day: u32) -> bool {
        matches_field(&self.day_of_month, day)
    }

    fn matches_month(&self, month: u32) -> bool {
        matches_field(&self.month, month)
    }

    fn matches_day_of_week(&self, day_of_week: u32) -> bool {
        matches_field(&self.day_of_week, day_of_week)
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
        let def = ScheduleDef {
            route,
            cron,
            payload,
            next_fire_time: Instant::now(),
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
