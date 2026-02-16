use std::time::Instant;
use bytes::Bytes;

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
        // Simplified: add 1 minute and find next match
        // Real implementation would walk through time checking cron fields
        // For now, return 1 minute from now (placeholder)
        from + std::time::Duration::from_secs(60)
    }
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
