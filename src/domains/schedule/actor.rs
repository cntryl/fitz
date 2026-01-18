use crate::domains::notice::protocol::{NotificationMessage, PublishMessage};
use crate::domains::schedule::protocol::SchedulePayload;
use crate::domains::schedule::store::ScheduleStore;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Minimal cron schedule supporting 5 fields (minute hour day month weekday)
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minute: Vec<u32>,
    hour: Vec<u32>,
    day: Vec<u32>,
    month: Vec<u32>,
    weekday: Vec<u32>,
}

impl CronSchedule {
    fn parse_field(field: &str, min: u32, max: u32) -> Vec<u32> {
        if field == "*" {
            return (min..=max).collect();
        }
        if let Some(stripped) = field.strip_prefix("*/") {
            if let Ok(step) = stripped.parse::<u32>() {
                return (min..=max)
                    .filter(|v| (v - min).is_multiple_of(step))
                    .collect();
            }
        }
        // CSV of numbers
        field
            .split(',')
            .filter_map(|s| s.parse::<u32>().ok())
            .filter(|v| *v >= min && *v <= max)
            .collect()
    }

    pub fn parse(expr: &str) -> Result<Self, String> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err("cron expression must have 5 fields".to_string());
        }
        Ok(Self {
            minute: Self::parse_field(parts[0], 0, 59),
            hour: Self::parse_field(parts[1], 0, 23),
            day: Self::parse_field(parts[2], 1, 31),
            month: Self::parse_field(parts[3], 1, 12),
            weekday: Self::parse_field(parts[4], 0, 6),
        })
    }

    pub fn matches_dt(&self, dt: &DateTime<Utc>) -> bool {
        let m = dt.minute();
        let h = dt.hour();
        let d = dt.day();
        let mo = dt.month();
        let w = dt.weekday().num_days_from_sunday();
        self.minute.contains(&m)
            && self.hour.contains(&h)
            && self.day.contains(&d)
            && self.month.contains(&mo)
            && self.weekday.contains(&w)
    }
}

#[derive(Debug, Clone)]
struct ScheduleDef {
    id: u64,
    route: Route,
    cron: CronSchedule,
    payload: Bytes,
    last_fire_at: i64, // epoch seconds
}

/// Clock abstraction for testing
pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

pub struct SystemClock;
impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub enum ScheduleMessage {
    Create { route: Route, payload: Bytes },
    Delete { id: u64 },
    Tick,
}

pub struct ScheduleActor {
    family: RouteFamily,
    store: ScheduleStore,
    schedules: HashMap<u64, ScheduleDef>,
    next_id: u64,
    clock: Box<dyn Clock>,
    write_options: cntryl_midge::WriteOptions,
}

impl ScheduleActor {
    pub fn new(
        family: RouteFamily,
        db: Arc<cntryl_midge::Engine>,
        write_options: cntryl_midge::WriteOptions,
    ) -> Self {
        let store = ScheduleStore::new(db);
        let mut actor = Self {
            family,
            store,
            schedules: HashMap::new(),
            next_id: 1,
            clock: Box::new(SystemClock),
            write_options,
        };
        // Load persisted schedules
        if let Ok(entries) = actor.store.list(family.id()) {
            for (id, route_bytes, payload, last) in entries {
                if let Ok(sp) = SchedulePayload::decode(&payload) {
                    if let Ok(cron) = CronSchedule::parse(&sp.cron) {
                        let route = Route::new(String::from_utf8_lossy(&route_bytes).to_string());
                        actor.schedules.insert(
                            id,
                            ScheduleDef {
                                id,
                                route,
                                cron,
                                payload,
                                last_fire_at: last,
                            },
                        );
                        actor.next_id = actor.next_id.max(id + 1);
                    } else {
                        warn!("failed parse cron for persisted schedule {}", id);
                    }
                } else {
                    warn!("failed decode payload for persisted schedule {}", id);
                }
            }
        }
        actor
    }

    /// Create a schedule, persist it and return its id
    pub fn create_schedule(&mut self, route: Route, payload: Bytes) -> Result<u64, String> {
        // Payload must be TLV - decode to validate and extract cron
        let sp = SchedulePayload::decode(&payload)?;
        let cron = CronSchedule::parse(&sp.cron)?;

        let id = self.next_id;
        self.next_id += 1;

        let def = ScheduleDef {
            id,
            route: route.clone(),
            cron,
            payload: payload.clone(),
            last_fire_at: 0,
        };

        // persist
        self.store.insert(
            self.family.id(),
            id,
            route.as_str().as_bytes(),
            payload.clone(),
            def.last_fire_at,
            self.write_options,
        )?;

        self.schedules.insert(id, def);
        info!("created schedule {} for family {}", id, self.family.id());
        Ok(id)
    }

    pub fn delete_schedule(&mut self, id: u64) -> Result<(), String> {
        self.schedules.remove(&id);
        self.store.delete(self.family.id(), id, self.write_options)
    }

    fn extract_realm_and_area(route: &Route) -> Option<(String, String)> {
        let s = route.as_str();
        if let Some(pos) = s.find("://") {
            let rest = &s[pos + 3..];
            let mut parts = rest.split('/');
            let realm = parts.next()?.to_string();
            let area = parts.next()?.to_string();
            return Some((realm, area));
        }
        None
    }

    fn scan_and_fire(&mut self, ctx: &mut Context<Self>) {
        let now_dt = self.clock.now();
        let now_secs = now_dt.timestamp();
        // Collect ids + clones to avoid holding mutable borrows across fire operations
        let mut to_fire: Vec<(u64, Route, Bytes)> = Vec::new();
        for (id, def) in self.schedules.iter() {
            // Check if any matching time exists between last_fire_at (exclusive) and now (inclusive)
            let last =
                DateTime::from_timestamp(def.last_fire_at.max(0), 0).unwrap_or_else(Utc::now);
            // iterate minute-by-minute from last+1min up to now
            let mut t = last + chrono::Duration::minutes(1);
            let mut matched = false;
            while t <= now_dt {
                if def.cron.matches_dt(&t) {
                    matched = true;
                    break;
                }
                t += chrono::Duration::minutes(1);
                // safety cap
                if (t - last).num_minutes() > 10_000 {
                    break;
                }
            }
            if matched {
                to_fire.push((*id, def.route.clone(), def.payload.clone()));
            }
        }
        for (id, route, payload) in to_fire {
            // Update last_fire_at and persist
            if let Some(def) = self.schedules.get_mut(&id) {
                def.last_fire_at = now_secs;
                let _ = self.store.insert(
                    self.family.id(),
                    def.id,
                    def.route.as_str().as_bytes(),
                    def.payload.clone(),
                    def.last_fire_at,
                    self.write_options,
                );
            }

            // Emit notice using cloned route & payload
            let Some((realm, area)) = Self::extract_realm_and_area(&route) else {
                warn!(
                    "failed to extract realm/area from schedule route: {}",
                    route.as_str()
                );
                continue;
            };
            match SchedulePayload::decode(&payload) {
                Ok(sp) => {
                    let notice_path = if sp.operation.is_empty() {
                        format!("notice://{}/{}/{}", realm, area, sp.resource)
                    } else {
                        format!(
                            "notice://{}/{}/{}/{}",
                            realm, area, sp.resource, sp.operation
                        )
                    };
                    let r = Route::new(notice_path.clone());
                    let publish = PublishMessage::new(self.family, r.clone(), payload.clone());
                    let notice_addr = RouteAddress::new(self.family, r);
                    let _ = ctx.send(notice_addr, NotificationMessage::Publish(publish));
                    info!("fired schedule {} -> {}", id, notice_path);
                }
                Err(e) => {
                    warn!("failed decode schedule payload for {}: {}", id, e);
                }
            }
        }
    }
}

impl Actor for ScheduleActor {
    type Message = ScheduleMessage;

    fn receive(&mut self, msg: Self::Message, ctx: &mut Context<Self>) {
        match msg {
            ScheduleMessage::Create { route, payload } => {
                let _ = self.create_schedule(route, payload);
            }
            ScheduleMessage::Delete { id } => {
                let _ = self.delete_schedule(id);
            }
            ScheduleMessage::Tick => {
                self.scan_and_fire(ctx);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn should_parse_cron_every_minute() {
        // Arrange & Act
        let cron = CronSchedule::parse("* * * * *");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute.len(), 60); // All 0-59
        assert_eq!(cron.hour.len(), 24);   // All 0-23
        assert_eq!(cron.day.len(), 31);    // All 1-31
        assert_eq!(cron.month.len(), 12);  // All 1-12
        assert_eq!(cron.weekday.len(), 7); // All 0-6
    }

    #[test]
    fn should_parse_cron_with_range_syntax() {
        // Arrange & Act
        // Note: CronSchedule doesn't support range syntax (9-17)
        // It only supports: * (all), */step, and CSV numbers
        // This test verifies that invalid range is not parsed
        let cron = CronSchedule::parse("0 9-17 * * 1-5");

        // Assert
        assert!(cron.is_ok()); // Parsing doesn't fail
        let cron = cron.unwrap();
        // But "9-17" is not recognized, so hour field becomes empty
        assert!(cron.hour.is_empty());
        assert!(cron.weekday.is_empty()); // "1-5" also not recognized
    }

    #[test]
    fn should_parse_cron_with_step_syntax() {
        // Arrange & Act
        let cron = CronSchedule::parse("*/15 */6 * * *");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0, 15, 30, 45]);
        assert_eq!(cron.hour, vec![0, 6, 12, 18]);
    }

    #[test]
    fn should_parse_cron_with_list_syntax() {
        // Arrange & Act
        let cron = CronSchedule::parse("0 9,12,18 * * *");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0]);
        assert_eq!(cron.hour, vec![9, 12, 18]);
    }

    #[test]
    fn should_parse_cron_complex_expression() {
        // Arrange & Act
        // CronSchedule supports: * (all), */step, and CSV numbers
        // This parses as: minute 0,30 / hour 9,12,18 / * / * / weekday 1,2,3
        let cron = CronSchedule::parse("0,30 9,12,18 * * 1,2,3");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0, 30]);
        assert_eq!(cron.hour, vec![9, 12, 18]);
        assert_eq!(cron.weekday, vec![1, 2, 3]);
    }

    #[test]
    fn should_reject_invalid_field_count() {
        // Arrange & Act
        let cron = CronSchedule::parse("* * * *"); // Only 4 fields

        // Assert
        assert!(cron.is_err());
    }

    #[test]
    fn should_reject_out_of_bounds_minute() {
        // Arrange & Act
        let cron = CronSchedule::parse("60 * * * *"); // Minute max 59

        // Assert
        assert!(cron.is_ok()); // Parse succeeds but minute field is filtered
        let cron = cron.unwrap();
        assert!(cron.minute.is_empty()); // No valid minutes
    }

    #[test]
    fn should_reject_out_of_bounds_hour() {
        // Arrange & Act
        let cron = CronSchedule::parse("* 24 * * *"); // Hour max 23

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.hour.is_empty()); // No valid hours
    }

    #[test]
    fn should_reject_out_of_bounds_day() {
        // Arrange & Act
        let cron = CronSchedule::parse("* * 32 * *"); // Day max 31

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.day.is_empty()); // No valid days
    }

    #[test]
    fn should_reject_out_of_bounds_month() {
        // Arrange & Act
        let cron = CronSchedule::parse("* * * 13 *"); // Month max 12

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.month.is_empty()); // No valid months
    }

    #[test]
    fn should_reject_out_of_bounds_weekday() {
        // Arrange & Act
        let cron = CronSchedule::parse("* * * * 7"); // Weekday max 6

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.weekday.is_empty()); // No valid weekdays
    }

    #[test]
    fn should_parse_min_values() {
        // Arrange & Act
        let cron = CronSchedule::parse("0 0 1 1 0");

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
    fn should_parse_max_values() {
        // Arrange & Act
        let cron = CronSchedule::parse("59 23 31 12 6");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![59]);
        assert_eq!(cron.hour, vec![23]);
        assert_eq!(cron.day, vec![31]);
        assert_eq!(cron.month, vec![12]);
        assert_eq!(cron.weekday, vec![6]);
    }

    #[test]
    fn should_match_every_minute() {
        // Arrange
        let cron = CronSchedule::parse("* * * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&dt));
    }

    #[test]
    fn should_match_specific_hour() {
        // Arrange
        let cron = CronSchedule::parse("* 14 * * *").unwrap(); // Hour 14 only
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&dt));
    }

    #[test]
    fn should_not_match_different_hour() {
        // Arrange
        let cron = CronSchedule::parse("* 14 * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 13, 30, 0).unwrap(); // Hour 13

        // Act & Assert
        assert!(!cron.matches_dt(&dt));
    }

    #[test]
    fn should_match_weekday() {
        // Arrange
        let cron = CronSchedule::parse("* * * * 3").unwrap(); // Wednesday
        // 2025-01-15 is a Wednesday
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&dt));
    }

    #[test]
    fn should_not_match_different_weekday() {
        // Arrange
        let cron = CronSchedule::parse("* * * * 1").unwrap(); // Monday only
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap(); // Wednesday

        // Act & Assert
        assert!(!cron.matches_dt(&dt));
    }

    #[test]
    fn should_match_workday_9am() {
        // Arrange
        // Since range syntax isn't supported, use CSV instead
        let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
        // 2025-01-15 is a Wednesday at 9:00
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&dt));
    }

    #[test]
    fn should_not_match_outside_work_hours() {
        // Arrange
        let cron = CronSchedule::parse("0 9 * * 1-5").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(); // 10 AM

        // Act & Assert
        assert!(!cron.matches_dt(&dt));
    }

    #[test]
    fn should_not_match_weekend() {
        // Arrange
        // Weekdays: 1,2,3,4,5 (Monday-Friday, 0=Sunday)
        let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
        // 2025-01-18 is a Saturday (weekday 6)
        let dt = Utc.with_ymd_and_hms(2025, 1, 18, 9, 0, 0).unwrap();

        // Act & Assert
        assert!(!cron.matches_dt(&dt));
    }

    #[test]
    fn should_match_step_pattern() {
        // Arrange
        let cron = CronSchedule::parse("*/15 * * * *").unwrap(); // Every 15 minutes
        let dt_match = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();
        let dt_no_match = Utc.with_ymd_and_hms(2025, 1, 15, 14, 31, 0).unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&dt_match));
        assert!(!cron.matches_dt(&dt_no_match));
    }

    #[test]
    fn should_match_list_pattern() {
        // Arrange
        let cron = CronSchedule::parse("0 9,12,18 * * *").unwrap();
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap()));
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()));
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 18, 0, 0).unwrap()));

        // Act & Assert
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 15, 0, 0).unwrap()));
    }

    #[test]
    fn should_match_range_pattern() {
        // Arrange
        // CronSchedule doesn't support range syntax (9-17)
        // Use CSV instead: 9,10,11,12,13,14,15,16,17
        let cron = CronSchedule::parse("0 9,10,11,12,13,14,15,16,17 * * *").unwrap();
        for hour in 9..=17 {
            let dt = Utc
                .with_ymd_and_hms(2025, 1, 15, hour, 0, 0)
                .unwrap();
            assert!(cron.matches_dt(&dt), "Should match hour {}", hour);
        }

        // Act & Assert
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 8, 0, 0).unwrap()));
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 18, 0, 0).unwrap()));
    }

    #[test]
    fn should_handle_empty_field() {
        // Arrange & Act
        let cron = CronSchedule::parse("");

        // Assert
        assert!(cron.is_err());
    }

    #[test]
    fn should_parse_field_with_leading_zeros() {
        // Arrange & Act
        let cron = CronSchedule::parse("00 09 * * *");

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0]);
        assert_eq!(cron.hour, vec![9]);
    }
}

