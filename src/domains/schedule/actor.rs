use crate::domains::schedule::protocol::SchedulePayload;
use crate::domains::schedule::store::ScheduleStore;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::domain_event::DomainPublishEvent;
use crate::runtime::routing::{Route, RouteFamily};
use bytes::Bytes;
use chrono::{DateTime, Datelike, Timelike, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Minimal cron schedule supporting 5 fields (minute hour day month weekday)
#[derive(Debug, Clone)]
pub struct CronSchedule {
    pub minute: Vec<u32>,
    pub hour: Vec<u32>,
    pub day: Vec<u32>,
    pub month: Vec<u32>,
    pub weekday: Vec<u32>,
}

impl CronSchedule {
    fn parse_field(field: &str, min: u32, max: u32) -> Vec<u32> {
        if field == "*" {
            return (min..=max).collect();
        }

        // Handle step syntax: */n
        if let Some(stripped) = field.strip_prefix("*/") {
            if let Ok(step) = stripped.parse::<u32>() {
                return (min..=max)
                    .filter(|v| (v - min).is_multiple_of(step) || *v == min)
                    .collect();
            }
        }

        // Handle comma-separated values with possible ranges
        let mut result = Vec::new();
        for part in field.split(',') {
            let part = part.trim();
            if let Some(dash_pos) = part.find('-') {
                // Range syntax: a-b
                let start_str = &part[..dash_pos];
                let end_str = &part[dash_pos + 1..];
                if let (Ok(start), Ok(end)) = (start_str.parse::<u32>(), end_str.parse::<u32>()) {
                    let range_start = start.max(min);
                    let range_end = end.min(max);
                    if range_start <= range_end {
                        for v in range_start..=range_end {
                            if !result.contains(&v) {
                                result.push(v);
                            }
                        }
                    }
                }
            } else if let Ok(num) = part.parse::<u32>() {
                // Single number
                if num >= min && num <= max && !result.contains(&num) {
                    result.push(num);
                }
            }
        }

        result.sort_unstable();
        result
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

    /// Compute next fire time after `from`.
    /// Scans forward up to 24 hours looking for a matching minute.
    /// Returns 24 hours in future if no match found.
    pub fn next_fire_after(&self, from: DateTime<Utc>) -> DateTime<Utc> {
        let mut candidate = from.with_second(0).unwrap() + chrono::Duration::minutes(1);
        for _ in 0..1440 {
            // 24 hours in minutes
            if self.matches_dt(&candidate) {
                return candidate;
            }
            candidate += chrono::Duration::minutes(1);
        }
        // Never matches: return far future
        from + chrono::Duration::days(1)
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ScheduleDef {
    id: u64,
    route: Route,
    cron: CronSchedule,
    payload: Bytes,
    next_fire_time: DateTime<Utc>, // Indexed for windowed scanning
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
        if let Ok(entries) = actor.store.list(family.as_u64()) {
            let now = actor.clock.now();
            for (id, route_bytes, payload) in entries {
                if let Ok(sp) = SchedulePayload::decode(&payload) {
                    if let Ok(cron) = CronSchedule::parse(&sp.cron) {
                        let route = Route::new(String::from_utf8_lossy(&route_bytes).to_string());
                        // Recompute next_fire_time from current cron
                        let next_fire = cron.next_fire_after(now);
                        actor.schedules.insert(
                            id,
                            ScheduleDef {
                                id,
                                route,
                                cron,
                                payload,
                                next_fire_time: next_fire,
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

        // Compute next fire time after now
        let now = self.clock.now();
        let next_fire = cron.next_fire_after(now);

        let def = ScheduleDef {
            id,
            route: route.clone(),
            cron,
            payload: payload.clone(),
            next_fire_time: next_fire,
        };

        // persist with index
        self.store.insert(
            self.family.as_u64(),
            id,
            route.as_str().as_bytes(),
            payload.clone(),
            next_fire,
            self.write_options,
        )?;

        self.schedules.insert(id, def);
        info!(
            "created schedule {} for family {} (next_fire: {})",
            id,
            self.family.as_u64(),
            next_fire
        );
        Ok(id)
    }

    pub fn delete_schedule(&mut self, id: u64) -> Result<(), String> {
        self.schedules.remove(&id);
        self.store
            .delete(self.family.as_u64(), id, self.write_options)
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

        // Window parameters: only scan schedules whose next_fire_time falls within this window
        // grace_period: re-check recent schedules (handles skipped ticks)
        // lookahead: pre-scan upcoming schedules
        const GRACE_PERIOD: i64 = 2; // seconds
        const LOOKAHEAD: i64 = 5; // seconds

        let window_start = now_dt - chrono::Duration::seconds(GRACE_PERIOD);
        let window_end = now_dt + chrono::Duration::seconds(LOOKAHEAD);

        // WINDOWED SCAN: only load schedules due in [window_start, window_end]
        // This is O(due_count), not O(total_count)
        let due_ids = match self
            .store
            .scan_window(self.family.as_u64(), window_start, window_end)
        {
            Ok(ids) => ids,
            Err(e) => {
                warn!("failed to scan schedule window: {}", e);
                return;
            }
        };

        let mut index_updates = Vec::new();

        // Dispatch notices for all due schedules
        for schedule_id in due_ids {
            let Some(def) = self.schedules.get(&schedule_id) else {
                warn!("schedule {} in index but not in memory", schedule_id);
                continue;
            };

            // Check if cron actually matches NOW (window scan is fuzzy due to buckets)
            if !def.cron.matches_dt(&now_dt) {
                continue; // Not actually due yet
            }

            // Emit events (best-effort, non-blocking, OUTSIDE transaction)
            // Two events are emitted:
            // 1. schedule:// route -> for own subscribers (SCHEDULE_NOTIFY)
            // 2. target resource route -> for cross-domain execution (e.g. notice://)
            match SchedulePayload::decode(&def.payload) {
                Ok(sp) => {
                    let realm_area = Self::extract_realm_and_area(&def.route);
                    let Some((realm, area)) = realm_area else {
                        warn!(
                            "failed to extract realm/area from schedule route: {}",
                            def.route.as_str()
                        );
                        continue;
                    };

                    // 1. Emit to own schedule subscribers (schedule:// route)
                    let schedule_fire_route = Route::new(format!(
                        "schedule://{}/{}/{}/fired",
                        realm, area, sp.target_resource
                    ));
                    let own_event = DomainPublishEvent::new(
                        self.family,
                        schedule_fire_route,
                        def.payload.clone(),
                    );
                    let _ = ctx.publish_event(own_event);

                    // 2. Execute target resource (cross-domain via DomainPublishEvent)
                    let target_path = if sp.target_operation.is_empty() {
                        format!("notice://{}/{}/{}", realm, area, sp.target_resource)
                    } else {
                        format!(
                            "notice://{}/{}/{}/{}",
                            realm, area, sp.target_resource, sp.target_operation
                        )
                    };

                    let target_route = Route::new(target_path.clone());
                    let exec_event = DomainPublishEvent::new(
                        self.family,
                        target_route,
                        def.payload.clone(),
                    );
                    let _ = ctx.publish_event(exec_event);
                    info!(
                        "schedule {} fired -> schedule subscribers + target at {}",
                        schedule_id, target_path
                    );

                    // Compute next fire time AFTER dispatching events
                    let next_fire = def.cron.next_fire_after(now_dt);

                    // Queue index update: will batch persist after all dispatches
                    index_updates.push((schedule_id, def.next_fire_time, next_fire));

                    // Update in-memory next_fire_time
                    // Safe because we're not in a transaction
                    if let Some(def_mut) = self.schedules.get_mut(&schedule_id) {
                        def_mut.next_fire_time = next_fire;
                    }
                }
                Err(e) => {
                    warn!("failed decode schedule payload for {}: {}", schedule_id, e);
                }
            }
        }

        // BATCHED INDEX UPDATE: persist all next_fire_time changes atomically
        // Crash after dispatch but before this commit = duplicate event (acceptable)
        // Crash before dispatch = missed event (would happen next matching time)
        if !index_updates.is_empty() {
            if let Err(e) = self.store.batch_update_index(
                self.family.as_u64(),
                index_updates,
                self.write_options,
            ) {
                warn!("failed to batch update schedule index: {}", e);
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
        // Arrange
        let expr = "* * * * *";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute.len(), 60); // All 0-59
        assert_eq!(cron.hour.len(), 24); // All 0-23
        assert_eq!(cron.day.len(), 31); // All 1-31
        assert_eq!(cron.month.len(), 12); // All 1-12
        assert_eq!(cron.weekday.len(), 7); // All 0-6
    }

    #[test]
    fn should_parse_cron_with_range_syntax() {
        // Arrange
        let expr = "0 9-17 * * 1-5";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        // Now range syntax IS supported
        assert_eq!(cron.minute, vec![0]);
        assert_eq!(cron.hour, vec![9, 10, 11, 12, 13, 14, 15, 16, 17]);
        assert_eq!(cron.weekday, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn should_parse_cron_with_step_syntax() {
        // Arrange
        let expr = "*/15 */6 * * *";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        // */15 for minutes: 0, 15, 30, 45
        assert_eq!(cron.minute, vec![0, 15, 30, 45]);
        // */6 for hours: 0, 6, 12, 18
        assert_eq!(cron.hour, vec![0, 6, 12, 18]);
    }

    #[test]
    fn should_parse_cron_with_list_syntax() {
        // Arrange
        let expr = "0 9,12,18 * * *";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0]);
        assert_eq!(cron.hour, vec![9, 12, 18]);
    }

    #[test]
    fn should_parse_cron_complex_expression() {
        // Arrange
        let expr = "0,30 9,12,18 * * 1,2,3";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0, 30]);
        assert_eq!(cron.hour, vec![9, 12, 18]);
        assert_eq!(cron.weekday, vec![1, 2, 3]);
    }

    #[test]
    fn should_reject_invalid_field_count() {
        // Arrange
        let expr = "* * * *"; // Only 4 fields

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_err());
    }

    #[test]
    fn should_reject_out_of_bounds_minute() {
        // Arrange
        let expr = "60 * * * *"; // Minute max 59

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok()); // Parse succeeds but minute field is filtered
        let cron = cron.unwrap();
        assert!(cron.minute.is_empty()); // No valid minutes
    }

    #[test]
    fn should_reject_out_of_bounds_hour() {
        // Arrange
        let expr = "* 24 * * *"; // Hour max 23

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.hour.is_empty()); // No valid hours
    }

    #[test]
    fn should_reject_out_of_bounds_day() {
        // Arrange
        let expr = "* * 32 * *"; // Day max 31

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.day.is_empty()); // No valid days
    }

    #[test]
    fn should_reject_out_of_bounds_month() {
        // Arrange
        let expr = "* * * 13 *"; // Month max 12

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.month.is_empty()); // No valid months
    }

    #[test]
    fn should_reject_out_of_bounds_weekday() {
        // Arrange
        let expr = "* * * * 7"; // Weekday max 6

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert!(cron.weekday.is_empty()); // No valid weekdays
    }

    #[test]
    fn should_parse_min_values() {
        // Arrange
        let expr = "0 0 1 1 0";

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
    fn should_parse_max_values() {
        // Arrange
        let expr = "59 23 31 12 6";

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

    #[test]
    fn should_match_every_minute() {
        // Arrange
        let cron = CronSchedule::parse("* * * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_match_specific_hour() {
        // Arrange
        let cron = CronSchedule::parse("* 14 * * *").unwrap(); // Hour 14 only
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_different_hour() {
        // Arrange
        let cron = CronSchedule::parse("* 14 * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 13, 30, 0).unwrap(); // Hour 13

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_weekday() {
        // Arrange
        let cron = CronSchedule::parse("* * * * 3").unwrap(); // Wednesday
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap(); // 2025-01-15 is Wednesday

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_different_weekday() {
        // Arrange
        let cron = CronSchedule::parse("* * * * 1").unwrap(); // Monday only
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap(); // Wednesday

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_workday_9am() {
        // Arrange
        let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap(); // Wednesday at 9:00

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(result);
    }

    #[test]
    fn should_not_match_outside_work_hours() {
        // Arrange
        let cron = CronSchedule::parse("0 9 * * 1-5").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 15, 10, 0, 0).unwrap(); // 10 AM

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_not_match_weekend() {
        // Arrange
        let cron = CronSchedule::parse("0 9 * * 1,2,3,4,5").unwrap();
        let dt = Utc.with_ymd_and_hms(2025, 1, 18, 9, 0, 0).unwrap(); // Saturday

        // Act
        let result = cron.matches_dt(&dt);

        // Assert
        assert!(!result);
    }

    #[test]
    fn should_match_step_pattern() {
        // Arrange
        let cron = CronSchedule::parse("*/15 * * * *").unwrap(); // Every 15 minutes
        let dt_match = Utc.with_ymd_and_hms(2025, 1, 15, 14, 30, 0).unwrap();
        let dt_no_match = Utc.with_ymd_and_hms(2025, 1, 15, 14, 31, 0).unwrap();

        // Act
        let result_match = cron.matches_dt(&dt_match);
        let result_no_match = cron.matches_dt(&dt_no_match);

        // Assert
        assert!(result_match);
        assert!(!result_no_match);
    }

    #[test]
    fn should_match_list_pattern() {
        // Arrange
        let cron = CronSchedule::parse("0 9,12,18 * * *").unwrap();

        // Act & Assert
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 9, 0, 0).unwrap()));
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 12, 0, 0).unwrap()));
        assert!(cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 18, 0, 0).unwrap()));
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 15, 0, 0).unwrap()));
    }

    #[test]
    fn should_match_range_pattern() {
        // Arrange
        let cron = CronSchedule::parse("0 9,10,11,12,13,14,15,16,17 * * *").unwrap();

        // Act & Assert
        for hour in 9..=17 {
            let dt = Utc.with_ymd_and_hms(2025, 1, 15, hour, 0, 0).unwrap();
            assert!(cron.matches_dt(&dt), "Should match hour {}", hour);
        }
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 8, 0, 0).unwrap()));
        assert!(!cron.matches_dt(&Utc.with_ymd_and_hms(2025, 1, 15, 18, 0, 0).unwrap()));
    }

    #[test]
    fn should_handle_empty_field() {
        // Arrange
        let expr = "";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_err());
    }

    #[test]
    fn should_parse_field_with_leading_zeros() {
        // Arrange
        let expr = "00 09 * * *";

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0]);
        assert_eq!(cron.hour, vec![9]);
    }

    #[test]
    fn should_parse_range_with_single_value() {
        // Arrange
        let expr = "5-5 * * * *"; // Range of single value

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![5]);
    }

    #[test]
    fn should_parse_mixed_csv_and_ranges() {
        // Arrange
        let expr = "0,15-20,30 * * * *"; // 0, 15-20, 30

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        assert_eq!(cron.minute, vec![0, 15, 16, 17, 18, 19, 20, 30]);
    }

    #[test]
    fn should_reject_invalid_range() {
        // Arrange
        let expr = "50-60 * * * *"; // Range extends beyond max

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        // Should parse only valid portion: 50-59
        assert_eq!(cron.minute, vec![50, 51, 52, 53, 54, 55, 56, 57, 58, 59]);
    }

    #[test]
    fn should_reject_reversed_range() {
        // Arrange
        let expr = "50-10 * * * *"; // Invalid: end < start

        // Act
        let cron = CronSchedule::parse(expr);

        // Assert
        assert!(cron.is_ok());
        let cron = cron.unwrap();
        // Reversed range results in empty
        assert!(cron.minute.is_empty());
    }
}
