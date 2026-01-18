use crate::domains::schedule::protocol::SchedulePayload;
use crate::domains::schedule::store::ScheduleStore;
use crate::runtime::actor::{Actor, Context};
use crate::runtime::routing::{Route, RouteFamily, RouteAddress};
use crate::domains::notification::protocol::{NotificationMessage, PublishMessage};
use bytes::Bytes;
use chrono::{DateTime, Utc, Datelike, Timelike};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, warn};

/// Minimal cron schedule supporting 5 fields (minute hour day month weekday)
#[derive(Debug, Clone)]
struct CronSchedule {
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
                return (min..=max).filter(|v| (v - min).is_multiple_of(step)).collect();
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

    fn matches_dt(&self, dt: &DateTime<Utc>) -> bool {
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
        self.store
            .insert(
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
            let last = DateTime::from_timestamp(def.last_fire_at.max(0), 0).unwrap_or_else(Utc::now);
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
                warn!("failed to extract realm/area from schedule route: {}", route.as_str());
                continue;
            };
            match SchedulePayload::decode(&payload) {
                Ok(sp) => {
                    let notice_path = if sp.operation.is_empty() {
                        format!("notice://{}/{}/{}", realm, area, sp.resource)
                    } else {
                        format!("notice://{}/{}/{}/{}", realm, area, sp.resource, sp.operation)
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
