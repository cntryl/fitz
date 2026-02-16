//! Schedule domain: route-based time-triggered fanout with cron expressions
//!
//! Schedules are identified by route string (not auto-generated IDs).
//! Each schedule stores:
//! - route: String - unique identity (e.g., "schedule://realm/area/resource/operation")
//! - cron: String - 5-field cron expression for timing
//! - payload: Bytes - arbitrary data to fanout to subscribers when fired
//!
//! On fire, schedule is published to all subscribers matching the route pattern.
//! Storage uses time-indexed keys with TTL for automatic expiry.

pub mod actor;
pub mod protocol;
pub mod store;

pub use actor::ScheduleActor;
pub use protocol::{
    CronSchedule, ScheduleDef, ScheduleError, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
};
pub use store::ScheduleStore;
