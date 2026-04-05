//! Schedule domain: durable route-based time-triggered fanout with cron expressions
//!
//! Schedules are identified by route string (not auto-generated IDs).
//! Each schedule stores:
//! - route: String - unique identity (e.g., "schedule://realm/area/resource/operation")
//! - cron: String - 5-field cron expression for timing
//! - payload: Bytes - arbitrary data to fanout to subscribers when fired
//!
//! On fire, schedule is published to all subscribers matching the route pattern.
//! Durable schedule definitions are boot-loaded from storage on broker start.
//! Missed executions are skipped forward to the next future fire time rather than
//! replayed after downtime. Schedule subscriptions and notifications remain
//! live, session-scoped delivery state only.

pub mod actor;
pub mod metrics;
pub mod protocol;
pub mod session;
pub mod store;

pub use actor::ScheduleActor;
pub use metrics::ScheduleMetrics;
pub use protocol::{
    CronSchedule, ScheduleCreateEntry, ScheduleDef, ScheduleError, ScheduleListEntry,
    ScheduleMessage, ScheduleResponse,
};
pub use session::SessionActor;
pub use store::ScheduleStore;
