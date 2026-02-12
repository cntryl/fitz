//! Schedule domain: time-based event scheduling with durable cron expressions
//!
//! - Schedules are TLV-only payloads persisted in Midge
//! - On Tick, scan for due schedules and emit events via DomainPublishEvent:
//!   1. To schedule:// subscribers (SCHEDULE_NOTIFY for clients observing fires)
//!   2. To target_resource route (cross-domain execution, e.g. notice://)
//! - Uses coarse coalescing semantics: missed ticks emit at most once and advance last_fire_at to now

pub mod actor;
pub mod protocol;
pub mod session;
pub mod store;

pub use actor::{CronSchedule, ScheduleActor, ScheduleMessage};
pub use session::SessionActor;
