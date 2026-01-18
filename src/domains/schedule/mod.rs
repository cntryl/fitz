//! Schedule domain: translates time → notice events
//!
//! - Schedules are TLV-only payloads persisted in Midge
//! - On Tick, scan for due schedules and emit a single notice per-due-schedule
//! - Uses coarse coalescing semantics: missed ticks emit at most once and advance last_fire_at to now

pub mod actor;
pub mod protocol;
pub mod store;

pub use actor::{CronSchedule, ScheduleActor, ScheduleMessage};
