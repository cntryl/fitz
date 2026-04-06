//! Schedule domain: Fitz's durable timing-intent primitive.
//!
//! Schedule durably stores timing intent only:
//! - schedule definitions
//! - next-fire computation
//! - due-occurrence claiming
//! - pending claimed occurrences
//! - retry of unacknowledged claimed occurrences after restart
//!
//! Schedule keeps all delivery state ephemeral:
//! - subscribers
//! - live notify routing
//! - session state
//!
//! Overdue schedules normalize forward to the next future occurrence after
//! downtime. Schedule does not replay historical occurrences, retain a durable
//! delivery backlog, or provide execution guarantees.

pub mod actor;
pub mod events;
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
