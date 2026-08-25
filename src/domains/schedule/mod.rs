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
//! - live notification registrations
//! - live notify routing
//! - session state
//!
//! Overdue schedules normalize forward to the next future occurrence after
//! downtime. Schedule does not replay historical occurrences, retain a durable
//! delivery backlog, or provide execution guarantees.

pub mod actor;
mod definition_validation;
pub(crate) mod list_wire_budget;
pub mod metrics;
pub mod protocol;
pub mod session;
pub mod sink;
pub mod store;

pub use actor::ScheduleActor;
pub use metrics::ScheduleMetrics;
pub use protocol::{
    CronSchedule, ScheduleClientNotification, ScheduleClientRequest, ScheduleClientResponse,
    ScheduleCreateEntry, ScheduleDef, ScheduleDeliveryMode, ScheduleFailure,
    ScheduleFailureCategory, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
};
pub use session::SessionActor;
pub use store::ScheduleStore;
