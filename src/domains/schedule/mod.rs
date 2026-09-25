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

pub(crate) mod actor;
mod definition_validation;
pub(crate) mod list_wire_budget;
pub mod metrics;
pub mod protocol;
pub(crate) mod sink;
pub(crate) mod store;

pub(crate) use actor::ScheduleActor;
pub use metrics::ScheduleMetrics;
pub use protocol::{
    CronSchedule, ScheduleClientNotification, ScheduleClientRequest, ScheduleClientResponse,
    ScheduleCreateEntry, ScheduleDef, ScheduleDeliveryMode, ScheduleFailure,
    ScheduleFailureCategory, ScheduleListEntry, ScheduleMessage, ScheduleResponse,
};
pub(crate) use sink::{
    validate_run_now_route, ScheduleDomain, ScheduleRunNowError, ScheduleRunNowOutcome,
    ScheduleRunNowResult, DEFAULT_SCHEDULE_PRELOAD_TIMEOUT,
};
pub(crate) use store::ScheduleStore;

/// Canonicalize a Schedule route for authorization: scheme-qualified as given.
///
/// # Errors
///
/// Never fails; the `Result` matches the other domains' canonicalizers.
#[allow(clippy::unnecessary_wraps)]
pub(crate) fn canonical_auth_route(route: &str) -> Result<std::borrow::Cow<'_, str>, String> {
    Ok(crate::runtime::auth_route::scheme_prefixed_route(
        "schedule", route,
    ))
}
