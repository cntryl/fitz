//! Shared validation for schedule definitions at actor and transport boundaries.

use super::{CronSchedule, ScheduleFailure, ScheduleFailureCategory};

pub(crate) fn schedule_definition_failure(route: &str, cron: &str) -> Option<ScheduleFailure> {
    if let Err(error) = super::protocol::validate_concrete_schedule_route(route) {
        return Some(ScheduleFailure::new(
            ScheduleFailureCategory::InvalidTarget,
            error,
        ));
    }
    CronSchedule::parse(cron)
        .err()
        .map(|error| ScheduleFailure::new(ScheduleFailureCategory::InvalidCron, error))
}
