//! Admission checks for operator-triggered Schedule run-now.

use super::model::ScheduleRunNowError;

/// Reject a run-now route that is not a concrete Schedule route.
///
/// # Errors
///
/// Returns [`ScheduleRunNowError::InvalidRoute`] with the grammar violation.
pub(crate) fn validate_run_now_route(route: &str) -> Result<(), ScheduleRunNowError> {
    let parsed = crate::domains::schedule::protocol::parse_concrete_schedule_route(route)
        .map_err(ScheduleRunNowError::InvalidRoute)?;
    if [
        parsed.realm.as_str(),
        parsed.area.as_str(),
        parsed.resource.as_str(),
        parsed.operation.as_str(),
    ]
    .iter()
    .any(|segment| segment.trim().is_empty())
    {
        return Err(ScheduleRunNowError::InvalidRoute(
            "schedule route segments must not be whitespace-only".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reject_wildcard_run_now_route_as_invalid() {
        // Arrange
        let route = "schedule://prod/jobs/*/send";

        // Act
        let result = validate_run_now_route(route);

        // Assert
        assert_eq!(
            result,
            Err(ScheduleRunNowError::InvalidRoute(
                "schedule route must not contain wildcards".to_string()
            ))
        );
    }

    #[test]
    fn should_reject_whitespace_only_run_now_route_segments_as_invalid() {
        // Arrange
        let routes = [
            "schedule:// /jobs/resource/send",
            "schedule://prod/ /resource/send",
            "schedule://prod/jobs/ /send",
            "schedule://prod/jobs/resource/ ",
        ];

        // Act
        let results = routes.map(validate_run_now_route);

        // Assert
        assert!(results.iter().all(|result| matches!(
            result,
            Err(ScheduleRunNowError::InvalidRoute(message))
                if message == "schedule route segments must not be whitespace-only"
        )));
    }

    #[test]
    fn should_accept_concrete_run_now_route() {
        // Arrange
        let route = "schedule://prod/jobs/billing/send";

        // Act
        let result = validate_run_now_route(route);

        // Assert
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn should_display_invalid_route_message_verbatim() {
        // Arrange
        let error = ScheduleRunNowError::InvalidRoute("bad route".to_string());

        // Act
        let message = error.to_string();

        // Assert
        assert_eq!(message, "bad route");
    }
}
