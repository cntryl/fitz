//! Concrete Schedule identity parsing, shared by actor and storage adapters.

use crate::runtime::routing::{route_exact_quad, route_scheme};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConcreteScheduleRoute {
    pub realm: String,
    pub area: String,
    pub resource: String,
    pub operation: String,
}

/// # Errors
///
/// Returns an error when `route` is not a concrete `schedule://` route with
/// non-empty realm, area, resource, and operation segments.
pub fn parse_concrete_schedule_route(route: &str) -> Result<ConcreteScheduleRoute, String> {
    let Some(scheme) = route_scheme(route) else {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    };
    if scheme != "schedule" {
        return Err("schedule route scheme must be schedule".to_string());
    }

    let Some(parts) = route_exact_quad(route) else {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    };
    if [parts.realm, parts.area, parts.resource, parts.operation]
        .iter()
        .any(|part| part.is_empty())
    {
        return Err(
            "schedule route must be schedule://{realm}/{area}/{resource}/{operation}".to_string(),
        );
    }
    if [parts.realm, parts.area, parts.resource, parts.operation]
        .iter()
        .any(|part| *part == "*" || *part == "**")
    {
        return Err("schedule route must not contain wildcards".to_string());
    }

    Ok(ConcreteScheduleRoute {
        realm: parts.realm.to_string(),
        area: parts.area.to_string(),
        resource: parts.resource.to_string(),
        operation: parts.operation.to_string(),
    })
}

/// # Errors
///
/// Returns an error when `route` is not a valid concrete `schedule://` route.
pub fn validate_concrete_schedule_route(route: &str) -> Result<(), String> {
    parse_concrete_schedule_route(route).map(|_| ())
}

#[cfg(test)]
#[path = "route_grammar_tests.rs"]
mod tests;
