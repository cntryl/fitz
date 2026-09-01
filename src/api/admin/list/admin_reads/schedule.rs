use super::super::{
    matches_family, route_quad, troubleshooting, ResourcePath, Response, RouteFamily, Runtime,
    ScheduleExecutionObservation, ScheduleExecutionObservationList,
    ScheduleExecutionObservationRequest, ScheduleMissedObservation, ScheduleMissedObservationList,
    ScheduleMissedObservationRequest,
};
use super::timestamp_ms_to_rfc3339;
use std::collections::HashMap;

/// Returns current schedule execution observations for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if the route family was not validated at the HTTP boundary.
#[must_use]
pub fn schedule_executions_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    request: &ScheduleExecutionObservationRequest,
) -> Response {
    let family = RouteFamily::try_from(request.family)
        .expect("admin route family is validated at the HTTP boundary");
    let pending_handoffs = runtime
        .schedule_list_pending_claims(family)
        .into_iter()
        .filter_map(|claim| {
            let route = route_quad(&claim.route)?;
            (path.matches(route.realm, route.area, route.resource)
                && request
                    .operation
                    .as_deref()
                    .is_none_or(|expected| route.operation == expected))
            .then(|| route.operation.to_string())
        })
        .fold(HashMap::<String, usize>::new(), |mut counts, operation| {
            let count = counts.entry(operation).or_default();
            *count = count.saturating_add(1);
            counts
        });
    let mut observations = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|schedule| {
            schedule.route_family == request.family
                && path.matches(&schedule.realm, &schedule.area, &schedule.resource)
                && request
                    .operation
                    .as_deref()
                    .is_none_or(|expected| schedule.operation == expected)
        })
        .skip(request.offset)
        .take(request.limit.saturating_add(1))
        .map(|schedule| {
            let pending_handoffs = pending_handoffs
                .get(&schedule.operation)
                .copied()
                .unwrap_or_default();
            ScheduleExecutionObservation {
                route_family: schedule.route_family,
                realm: schedule.realm,
                area: schedule.area,
                resource: schedule.resource,
                operation: schedule.operation,
                delivery_mode: schedule.delivery_mode,
                status: if schedule.last_run.is_some() {
                    "acknowledged_handoff".to_string()
                } else {
                    "scheduled".to_string()
                },
                cron: schedule.cron,
                next_run: schedule.next_run,
                last_run: schedule.last_run,
                executions_total: schedule.executions_total,
                pending_handoffs,
            }
        })
        .collect::<Vec<_>>();
    let has_more = observations.len() > request.limit;
    observations.truncate(request.limit);

    crate::api::admin::json_response(ScheduleExecutionObservationList {
        route_family: request.family,
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        offset: request.offset,
        limit: request.limit,
        has_more,
        observations,
    })
}

/// Returns pending schedule handoff observations for the requested scope.
pub(crate) fn schedule_missed_observations(
    runtime: &Runtime,
    request: &ScheduleMissedObservationRequest,
) -> Response {
    let now_ms = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    let observations = runtime
        .schedule_list_pending_claims(
            RouteFamily::try_from(request.family)
                .expect("admin route family is validated at the HTTP boundary"),
        )
        .into_iter()
        .filter_map(|claim| {
            let route = route_quad(&claim.route)?;
            if request
                .realm
                .as_deref()
                .is_none_or(|value| route.realm == value)
                && request
                    .area
                    .as_deref()
                    .is_none_or(|value| route.area == value)
                && request
                    .resource
                    .as_deref()
                    .is_none_or(|value| route.resource == value)
                && request
                    .operation
                    .as_deref()
                    .is_none_or(|value| route.operation == value)
            {
                Some(ScheduleMissedObservation {
                    route_family: request.family,
                    realm: route.realm.to_string(),
                    area: route.area.to_string(),
                    resource: route.resource.to_string(),
                    operation: route.operation.to_string(),
                    delivery_mode: claim.delivery_mode,
                    fire_ms: claim.fire_ms,
                    fire_at: timestamp_ms_to_rfc3339(claim.fire_ms),
                    claimed_at: timestamp_ms_to_rfc3339(claim.claimed_at_ms),
                    age_seconds: now_ms.saturating_sub(claim.claimed_at_ms) / 1_000,
                    status: "pending_handoff_ack".to_string(),
                })
            } else {
                None
            }
        })
        .take(request.limit)
        .collect();

    crate::api::admin::json_response(ScheduleMissedObservationList {
        route_family: request.family,
        limit: request.limit,
        observations,
    })
}

/// Returns recent schedule timeline events for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
#[must_use]
pub fn schedule_events_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Response {
    let schedules = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|schedule| matches_family(family, schedule.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::schedule_resource_timeline(
        &schedules,
        runtime.schedule_pending_fire_claims(),
        runtime.schedule_pending_ack_retries(),
        runtime.schedule_oldest_pending_claim_age_seconds(),
        runtime.schedule_notify_failures(),
        runtime.schedule_ack_failures(),
        runtime.schedule_overdue_normalizations(),
        path,
        limit,
    ))
}
