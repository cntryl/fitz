use super::super::*;
use super::timestamp_ms_to_rfc3339;

pub async fn schedule_executions_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    limit: usize,
) -> Result<Response, Infallible> {
    let observations = runtime
        .schedule_list_schedules(Some(path.realm))
        .into_iter()
        .filter(|schedule| {
            schedule.route_family == family
                && path.matches(&schedule.realm, &schedule.area, &schedule.resource)
        })
        .take(limit)
        .map(|schedule| ScheduleExecutionObservation {
            route_family: schedule.route_family,
            realm: schedule.realm,
            area: schedule.area,
            resource: schedule.resource,
            operation: schedule.operation,
            status: if schedule.last_run.is_some() {
                "acknowledged_handoff".to_string()
            } else {
                "scheduled".to_string()
            },
            cron: schedule.cron,
            next_run: schedule.next_run,
            last_run: schedule.last_run,
            executions_total: schedule.executions_total,
        })
        .collect();

    crate::api::admin::json_response(ScheduleExecutionObservationList {
        route_family: family,
        realm: path.realm.to_string(),
        area: path.area.to_string(),
        resource: path.resource.to_string(),
        limit,
        observations,
    })
}

pub(crate) async fn schedule_missed_observations(
    runtime: Arc<Runtime>,
    family: u64,
    realm: Option<String>,
    area: Option<String>,
    resource: Option<String>,
    limit: usize,
) -> Result<Response, Infallible> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let observations = runtime
        .schedule_list_pending_claims(RouteFamily::new(family))
        .into_iter()
        .filter_map(|claim| {
            let route = route_quad(&claim.route)?;
            if realm.as_ref().is_none_or(|value| route.realm == value)
                && area.as_ref().is_none_or(|value| route.area == value)
                && resource
                    .as_ref()
                    .is_none_or(|value| route.resource == value)
            {
                Some(ScheduleMissedObservation {
                    route_family: family,
                    realm: route.realm.to_string(),
                    area: route.area.to_string(),
                    resource: route.resource.to_string(),
                    operation: route.operation.to_string(),
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
        .take(limit)
        .collect();

    crate::api::admin::json_response(ScheduleMissedObservationList {
        route_family: family,
        limit,
        observations,
    })
}

pub async fn schedule_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
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
        runtime.schedule_pending_claims_expired_total(),
        runtime.schedule_pending_claim_cleanup_failures_total(),
        path,
        limit,
    ))
}
