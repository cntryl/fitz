use super::super::*;

pub(crate) async fn rpc_call_observations(
    runtime: Arc<Runtime>,
    request: RpcCallObservationRequest,
) -> Result<Response, Infallible> {
    let scope_matches = |route: &str| {
        let Some(parsed) = parse_rpc_operation(route) else {
            return false;
        };
        request
            .realm
            .as_ref()
            .map(|value| parsed.realm == *value)
            .unwrap_or(true)
            && request
                .area
                .as_ref()
                .map(|value| parsed.area == *value)
                .unwrap_or(true)
            && request
                .resource
                .as_ref()
                .map(|value| parsed.resource == *value)
                .unwrap_or(true)
            && request
                .operation
                .as_ref()
                .map(|value| parsed.operation == *value)
                .unwrap_or(true)
    };

    let mut observations = Vec::new();
    for pending in runtime.rpc_list_pending(request.realm.as_deref()) {
        if observations.len() >= request.limit {
            break;
        }
        if pending.route_family != request.family
            || !scope_matches(&pending.route)
            || !request
                .query
                .as_ref()
                .map(|needle| {
                    pending.correlation_id.contains(needle)
                        || pending.route.contains(needle)
                        || pending
                            .worker_session_id
                            .as_ref()
                            .map(|session| session.contains(needle))
                            .unwrap_or(false)
                })
                .unwrap_or(true)
        {
            continue;
        }
        if let Some(parsed) = parse_rpc_operation(&pending.route) {
            observations.push(RpcCallObservation {
                route_family: request.family,
                realm: parsed.realm,
                area: parsed.area,
                resource: parsed.resource,
                operation: Some(parsed.operation),
                route: pending.route,
                correlation_id: Some(pending.correlation_id),
                state: "pending".to_string(),
                submitted_at: Some(pending.submitted_at),
                registered_at: None,
                age_seconds: Some(pending.age_seconds),
                worker_session_id: pending.worker_session_id,
                requests_handled: None,
                average_latency_ms: None,
            });
        }
    }
    for worker in runtime.rpc_list_workers(request.realm.as_deref()) {
        if observations.len() >= request.limit {
            break;
        }
        if worker.route_family != request.family
            || !scope_matches(&worker.route)
            || !request
                .query
                .as_ref()
                .map(|needle| worker.route.contains(needle) || worker.session_id.contains(needle))
                .unwrap_or(true)
        {
            continue;
        }
        if let Some(parsed) = parse_rpc_operation(&worker.route) {
            observations.push(RpcCallObservation {
                route_family: request.family,
                realm: parsed.realm,
                area: parsed.area,
                resource: parsed.resource,
                operation: Some(parsed.operation),
                route: worker.route,
                correlation_id: None,
                state: "worker_registered".to_string(),
                submitted_at: None,
                registered_at: Some(worker.registered_at),
                age_seconds: None,
                worker_session_id: Some(worker.session_id),
                requests_handled: Some(worker.requests_handled),
                average_latency_ms: Some(worker.average_latency_ms),
            });
        }
    }

    crate::api::admin::json_response(RpcCallObservationList {
        route_family: request.family,
        limit: request.limit,
        observations,
    })
}

pub async fn rpc_workers_for_operation(
    runtime: Arc<Runtime>,
    path: &RpcOperationPath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let workers = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| {
            matches_family(family, worker.route_family)
                && matches_operation_route(&worker.route, path)
        })
        .collect();
    crate::api::admin::json_response(RpcWorkersList { workers })
}

pub async fn rpc_pending(
    runtime: Arc<Runtime>,
    realm: Option<&str>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let requests = runtime
        .rpc_list_pending(realm)
        .into_iter()
        .filter(|request| matches_family(family, request.route_family))
        .collect();
    crate::api::admin::json_response(RpcPendingList { requests })
}

pub async fn rpc_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let workers = runtime
        .rpc_list_workers(Some(path.realm))
        .into_iter()
        .filter(|worker| matches_family(family, worker.route_family))
        .collect::<Vec<_>>();
    let pending = runtime
        .rpc_list_pending(Some(path.realm))
        .into_iter()
        .filter(|request| matches_family(family, request.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::rpc_resource_timeline(
        &workers, &pending, path, limit,
    ))
}
