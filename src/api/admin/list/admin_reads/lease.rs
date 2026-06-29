use super::super::{
    matches_family, troubleshooting, Arc, HashMap, Infallible, LeaseSearchItem, LeaseSearchRequest,
    LeaseSearchResponse, ResourcePath, Response, Runtime,
};

/// Returns the current lease search view for the requested scope.
pub(crate) async fn lease_search(
    runtime: Arc<Runtime>,
    request: LeaseSearchRequest,
) -> Result<Response, Infallible> {
    let leases = runtime.lease_list_leases(request.realm.as_deref());
    let waiters = runtime.lease_list_waiters();
    let waiter_counts = waiters.iter().fold(HashMap::new(), |mut counts, waiter| {
        *counts
            .entry((
                waiter.route_family,
                waiter.realm.clone(),
                waiter.area.clone(),
                waiter.resource.clone(),
            ))
            .or_insert(0usize) += 1;
        counts
    });
    let include_owned = request
        .state
        .as_deref()
        .is_none_or(|value| value == "owned" || value == "contention");
    let include_waiting = request
        .state
        .as_deref()
        .is_none_or(|value| value == "waiting" || value == "contention");
    let owner_matches = |value: &str| {
        request
            .owner
            .as_ref()
            .is_none_or(|needle| value.contains(needle))
    };
    let scope_matches =
        |item_family: u64, item_realm: &str, item_area: &str, item_resource: &str| {
            item_family == request.family
                && request
                    .realm
                    .as_ref()
                    .is_none_or(|value| item_realm == value)
                && request.area.as_ref().is_none_or(|value| item_area == value)
                && request
                    .resource
                    .as_ref()
                    .is_none_or(|value| item_resource == value)
        };

    let mut items = Vec::new();
    if include_owned {
        for lease in leases {
            if items.len() >= request.limit {
                break;
            }
            if !scope_matches(
                lease.route_family,
                &lease.realm,
                &lease.area,
                &lease.resource,
            ) || !owner_matches(&lease.owner_session_id)
            {
                continue;
            }
            let pending_waiters = waiter_counts
                .get(&(
                    lease.route_family,
                    lease.realm.clone(),
                    lease.area.clone(),
                    lease.resource.clone(),
                ))
                .copied()
                .unwrap_or(0);
            if request.state.as_deref() == Some("contention") && pending_waiters == 0 {
                continue;
            }
            items.push(LeaseSearchItem {
                route_family: lease.route_family,
                realm: lease.realm,
                area: lease.area,
                resource: lease.resource,
                state: if pending_waiters > 0 {
                    "owned_with_waiters".to_string()
                } else {
                    "owned".to_string()
                },
                owner_id: Some(lease.owner_session_id.clone()),
                owner_session_id: Some(lease.owner_session_id),
                queued_token: Some(lease.fencing_token),
                expires_at: Some(lease.expires_at),
                acquired_at: Some(lease.acquired_at),
                renewals: Some(lease.renewals),
                pending_waiters,
            });
        }
    }
    if include_waiting {
        for waiter in waiters {
            if items.len() >= request.limit {
                break;
            }
            if !scope_matches(
                waiter.route_family,
                &waiter.realm,
                &waiter.area,
                &waiter.resource,
            ) || !(owner_matches(&waiter.owner_id) || owner_matches(&waiter.session_id))
            {
                continue;
            }
            items.push(LeaseSearchItem {
                route_family: waiter.route_family,
                realm: waiter.realm,
                area: waiter.area,
                resource: waiter.resource,
                state: "waiting".to_string(),
                owner_id: Some(waiter.owner_id),
                owner_session_id: Some(waiter.session_id),
                queued_token: Some(waiter.queued_token),
                expires_at: Some(waiter.expires_at),
                acquired_at: None,
                renewals: None,
                pending_waiters: 0,
            });
        }
    }

    crate::api::admin::json_response(LeaseSearchResponse {
        route_family: request.family,
        limit: request.limit,
        items,
    })
}

/// Returns recent lease timeline events for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
pub async fn lease_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let leases = runtime
        .lease_list_leases(Some(path.realm))
        .into_iter()
        .filter(|lease| matches_family(family, lease.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::lease_resource_timeline(
        &leases, path, limit,
    ))
}
