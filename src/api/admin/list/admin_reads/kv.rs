use super::super::{matches_family, troubleshooting, ResourcePath, Response, Runtime};

/// Returns recent KV timeline events for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
#[must_use]
pub fn kv_events_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Response {
    let transactions = runtime
        .kv_list_transactions(Some(path.realm))
        .into_iter()
        .filter(|tx| matches_family(family, tx.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::kv_resource_timeline(
        &transactions,
        path,
        limit,
    ))
}
