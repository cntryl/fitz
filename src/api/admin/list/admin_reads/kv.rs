use super::super::*;

pub async fn kv_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
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
