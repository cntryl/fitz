use super::super::{
    troubleshooting, Arc, Infallible, QueueDeadLettersList, QueueInflightList, ResourcePath,
    Response, Runtime,
};

/// Returns current queue inflight entries for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
pub async fn queue_inflight_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let inflight = runtime
        .queue_list_inflight(Some(path.realm))
        .into_iter()
        .filter(|entry| {
            path.matches(&entry.realm, &entry.area, &entry.resource)
                && family.is_none_or(|value| entry.family == value)
        })
        .collect();
    crate::api::admin::json_response(QueueInflightList { inflight })
}

/// Returns current queue dead-letter entries for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
pub async fn queue_dead_letters_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    let messages = runtime
        .queue_list_dead_letters(Some(path.realm))
        .into_iter()
        .filter(|message| {
            path.matches(&message.realm, &message.area, &message.resource)
                && family.is_none_or(|value| message.family == value)
        })
        .collect();
    crate::api::admin::json_response(QueueDeadLettersList { messages })
}

/// Returns recent queue timeline events for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
pub async fn queue_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let queues = runtime.queue_list_queues(Some(path.realm));
    let inflight = runtime.queue_list_inflight(Some(path.realm));
    let dead_letters = runtime.queue_list_dead_letters(Some(path.realm));
    crate::api::admin::json_response(troubleshooting::queue_resource_timeline(
        &queues,
        &inflight,
        &dead_letters,
        path,
        family,
        limit,
    ))
}
