use super::super::*;

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
                && family.map(|value| entry.family == value).unwrap_or(true)
        })
        .collect();
    crate::api::admin::json_response(QueueInflightList { inflight })
}

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
                && family.map(|value| message.family == value).unwrap_or(true)
        })
        .collect();
    crate::api::admin::json_response(QueueDeadLettersList { messages })
}

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
