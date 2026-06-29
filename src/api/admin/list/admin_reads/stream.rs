use super::super::*;

fn stream_read_item_to_admin_record(
    route_family: u64,
    path: &ResourcePath<'_>,
    item: crate::domains::stream::protocol::StreamReadItem,
) -> Option<StreamAdminRecord> {
    match item {
        crate::domains::stream::protocol::StreamReadItem::Event(record) => {
            Some(StreamAdminRecord {
                route_family,
                realm: path.realm.to_string(),
                area: path.area.to_string(),
                resource: path.resource.to_string(),
                resource_offset: record.resource_offset,
                area_offset: record.area_offset,
                realm_offset: record.realm_offset,
                created_at_ms: record.created_at,
                body: kv_byte_value(record.body.as_ref()),
                metadata: record.metadata.as_deref().map(kv_byte_value),
            })
        }
        crate::domains::stream::protocol::StreamReadItem::Filtered { .. }
        | crate::domains::stream::protocol::StreamReadItem::FilteredRange { .. } => None,
    }
}

pub async fn stream_records_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: u64,
    from_offset: u64,
    limit: usize,
    discriminator: Option<String>,
) -> Result<Response, Infallible> {
    match runtime.stream_read_resource_records(AdminStreamReadRequest {
        family: RouteFamily::new(family),
        realm: path.realm,
        area: path.area,
        resource: path.resource,
        from_offset,
        limit: limit as u64,
        discriminator,
    }) {
        Ok((items, cursor)) => {
            let records = items
                .into_iter()
                .filter_map(|item| stream_read_item_to_admin_record(family, path, item))
                .collect();
            crate::api::admin::json_response(StreamRecordsResponse {
                route_family: family,
                realm: Some(path.realm.to_string()),
                area: Some(path.area.to_string()),
                resource: Some(path.resource.to_string()),
                from_offset,
                limit,
                has_more: cursor.has_more,
                records,
            })
        }
        Err(error) => Ok(crate::api::admin::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            &error,
        )),
    }
}

pub(crate) async fn stream_search(
    runtime: Arc<Runtime>,
    request: StreamSearchRequest,
) -> Result<Response, Infallible> {
    let mut remaining = request.limit;
    let mut has_more = false;
    let mut records = Vec::new();
    let streams = runtime
        .stream_list_streams(request.realm.as_deref())
        .into_iter()
        .filter(|item| item.route_family == request.family)
        .filter(|item| {
            request
                .area
                .as_ref()
                .is_none_or(|value| item.area == *value)
        })
        .filter(|item| {
            request
                .resource
                .as_ref()
                .is_none_or(|value| item.resource == *value)
        })
        .collect::<Vec<_>>();

    for stream in streams {
        if remaining == 0 {
            has_more = true;
            break;
        }
        let path = ResourcePath {
            realm: &stream.realm,
            area: &stream.area,
            resource: &stream.resource,
        };
        let response = runtime.stream_read_resource_records(AdminStreamReadRequest {
            family: RouteFamily::new(request.family),
            realm: &stream.realm,
            area: &stream.area,
            resource: &stream.resource,
            from_offset: request.from_offset,
            limit: remaining as u64,
            discriminator: request.discriminator.clone(),
        });
        let (items, cursor) = match response {
            Ok(value) => value,
            Err(error) => {
                return Ok(crate::api::admin::error_response(
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    &error,
                ));
            }
        };
        has_more = has_more || cursor.has_more;
        for item in items {
            if let Some(record) = stream_read_item_to_admin_record(request.family, &path, item) {
                records.push(record);
                remaining = remaining.saturating_sub(1);
                if remaining == 0 {
                    break;
                }
            }
        }
    }

    crate::api::admin::json_response(StreamRecordsResponse {
        route_family: request.family,
        realm: request.realm,
        area: request.area,
        resource: request.resource,
        from_offset: request.from_offset,
        limit: request.limit,
        has_more,
        records,
    })
}

pub async fn stream_events_for_resource(
    runtime: Arc<Runtime>,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Result<Response, Infallible> {
    let streams = runtime
        .stream_list_streams(Some(path.realm))
        .into_iter()
        .filter(|stream| matches_family(family, stream.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::stream_resource_timeline(
        &streams, path, limit,
    ))
}
