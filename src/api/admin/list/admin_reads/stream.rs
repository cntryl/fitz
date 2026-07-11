use super::super::{
    kv_byte_value, matches_family, troubleshooting, AdminStreamReadRequest, ResourcePath, Response,
    RouteFamily, Runtime, StreamAdminRecord, StreamRecordsResponse, StreamSearchRequest,
};

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

/// Returns committed stream records for a specific resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
///
/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of `family`.
pub fn stream_records_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: u64,
    from_offset: u64,
    limit: usize,
    discriminator: Option<&str>,
) -> Response {
    match runtime.stream_read_resource_records(AdminStreamReadRequest {
        family: RouteFamily::try_from(family)
            .expect("admin route family is validated at the HTTP boundary"),
        realm: path.realm,
        area: path.area,
        resource: path.resource,
        from_offset,
        limit: u64::try_from(limit).unwrap_or(u64::MAX),
        discriminator: discriminator.map(ToOwned::to_owned),
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
        Err(error) => {
            crate::api::admin::error_response(hyper::StatusCode::SERVICE_UNAVAILABLE, &error)
        }
    }
}

/// # Panics
///
/// Panics if called without prior HTTP-boundary validation of the request family.
pub(crate) fn stream_search(runtime: &Runtime, request: &StreamSearchRequest) -> Response {
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
            family: RouteFamily::try_from(request.family)
                .expect("admin route family is validated at the HTTP boundary"),
            realm: &stream.realm,
            area: &stream.area,
            resource: &stream.resource,
            from_offset: request.from_offset,
            limit: u64::try_from(remaining).unwrap_or(u64::MAX),
            discriminator: request.discriminator.clone(),
        });
        let (items, cursor) = match response {
            Ok(value) => value,
            Err(error) => {
                return crate::api::admin::error_response(
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    &error,
                );
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
        realm: request.realm.clone(),
        area: request.area.clone(),
        resource: request.resource.clone(),
        from_offset: request.from_offset,
        limit: request.limit,
        has_more,
        records,
    })
}

/// Returns recent stream timeline events for the given resource.
///
/// # Errors
///
/// Propagates JSON response construction failures from the admin HTTP layer.
#[must_use]
pub fn stream_events_for_resource(
    runtime: &Runtime,
    path: &ResourcePath<'_>,
    family: Option<u64>,
    limit: usize,
) -> Response {
    let streams = runtime
        .stream_list_streams(Some(path.realm))
        .into_iter()
        .filter(|stream| matches_family(family, stream.route_family))
        .collect::<Vec<_>>();
    crate::api::admin::json_response(troubleshooting::stream_resource_timeline(
        &streams, path, limit,
    ))
}
