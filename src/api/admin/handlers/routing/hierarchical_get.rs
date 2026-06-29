use super::*;

pub(super) async fn handle_hierarchical_get(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let path = uri.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal) {
        Ok(parsed) => parsed,
        Err(response) => return Ok(*response),
    };

    match tail {
        ["stats"] => stats::handle_domain_stats(runtime, scheme).await,
        ["search"] if scheme == "stream" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let from_offset = match parse_optional_u64_param(uri, "from_offset") {
                Ok(offset) => offset.unwrap_or(0),
                Err(response) => return Ok(*response),
            };
            let realm = list::parse_optional_string_query_param(uri, "realm");
            let area = list::parse_optional_string_query_param(uri, "area");
            let resource = list::parse_optional_string_query_param(uri, "resource");
            let discriminator = list::parse_optional_string_query_param(uri, "discriminator")
                .or_else(|| list::parse_optional_string_query_param(uri, "q"));
            list::stream_search(
                runtime,
                list::StreamSearchRequest {
                    family,
                    realm,
                    area,
                    resource,
                    from_offset,
                    limit,
                    discriminator,
                },
            )
            .await
        }
        ["missed"] if scheme == "schedule" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::schedule_missed_observations(
                runtime,
                family,
                list::parse_optional_string_query_param(uri, "realm"),
                list::parse_optional_string_query_param(uri, "area"),
                list::parse_optional_string_query_param(uri, "resource"),
                limit,
            )
            .await
        }
        ["search"] if scheme == "lease" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::lease_search(
                runtime,
                list::LeaseSearchRequest {
                    family,
                    realm: list::parse_optional_string_query_param(uri, "realm"),
                    area: list::parse_optional_string_query_param(uri, "area"),
                    resource: list::parse_optional_string_query_param(uri, "resource"),
                    owner: list::parse_optional_string_query_param(uri, "owner"),
                    state: list::parse_optional_string_query_param(uri, "state"),
                    limit,
                },
            )
            .await
        }
        ["deliveries"] if scheme == "notice" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::notice_delivery_observations(
                runtime,
                family,
                list::parse_optional_string_query_param(uri, "realm"),
                list::parse_optional_string_query_param(uri, "area"),
                list::parse_optional_string_query_param(uri, "resource"),
                list::parse_optional_string_query_param(uri, "q"),
                limit,
            )
            .await
        }
        ["calls"] if scheme == "rpc" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::rpc_call_observations(
                runtime,
                list::RpcCallObservationRequest {
                    family,
                    realm: list::parse_optional_string_query_param(uri, "realm"),
                    area: list::parse_optional_string_query_param(uri, "area"),
                    resource: list::parse_optional_string_query_param(uri, "resource"),
                    operation: list::parse_optional_string_query_param(uri, "operation"),
                    query: list::parse_optional_string_query_param(uri, "q")
                        .or_else(|| list::parse_optional_string_query_param(uri, "correlation_id")),
                    limit,
                },
            )
            .await
        }
        ["realms"] => handle_realms_collection(scheme, runtime, scope.filter()),
        ["realms", realm, "watermarks"] if scheme == "stream" => {
            super::json_response(list::stream_realm_watermark_detail(runtime.as_ref(), realm))
        }
        ["realms", realm] if scheme == "queue" => super::json_response(list::queue_realm_detail(
            runtime.as_ref(),
            realm,
            scope.filter(),
        )),
        ["realms", realm] => super::json_response(list::RealmDetail {
            realm: (*realm).to_string(),
        }),
        ["realms", realm, "areas"] => {
            handle_areas_collection(scheme, runtime, realm, scope.filter())
        }
        ["realms", realm, "areas", area, "watermarks"] if scheme == "stream" => {
            super::json_response(list::stream_area_watermark_detail(
                runtime.as_ref(),
                realm,
                area,
            ))
        }
        ["realms", realm, "areas", area] if scheme == "queue" => super::json_response(
            list::queue_area_detail(runtime.as_ref(), realm, area, scope.filter()),
        ),
        ["realms", realm, "areas", area] => super::json_response(list::AreaDetail {
            realm: (*realm).to_string(),
            area: (*area).to_string(),
        }),
        ["realms", realm, "areas", area, "resources"] => {
            handle_resources_collection(scheme, runtime, realm, area, scope.filter())
        }
        ["realms", realm, "areas", area, "resources", resource] => {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            handle_resource_detail(scheme, runtime, realm, area, resource, family)
        }
        ["realms", realm, "areas", area, "resources", resource, "events"] => {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match parse_event_limit(uri) {
                Ok(limit) => limit,
                Err(response) => return Ok(*response),
            };
            let path = list::ResourcePath {
                realm,
                area,
                resource,
            };

            match scheme {
                "kv" => list::kv_events_for_resource(runtime, &path, family, limit).await,
                "queue" => list::queue_events_for_resource(runtime, &path, family, limit).await,
                "stream" => list::stream_events_for_resource(runtime, &path, family, limit).await,
                "lease" => list::lease_events_for_resource(runtime, &path, family, limit).await,
                "schedule" => {
                    list::schedule_events_for_resource(runtime, &path, family, limit).await
                }
                "notice" => list::notice_events_for_resource(runtime, &path, family, limit).await,
                "rpc" => list::rpc_events_for_resource(runtime, &path, family, limit).await,
                _ => Ok(super::not_found()),
            }
        }
        ["realms", realm, "areas", area, "resources", resource, "records"]
            if scheme == "stream" =>
        {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let from_offset = match parse_optional_u64_param(uri, "from_offset") {
                Ok(offset) => offset.unwrap_or(0),
                Err(response) => return Ok(*response),
            };
            list::stream_records_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
                from_offset,
                limit,
                list::parse_optional_string_query_param(uri, "discriminator")
                    .or_else(|| list::parse_optional_string_query_param(uri, "q")),
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "executions"]
            if scheme == "schedule" =>
        {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let limit = match list::parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::schedule_executions_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
                limit,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "compare"] => {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let against_realm = match parse_required_string_query_param(uri, "against_realm") {
                Ok(value) => value,
                Err(response) => return Ok(*response),
            };
            let against_area = match parse_required_string_query_param(uri, "against_area") {
                Ok(value) => value,
                Err(response) => return Ok(*response),
            };
            let against_resource = match parse_required_string_query_param(uri, "against_resource")
            {
                Ok(value) => value,
                Err(response) => return Ok(*response),
            };
            let against_family =
                match parse_optional_allowed_family_param(uri, principal, "against_family") {
                    Ok(Some(family)) => Some(family),
                    Ok(None) if matches!(scope, AdminFamilyScope::Legacy) && scheme == "queue" => {
                        None
                    }
                    Ok(None) => family,
                    Err(response) => return Ok(*response),
                };
            let path = list::ResourcePath {
                realm,
                area,
                resource,
            };
            let against_path = list::ResourcePath {
                realm: &against_realm,
                area: &against_area,
                resource: &against_resource,
            };

            let comparison = match scheme {
                "kv" => list::kv_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "queue" => list::queue_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "stream" => list::stream_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "lease" => list::lease_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "schedule" => list::schedule_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "notice" => list::notice_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "rpc" => list::rpc_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                _ => return Ok(super::not_found()),
            };

            super::json_response(comparison)
        }
        ["realms", realm, "areas", area, "resources", resource, "transactions"]
            if scheme == "kv" =>
        {
            list::kv_transactions_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "rows"] if scheme == "kv" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let starts_with = match list::parse_optional_kv_query_bytes(uri, "starts_with") {
                Ok(prefix) => prefix.unwrap_or_default(),
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let cursor = match list::parse_optional_kv_cursor(uri) {
                Ok(cursor) => cursor,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let limit = match list::parse_kv_scan_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::kv_rows_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
                starts_with,
                cursor,
                limit,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "value"] if scheme == "kv" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let key = match list::parse_kv_query_bytes(uri, "key") {
                Ok(key) => key,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::kv_committed_value_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
                key,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "prefix"] if scheme == "kv" => {
            let family = match require_concrete_route_family(scope, uri, principal) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            let prefix = match list::parse_kv_query_bytes(uri, "prefix") {
                Ok(prefix) => prefix,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            let limit = match list::parse_kv_scan_limit(uri) {
                Ok(limit) => limit,
                Err(message) => {
                    return Ok(super::error_response(StatusCode::BAD_REQUEST, &message));
                }
            };
            list::kv_prefix_scan_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
                prefix,
                limit,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "inflight"]
            if scheme == "queue" =>
        {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            list::queue_inflight_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "dead-letters"]
            if scheme == "queue" =>
        {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Ok(*response),
            };
            list::queue_dead_letters_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "subscriptions"]
            if scheme == "notice" =>
        {
            list::notice_subscriptions_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "operations"]
            if scheme == "rpc" =>
        {
            super::json_response(list::rpc_operations(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation]
            if scheme == "rpc" =>
        {
            super::json_response(list::rpc_operation_detail(
                runtime.as_ref(),
                &list::RpcOperationPath {
                    realm,
                    area,
                    resource,
                    operation,
                },
                scope.filter(),
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation, "workers"]
            if scheme == "rpc" =>
        {
            list::rpc_workers_for_operation(
                runtime,
                &list::RpcOperationPath {
                    realm,
                    area,
                    resource,
                    operation,
                },
                scope.filter(),
            )
            .await
        }
        ["pending"] if scheme == "rpc" => list::rpc_pending(runtime, None, scope.filter()).await,
        _ => Ok(super::not_found()),
    }
}
