use super::super::collections_and_details::{
    handle_areas_collection, handle_realms_collection, handle_resource_detail,
    handle_resources_collection,
};
use super::{
    error_response, json_response, not_found, parse_domain_path, parse_event_limit,
    parse_optional_allowed_family_param, parse_optional_u64_param,
    parse_required_string_query_param, require_concrete_route_family, resource_family_filter,
    AdminFamilyScope,
};
use crate::api::admin::auth::AdminPrincipal;
use crate::api::admin::{list, stats};
use crate::api::http::Response;
use crate::boot::Runtime;
use hyper::StatusCode;
use std::convert::Infallible;
use std::sync::Arc;

type HttpError = Box<Response>;

pub(super) async fn handle_hierarchical_get(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let Some(permit) = runtime.try_acquire_admin_blocking_permit() else {
        return Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Admin blocking executor is saturated",
        ));
    };
    let uri = uri.clone();
    let principal = principal.clone();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handle_hierarchical_get_blocking(&uri, &runtime, &principal)
    })
    .await
    {
        Ok(result) => result,
        Err(error) => Ok(error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("Admin blocking executor failed: {error}"),
        )),
    }
}

fn handle_hierarchical_get_blocking(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let path = uri.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal, runtime) {
        Ok(parsed) => parsed,
        Err(response) => return Ok(*response),
    };

    if let Some(response) =
        handle_domain_collection_routes(uri, runtime, principal, scope, scheme, tail)
    {
        return Ok(response);
    }

    if let Some(response) = handle_resource_collection_routes(uri, runtime, scope, scheme, tail) {
        return Ok(response);
    }

    if let Some(response) =
        handle_resource_detail_routes(uri, runtime, principal, scope, scheme, tail)?
    {
        return Ok(response);
    }

    Ok(not_found())
}

fn handle_domain_collection_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    let response = match tail {
        ["stats"] => Some(stats::handle_domain_stats(
            runtime.as_ref(),
            scheme,
            scope.filter(),
        )),
        ["search"] if scheme == "stream" => {
            let request = match build_stream_search_request(uri, principal, scope) {
                Ok(request) => request,
                Err(response) => return Some(*response),
            };
            Some(list::stream_search(runtime.as_ref(), &request))
        }
        ["missed"] if scheme == "schedule" => {
            let request = match build_schedule_missed_observation_request(uri, principal, scope) {
                Ok(request) => request,
                Err(response) => return Some(*response),
            };
            Some(list::schedule_missed_observations(
                runtime.as_ref(),
                &request,
            ))
        }
        ["search"] if scheme == "lease" => {
            let request = match build_lease_search_request(uri, principal, scope) {
                Ok(request) => request,
                Err(response) => return Some(*response),
            };
            Some(list::lease_search(runtime.as_ref(), &request))
        }
        ["deliveries"] if scheme == "notice" => Some(list::notice_delivery_observations(
            runtime.as_ref(),
            match require_family_for_search(uri, principal, scope) {
                Ok(family) => family,
                Err(response) => return Some(*response),
            },
            list::parse_optional_string_query_param(uri, "realm").as_deref(),
            list::parse_optional_string_query_param(uri, "area").as_deref(),
            list::parse_optional_string_query_param(uri, "resource").as_deref(),
            list::parse_optional_string_query_param(uri, "q").as_deref(),
            match parse_admin_record_limit(uri) {
                Ok(limit) => limit,
                Err(response) => return Some(*response),
            },
        )),
        ["calls"] if scheme == "rpc" => {
            let request = match build_rpc_call_observation_request(uri, principal, scope) {
                Ok(request) => request,
                Err(response) => return Some(*response),
            };
            Some(list::rpc_call_observations(runtime.as_ref(), &request))
        }
        _ => None,
    };

    response
}

fn handle_resource_collection_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    let response =
        match tail {
            ["realms"] => Some(handle_realms_collection(scheme, runtime, scope.filter())),
            ["realms", realm, "watermarks"] if scheme == "stream" => Some(json_response(
                list::stream_realm_watermark_detail(runtime.as_ref(), realm, scope.filter()),
            )),
            ["realms", realm] if scheme == "queue" => Some(json_response(
                list::queue_realm_detail(runtime.as_ref(), realm, scope.filter()),
            )),
            ["realms", realm] => Some(json_response(list::RealmDetail {
                realm: (*realm).to_string(),
            })),
            ["realms", realm, "areas"] => Some(handle_areas_collection(
                scheme,
                runtime,
                realm,
                scope.filter(),
            )),
            ["realms", realm, "areas", area, "watermarks"] if scheme == "stream" => {
                Some(json_response(list::stream_area_watermark_detail(
                    runtime.as_ref(),
                    realm,
                    area,
                    scope.filter(),
                )))
            }
            ["realms", realm, "areas", area] if scheme == "queue" => Some(json_response(
                list::queue_area_detail(runtime.as_ref(), realm, area, scope.filter()),
            )),
            ["realms", realm, "areas", area] => Some(json_response(list::AreaDetail {
                realm: (*realm).to_string(),
                area: (*area).to_string(),
            })),
            ["realms", realm, "areas", area, "resources"] => Some(handle_resources_collection(
                scheme,
                runtime,
                realm,
                area,
                scope.filter(),
            )),
            ["realms", realm, "areas", area, "resources", resource] => {
                let family = match resource_family_filter(scope, uri, scheme) {
                    Ok(family) => family,
                    Err(response) => return Some(*response),
                };
                Some(handle_resource_detail(
                    scheme, runtime, realm, area, resource, family,
                ))
            }
            _ => None,
        };

    response
}

fn handle_resource_detail_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Result<Option<Response>, Infallible> {
    if let Some(response) =
        handle_resource_activity_routes(uri, runtime, principal, scope, scheme, tail)
    {
        return Ok(Some(response));
    }

    if let Some(response) =
        handle_resource_state_routes(uri, runtime, principal, scope, scheme, tail)?
    {
        return Ok(Some(response));
    }

    Ok(None)
}

fn handle_resource_activity_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    let response = match tail {
        ["realms", realm, "areas", area, "resources", resource, "events"] => Some(
            handle_resource_events(uri, runtime, scope, scheme, realm, area, resource),
        ),
        ["realms", realm, "areas", area, "resources", resource, "records"]
            if scheme == "stream" =>
        {
            Some(handle_stream_records(
                uri, runtime, principal, scope, realm, area, resource,
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "executions"]
            if scheme == "schedule" =>
        {
            Some(handle_schedule_executions(
                uri, runtime, principal, scope, realm, area, resource,
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "compare"] => {
            let path = list::ResourcePath {
                realm,
                area,
                resource,
            };
            Some(handle_resource_compare(
                uri, runtime, principal, scope, scheme, &path,
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "transactions"]
            if scheme == "kv" =>
        {
            Some(list::kv_transactions_for_resource(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            ))
        }
        _ => None,
    };

    response
}

fn handle_resource_state_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Result<Option<Response>, Infallible> {
    if let Some(response) =
        handle_kv_resource_state_routes(uri, runtime, principal, scope, scheme, tail)?
    {
        return Ok(Some(response));
    }

    if let Some(response) = handle_queue_notice_rpc_state_routes(uri, runtime, scope, scheme, tail)
    {
        return Ok(Some(response));
    }

    Ok(None)
}

fn handle_kv_resource_state_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Result<Option<Response>, Infallible> {
    let response = match tail {
        ["realms", realm, "areas", area, "resources", resource, "rows"] if scheme == "kv" => Some(
            handle_kv_rows(uri, runtime, principal, scope, realm, area, resource)?,
        ),
        ["realms", realm, "areas", area, "resources", resource, "value"] if scheme == "kv" => Some(
            handle_kv_value(uri, runtime, principal, scope, realm, area, resource)?,
        ),
        ["realms", realm, "areas", area, "resources", resource, "prefix"] if scheme == "kv" => {
            Some(handle_kv_prefix(
                uri, runtime, principal, scope, realm, area, resource,
            )?)
        }
        _ => None,
    };

    Ok(response)
}

fn handle_queue_notice_rpc_state_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    if let Some(response) = handle_queue_state_routes(uri, runtime, scope, scheme, tail) {
        return Some(response);
    }

    if let Some(response) = handle_notice_rpc_state_routes(runtime, scope, scheme, tail) {
        return Some(response);
    }

    None
}

fn handle_queue_state_routes(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    let response = match tail {
        ["realms", realm, "areas", area, "resources", resource, "inflight"]
            if scheme == "queue" =>
        {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Some(*response),
            };
            Some(list::queue_inflight_for_resource(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "dead-letters"]
            if scheme == "queue" =>
        {
            let family = match resource_family_filter(scope, uri, scheme) {
                Ok(family) => family,
                Err(response) => return Some(*response),
            };
            Some(list::queue_dead_letters_for_resource(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                family,
            ))
        }
        _ => None,
    };

    response
}

fn handle_notice_rpc_state_routes(
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    scheme: &str,
    tail: &[&str],
) -> Option<Response> {
    let response = match tail {
        ["realms", realm, "areas", area, "resources", resource, "subscriptions"]
            if scheme == "notice" =>
        {
            Some(list::notice_subscriptions_for_resource(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations"]
            if scheme == "rpc" =>
        {
            Some(json_response(list::rpc_operations(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
                scope.filter(),
            )))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation]
            if scheme == "rpc" =>
        {
            Some(json_response(list::rpc_operation_detail(
                runtime.as_ref(),
                &list::RpcOperationPath {
                    realm,
                    area,
                    resource,
                    operation,
                },
                scope.filter(),
            )))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation, "workers"]
            if scheme == "rpc" =>
        {
            Some(list::rpc_workers_for_operation(
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
        ["pending"] if scheme == "rpc" => {
            Some(list::rpc_pending(runtime.as_ref(), None, scope.filter()))
        }
        _ => None,
    };

    response
}

fn handle_resource_events(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    scheme: &str,
    realm: &str,
    area: &str,
    resource: &str,
) -> Response {
    let family = match resource_family_filter(scope, uri, scheme) {
        Ok(family) => family,
        Err(response) => return *response,
    };
    let limit = match parse_event_limit(uri) {
        Ok(limit) => limit,
        Err(response) => return *response,
    };
    let path = list::ResourcePath {
        realm,
        area,
        resource,
    };

    match scheme {
        "kv" => list::kv_events_for_resource(runtime.as_ref(), &path, family, limit),
        "queue" => list::queue_events_for_resource(runtime.as_ref(), &path, family, limit),
        "stream" => list::stream_events_for_resource(runtime.as_ref(), &path, family, limit),
        "lease" => list::lease_events_for_resource(runtime.as_ref(), &path, family, limit),
        "schedule" => list::schedule_events_for_resource(runtime.as_ref(), &path, family, limit),
        "notice" => list::notice_events_for_resource(runtime.as_ref(), &path, family, limit),
        "rpc" => list::rpc_events_for_resource(runtime.as_ref(), &path, family, limit),
        _ => not_found(),
    }
}

fn handle_stream_records(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
) -> Response {
    let family = match require_concrete_route_family(scope, uri, principal) {
        Ok(family) => family,
        Err(response) => return *response,
    };
    let limit = match list::parse_admin_record_limit(uri) {
        Ok(limit) => limit,
        Err(message) => return error_response(StatusCode::BAD_REQUEST, &message),
    };
    let from_offset = match parse_optional_u64_param(uri, "from_offset") {
        Ok(offset) => offset.unwrap_or(0),
        Err(response) => return *response,
    };

    list::stream_records_for_resource(
        runtime.as_ref(),
        &list::ResourcePath {
            realm,
            area,
            resource,
        },
        family,
        from_offset,
        limit,
        list::parse_optional_string_query_param(uri, "discriminator")
            .or_else(|| list::parse_optional_string_query_param(uri, "q"))
            .as_deref(),
    )
}

fn handle_schedule_executions(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
) -> Response {
    let request = match build_schedule_execution_observation_request(uri, principal, scope) {
        Ok(request) => request,
        Err(response) => return *response,
    };

    list::schedule_executions_for_resource(
        runtime.as_ref(),
        &list::ResourcePath {
            realm,
            area,
            resource,
        },
        &request,
    )
}

fn handle_resource_compare(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    scheme: &str,
    path: &list::ResourcePath<'_>,
) -> Response {
    let family = match resource_family_filter(scope, uri, scheme) {
        Ok(family) => family,
        Err(response) => return *response,
    };
    let against_realm = match parse_required_string_query_param(uri, "against_realm") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let against_area = match parse_required_string_query_param(uri, "against_area") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let against_resource = match parse_required_string_query_param(uri, "against_resource") {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let against_family = match parse_optional_allowed_family_param(uri, principal, "against_family")
    {
        Ok(Some(family)) => Some(family),
        Ok(None) => family,
        Err(response) => return *response,
    };
    let against_path = list::ResourcePath {
        realm: &against_realm,
        area: &against_area,
        resource: &against_resource,
    };

    let comparison = match scheme {
        "kv" => list::kv_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "queue" => list::queue_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "stream" => list::stream_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "lease" => list::lease_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "schedule" => list::schedule_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "notice" => list::notice_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        "rpc" => list::rpc_compare_detail(
            runtime.as_ref(),
            path,
            family,
            &against_path,
            against_family,
        ),
        _ => return not_found(),
    };

    json_response(comparison)
}

fn handle_kv_rows(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
) -> Result<Response, Infallible> {
    let family = match require_concrete_route_family(scope, uri, principal) {
        Ok(family) => family,
        Err(response) => return Ok(*response),
    };
    let starts_with = match list::parse_optional_kv_query_bytes(uri, "starts_with") {
        Ok(prefix) => prefix.unwrap_or_default(),
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };
    let cursor = match list::parse_optional_kv_cursor(uri) {
        Ok(cursor) => cursor,
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };
    let limit = match list::parse_kv_scan_limit(uri) {
        Ok(limit) => limit,
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };

    list::kv_rows_for_resource(
        runtime.as_ref(),
        &list::ResourcePath {
            realm,
            area,
            resource,
        },
        family,
        &starts_with,
        cursor.as_deref(),
        limit,
    )
}

fn handle_kv_value(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
) -> Result<Response, Infallible> {
    let family = match require_concrete_route_family(scope, uri, principal) {
        Ok(family) => family,
        Err(response) => return Ok(*response),
    };
    let key = match list::parse_kv_query_bytes(uri, "key") {
        Ok(key) => key,
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };

    list::kv_committed_value_for_resource(
        runtime.as_ref(),
        &list::ResourcePath {
            realm,
            area,
            resource,
        },
        family,
        &key,
    )
}

fn handle_kv_prefix(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
) -> Result<Response, Infallible> {
    let family = match require_concrete_route_family(scope, uri, principal) {
        Ok(family) => family,
        Err(response) => return Ok(*response),
    };
    let prefix = match list::parse_kv_query_bytes(uri, "prefix") {
        Ok(prefix) => prefix,
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };
    let limit = match list::parse_kv_scan_limit(uri) {
        Ok(limit) => limit,
        Err(message) => return Ok(error_response(StatusCode::BAD_REQUEST, &message)),
    };

    list::kv_prefix_scan_for_resource(
        runtime.as_ref(),
        &list::ResourcePath {
            realm,
            area,
            resource,
        },
        family,
        &prefix,
        limit,
    )
}

fn require_family_for_search(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<u64, HttpError> {
    require_concrete_route_family(scope, uri, principal)
}

fn parse_admin_record_limit(uri: &hyper::Uri) -> Result<usize, HttpError> {
    list::parse_admin_record_limit(uri)
        .map_err(|message| Box::new(error_response(StatusCode::BAD_REQUEST, &message)))
}

fn build_stream_search_request(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<list::StreamSearchRequest, HttpError> {
    Ok(list::StreamSearchRequest {
        family: require_family_for_search(uri, principal, scope)?,
        realm: list::parse_optional_string_query_param(uri, "realm"),
        area: list::parse_optional_string_query_param(uri, "area"),
        resource: list::parse_optional_string_query_param(uri, "resource"),
        from_offset: parse_optional_u64_param(uri, "from_offset")?.unwrap_or(0),
        limit: parse_admin_record_limit(uri)?,
        discriminator: list::parse_optional_string_query_param(uri, "discriminator")
            .or_else(|| list::parse_optional_string_query_param(uri, "q")),
    })
}

fn build_lease_search_request(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<list::LeaseSearchRequest, HttpError> {
    Ok(list::LeaseSearchRequest {
        family: require_family_for_search(uri, principal, scope)?,
        realm: list::parse_optional_string_query_param(uri, "realm"),
        area: list::parse_optional_string_query_param(uri, "area"),
        resource: list::parse_optional_string_query_param(uri, "resource"),
        owner: list::parse_optional_string_query_param(uri, "owner"),
        state: list::parse_optional_string_query_param(uri, "state"),
        limit: parse_admin_record_limit(uri)?,
    })
}

fn build_rpc_call_observation_request(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<list::RpcCallObservationRequest, HttpError> {
    Ok(list::RpcCallObservationRequest {
        family: require_family_for_search(uri, principal, scope)?,
        realm: list::parse_optional_string_query_param(uri, "realm"),
        area: list::parse_optional_string_query_param(uri, "area"),
        resource: list::parse_optional_string_query_param(uri, "resource"),
        operation: list::parse_optional_string_query_param(uri, "operation"),
        query: list::parse_optional_string_query_param(uri, "q")
            .or_else(|| list::parse_optional_string_query_param(uri, "correlation_id")),
        limit: parse_admin_record_limit(uri)?,
    })
}

fn build_schedule_execution_observation_request(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<list::ScheduleExecutionObservationRequest, HttpError> {
    let offset = parse_optional_u64_param(uri, "offset")?.unwrap_or_default();
    Ok(list::ScheduleExecutionObservationRequest {
        family: require_family_for_search(uri, principal, scope)?,
        operation: list::parse_optional_string_query_param(uri, "operation"),
        offset: usize::try_from(offset).map_err(|_| {
            Box::new(error_response(
                StatusCode::BAD_REQUEST,
                "Query parameter 'offset' is too large",
            ))
        })?,
        limit: parse_admin_record_limit(uri)?,
    })
}

fn build_schedule_missed_observation_request(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    scope: AdminFamilyScope,
) -> Result<list::ScheduleMissedObservationRequest, HttpError> {
    Ok(list::ScheduleMissedObservationRequest {
        family: require_family_for_search(uri, principal, scope)?,
        realm: list::parse_optional_string_query_param(uri, "realm"),
        area: list::parse_optional_string_query_param(uri, "area"),
        resource: list::parse_optional_string_query_param(uri, "resource"),
        operation: list::parse_optional_string_query_param(uri, "operation"),
        limit: parse_admin_record_limit(uri)?,
    })
}
