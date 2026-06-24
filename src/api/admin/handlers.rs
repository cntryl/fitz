// ! Main HTTP request handler for admin API

use crate::api::admin::auth::{self, AdminPrincipal, AuthFailure, SessionResponse};
use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use crate::runtime::routing::RouteFamily;
use hyper::{Method, StatusCode};
use serde::Serialize;
use std::convert::Infallible;
use std::sync::Arc;

use super::list;
use super::metrics;
use super::probes;
use super::search;
use super::stats;
use super::topology;

#[derive(Debug, Clone, Serialize)]
struct AdminFeaturesResponse {
    admin_auth_required: bool,
    admin_auth_mode: &'static str,
    route_families: Vec<String>,
    route_families_wildcard: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RuntimeDrainResponse {
    lifecycle_state: &'static str,
    active_sessions: usize,
    drain_grace_seconds: u64,
    drain_started_epoch_ms: Option<u64>,
    drain_deadline_epoch_ms: Option<u64>,
    close_reason: String,
}

pub async fn handle_request<B>(
    req: hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible>
where
    B: hyper::body::Body + Send,
{
    let assume_external_tls = runtime.assume_external_tls();
    let response = handle_request_inner(req, runtime).await?;
    Ok(super::with_browser_security_headers(
        response,
        assume_external_tls,
    ))
}

async fn handle_request_inner<B>(
    req: hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible>
where
    B: hyper::body::Body + Send,
{
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    match (method, path.as_str()) {
        (Method::GET, "/livez") => probes::handle_liveness().await,
        (Method::GET, "/targetz") => probes::handle_targetz(runtime).await,
        (Method::GET, "/healthz") => probes::handle_healthz(runtime).await,
        (Method::GET, "/readyz") => probes::handle_readiness(runtime).await,
        (Method::GET, "/startupz") => probes::handle_startup(runtime).await,
        (Method::GET, "/health") => probes::handle_health(runtime).await,

        (Method::POST, "/api/v1/session") => handle_login(req, runtime).await,
        (Method::GET, "/api/v1/session") => handle_current_session(req, runtime).await,
        (Method::DELETE, "/api/v1/session") => handle_logout(&req, runtime).await,
        (Method::GET, "/api/v1/features") => handle_features(runtime).await,

        (Method::POST, "/api/v1/runtime/drain") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_same_origin(&req, &runtime) {
                return Ok(*response);
            }
            handle_runtime_drain(runtime).await
        }

        (Method::GET, "/metrics") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            metrics::handle_metrics(runtime).await
        }

        (Method::GET, "/api/v1/sessions") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            list::list_sessions(runtime).await
        }

        (Method::GET, "/api/v1/stats") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            stats::handle_global_stats(runtime).await
        }

        (Method::GET, "/api/v1/topology") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            topology::handle_topology(runtime).await
        }

        (Method::GET, "/api/v1/troubleshooting") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            stats::handle_global_troubleshooting(runtime).await
        }

        (Method::GET, "/api/v1/search") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            search::handle_search(req.uri(), runtime, &principal).await
        }

        (Method::GET, path) if path.starts_with("/api/v1/") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            handle_hierarchical_get(req.uri(), runtime, &principal).await
        }

        (Method::POST, path) if path.starts_with("/api/v1/") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_same_origin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            handle_hierarchical_post(&req, runtime).await
        }

        (Method::DELETE, path) if path.starts_with("/api/v1/") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_same_origin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            handle_hierarchical_delete(&req, runtime).await
        }

        (Method::GET, _) => super::assets::serve_request(&req),
        _ => Ok(super::not_found()),
    }
}

async fn handle_hierarchical_get(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let path = uri.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 4 || segments[0] != "api" || segments[1] != "v1" {
        return Ok(super::not_found());
    }

    let scheme = segments[2];
    let tail = &segments[3..];

    match tail {
        ["stats"] => stats::handle_domain_stats(runtime, scheme).await,
        ["search"] if scheme == "stream" => {
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
        ["realms"] => handle_realms_collection(scheme, runtime),
        ["realms", realm, "watermarks"] if scheme == "stream" => {
            super::json_response(list::stream_realm_watermark_detail(runtime.as_ref(), realm))
        }
        ["realms", realm] => super::json_response(list::RealmDetail {
            realm: (*realm).to_string(),
        }),
        ["realms", realm, "areas"] => handle_areas_collection(scheme, runtime, realm),
        ["realms", realm, "areas", area, "watermarks"] if scheme == "stream" => {
            super::json_response(list::stream_area_watermark_detail(
                runtime.as_ref(),
                realm,
                area,
            ))
        }
        ["realms", realm, "areas", area] => super::json_response(list::AreaDetail {
            realm: (*realm).to_string(),
            area: (*area).to_string(),
        }),
        ["realms", realm, "areas", area, "resources"] => {
            handle_resources_collection(scheme, runtime, realm, area)
        }
        ["realms", realm, "areas", area, "resources", resource] => {
            let family = if scheme == "queue" {
                match parse_optional_queue_family(uri) {
                    Ok(family) => family,
                    Err(response) => return Ok(*response),
                }
            } else {
                None
            };
            handle_resource_detail(scheme, runtime, realm, area, resource, family)
        }
        ["realms", realm, "areas", area, "resources", resource, "events"] => {
            let family = if scheme == "queue" {
                match parse_optional_queue_family(uri) {
                    Ok(family) => family,
                    Err(response) => return Ok(*response),
                }
            } else {
                None
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
                "kv" => list::kv_events_for_resource(runtime, &path, limit).await,
                "queue" => list::queue_events_for_resource(runtime, &path, family, limit).await,
                "stream" => list::stream_events_for_resource(runtime, &path, limit).await,
                "lease" => list::lease_events_for_resource(runtime, &path, limit).await,
                "schedule" => list::schedule_events_for_resource(runtime, &path, limit).await,
                "notice" => list::notice_events_for_resource(runtime, &path, limit).await,
                "rpc" => list::rpc_events_for_resource(runtime, &path, limit).await,
                _ => Ok(super::not_found()),
            }
        }
        ["realms", realm, "areas", area, "resources", resource, "records"]
            if scheme == "stream" =>
        {
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = if scheme == "queue" {
                match parse_optional_u64_param(uri, "family") {
                    Ok(family) => family,
                    Err(response) => return Ok(*response),
                }
            } else {
                None
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
            let against_family = if scheme == "queue" {
                match parse_optional_u64_param(uri, "against_family") {
                    Ok(family) => family,
                    Err(response) => return Ok(*response),
                }
            } else {
                None
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
                "kv" => list::kv_compare_detail(runtime.as_ref(), &path, &against_path),
                "queue" => list::queue_compare_detail(
                    runtime.as_ref(),
                    &path,
                    family,
                    &against_path,
                    against_family,
                ),
                "stream" => list::stream_compare_detail(runtime.as_ref(), &path, &against_path),
                "lease" => list::lease_compare_detail(runtime.as_ref(), &path, &against_path),
                "schedule" => list::schedule_compare_detail(runtime.as_ref(), &path, &against_path),
                "notice" => list::notice_compare_detail(runtime.as_ref(), &path, &against_path),
                "rpc" => list::rpc_compare_detail(runtime.as_ref(), &path, &against_path),
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
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "value"] if scheme == "kv" => {
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match require_allowed_route_family(uri, principal) {
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
            let family = match parse_optional_queue_family(uri) {
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
            let family = match parse_optional_queue_family(uri) {
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
            )
            .await
        }
        ["pending"] if scheme == "rpc" => list::rpc_pending(runtime, None).await,
        _ => Ok(super::not_found()),
    }
}

async fn handle_hierarchical_post<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible> {
    let path = req.uri().path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 4 || segments[0] != "api" || segments[1] != "v1" {
        return Ok(super::not_found());
    }

    let scheme = segments[2];
    let tail = &segments[3..];

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id, "replay"]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_replay(req.uri(), runtime, realm, area, resource, message_id)
        }
        _ => Ok(super::not_found()),
    }
}

async fn handle_hierarchical_delete<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible> {
    let path = req.uri().path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 4 || segments[0] != "api" || segments[1] != "v1" {
        return Ok(super::not_found());
    }

    let scheme = segments[2];
    let tail = &segments[3..];

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_purge(req.uri(), runtime, realm, area, resource, message_id)
        }
        _ => Ok(super::not_found()),
    }
}

fn handle_realms_collection(scheme: &str, runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_realms(&resources))
}

fn handle_areas_collection(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
) -> Result<Response, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_areas(&resources, realm))
}

fn handle_resources_collection(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
) -> Result<Response, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_resources(&resources, realm, area))
}

fn handle_resource_detail(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
    resource: &str,
    queue_family: Option<u64>,
) -> Result<Response, Infallible> {
    let path = list::ResourcePath {
        realm,
        area,
        resource,
    };

    match scheme {
        "kv" => super::json_response(list::kv_detail(runtime.as_ref(), &path)),
        "queue" => super::json_response(list::queue_detail(runtime.as_ref(), &path, queue_family)),
        "stream" => super::json_response(list::stream_detail(runtime.as_ref(), &path)),
        "lease" => super::json_response(list::lease_detail(runtime.as_ref(), &path)),
        "schedule" => super::json_response(list::schedule_detail(runtime.as_ref(), &path)),
        "notice" => super::json_response(list::notice_detail(runtime.as_ref(), &path)),
        "rpc" => super::json_response(list::OperationCollection {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            operations: vec![],
        }),
        _ => Ok(super::not_found()),
    }
}

fn resources_for_scheme(scheme: &str, runtime: &Runtime) -> Vec<list::ResourceRef> {
    match scheme {
        "kv" => list::kv_resources(runtime),
        "queue" => list::queue_resources(runtime),
        "stream" => list::stream_resources(runtime),
        "lease" => list::lease_resources(runtime),
        "schedule" => list::schedule_resources(runtime),
        "notice" => list::notice_resources(runtime),
        "rpc" => list::rpc_resources(runtime),
        _ => vec![],
    }
}

async fn handle_login<B>(
    req: hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible>
where
    B: hyper::body::Body + Send,
{
    let admin_auth = runtime.admin_auth();
    if !admin_auth.login_required() {
        return Ok(no_content_response());
    }

    if !admin_auth.is_configured() {
        return Ok(super::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Admin authentication is not configured",
        ));
    }

    if let Err(response) = require_same_origin(&req, &runtime) {
        return Ok(*response);
    }

    let login = match auth::parse_login_request(req).await {
        Ok(login) => login,
        Err(_) => {
            return Ok(super::error_response(
                StatusCode::BAD_REQUEST,
                "Invalid login request",
            ));
        }
    };

    let principal = match admin_auth.authenticate_credentials(&login.username, &login.password) {
        Ok(principal) => principal,
        Err(err) => return Ok(auth_error_response(err)),
    };

    let cookie = match admin_auth.issue_session_cookie(&principal) {
        Ok(cookie) => cookie,
        Err(err) => return Ok(auth_error_response(err)),
    };

    Ok(auth::session_created_response(&cookie))
}

async fn handle_current_session<B>(
    req: hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible> {
    let principal = match require_admin(&req, &runtime) {
        Ok(principal) => principal,
        Err(response) => return Ok(*response),
    };

    super::json_response(SessionResponse {
        authenticated: true,
        username: principal.username,
        route_families: principal.route_family_access.route_families(),
        route_families_wildcard: principal.route_family_access.is_wildcard(),
    })
}

async fn handle_logout<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible> {
    if let Err(response) = require_same_origin(req, &runtime) {
        return Ok(*response);
    }
    let admin_auth = runtime.admin_auth();
    Ok(auth::session_deleted_response(
        &admin_auth.clear_session_cookie(),
    ))
}

async fn handle_features(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    let admin_auth = runtime.admin_auth();
    let route_family_access = if admin_auth.login_required() {
        None
    } else {
        Some(admin_auth.configured_route_family_access())
    };
    super::json_response(AdminFeaturesResponse {
        admin_auth_required: admin_auth.login_required(),
        admin_auth_mode: admin_auth.auth_mode(),
        route_families: route_family_access
            .as_ref()
            .map(|access| access.route_families())
            .unwrap_or_default(),
        route_families_wildcard: route_family_access
            .as_ref()
            .map(|access| access.is_wildcard())
            .unwrap_or(false),
    })
}

fn require_data_plane_ready(runtime: &Arc<Runtime>) -> Result<(), Box<Response>> {
    if runtime.is_ready_for_traffic() {
        return Ok(());
    }

    let response = super::error_response(StatusCode::SERVICE_UNAVAILABLE, "data plane not ready");
    Err(Box::new(response))
}

async fn handle_runtime_drain(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    runtime.begin_drain();
    super::json_response(RuntimeDrainResponse {
        lifecycle_state: runtime.lifecycle_state().as_str(),
        active_sessions: runtime.session_count(),
        drain_grace_seconds: runtime.drain_grace_seconds(),
        drain_started_epoch_ms: runtime.drain_started_epoch_ms(),
        drain_deadline_epoch_ms: runtime.drain_deadline_epoch_ms(),
        close_reason: runtime.drain_close_reason(),
    })
}

fn require_admin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<AdminPrincipal, Box<Response>> {
    runtime
        .admin_auth()
        .principal_from_request(req)
        .map_err(|err| Box::new(auth_error_response(err)))
}

fn require_same_origin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    runtime
        .admin_auth()
        .validate_same_origin(req)
        .map_err(|err| Box::new(auth_error_response(err)))
}

fn auth_error_response(err: AuthFailure) -> Response {
    super::error_response(err.status_code(), err.message())
}

fn parse_optional_queue_family(uri: &hyper::Uri) -> Result<Option<u64>, Box<Response>> {
    parse_optional_u64_param(uri, "family")
}

fn parse_optional_u64_param(uri: &hyper::Uri, key: &str) -> Result<Option<u64>, Box<Response>> {
    list::parse_optional_u64_query_param(uri, key)
        .map_err(|message| Box::new(super::error_response(StatusCode::BAD_REQUEST, &message)))
}

fn require_allowed_route_family(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
) -> Result<u64, Box<Response>> {
    let family = match parse_optional_u64_param(uri, "route_family")? {
        Some(family) => family,
        None => {
            return Err(Box::new(super::error_response(
                StatusCode::BAD_REQUEST,
                "Missing route_family query parameter",
            )));
        }
    };

    if !principal.route_family_access.allows(&family.to_string()) {
        return Err(Box::new(super::error_response(
            StatusCode::FORBIDDEN,
            "Route family is not allowed for this admin session",
        )));
    }

    Ok(family)
}

fn parse_required_string_query_param(uri: &hyper::Uri, key: &str) -> Result<String, Box<Response>> {
    match list::parse_query_params(uri)
        .get(key)
        .cloned()
        .filter(|value| !value.is_empty())
    {
        Some(value) => Ok(value),
        None => Err(Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            &format!("Missing {} query parameter", key),
        ))),
    }
}

fn parse_event_limit(uri: &hyper::Uri) -> Result<usize, Box<Response>> {
    list::parse_limit_query_param(uri, 20, 50)
        .map_err(|message| Box::new(super::error_response(StatusCode::BAD_REQUEST, &message)))
}

fn require_queue_family(uri: &hyper::Uri) -> Result<u64, Box<Response>> {
    match parse_optional_queue_family(uri)? {
        Some(family) => Ok(family),
        None => Err(Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Missing family query parameter",
        ))),
    }
}

fn parse_message_id(value: &str) -> Result<u64, Box<Response>> {
    value.parse::<u64>().map_err(|_| {
        Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Invalid message_id path parameter",
        ))
    })
}

fn handle_queue_dead_letter_replay(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
    resource: &str,
    message_id: &str,
) -> Result<Response, Infallible> {
    let family = match require_queue_family(uri) {
        Ok(family) => family,
        Err(response) => return Ok(*response),
    };
    let message_id = match parse_message_id(message_id) {
        Ok(message_id) => message_id,
        Err(response) => return Ok(*response),
    };

    match runtime.queue_replay_dead_letter(
        RouteFamily::new(family),
        realm,
        area,
        resource,
        message_id,
    ) {
        Ok(true) => Ok(no_content_response()),
        Ok(false) => Ok(super::not_found()),
        Err(message) => Ok(super::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &message,
        )),
    }
}

fn handle_queue_dead_letter_purge(
    uri: &hyper::Uri,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
    resource: &str,
    message_id: &str,
) -> Result<Response, Infallible> {
    let family = match require_queue_family(uri) {
        Ok(family) => family,
        Err(response) => return Ok(*response),
    };
    let message_id = match parse_message_id(message_id) {
        Ok(message_id) => message_id,
        Err(response) => return Ok(*response),
    };

    match runtime.queue_purge_dead_letter(
        RouteFamily::new(family),
        realm,
        area,
        resource,
        message_id,
    ) {
        Ok(true) => Ok(no_content_response()),
        Ok(false) => Ok(super::not_found()),
        Err(message) => Ok(super::error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &message,
        )),
    }
}

fn no_content_response() -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::default())
        .unwrap()
}
