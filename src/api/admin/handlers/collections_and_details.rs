use super::auth_and_mutations::{
    auth_error_response, no_content_response, require_admin, require_same_origin,
};
use super::{
    auth, list, AdminFeaturesResponse, Infallible, Response, Runtime, SessionResponse, StatusCode,
};
use std::sync::Arc;

pub(super) fn handle_realms_collection(
    scheme: &str,
    runtime: &Arc<Runtime>,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    if scheme == "queue" {
        let queues = runtime
            .queue_list_queues(None)
            .into_iter()
            .filter(|queue| family.is_none_or(|value| queue.family == value))
            .collect::<Vec<_>>();
        return super::json_response(list::collect_queue_realms(&queues));
    }

    let resources = resources_for_scheme(scheme, runtime.as_ref(), family);
    super::json_response(list::collect_realms(&resources))
}

pub(super) fn handle_areas_collection(
    scheme: &str,
    runtime: &Arc<Runtime>,
    realm: &str,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    if scheme == "queue" {
        let queues = runtime
            .queue_list_queues(Some(realm))
            .into_iter()
            .filter(|queue| family.is_none_or(|value| queue.family == value))
            .collect::<Vec<_>>();
        return super::json_response(list::collect_queue_areas(&queues, realm));
    }

    let resources = resources_for_scheme(scheme, runtime.as_ref(), family);
    super::json_response(list::collect_areas(&resources, realm))
}

pub(super) fn handle_resources_collection(
    scheme: &str,
    runtime: &Arc<Runtime>,
    realm: &str,
    area: &str,
    family: Option<u64>,
) -> Result<Response, Infallible> {
    if scheme == "queue" {
        let queues = runtime
            .queue_list_queues(Some(realm))
            .into_iter()
            .filter(|queue| family.is_none_or(|value| queue.family == value))
            .collect::<Vec<_>>();
        return super::json_response(list::collect_queue_resources(&queues, realm, area));
    }

    if scheme == "kv" {
        return super::json_response(list::collect_kv_resources(
            runtime.as_ref(),
            realm,
            area,
            family,
        ));
    }

    let resources = resources_for_scheme(scheme, runtime.as_ref(), family);
    super::json_response(list::collect_resources(&resources, realm, area))
}

pub(super) fn handle_resource_detail(
    scheme: &str,
    runtime: &Arc<Runtime>,
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
        "kv" => super::json_response(list::kv_detail(runtime.as_ref(), &path, queue_family)),
        "queue" => super::json_response(list::queue_detail(runtime.as_ref(), &path, queue_family)),
        "stream" => {
            super::json_response(list::stream_detail(runtime.as_ref(), &path, queue_family))
        }
        "lease" => super::json_response(list::lease_detail(runtime.as_ref(), &path, queue_family)),
        "schedule" => {
            super::json_response(list::schedule_detail(runtime.as_ref(), &path, queue_family))
        }
        "notice" => {
            super::json_response(list::notice_detail(runtime.as_ref(), &path, queue_family))
        }
        "rpc" => super::json_response(list::OperationCollection {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            operations: vec![],
        }),
        _ => Ok(super::not_found()),
    }
}

pub(super) fn resources_for_scheme(
    scheme: &str,
    runtime: &Runtime,
    family: Option<u64>,
) -> Vec<list::ResourceRef> {
    match scheme {
        "kv" => list::kv_resources(runtime, family),
        "queue" => list::queue_resources(runtime, family),
        "stream" => list::stream_resources(runtime, family),
        "lease" => list::lease_resources(runtime, family),
        "schedule" => list::schedule_resources(runtime, family),
        "notice" => list::notice_resources(runtime, family),
        "rpc" => list::rpc_resources(runtime, family),
        _ => vec![],
    }
}

pub(super) async fn handle_login<B>(
    req: hyper::Request<B>,
    runtime: &Arc<Runtime>,
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

    if let Err(response) = require_same_origin(&req, runtime) {
        return Ok(*response);
    }

    let Ok(login) = auth::parse_login_request(req).await else {
        return Ok(super::error_response(
            StatusCode::BAD_REQUEST,
            "Invalid login request",
        ));
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

pub(super) async fn handle_current_session<B>(
    req: hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<Response, Infallible> {
    let principal = match require_admin(&req, runtime) {
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

pub(super) async fn handle_logout<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<Response, Infallible> {
    if let Err(response) = require_same_origin(req, runtime) {
        return Ok(*response);
    }
    let admin_auth = runtime.admin_auth();
    Ok(auth::session_deleted_response(
        &admin_auth.clear_session_cookie(),
    ))
}

pub(super) async fn handle_features(runtime: &Arc<Runtime>) -> Result<Response, Infallible> {
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
            .map(crate::api::admin::auth::AdminRouteFamilyAccess::route_families)
            .unwrap_or_default(),
        route_families_wildcard: route_family_access
            .as_ref()
            .is_some_and(crate::api::admin::auth::AdminRouteFamilyAccess::is_wildcard),
    })
}
