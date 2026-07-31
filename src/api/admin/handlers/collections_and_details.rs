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
) -> Response {
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
) -> Response {
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
) -> Response {
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

    match scheme {
        "stream" => {
            return super::json_response(list::collect_stream_resources(
                runtime, realm, area, family,
            ));
        }
        "lease" => {
            return super::json_response(list::collect_lease_resources(
                runtime, realm, area, family,
            ));
        }
        "notice" => {
            return super::json_response(list::collect_notice_resources(
                runtime, realm, area, family,
            ));
        }
        "rpc" => {
            return super::json_response(list::collect_rpc_resources(runtime, realm, area, family));
        }
        "schedule" => {
            return super::json_response(list::collect_schedule_resources(
                runtime, realm, area, family,
            ));
        }
        _ => {}
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
) -> Response {
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
        _ => super::not_found(),
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

    let client = req
        .extensions()
        .get::<auth::AdminClientIp>()
        .copied()
        .unwrap_or_default();
    let Ok(login) = auth::parse_login_request(req).await else {
        return Ok(super::error_response(
            StatusCode::BAD_REQUEST,
            "Invalid login request",
        ));
    };

    let Some(permit) = runtime.try_acquire_admin_blocking_permit() else {
        return Ok(super::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Admin authentication executor is saturated",
        ));
    };
    if let Err(error) = admin_auth.begin_login_attempt(client) {
        return Ok(auth_error_response(error));
    }
    let verifier = admin_auth.clone();
    let username = login.username;
    let password = login.password;
    let verification = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        verifier.authenticate_credentials(&username, &password)
    })
    .await;
    let principal = match verification {
        Ok(Ok(principal)) => {
            admin_auth.complete_login_attempt(client, true);
            principal
        }
        Ok(Err(error)) => return Ok(auth_error_response(error)),
        Err(_) => {
            return Ok(super::error_response(
                StatusCode::SERVICE_UNAVAILABLE,
                "Admin authentication executor failed",
            ));
        }
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

    Ok(super::json_response(SessionResponse {
        authenticated: true,
        username: principal.username,
        route_families: if principal.route_family_access.is_wildcard() {
            runtime.admin_auth().provisioned_route_families()
        } else {
            principal.route_family_access.route_families()
        },
        route_families_wildcard: principal.route_family_access.is_wildcard(),
    }))
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
    let login_required = admin_auth.login_required();
    let route_family_access = if login_required {
        None
    } else {
        Some(admin_auth.configured_route_family_access())
    };
    Ok(super::json_response(AdminFeaturesResponse {
        admin_auth_required: login_required,
        admin_auth_mode: admin_auth.auth_mode(),
        route_families: if login_required {
            Vec::new()
        } else {
            admin_auth.provisioned_route_families()
        },
        route_families_wildcard: route_family_access
            .as_ref()
            .is_some_and(crate::api::admin::auth::AdminRouteFamilyAccess::is_wildcard),
    }))
}
