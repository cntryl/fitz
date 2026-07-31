use super::{
    list, AdminFamilyScope, AdminPrincipal, AuthFailure, Body, Infallible, Response, RouteFamily,
    Runtime, RuntimeDrainResponse, StatusCode,
};
use std::sync::Arc;

pub(super) fn require_data_plane_ready(runtime: &Arc<Runtime>) -> Result<(), Box<Response>> {
    if runtime.is_ready_for_traffic() {
        return Ok(());
    }

    let response = super::error_response(StatusCode::SERVICE_UNAVAILABLE, "data plane not ready");
    Err(Box::new(response))
}

pub(super) async fn handle_runtime_drain(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    runtime.begin_drain();
    Ok(super::json_response(RuntimeDrainResponse {
        lifecycle_state: runtime.lifecycle_state().as_str(),
        active_sessions: runtime.session_count(),
        drain_grace_seconds: runtime.drain_grace_seconds(),
        drain_started_epoch_ms: runtime.drain_started_epoch_ms(),
        drain_deadline_epoch_ms: runtime.drain_deadline_epoch_ms(),
        close_reason: runtime.drain_close_reason(),
    }))
}

pub(super) fn require_admin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<AdminPrincipal, Box<Response>> {
    runtime
        .admin_auth()
        .principal_from_request(req)
        .map_err(|err| Box::new(auth_error_response(err)))
}

pub(super) fn require_same_origin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    runtime
        .admin_auth()
        .validate_same_origin(req)
        .map_err(|err| Box::new(auth_error_response(err)))
}

pub(super) fn auth_error_response(err: AuthFailure) -> Response {
    super::error_response(err.status_code(), err.message())
}

pub(super) fn parse_domain_path<'a>(
    segments: &'a [&'a str],
    principal: &AdminPrincipal,
    runtime: &Arc<Runtime>,
) -> Result<(AdminFamilyScope, &'a str, &'a [&'a str]), Box<Response>> {
    if segments.len() < 3 || segments[0] != "api" || segments[1] != "v1" {
        return Err(Box::new(super::not_found()));
    }

    if segments.len() < 4 {
        return Err(Box::new(super::not_found()));
    }

    let scope = parse_provisioned_family_scope(segments[2], principal, runtime)?;
    let scheme = segments[3];
    if !is_admin_domain_scheme(scheme) {
        return Err(Box::new(super::not_found()));
    }

    Ok((scope, scheme, &segments[4..]))
}

pub(super) fn parse_provisioned_family_scope(
    value: &str,
    principal: &AdminPrincipal,
    runtime: &Arc<Runtime>,
) -> Result<AdminFamilyScope, Box<Response>> {
    let scope = parse_family_scope(value, principal)?;

    if let AdminFamilyScope::Family(family) = scope {
        let family = u32::try_from(family).map_err(|_| {
            Box::new(super::error_response(
                StatusCode::BAD_REQUEST,
                "Route family exceeds the supported u32 range",
            ))
        })?;
        if !runtime.admin_auth().is_provisioned_route_family(family) {
            return Err(Box::new(super::error_response(
                StatusCode::NOT_FOUND,
                "Route family is not provisioned",
            )));
        }
    }

    Ok(scope)
}

pub(super) fn is_admin_domain_scheme(value: &str) -> bool {
    matches!(
        value,
        "kv" | "queue" | "stream" | "lease" | "schedule" | "notice" | "rpc"
    )
}

pub(super) fn parse_family_scope(
    value: &str,
    principal: &AdminPrincipal,
) -> Result<AdminFamilyScope, Box<Response>> {
    if value == "all" {
        if principal.route_family_access.is_wildcard() {
            return Ok(AdminFamilyScope::All);
        }
        return Err(Box::new(super::error_response(
            StatusCode::FORBIDDEN,
            "Route family is not allowed for this admin session",
        )));
    }

    let family = value.parse::<u64>().map_err(|_| {
        Box::new(super::error_response(
            StatusCode::NOT_FOUND,
            "Route family path segment is required",
        ))
    })?;

    RouteFamily::try_from(family).map_err(|_| {
        Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family exceeds the supported u32 range",
        ))
    })?;

    if !principal.route_family_access.allows(&family.to_string()) {
        return Err(Box::new(super::error_response(
            StatusCode::FORBIDDEN,
            "Route family is not allowed for this admin session",
        )));
    }

    Ok(AdminFamilyScope::Family(family))
}

#[allow(clippy::unnecessary_wraps)]
pub(super) fn resource_family_filter(
    scope: AdminFamilyScope,
    _uri: &hyper::Uri,
    _scheme: &str,
) -> Result<Option<u64>, Box<Response>> {
    match scope {
        AdminFamilyScope::Family(family) => Ok(Some(family)),
        AdminFamilyScope::All => Ok(None),
    }
}

pub(super) fn require_concrete_route_family(
    scope: AdminFamilyScope,
    _uri: &hyper::Uri,
    _principal: &AdminPrincipal,
) -> Result<u64, Box<Response>> {
    match scope {
        AdminFamilyScope::Family(family) => Ok(family),
        AdminFamilyScope::All => Err(Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family path segment must be concrete for this endpoint",
        ))),
    }
}

pub(super) fn require_concrete_queue_family(
    scope: AdminFamilyScope,
    _uri: &hyper::Uri,
) -> Result<u64, Box<Response>> {
    match scope {
        AdminFamilyScope::Family(family) => Ok(family),
        AdminFamilyScope::All => Err(Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family path segment must be concrete for this endpoint",
        ))),
    }
}

pub(super) fn parse_optional_allowed_family_param(
    uri: &hyper::Uri,
    principal: &AdminPrincipal,
    key: &str,
) -> Result<Option<u64>, Box<Response>> {
    let family = parse_optional_u64_param(uri, key)?;
    if let Some(family) = family {
        validate_route_family_value(family)?;
        if !principal.route_family_access.allows(&family.to_string()) {
            return Err(Box::new(super::error_response(
                StatusCode::FORBIDDEN,
                "Route family is not allowed for this admin session",
            )));
        }
    }
    Ok(family)
}

pub(super) fn parse_optional_u64_param(
    uri: &hyper::Uri,
    key: &str,
) -> Result<Option<u64>, Box<Response>> {
    list::parse_optional_u64_query_param(uri, key)
        .map_err(|message| Box::new(super::error_response(StatusCode::BAD_REQUEST, &message)))
}

fn validate_route_family_value(family: u64) -> Result<(), Box<Response>> {
    RouteFamily::try_from(family).map(|_| ()).map_err(|_| {
        Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family exceeds the supported u32 range",
        ))
    })
}

pub(super) fn parse_required_string_query_param(
    uri: &hyper::Uri,
    key: &str,
) -> Result<String, Box<Response>> {
    match list::parse_query_params(uri)
        .get(key)
        .cloned()
        .filter(|value| !value.is_empty())
    {
        Some(value) => Ok(value),
        None => Err(Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            &format!("Missing {key} query parameter"),
        ))),
    }
}

pub(super) fn parse_event_limit(uri: &hyper::Uri) -> Result<usize, Box<Response>> {
    list::parse_limit_query_param(uri, 20, 50)
        .map_err(|message| Box::new(super::error_response(StatusCode::BAD_REQUEST, &message)))
}

pub(super) fn parse_message_id(value: &str) -> Result<u64, Box<Response>> {
    value.parse::<u64>().map_err(|_| {
        Box::new(super::error_response(
            StatusCode::BAD_REQUEST,
            "Invalid message_id path parameter",
        ))
    })
}

pub(super) fn handle_queue_dead_letter_replay(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
    message_id: &str,
) -> Response {
    let family = match require_concrete_queue_family(scope, uri) {
        Ok(family) => family,
        Err(response) => return *response,
    };
    let message_id = match parse_message_id(message_id) {
        Ok(message_id) => message_id,
        Err(response) => return *response,
    };

    let Ok(family) = RouteFamily::try_from(family) else {
        return super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family exceeds the supported u32 range",
        );
    };

    match runtime.queue_replay_dead_letter(family, realm, area, resource, message_id) {
        Ok(true) => no_content_response(),
        Ok(false) => super::not_found(),
        Err(message) => super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

pub(super) fn handle_queue_dead_letter_purge(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    scope: AdminFamilyScope,
    realm: &str,
    area: &str,
    resource: &str,
    message_id: &str,
) -> Response {
    let family = match require_concrete_queue_family(scope, uri) {
        Ok(family) => family,
        Err(response) => return *response,
    };
    let message_id = match parse_message_id(message_id) {
        Ok(message_id) => message_id,
        Err(response) => return *response,
    };

    let Ok(family) = RouteFamily::try_from(family) else {
        return super::error_response(
            StatusCode::BAD_REQUEST,
            "Route family exceeds the supported u32 range",
        );
    };

    match runtime.queue_purge_dead_letter(family, realm, area, resource, message_id) {
        Ok(true) => no_content_response(),
        Ok(false) => super::not_found(),
        Err(message) => super::error_response(StatusCode::INTERNAL_SERVER_ERROR, &message),
    }
}

pub(super) fn no_content_response() -> Response {
    hyper::http::Response::builder()
        .status(StatusCode::NO_CONTENT)
        .body(Body::default())
        .unwrap()
}
