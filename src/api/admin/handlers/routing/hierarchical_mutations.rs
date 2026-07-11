use super::{
    handle_queue_dead_letter_purge, handle_queue_dead_letter_replay, parse_domain_path, Infallible,
    Response, Runtime,
};
use crate::api::admin::auth::AdminPrincipal;
use std::sync::Arc;

pub(super) async fn handle_hierarchical_post<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let Some(permit) = runtime.try_acquire_admin_blocking_permit() else {
        return Ok(super::super::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            "Admin blocking executor is saturated",
        ));
    };
    let uri = req.uri().clone();
    let principal = principal.clone();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handle_hierarchical_post_blocking(&uri, &runtime, &principal)
    })
    .await
    {
        Ok(result) => Ok(result),
        Err(error) => Ok(super::super::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            &format!("Admin blocking executor failed: {error}"),
        )),
    }
}

fn handle_hierarchical_post_blocking(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Response {
    let path = uri.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id, "replay"]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_replay(uri, runtime, scope, realm, area, resource, message_id)
        }
        _ => super::not_found(),
    }
}

pub(super) async fn handle_hierarchical_delete<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let Some(permit) = runtime.try_acquire_admin_blocking_permit() else {
        return Ok(super::super::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            "Admin blocking executor is saturated",
        ));
    };
    let uri = req.uri().clone();
    let principal = principal.clone();
    match tokio::task::spawn_blocking(move || {
        let _permit = permit;
        handle_hierarchical_delete_blocking(&uri, &runtime, &principal)
    })
    .await
    {
        Ok(result) => Ok(result),
        Err(error) => Ok(super::super::error_response(
            hyper::StatusCode::SERVICE_UNAVAILABLE,
            &format!("Admin blocking executor failed: {error}"),
        )),
    }
}

fn handle_hierarchical_delete_blocking(
    uri: &hyper::Uri,
    runtime: &Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Response {
    let path = uri.path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_purge(uri, runtime, scope, realm, area, resource, message_id)
        }
        _ => super::not_found(),
    }
}
