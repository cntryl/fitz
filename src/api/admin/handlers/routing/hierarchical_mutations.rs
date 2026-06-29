use super::*;

pub(super) async fn handle_hierarchical_post<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let path = req.uri().path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal) {
        Ok(parsed) => parsed,
        Err(response) => return Ok(*response),
    };

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id, "replay"]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_replay(
                req.uri(),
                runtime,
                scope,
                realm,
                area,
                resource,
                message_id,
            )
        }
        _ => Ok(super::not_found()),
    }
}

pub(super) async fn handle_hierarchical_delete<B>(
    req: &hyper::Request<B>,
    runtime: Arc<Runtime>,
    principal: &AdminPrincipal,
) -> Result<Response, Infallible> {
    let path = req.uri().path();
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal) {
        Ok(parsed) => parsed,
        Err(response) => return Ok(*response),
    };

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_purge(
                req.uri(),
                runtime,
                scope,
                realm,
                area,
                resource,
                message_id,
            )
        }
        _ => Ok(super::not_found()),
    }
}
