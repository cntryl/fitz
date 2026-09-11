use super::{
    handle_queue_dead_letter_purge, handle_queue_dead_letter_replay, parse_domain_path, Infallible,
    Response, Runtime,
};
use crate::api::admin::auth::AdminPrincipal;
use crate::domains::schedule::sink::{ScheduleRunNowOutcome, ScheduleRunNowResult};
use crate::runtime::routing::RouteFamily;
use chrono::Utc;
use percent_encoding::percent_decode_str;
use serde::Serialize;
use std::borrow::Cow;
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
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal, runtime) {
        Ok(parsed) => parsed,
        Err(response) => return *response,
    };

    match tail {
        ["realms", realm, "areas", area, "resources", resource, "operations", operation, "run"]
            if scheme == "schedule" =>
        {
            let family = match scope {
                super::super::AdminFamilyScope::Family(family) => family,
                super::super::AdminFamilyScope::All => {
                    return super::super::error_response(
                        hyper::StatusCode::BAD_REQUEST,
                        "Route family path segment must be concrete for this endpoint",
                    )
                }
            };
            let decoded = [realm, area, resource, operation]
                .map(|value| decode_schedule_path_segment(value))
                .into_iter()
                .collect::<Result<Vec<_>, _>>();
            let decoded = match decoded {
                Ok(decoded) => decoded,
                Err(message) => {
                    return super::super::error_response(hyper::StatusCode::BAD_REQUEST, message)
                }
            };
            let route = format!(
                "schedule://{}/{}/{}/{}",
                decoded[0], decoded[1], decoded[2], decoded[3]
            );
            if let Err(error) =
                crate::domains::schedule::protocol::validate_concrete_schedule_route(&route)
            {
                return super::super::error_response(hyper::StatusCode::BAD_REQUEST, &error);
            }
            let result = runtime.schedule_run_now(
                RouteFamily::try_from(family).expect("validated route family"),
                route.clone(),
                std::time::Duration::from_secs(1),
            );
            match result {
                Ok(Some(result)) => {
                    let response = ScheduleRunNowResponse::from_result(family, route, result);
                    tracing::info!(
                        audit = "schedule_run_now",
                        username = %principal.username,
                        route_family = family,
                        route = %response.route,
                        outcome = %response.outcome,
                        matched_subscriptions = response.matched_subscriptions,
                        attempted_handoffs = response.attempted_handoffs,
                        accepted_handoffs = response.accepted_handoffs,
                        "admin schedule run-now"
                    );
                    super::super::json_response(response)
                }
                Ok(None) => super::super::error_response(
                    hyper::StatusCode::NOT_FOUND,
                    "Schedule definition not found",
                ),
                Err(error) if error.contains("timed out") => super::super::error_response(
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    &format!("Schedule run-now outcome may be unknown; inspect consumers before triggering again: {error}"),
                ),
                Err(error) => super::super::error_response(
                    hyper::StatusCode::SERVICE_UNAVAILABLE,
                    &error,
                ),
            }
        }
        ["realms", realm, "areas", area, "resources", resource, "dead-letters", message_id, "replay"]
            if scheme == "queue" =>
        {
            handle_queue_dead_letter_replay(uri, runtime, scope, realm, area, resource, message_id)
        }
        _ => super::not_found(),
    }
}

fn decode_schedule_path_segment(value: &str) -> Result<Cow<'_, str>, &'static str> {
    let bytes = value.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%'
            && (index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit())
        {
            return Err("Schedule route path contains invalid percent encoding");
        }
        index += if bytes[index] == b'%' { 3 } else { 1 };
    }
    percent_decode_str(value)
        .decode_utf8()
        .map_err(|_| "Schedule route path contains invalid UTF-8")
}

#[derive(Debug, Serialize)]
struct ScheduleRunNowResponse {
    route_family: u64,
    route: String,
    delivery_mode: crate::domains::schedule::ScheduleDeliveryMode,
    triggered_at: String,
    outcome: &'static str,
    matched_subscriptions: usize,
    attempted_handoffs: usize,
    accepted_handoffs: usize,
}

impl ScheduleRunNowResponse {
    fn from_result(route_family: u64, route: String, result: ScheduleRunNowResult) -> Self {
        let outcome = match result.outcome {
            ScheduleRunNowOutcome::HandoffAccepted => "handoff_accepted",
            ScheduleRunNowOutcome::NoLiveSubscriptions => "no_live_subscriptions",
            ScheduleRunNowOutcome::NoHandoffAccepted => "no_handoff_accepted",
        };
        Self {
            route_family,
            route,
            delivery_mode: result.delivery_mode,
            triggered_at: Utc::now().to_rfc3339(),
            outcome,
            matched_subscriptions: result.matched_subscriptions,
            attempted_handoffs: result.attempted_handoffs,
            accepted_handoffs: result.accepted_handoffs,
        }
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
    let (scope, scheme, tail) = match parse_domain_path(&segments, principal, runtime) {
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

#[cfg(test)]
mod tests {
    use super::decode_schedule_path_segment;

    #[test]
    fn should_decode_schedule_route_path_segments_before_lookup() {
        // Arrange
        let encoded = "run%20daily%2Bmanual";

        // Act
        let decoded = decode_schedule_path_segment(encoded);

        // Assert
        assert_eq!(decoded.as_deref(), Ok("run daily+manual"));
    }

    #[test]
    fn should_reject_invalid_schedule_route_percent_encoding() {
        // Arrange
        let encoded = "run%2";

        // Act
        let decoded = decode_schedule_path_segment(encoded);

        // Assert
        assert_eq!(
            decoded,
            Err("Schedule route path contains invalid percent encoding")
        );
    }
}
