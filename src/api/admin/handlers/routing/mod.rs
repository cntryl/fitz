// ! Main HTTP request handler for admin API

mod hierarchical_get;
mod hierarchical_mutations;

use super::auth_and_mutations::{
    handle_queue_dead_letter_purge, handle_queue_dead_letter_replay, handle_runtime_drain,
    parse_domain_path, parse_event_limit, parse_optional_allowed_family_param,
    parse_optional_u64_param, parse_provisioned_family_scope, parse_required_string_query_param,
    require_admin, require_concrete_route_family, require_data_plane_ready, require_same_origin,
    resource_family_filter,
};
use super::collections_and_details::{
    handle_current_session, handle_features, handle_login, handle_logout,
};
use super::{error_response, json_response, not_found};
use crate::api::http::Response;
use crate::boot::Runtime;
use hyper::Method;
use serde::Serialize;
use std::convert::Infallible;
use std::sync::Arc;

use super::list;
use super::metrics;
use super::probes;
use super::search;
use super::stats;
use super::topology;
use hierarchical_get::handle_hierarchical_get;
use hierarchical_mutations::{handle_hierarchical_delete, handle_hierarchical_post};

#[derive(Debug, Clone, Serialize)]
pub(super) struct AdminFeaturesResponse {
    pub(crate) admin_auth_required: bool,
    pub(crate) admin_auth_mode: &'static str,
    pub(crate) route_families: Vec<String>,
    pub(crate) route_families_wildcard: bool,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct RuntimeDrainResponse {
    pub(crate) lifecycle_state: &'static str,
    pub(crate) active_sessions: usize,
    pub(crate) drain_grace_seconds: u64,
    pub(crate) drain_started_epoch_ms: Option<u64>,
    pub(crate) drain_deadline_epoch_ms: Option<u64>,
    pub(crate) close_reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AdminFamilyScope {
    All,
    Family(u64),
}

impl AdminFamilyScope {
    fn filter(self) -> Option<u64> {
        match self {
            Self::Family(family) => Some(family),
            Self::All => None,
        }
    }
}

/// Handles admin HTTP requests and applies browser security headers.
///
/// # Errors
///
/// This function returns `Result` to satisfy the Hyper service boundary even
/// though the current handler path is infallible.
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

#[allow(clippy::too_many_lines)]
pub(super) async fn handle_request_inner<B>(
    req: hyper::Request<B>,
    runtime: Arc<Runtime>,
) -> Result<Response, Infallible>
where
    B: hyper::body::Body + Send,
{
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    match (method, path.as_str()) {
        (Method::GET, "/livez") => Ok(probes::handle_liveness(runtime.as_ref())),
        (Method::GET, "/targetz") => Ok(probes::handle_targetz(runtime.as_ref())),
        (Method::GET, "/healthz") => Ok(probes::handle_healthz(runtime.as_ref())),
        (Method::GET, "/readyz") => Ok(probes::handle_readiness(runtime.as_ref())),
        (Method::GET, "/startupz") => Ok(probes::handle_startup(runtime.as_ref())),
        (Method::GET, "/health") => Ok(probes::handle_health(runtime.as_ref())),
        (Method::POST, "/destroyer/failpoints/notice-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_notice_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "notice" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/queue-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_queue_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "queue" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/kv-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_kv_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "kv" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/lease-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_lease_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "lease" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/schedule-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_schedule_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "schedule" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/stream-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_stream_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "stream" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/rpc-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_rpc_actor_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domain": "rpc" }),
            ))
        }
        (Method::POST, "/destroyer/failpoints/all-domain-actor-panic") => {
            if !actor_failpoints_enabled(std::env::var("FITZ_DESTROYER_FAILPOINTS").ok().as_deref())
            {
                return Ok(not_found());
            }
            runtime.panic_all_domain_actors_for_failpoint();
            Ok(json_response(
                serde_json::json!({ "injected": true, "domains": 7 }),
            ))
        }

        (Method::POST, "/api/v1/session") => handle_login(req, &runtime).await,
        (Method::GET, "/api/v1/session") => handle_current_session(req, &runtime).await,
        (Method::DELETE, "/api/v1/session") => handle_logout(&req, &runtime).await,
        (Method::GET, "/api/v1/features") => handle_features(&runtime).await,

        // Raw Prometheus is exposed only by the dedicated metrics listener.
        // Keep the main authenticated listener from treating this path as an
        // SPA route.
        (Method::GET, "/metrics") => Ok(not_found()),

        (Method::POST, "/api/v1/runtime/drain") => {
            if let Err(response) = require_wildcard_admin_with_origin(&req, &runtime) {
                return Ok(*response);
            }
            handle_runtime_drain(runtime).await
        }

        (Method::GET, "/api/v1/sessions") => {
            if let Err(response) = require_wildcard_admin_only(&req, &runtime) {
                return Ok(*response);
            }
            Ok(list::list_sessions(runtime.as_ref()))
        }

        (Method::GET, path) if path.starts_with("/api/v1/sessions/") => Ok(super::not_found()),

        (Method::GET, "/api/v1/stats") => {
            if let Err(response) = require_wildcard_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(stats::handle_global_stats(runtime.as_ref()))
        }

        (Method::GET, "/api/v1/topology") => {
            if let Err(response) = require_wildcard_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(topology::handle_topology(runtime.as_ref()))
        }

        (Method::GET, "/api/v1/troubleshooting") => {
            if let Err(response) = require_wildcard_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(stats::handle_global_troubleshooting(runtime.as_ref()))
        }

        (Method::GET, path) if path.starts_with("/api/v1/") && path.ends_with("/metrics") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
            if segments.len() != 4 || segments[0] != "api" || segments[1] != "v1" {
                return Ok(not_found());
            }
            let scope = match parse_provisioned_family_scope(segments[2], &principal, &runtime) {
                Ok(scope) => scope,
                Err(response) => return Ok(*response),
            };
            Ok(metrics::handle_structured_metrics(
                runtime.as_ref(),
                scope.filter(),
            ))
        }

        (Method::GET, path)
            if path.starts_with("/api/v1/") && path.trim_matches('/').split('/').count() == 4 =>
        {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
            let scope = match parse_provisioned_family_scope(segments[2], &principal, &runtime) {
                Ok(scope) => scope,
                Err(response) => return Ok(*response),
            };
            match scope {
                AdminFamilyScope::All => match segments[3] {
                    "sessions" => Ok(list::list_sessions(runtime.as_ref())),
                    "stats" => Ok(stats::handle_global_stats(runtime.as_ref())),
                    "topology" => Ok(topology::handle_topology(runtime.as_ref())),
                    "troubleshooting" => Ok(stats::handle_global_troubleshooting(runtime.as_ref())),
                    _ => Ok(not_found()),
                },
                AdminFamilyScope::Family(family) => match segments[3] {
                    "sessions" => Ok(list::list_sessions_for_family(runtime.as_ref(), family)),
                    "stats" => Ok(stats::handle_family_stats(runtime.as_ref(), family)),
                    "topology" => Ok(topology::handle_family_topology(runtime.as_ref(), family)),
                    "troubleshooting" => Ok(stats::handle_family_troubleshooting(
                        runtime.as_ref(),
                        family,
                    )),
                    _ => Ok(not_found()),
                },
            }
        }

        (Method::GET, "/api/v1/search") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            Ok(search::handle_search(
                req.uri(),
                runtime.as_ref(),
                &principal,
            ))
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
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_origin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            handle_hierarchical_post(&req, runtime, &principal).await
        }

        (Method::DELETE, path) if path.starts_with("/api/v1/") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_origin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            handle_hierarchical_delete(&req, runtime, &principal).await
        }

        (Method::GET, _) => Ok(super::assets::serve_request(&req)),
        _ => Ok(super::not_found()),
    }
}

fn require_wildcard_admin_only<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    let principal = require_admin(req, runtime)?;
    if principal.route_family_access.is_wildcard() {
        Ok(())
    } else {
        Err(Box::new(super::error_response(
            hyper::StatusCode::FORBIDDEN,
            "Wildcard Route Family authority is required for this broker-global endpoint",
        )))
    }
}

fn require_wildcard_admin_and_ready<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_wildcard_admin_only(req, runtime)?;
    require_data_plane_ready(runtime)
}

fn require_wildcard_admin_with_origin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_wildcard_admin_only(req, runtime)?;
    require_same_origin(req, runtime)
}

fn require_origin_and_ready<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_same_origin(req, runtime)?;
    require_data_plane_ready(runtime)
}

fn actor_failpoints_enabled(value: Option<&str>) -> bool {
    value == Some("enabled")
}

#[cfg(test)]
mod failpoint_tests {
    use super::*;

    #[test]
    fn should_enable_actor_failpoints_only_for_exact_opt_in_value() {
        // Arrange
        // Act
        // Assert
        assert!(actor_failpoints_enabled(Some("enabled")));
        assert!(!actor_failpoints_enabled(None));
        assert!(!actor_failpoints_enabled(Some("true")));
    }
}
