// ! Main HTTP request handler for admin API

mod hierarchical_get;
mod hierarchical_mutations;

use super::auth_and_mutations::{
    handle_queue_dead_letter_purge, handle_queue_dead_letter_replay, handle_runtime_drain,
    parse_domain_path, parse_event_limit, parse_optional_allowed_family_param,
    parse_optional_u64_param, parse_required_string_query_param, require_admin,
    require_concrete_route_family, require_data_plane_ready, require_same_origin,
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
    Legacy,
    All,
    Family(u64),
}

impl AdminFamilyScope {
    fn filter(self) -> Option<u64> {
        match self {
            Self::Family(family) => Some(family),
            Self::Legacy | Self::All => None,
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

        (Method::POST, "/api/v1/session") => handle_login(req, &runtime).await,
        (Method::GET, "/api/v1/session") => handle_current_session(req, &runtime).await,
        (Method::DELETE, "/api/v1/session") => handle_logout(&req, &runtime).await,
        (Method::GET, "/api/v1/features") => handle_features(&runtime).await,

        (Method::POST, "/api/v1/runtime/drain") => {
            if let Err(response) = require_admin_with_origin(&req, &runtime) {
                return Ok(*response);
            }
            handle_runtime_drain(runtime).await
        }

        (Method::GET, "/metrics") => {
            if let Err(response) = require_admin_only(&req, &runtime) {
                return Ok(*response);
            }
            Ok(metrics::handle_metrics(runtime.as_ref()))
        }

        (Method::GET, "/api/v1/sessions") => {
            if let Err(response) = require_admin_only(&req, &runtime) {
                return Ok(*response);
            }
            Ok(list::list_sessions(runtime.as_ref()))
        }

        (Method::GET, path) if path.starts_with("/api/v1/sessions/") => Ok(super::not_found()),

        (Method::GET, "/api/v1/stats") => {
            if let Err(response) = require_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(stats::handle_global_stats(runtime.as_ref()))
        }

        (Method::GET, "/api/v1/topology") => {
            if let Err(response) = require_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(topology::handle_topology(runtime.as_ref()))
        }

        (Method::GET, "/api/v1/troubleshooting") => {
            if let Err(response) = require_admin_and_ready(&req, &runtime) {
                return Ok(*response);
            }
            Ok(stats::handle_global_troubleshooting(runtime.as_ref()))
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

fn require_admin_only<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_admin(req, runtime).map(|_| ())
}

fn require_admin_and_ready<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_admin(req, runtime)?;
    require_data_plane_ready(runtime)
}

fn require_admin_with_origin<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_admin(req, runtime)?;
    require_same_origin(req, runtime)
}

fn require_origin_and_ready<B>(
    req: &hyper::Request<B>,
    runtime: &Arc<Runtime>,
) -> Result<(), Box<Response>> {
    require_same_origin(req, runtime)?;
    require_data_plane_ready(runtime)
}
