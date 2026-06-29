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
    handle_areas_collection, handle_current_session, handle_features, handle_login, handle_logout,
    handle_realms_collection, handle_resource_detail, handle_resources_collection,
};
use super::{error_response, json_response, not_found};
use crate::api::admin::auth::AdminPrincipal;
use crate::api::http::Response;
use crate::boot::Runtime;
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
        (Method::GET, "/livez") => probes::handle_liveness().await,
        (Method::GET, "/targetz") => probes::handle_targetz(runtime).await,
        (Method::GET, "/healthz") => probes::handle_healthz(runtime).await,
        (Method::GET, "/readyz") => probes::handle_readiness(runtime).await,
        (Method::GET, "/startupz") => probes::handle_startup(runtime).await,
        (Method::GET, "/health") => probes::handle_health(runtime).await,

        (Method::POST, "/api/v1/session") => handle_login(req, &runtime).await,
        (Method::GET, "/api/v1/session") => handle_current_session(req, &runtime).await,
        (Method::DELETE, "/api/v1/session") => handle_logout(&req, &runtime).await,
        (Method::GET, "/api/v1/features") => handle_features(&runtime).await,

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

        (Method::GET, path) if path.starts_with("/api/v1/sessions/") => Ok(super::not_found()),

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
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_same_origin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            handle_hierarchical_post(&req, runtime, &principal).await
        }

        (Method::DELETE, path) if path.starts_with("/api/v1/") => {
            let principal = match require_admin(&req, &runtime) {
                Ok(principal) => principal,
                Err(response) => return Ok(*response),
            };
            if let Err(response) = require_same_origin(&req, &runtime) {
                return Ok(*response);
            }
            if let Err(response) = require_data_plane_ready(&runtime) {
                return Ok(*response);
            }
            handle_hierarchical_delete(&req, runtime, &principal).await
        }

        (Method::GET, _) => Ok(super::assets::serve_request(&req)),
        _ => Ok(super::not_found()),
    }
}
