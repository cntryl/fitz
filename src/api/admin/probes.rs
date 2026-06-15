//! Kubernetes probe handlers (liveness, readiness, startup)

use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use hyper::StatusCode;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadyStatus {
    pub status: &'static str,
    pub checks: CheckResults,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckResults {
    pub storage: &'static str,
    pub storage_writer_lease: &'static str,
    pub domains_initialized: &'static str,
    pub auth_configuration: &'static str,
    pub startup_complete: &'static str,
    pub accepting_traffic: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupStatus {
    pub status: &'static str,
    pub startup_time_seconds: f64,
}

/// Liveness probe - is the application alive?
/// Returns 503 only if deadlocked/panicked
pub async fn handle_liveness() -> Result<Response, Infallible> {
    let response = HealthStatus { status: "ok" };

    Ok(hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

fn readiness_status(runtime: &Runtime) -> ReadyStatus {
    let storage_ready = runtime.is_storage_ready();
    let auth_ready = runtime.is_auth_config_ready();
    let domains_ready = runtime.are_domains_ready();
    let startup_complete = runtime.is_startup_complete();
    let accepting_traffic = !runtime.is_shutting_down();

    let ready =
        storage_ready && auth_ready && domains_ready && startup_complete && accepting_traffic;

    ReadyStatus {
        status: if ready { "ready" } else { "not_ready" },
        checks: CheckResults {
            storage: if storage_ready { "ok" } else { "not_ready" },
            // Storage is only marked ready after Midge opens successfully, which
            // includes holding the single-writer lease for the active engine.
            storage_writer_lease: if storage_ready { "ok" } else { "not_ready" },
            domains_initialized: if domains_ready { "ok" } else { "not_ready" },
            auth_configuration: if auth_ready { "ok" } else { "not_ready" },
            startup_complete: if startup_complete { "ok" } else { "not_ready" },
            accepting_traffic: if accepting_traffic { "ok" } else { "not_ready" },
        },
    }
}

/// Deployment-safe health probe.
///
/// This mirrors readiness so load balancers do not route traffic until the
/// broker has completed startup and can serve as the active single writer.
pub async fn handle_healthz(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    handle_readiness(runtime).await
}

/// Readiness probe - is the application ready to accept traffic?
pub async fn handle_readiness(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    let response = readiness_status(runtime.as_ref());

    if response.status != "ready" {
        return Ok(hyper::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }

    Ok(hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

/// Startup probe - has the application completed startup?
pub async fn handle_startup(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    if !runtime.is_startup_complete() {
        let response = HealthStatus { status: "starting" };

        return Ok(hyper::http::Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }

    let response = StartupStatus {
        status: "started",
        startup_time_seconds: runtime.startup_duration().as_secs_f64(),
    };

    Ok(hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

/// Legacy health check
pub async fn handle_health(runtime: Arc<Runtime>) -> Result<Response, Infallible> {
    handle_healthz(runtime).await
}
