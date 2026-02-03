//! Kubernetes probe handlers (liveness, readiness, startup)

use crate::boot::Runtime;
use hyper::{Body, Response, StatusCode};
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
    pub domains_initialized: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartupStatus {
    pub status: &'static str,
    pub startup_time_seconds: f64,
}

/// Liveness probe - is the application alive?
/// Returns 503 only if deadlocked/panicked
pub async fn handle_liveness() -> Result<Response<Body>, Infallible> {
    let response = HealthStatus { status: "ok" };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

/// Readiness probe - is the application ready to accept traffic?
pub async fn handle_readiness(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let storage_ready = runtime.is_storage_ready();
    let domains_ready = runtime.are_domains_ready();

    if !storage_ready || !domains_ready {
        let response = ReadyStatus {
            status: "not_ready",
            checks: CheckResults {
                storage: if storage_ready { "ok" } else { "not_ready" },
                domains_initialized: if domains_ready { "ok" } else { "not_ready" },
            },
        };

        return Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }

    let response = ReadyStatus {
        status: "ready",
        checks: CheckResults {
            storage: "ok",
            domains_initialized: "ok",
        },
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

/// Startup probe - has the application completed startup?
pub async fn handle_startup(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    if !runtime.is_startup_complete() {
        let response = HealthStatus { status: "starting" };

        return Ok(Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .header("Content-Type", "application/json")
            .body(Body::from(serde_json::to_string(&response).unwrap()))
            .unwrap());
    }

    let response = StartupStatus {
        status: "started",
        startup_time_seconds: runtime.startup_duration().as_secs_f64(),
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_string(&response).unwrap()))
        .unwrap())
}

/// Legacy health check
pub async fn handle_health() -> Result<Response<Body>, Infallible> {
    handle_liveness().await
}
