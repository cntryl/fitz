//! Admin REST API module
//!
//! Provides HTTP endpoints for observability, health checks, and operational control.
//! All endpoints coexist with data plane on same port (path-based routing).

pub mod handlers;
mod list;
mod metrics;
mod probes;
mod stats;

pub use handlers::handle_request;
pub use list::*;
pub use probes::{HealthStatus, ReadyStatus, StartupStatus};
pub use stats::{DomainStats, GlobalStats};

use hyper::{Body, Response, StatusCode};
use serde::Serialize;
use std::convert::Infallible;

/// Helper to create JSON responses
pub(crate) fn json_response<T: Serialize>(data: T) -> Result<Response<Body>, Infallible> {
    match serde_json::to_string(&data) {
        Ok(json) => Ok(Response::builder()
            .status(StatusCode::OK)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap()),
        Err(_) => Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Failed to serialize response"))
            .unwrap()),
    }
}

/// Helper to create unauthorized response
pub(crate) fn unauthorized() -> Response<Body> {
    Response::builder()
        .status(StatusCode::UNAUTHORIZED)
        .header("WWW-Authenticate", "Bearer")
        .body(Body::from(r#"{"error":"Unauthorized"}"#))
        .unwrap()
}

/// Helper to create not found response
pub(crate) fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from(r#"{"error":"Not Found"}"#))
        .unwrap()
}

/// Helper to create not implemented response
pub(crate) fn not_implemented() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_IMPLEMENTED)
        .body(Body::from(r#"{"error":"Not Implemented"}"#))
        .unwrap()
}

/// Helper to create method not allowed response
#[allow(dead_code)] // TODO: Use for POST/PUT/DELETE handling
pub(crate) fn method_not_allowed() -> Response<Body> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .body(Body::from(r#"{"error":"Method Not Allowed"}"#))
        .unwrap()
}
