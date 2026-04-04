//! Admin REST API module
//!
//! Provides HTTP endpoints for observability, health checks, and operational control.
//! All endpoints coexist with data plane on same port (path-based routing).

pub mod auth;
pub mod handlers;
mod list;
mod metrics;
mod probes;
pub(crate) mod read_model;
mod stats;

pub use handlers::handle_request;
pub use list::*;
pub use probes::{HealthStatus, ReadyStatus, StartupStatus};

use hyper::{Body, Response, StatusCode};
use serde::Serialize;
use std::convert::Infallible;

/// Helper to create JSON responses
pub(crate) fn json_response<T: Serialize>(data: T) -> Result<Response<Body>, Infallible> {
    json_response_with_status(StatusCode::OK, data)
}

pub(crate) fn json_response_with_status<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response<Body>, Infallible> {
    match serde_json::to_string(&data) {
        Ok(json) => Ok(Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap()),
        Err(_) => Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Failed to serialize response"))
            .unwrap()),
    }
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(format!(r#"{{"error":"{}"}}"#, message)))
        .unwrap()
}

/// Helper to create not found response
pub(crate) fn not_found() -> Response<Body> {
    error_response(StatusCode::NOT_FOUND, "Not Found")
}
