//! Admin REST API module
//!
//! Provides HTTP endpoints for observability, health checks, and operational control.
//! All endpoints coexist with data plane on same port (path-based routing).

mod assets;
pub mod auth;
pub mod handlers;
mod list;
mod metrics;
mod probes;
pub(crate) mod read_model;
mod stats;
pub(crate) mod troubleshooting;

pub(crate) use stats::{build_global_stats, build_global_troubleshooting};

pub use handlers::handle_request;
pub use list::*;
pub use probes::{HealthStatus, ReadyStatus, StartupStatus};

use crate::api::http::{Body, Response};
use hyper::StatusCode;
use serde::Serialize;
use std::convert::Infallible;

/// Helper to create JSON responses
pub(crate) fn json_response<T: Serialize>(data: T) -> Result<Response, Infallible> {
    json_response_with_status(StatusCode::OK, data)
}

pub(crate) fn json_response_with_status<T: Serialize>(
    status: StatusCode,
    data: T,
) -> Result<Response, Infallible> {
    match serde_json::to_string(&data) {
        Ok(json) => Ok(hyper::http::Response::builder()
            .status(status)
            .header("Content-Type", "application/json")
            .body(Body::from(json))
            .unwrap()),
        Err(_) => Ok(hyper::http::Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(Body::from("Failed to serialize response"))
            .unwrap()),
    }
}

pub(crate) fn error_response(status: StatusCode, message: &str) -> Response {
    hyper::http::Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Body::from(format!(r#"{{"error":"{}"}}"#, message)))
        .unwrap()
}

/// Helper to create not found response
pub(crate) fn not_found() -> Response {
    error_response(StatusCode::NOT_FOUND, "Not Found")
}
