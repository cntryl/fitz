//! Prometheus metrics endpoint.

mod broker;
mod collector;
mod domains;
mod rendering;

use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use hyper::StatusCode;

/// Handle /metrics endpoint (Prometheus format)
pub fn handle_metrics(runtime: &Runtime) -> Response {
    let metrics = generate_prometheus_metrics(runtime);

    hyper::http::Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4")
        .body(Body::from(metrics))
        .unwrap()
}

/// Generate Prometheus-format metrics
fn generate_prometheus_metrics(runtime: &Runtime) -> String {
    let mut output = String::new();

    broker::append_broker_metrics(&mut output, runtime);
    collector::append_observability_metrics(&mut output);
    domains::append_domain_metrics(&mut output, runtime);

    output
}

#[cfg(test)]
mod tests;
