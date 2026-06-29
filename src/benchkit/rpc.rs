//! RPC benchmarking helpers

use crate::domains::rpc::RpcRouteActor;
use crate::runtime::actor::Context;
use crate::runtime::router::Router;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

/// Create an RPC context for benchmarking
///
/// Creates a minimal context with a router suitable for RPC benchmarks.
///
/// # Arguments
/// * `route_str` - The route string for the RPC endpoint
#[must_use]
pub fn create_bench_rpc_context(route_str: &str) -> Context<RpcRouteActor> {
    let router = Arc::new(Router::new());
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new(route_str));
    Context::new(addr, router)
}
