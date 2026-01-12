//! Lease test helpers

use crate::domains::lease::LeaseActor;
use crate::runtime::actor::Context;
use crate::runtime::router::Router;
use crate::runtime::routing::{Route, RouteAddress, RouteFamily};
use std::sync::Arc;

/// Create a lease actor context for testing
///
/// # Arguments
/// * `route_str` - Optional custom route (defaults to standard lease route)
///
/// # Example
/// ```ignore
/// let ctx = create_test_lease_context(None);
/// let ctx = create_test_lease_context(Some("lease://realm/locks/test/acquire"));
/// ```
pub fn create_test_lease_context(route_str: Option<&str>) -> Context<LeaseActor> {
    let router = Arc::new(Router::new());
    let route = route_str.unwrap_or("lease://realm/locks/test/acquire");
    let addr = RouteAddress::new(RouteFamily::new(1), Route::new(route));
    Context::new(addr, router)
}
