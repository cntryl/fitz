mod auth_and_mutations;
mod collections_and_details;
mod routing;

use crate::api::admin::auth::{self, AdminPrincipal, AuthFailure, SessionResponse};
use crate::api::admin::{
    assets, error_response, json_response, list, metrics, not_found, probes, search, stats,
    topology, with_browser_security_headers,
};
use crate::api::http::{Body, Response};
use crate::boot::Runtime;
use crate::runtime::routing::RouteFamily;
use hyper::StatusCode;
pub use routing::handle_request;
use routing::{AdminFamilyScope, AdminFeaturesResponse, RuntimeDrainResponse};
use std::convert::Infallible;
