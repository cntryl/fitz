//! Admin UI asset serving.
//!
//! This module owns the production static-asset contract for the admin SPA,
//! including lookup, MIME mapping, SPA fallback, compression negotiation, and
//! ETag handling. The request handler delegates here so the rest of the admin
//! API remains unchanged.

mod cache;
mod compression;
mod model;
mod paths;
mod response;
mod server;

use crate::api::http::Response;
use once_cell::sync::Lazy;
use std::convert::Infallible;
use std::path::Path;

const CACHE_CONTROL: &str = "public, max-age=3600";
const INDEX_PATH: &str = "index.html";
const PUBLIC_ASSET_ROOT: &str = "/app/public";
const VARY_ACCEPT_ENCODING: &str = "Accept-Encoding";

static PUBLIC_ASSET_SERVER: Lazy<server::AssetServer> =
    Lazy::new(|| server::AssetServer::new(Path::new(PUBLIC_ASSET_ROOT)));

pub(crate) fn serve_request<B>(req: &hyper::Request<B>) -> Result<Response, Infallible> {
    PUBLIC_ASSET_SERVER.serve(req.uri().path(), req.headers())
}

#[cfg(test)]
mod tests;
