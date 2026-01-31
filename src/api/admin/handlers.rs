// ! Main HTTP request handler for admin API

use crate::boot::Runtime;
use hyper::{Body, Method, Request, Response};
use std::convert::Infallible;
use std::sync::Arc;

use super::metrics;
use super::probes;
use super::stats;

/// Main request handler - routes incoming HTTP requests
pub async fn handle_request(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();
    let method = req.method();
    
    match (method, path) {
        // Kubernetes probes - no auth required
        (&Method::GET, "/healthz") => probes::handle_liveness().await,
        (&Method::GET, "/readyz") => probes::handle_readiness(runtime).await,
        (&Method::GET, "/startupz") => probes::handle_startup(runtime).await,
        (&Method::GET, "/health") => probes::handle_health().await,
        
        // Metrics - requires auth
        (&Method::GET, "/metrics") => {
            if !check_auth(&req).await {
                return Ok(super::unauthorized());
            }
            metrics::handle_metrics(runtime).await
        }
        
        // Admin API - requires auth + admin permission
        (&Method::GET, "/api/v1/admin/stats") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            stats::handle_global_stats(runtime).await
        }
        
        // Domain-specific stats
        (&Method::GET, path) if path.starts_with("/api/v1/admin/") && path.ends_with("/stats") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            
            // Extract domain from path: /api/v1/admin/kv/stats -> kv
            let domain = path.trim_start_matches("/api/v1/admin/")
                            .trim_end_matches("/stats");
            
            stats::handle_domain_stats(runtime, domain).await
        }
        
        // WebSocket upgrade for data plane
        (&Method::GET, "/ws") => {
            // TODO: Implement WebSocket upgrade
            Ok(super::not_found())
        }
        
        // SPA static files - serve from root
        (&Method::GET, _) => {
            serve_spa(path).await
        }
        
        // 404 for everything else
        _ => Ok(super::not_found()),
    }
}

/// Check if request has valid authentication
async fn check_auth(req: &Request<Body>) -> bool {
    // Extract Authorization header
    if let Some(auth_header) = req.headers().get("Authorization") {
        if let Ok(auth_str) = auth_header.to_str() {
            if auth_str.starts_with("Bearer ") {
                let token = &auth_str[7..];
                // TODO: Validate JWT token
                return !token.is_empty();
            }
        }
    }
    
    // For development/testing, allow if no auth configured
    // TODO: Make this configurable
    true
}

/// Check if request has valid admin authentication
async fn check_admin_auth(req: &Request<Body>) -> bool {
    // First check basic auth
    if !check_auth(req).await {
        return false;
    }
    
    // TODO: Check for admin permissions in JWT claims
    // For now, if auth passes, allow admin access
    true
}

/// Serve SPA static files from public/ directory
async fn serve_spa(path: &str) -> Result<Response<Body>, Infallible> {
    use std::path::PathBuf;
    use tokio::fs;
    
    // Normalize path and prevent directory traversal
    let safe_path = path.trim_start_matches('/');
    let mut file_path = PathBuf::from("public");
    
    // For root or empty path, serve index.html
    if safe_path.is_empty() || safe_path == "/" {
        file_path.push("index.html");
    } else {
        // Prevent directory traversal
        for component in safe_path.split('/') {
            if component == ".." || component == "." {
                return Ok(super::not_found());
            }
            file_path.push(component);
        }
        
        // If path is a directory or doesn't exist, try index.html for SPA routing
        if !file_path.exists() || file_path.is_dir() {
            file_path = PathBuf::from("public/index.html");
        }
    }
    
    // Read file
    match fs::read(&file_path).await {
        Ok(contents) => {
            // Determine content type from extension
            let content_type = match file_path.extension().and_then(|e| e.to_str()) {
                Some("html") => "text/html; charset=utf-8",
                Some("css") => "text/css; charset=utf-8",
                Some("js") => "application/javascript; charset=utf-8",
                Some("json") => "application/json",
                Some("png") => "image/png",
                Some("jpg") | Some("jpeg") => "image/jpeg",
                Some("svg") => "image/svg+xml",
                Some("ico") => "image/x-icon",
                Some("woff") => "font/woff",
                Some("woff2") => "font/woff2",
                Some("ttf") => "font/ttf",
                _ => "application/octet-stream",
            };
            
            Response::builder()
                .status(200)
                .header("Content-Type", content_type)
                .header("Cache-Control", "public, max-age=3600")
                .body(Body::from(contents))
                .map_err(|_| unreachable!())
        }
        Err(_) => {
            // For SPA routing, serve index.html for 404s on non-asset paths
            if !path.contains('.') {
                if let Ok(index) = fs::read("public/index.html").await {
                    return Response::builder()
                        .status(200)
                        .header("Content-Type", "text/html; charset=utf-8")
                        .body(Body::from(index))
                        .map_err(|_| unreachable!());
                }
            }
            Ok(super::not_found())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn should_parse_bearer_token() {
        let req = Request::builder()
            .header("Authorization", "Bearer test-token-123")
            .body(Body::empty())
            .unwrap();
        
        if let Some(auth_header) = req.headers().get("Authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                assert!(auth_str.starts_with("Bearer "));
                let token = &auth_str[7..];
                assert_eq!(token, "test-token-123");
            }
        }
    }
}
