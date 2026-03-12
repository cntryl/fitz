// ! Main HTTP request handler for admin API

use crate::boot::Runtime;
use hyper::{Body, Method, Request, Response};
use std::convert::Infallible;
use std::sync::Arc;

use super::list;
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
            let domain = path
                .trim_start_matches("/api/v1/admin/")
                .trim_end_matches("/stats");

            stats::handle_domain_stats(runtime, domain).await
        }

        // List endpoints - KV domain
        (&Method::GET, "/api/v1/admin/kv/transactions") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_kv_transactions(runtime, realm).await
        }

        // List endpoints - Stream domain
        (&Method::GET, "/api/v1/admin/stream/streams") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_streams(runtime, realm).await
        }

        // List endpoints - Notice domain
        (&Method::GET, "/api/v1/admin/notice/subscriptions") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            let route_pattern = params.get("route_pattern").map(|s| s.as_str());
            list::handle_list_notice_subscriptions(runtime, realm, route_pattern).await
        }

        (&Method::GET, "/api/v1/admin/notice/routes") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_notice_routes(runtime, realm).await
        }

        // List endpoints - Queue domain
        (&Method::GET, "/api/v1/admin/queue/queues") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_queues(runtime, realm).await
        }

        (&Method::GET, "/api/v1/admin/queue/leases") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_queue_leases(runtime, realm).await
        }

        // List endpoints - RPC domain
        (&Method::GET, "/api/v1/admin/rpc/workers") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_rpc_workers(runtime, realm).await
        }

        (&Method::GET, "/api/v1/admin/rpc/pending") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_rpc_pending(runtime, realm).await
        }

        // List endpoints - Lease domain
        (&Method::GET, "/api/v1/admin/lease/leases") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_leases(runtime, realm).await
        }

        // List endpoints - Schedule domain
        (&Method::GET, "/api/v1/admin/schedule/schedules") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_schedules(runtime, realm).await
        }

        // List endpoints - Sessions
        (&Method::GET, "/api/v1/admin/sessions") => {
            if !check_admin_auth(&req).await {
                return Ok(super::unauthorized());
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::handle_list_sessions(runtime, realm).await
        }

        // WebSocket upgrade for data plane
        (&Method::GET, "/ws") => Ok(Response::builder()
            .status(426)
            .header("Content-Type", "text/plain; charset=utf-8")
            .header("Upgrade", "websocket")
            .body(Body::from("WebSocket upgrade required"))
            .unwrap()),

        // SPA static files - serve from root
        (&Method::GET, _) => serve_spa(path).await,

        // 404 for everything else
        _ => Ok(super::not_found()),
    }
}

/// Check if request has valid authentication
async fn check_auth(req: &Request<Body>) -> bool {
    authenticate_request(req).await.is_ok()
}

/// Check if request has valid admin authentication
async fn check_admin_auth(req: &Request<Body>) -> bool {
    authenticate_request(req)
        .await
        .map(|claims| has_admin_access(&claims))
        .unwrap_or(false)
}

async fn authenticate_request(req: &Request<Body>) -> Result<crate::auth::Claims, String> {
    let auth_header = req
        .headers()
        .get("Authorization")
        .ok_or_else(|| "missing Authorization header".to_string())?;
    let auth_str = auth_header
        .to_str()
        .map_err(|_| "invalid Authorization header".to_string())?;
    let token = auth_str
        .strip_prefix("Bearer ")
        .ok_or_else(|| "expected Bearer token".to_string())?;

    let (_permissions, claims) = crate::auth::permissions_from_verified_jwt(token).await?;
    Ok(claims)
}

fn has_admin_access(claims: &crate::auth::Claims) -> bool {
    if claims
        .roles
        .iter()
        .any(|role| matches!(role.as_str(), "admin" | "fitz-admin" | "fitz.admin"))
    {
        return true;
    }

    let permissions = crate::session::permissions::SessionPermissions::from_permissions(
        claims.permissions.clone(),
    );
    permissions.allows(
        &crate::runtime::routing::Route::new("admin://system"),
        crate::auth::Access::Read,
    ) || permissions.allows(
        &crate::runtime::routing::Route::new("admin://system"),
        crate::auth::Access::Write,
    )
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
        // Arrange
        let req = Request::builder()
            .header("Authorization", "Bearer test-token-123")
            .body(Body::empty())
            .unwrap();

        // Act
        if let Some(auth_header) = req.headers().get("Authorization") {
            if let Ok(auth_str) = auth_header.to_str() {
                // Assert
                assert!(auth_str.starts_with("Bearer "));
                let token = &auth_str[7..];
                assert_eq!(token, "test-token-123");
            }
        }
    }
}
