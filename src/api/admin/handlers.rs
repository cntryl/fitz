// ! Main HTTP request handler for admin API

use crate::api::admin::auth::{self, AdminPrincipal, AuthFailure, SessionResponse};
use crate::boot::Runtime;
use hyper::{Body, Method, Request, Response, StatusCode};
use std::convert::Infallible;
use std::sync::Arc;

use super::list;
use super::metrics;
use super::probes;

pub async fn handle_request(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    match (method, path.as_str()) {
        (Method::GET, "/healthz") => probes::handle_liveness().await,
        (Method::GET, "/readyz") => probes::handle_readiness(runtime).await,
        (Method::GET, "/startupz") => probes::handle_startup(runtime).await,
        (Method::GET, "/health") => probes::handle_health().await,

        (Method::POST, "/api/v1/session") => handle_login(req, runtime).await,
        (Method::GET, "/api/v1/session") => handle_current_session(req, runtime).await,
        (Method::DELETE, "/api/v1/session") => handle_logout(runtime).await,

        (Method::GET, "/metrics") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            metrics::handle_metrics(runtime).await
        }

        (Method::GET, "/api/v1/sessions") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            let params = list::parse_query_params(req.uri());
            let realm = params.get("realm").map(|s| s.as_str());
            list::list_sessions(runtime, realm).await
        }

        (Method::GET, path) if path.starts_with("/api/v1/") => {
            if let Err(response) = require_admin(&req, &runtime) {
                return Ok(*response);
            }
            handle_hierarchical_get(path, runtime).await
        }

        (Method::GET, _) => serve_spa(path.as_str()).await,
        _ => Ok(super::not_found()),
    }
}

async fn handle_hierarchical_get(
    path: &str,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let segments: Vec<&str> = path.trim_start_matches('/').split('/').collect();
    if segments.len() < 4 || segments[0] != "api" || segments[1] != "v1" {
        return Ok(super::not_found());
    }

    let scheme = segments[2];
    let tail = &segments[3..];

    match tail {
        ["realms"] => handle_realms_collection(scheme, runtime),
        ["realms", realm] => super::json_response(list::RealmDetail {
            realm: (*realm).to_string(),
        }),
        ["realms", realm, "areas"] => handle_areas_collection(scheme, runtime, realm),
        ["realms", realm, "areas", area] => super::json_response(list::AreaDetail {
            realm: (*realm).to_string(),
            area: (*area).to_string(),
        }),
        ["realms", realm, "areas", area, "resources"] => {
            handle_resources_collection(scheme, runtime, realm, area)
        }
        ["realms", realm, "areas", area, "resources", resource] => {
            handle_resource_detail(scheme, runtime, realm, area, resource)
        }
        ["realms", realm, "areas", area, "resources", resource, "transactions"]
            if scheme == "kv" =>
        {
            list::kv_transactions_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "leases"] if scheme == "queue" => {
            list::queue_leases_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "subscriptions"]
            if scheme == "notice" =>
        {
            list::notice_subscriptions_for_resource(
                runtime,
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
            )
            .await
        }
        ["realms", realm, "areas", area, "resources", resource, "operations"]
            if scheme == "rpc" =>
        {
            super::json_response(list::rpc_operations(
                runtime.as_ref(),
                &list::ResourcePath {
                    realm,
                    area,
                    resource,
                },
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation]
            if scheme == "rpc" =>
        {
            super::json_response(list::rpc_operation_detail(
                runtime.as_ref(),
                &list::RpcOperationPath {
                    realm,
                    area,
                    resource,
                    operation,
                },
            ))
        }
        ["realms", realm, "areas", area, "resources", resource, "operations", operation, "workers"]
            if scheme == "rpc" =>
        {
            list::rpc_workers_for_operation(
                runtime,
                &list::RpcOperationPath {
                    realm,
                    area,
                    resource,
                    operation,
                },
            )
            .await
        }
        ["pending"] if scheme == "rpc" => list::rpc_pending(runtime, None).await,
        _ => Ok(super::not_found()),
    }
}

fn handle_realms_collection(
    scheme: &str,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_realms(&resources))
}

fn handle_areas_collection(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
) -> Result<Response<Body>, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_areas(&resources, realm))
}

fn handle_resources_collection(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
) -> Result<Response<Body>, Infallible> {
    let resources = resources_for_scheme(scheme, runtime.as_ref());
    super::json_response(list::collect_resources(&resources, realm, area))
}

fn handle_resource_detail(
    scheme: &str,
    runtime: Arc<Runtime>,
    realm: &str,
    area: &str,
    resource: &str,
) -> Result<Response<Body>, Infallible> {
    let path = list::ResourcePath {
        realm,
        area,
        resource,
    };

    match scheme {
        "kv" => super::json_response(list::kv_detail(runtime.as_ref(), &path)),
        "queue" => super::json_response(list::queue_detail(runtime.as_ref(), &path)),
        "stream" => super::json_response(list::stream_detail(runtime.as_ref(), &path)),
        "lease" => super::json_response(list::lease_detail(runtime.as_ref(), &path)),
        "schedule" => super::json_response(list::schedule_detail(runtime.as_ref(), &path)),
        "notice" => super::json_response(list::notice_detail(runtime.as_ref(), &path)),
        "rpc" => super::json_response(list::OperationCollection {
            realm: realm.to_string(),
            area: area.to_string(),
            resource: resource.to_string(),
            operations: vec![],
        }),
        _ => Ok(super::not_found()),
    }
}

fn resources_for_scheme(scheme: &str, runtime: &Runtime) -> Vec<list::ResourceRef> {
    match scheme {
        "kv" => list::kv_resources(runtime),
        "queue" => list::queue_resources(runtime),
        "stream" => list::stream_resources(runtime),
        "lease" => list::lease_resources(runtime),
        "schedule" => list::schedule_resources(runtime),
        "notice" => list::notice_resources(runtime),
        "rpc" => list::rpc_resources(runtime),
        _ => vec![],
    }
}

async fn handle_login(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let admin_auth = runtime.admin_auth();
    if !admin_auth.is_configured() {
        return Ok(super::error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            "Admin authentication is not configured",
        ));
    }

    let login = match auth::parse_login_request(req).await {
        Ok(login) => login,
        Err(_) => {
            return Ok(super::error_response(
                StatusCode::BAD_REQUEST,
                "Invalid login request",
            ));
        }
    };

    let principal = match admin_auth.authenticate_credentials(&login.username, &login.password) {
        Ok(principal) => principal,
        Err(err) => return Ok(auth_error_response(err)),
    };

    let cookie = match admin_auth.issue_session_cookie(&principal) {
        Ok(cookie) => cookie,
        Err(err) => return Ok(auth_error_response(err)),
    };

    Ok(auth::session_created_response(&cookie))
}

async fn handle_current_session(
    req: Request<Body>,
    runtime: Arc<Runtime>,
) -> Result<Response<Body>, Infallible> {
    let principal = match require_admin(&req, &runtime) {
        Ok(principal) => principal,
        Err(response) => return Ok(*response),
    };

    super::json_response(SessionResponse {
        authenticated: true,
        username: principal.username,
    })
}

async fn handle_logout(runtime: Arc<Runtime>) -> Result<Response<Body>, Infallible> {
    let admin_auth = runtime.admin_auth();
    Ok(auth::session_deleted_response(
        &admin_auth.clear_session_cookie(),
    ))
}

fn require_admin(
    req: &Request<Body>,
    runtime: &Arc<Runtime>,
) -> Result<AdminPrincipal, Box<Response<Body>>> {
    runtime
        .admin_auth()
        .principal_from_request(req)
        .map_err(|err| Box::new(auth_error_response(err)))
}

fn auth_error_response(err: AuthFailure) -> Response<Body> {
    super::error_response(err.status_code(), err.message())
}

async fn serve_spa(path: &str) -> Result<Response<Body>, Infallible> {
    use std::path::PathBuf;
    use tokio::fs;

    let safe_path = path.trim_start_matches('/');
    let mut file_path = PathBuf::from("public");

    if safe_path.is_empty() || safe_path == "/" {
        file_path.push("index.html");
    } else {
        for component in safe_path.split('/') {
            if component == ".." || component == "." {
                return Ok(super::not_found());
            }
            file_path.push(component);
        }

        if !file_path.exists() || file_path.is_dir() {
            file_path = PathBuf::from("public/index.html");
        }
    }

    match fs::read(&file_path).await {
        Ok(contents) => {
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
