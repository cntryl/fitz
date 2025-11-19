//! HTTP transport (health probes + websocket upgrade)
//!
//! - Hyper handles HTTP & upgrade
//! - After upgrade, tungstenite manages WS framing
//! - WS frames go directly to EngineHandle::on_frame()
//! - Engine is 100% synchronous
//!
//! This layer is just the async->sync boundary.

use crate::core::engine::EnginePool;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use std::{convert::Infallible, net::SocketAddr};

#[derive(Clone)]
pub struct HttpTransport {
    addr: SocketAddr,
    engine: EnginePool,
}

impl HttpTransport {
    pub fn new(addr: SocketAddr, engine: EnginePool) -> Self {
        Self { addr, engine }
    }

    pub async fn run(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let make_svc = make_service_fn(move |_conn| {
            let engine = self.engine.clone();
            async move {
                Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                    let engine = engine.clone();
                    async move { handle_request(req, engine).await }
                }))
            }
        });

        Server::bind(&self.addr).serve(make_svc).await?;
        Ok(())
    }
}

pub async fn handle_request(
    req: Request<Body>,
    engine: EnginePool,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();

    match path {
        "/healthz" | "/livez" | "/startupz" | "/readyz" => Ok(Response::new(Body::from("ok"))),

        "/rpc/sys/token/issue" => handle_token_issue(req).await,

        "/connect" => {
            // --- WebSocket Upgrade with JWT Authentication ---
            if !is_websocket_request(&req) {
                let mut r = Response::new(Body::from("upgrade required"));
                *r.status_mut() = StatusCode::UPGRADE_REQUIRED;
                return Ok(r);
            }

            // Extract JWT from Authorization header or Sec-WebSocket-Protocol
            let token = extract_jwt_from_request(&req);
            let (route_family, session_auth) = match token {
                Some(jwt) => {
                    // Verify JWT and extract claims
                    match verify_jwt_and_build_session(&jwt) {
                        Ok((rf, session)) => (rf, session),
                        Err(e) => {
                            tracing::warn!("JWT verification failed: {}", e);
                            let mut r = Response::new(Body::from("authentication failed"));
                            *r.status_mut() = StatusCode::UNAUTHORIZED;
                            return Ok(r);
                        }
                    }
                }
                None => {
                    let mut r = Response::new(Body::from("missing authentication token"));
                    *r.status_mut() = StatusCode::UNAUTHORIZED;
                    return Ok(r);
                }
            };

            let accept_header = ws_accept_key(&req);
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;

            {
                let headers = response.headers_mut();
                headers.insert("Upgrade", "websocket".parse().unwrap());
                headers.insert("Connection", "Upgrade".parse().unwrap());
                headers.insert("Sec-WebSocket-Accept", accept_header.parse().unwrap());
            }

            let engine_for_ws = engine.clone();
            tokio::spawn(async move {
                match hyper::upgrade::on(req).await {
                    Ok(upgraded) => match tokio_tungstenite::accept_async(upgraded).await {
                        Ok(ws_stream) => {
                            if let Err(e) = crate::transport::ws::handle_upgraded_connection(
                                ws_stream,
                                engine_for_ws,
                                session_auth,
                                route_family,
                            )
                            .await
                            {
                                tracing::error!("ws error: {}", e);
                            }
                        }
                        Err(e) => tracing::error!("ws handshake failed: {}", e),
                    },
                    Err(e) => tracing::error!("upgrade failed: {}", e),
                }
            });

            Ok(response)
        }

        _ => {
            let mut nf = Response::new(Body::from("not found"));
            *nf.status_mut() = StatusCode::NOT_FOUND;
            Ok(nf)
        }
    }
}

async fn handle_token_issue(req: Request<Body>) -> Result<Response<Body>, Infallible> {
    use hyper::Method;

    if req.method() != Method::POST {
        let mut r = Response::new(Body::from("method not allowed"));
        *r.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(r);
    }

    let body = hyper::body::to_bytes(req.into_body())
        .await
        .unwrap_or_default();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);

    let client_id = v.get("client_id").and_then(|s| s.as_str()).unwrap_or("");
    let client_secret = v
        .get("client_secret")
        .and_then(|s| s.as_str())
        .unwrap_or("");

    if crate::config::load().auth.no_auth {
        return Ok(json_ok_token("mock:dev"));
    }

    if let Some(tok) = crate::authn::issue_token_for_client(client_id, client_secret) {
        return Ok(json_ok_token(&tok));
    }

    let mut r = Response::new(Body::from("invalid credentials"));
    *r.status_mut() = StatusCode::UNAUTHORIZED;
    Ok(r)
}

fn json_ok_token(token: &str) -> Response<Body> {
    let body = serde_json::json!({
        "access_token": token,
        "token_type": "Bearer",
        "expires_in": 3600
    });

    let mut resp = Response::new(Body::from(body.to_string()));
    resp.headers_mut().insert(
        hyper::header::CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    resp
}

/// Extract JWT from request - tries Authorization header first, then WebSocket protocol
fn extract_jwt_from_request(req: &Request<Body>) -> Option<String> {
    // Try Authorization: Bearer <token> header
    if let Some(auth_header) = req.headers().get(hyper::header::AUTHORIZATION) {
        if let Ok(auth_str) = auth_header.to_str() {
            if let Some(token) = auth_str.strip_prefix("Bearer ") {
                return Some(token.to_string());
            }
        }
    }

    // Try Sec-WebSocket-Protocol: bearer,<token> or similar
    if let Some(protocol_header) = req.headers().get("Sec-WebSocket-Protocol") {
        if let Ok(protocol_str) = protocol_header.to_str() {
            // Parse "bearer,<token>" or "Bearer <token>"
            for part in protocol_str.split(',') {
                let trimmed = part.trim();
                if trimmed.starts_with("bearer ") || trimmed.starts_with("Bearer ") {
                    return Some(trimmed[7..].to_string());
                }
            }
        }
    }

    None
}

/// Verify JWT and build SessionAuth
fn verify_jwt_and_build_session(jwt: &str) -> Result<(String, crate::authz::SessionAuth), String> {
    // Parse JWT and extract claims
    let claims = crate::authz::mock_jwks::validate_mock_token(jwt)
        .ok_or_else(|| "invalid token format".to_string())?;

    // Extract route_family (realm) from claims
    let route_family = claims.aud.clone().unwrap_or_else(|| claims.sub.clone());

    // Extract subject
    let subject = claims.sub.clone();

    // Extract permissions from both scopes and roles
    let mut permissions = Vec::new();

    // Add from scope (space-separated string)
    if let Some(scope_str) = &claims.scope {
        permissions.extend(scope_str.split_whitespace().map(|s| s.to_string()));
    }

    // Add from roles (array of strings)
    if let Some(roles) = &claims.roles {
        permissions.extend(roles.iter().cloned());
    }

    // Filter permissions to only those matching expected patterns:
    // - read:scheme://realm/...
    // - write:scheme://realm/...
    // - *:scheme://realm/...
    // - scheme://realm/...
    let filtered_permissions: Vec<String> = permissions
        .into_iter()
        .filter(|perm| {
            // Check if it matches permission patterns
            perm.contains("://") ||                           // route-like: kv://...
            perm.starts_with("read:") ||                      // intent: read:...
            perm.starts_with("write:") ||                     // intent: write:...
            perm.starts_with("*:") ||                         // wildcard intent
            perm == "*" // full wildcard
        })
        .collect();

    // Build permission grants from filtered permissions
    let grants = crate::authz::PermissionGrants::from_scopes(&route_family, &filtered_permissions);

    // Create session
    let session = crate::authz::SessionAuth {
        subject,
        route_family: route_family.clone(),
        scopes: filtered_permissions,
        grants,
    };

    Ok((route_family, session))
}

fn is_websocket_request(req: &Request<Body>) -> bool {
    req.headers()
        .get("Sec-WebSocket-Key")
        .and_then(|v| v.to_str().ok())
        .is_some()
}

fn ws_accept_key(req: &Request<Body>) -> String {
    use base64::{engine::general_purpose, Engine as _};
    use sha1::{Digest, Sha1};

    let key = req
        .headers()
        .get("Sec-WebSocket-Key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    const MAGIC: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

    let mut h = Sha1::new();
    h.update(format!("{}{}", key, MAGIC).as_bytes());
    general_purpose::STANDARD.encode(h.finalize())
}
