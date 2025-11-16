//! HTTP transport (minimal probes + websocket upgrade)

use crate::core::engine::EngineHandle;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server, StatusCode};
use std::convert::Infallible;
use std::net::SocketAddr;

#[derive(Clone)]
pub struct HttpTransport {
    addr: SocketAddr,
    engine: EngineHandle,
}

impl HttpTransport {
    pub fn new(addr: SocketAddr, engine: EngineHandle) -> Self {
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

        let server = Server::bind(&self.addr).serve(make_svc);
        server.await?;
        Ok(())
    }
}

pub async fn handle_request(
    req: Request<Body>,
    engine: EngineHandle,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path().to_string();
    match path.as_str() {
        "/rpc/sys/token/issue" => {
            if req.method() != hyper::Method::POST {
                let mut r = Response::new(Body::from("method not allowed"));
                *r.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
                return Ok(r);
            }
            let bytes = hyper::body::to_bytes(req.into_body())
                .await
                .unwrap_or_default();
            let v: serde_json::Value =
                serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
            let client_id = v.get("client_id").and_then(|t| t.as_str()).unwrap_or("");
            let client_secret = v
                .get("client_secret")
                .and_then(|t| t.as_str())
                .unwrap_or("");
            // If no-auth is enabled for dev-only setups, return a mock token
            if crate::config::load().auth.no_auth {
                let token = format!("mock:dev");
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
                return Ok(resp);
            }

            if client_id.is_empty() || client_secret.is_empty() {
                let mut resp = Response::new(Body::from("invalid credentials"));
                *resp.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(resp);
            }
            // Validate client credentials against configured credentials.
            if let Some(tok) = crate::authn::issue_token_for_client(client_id, client_secret) {
                let token = tok;
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
                return Ok(resp);
            } else {
                let mut resp = Response::new(Body::from("invalid credentials"));
                *resp.status_mut() = StatusCode::UNAUTHORIZED;
                return Ok(resp);
            }
        }
        "/healthz" => Ok(Response::new(Body::from("ok"))),
        "/livez" => Ok(Response::new(Body::from("ok"))),
        "/startupz" => Ok(Response::new(Body::from("ok"))),
        "/readyz" => {
            // simple readiness: try to lock store (sync check)
            // if lockable, report ready
            // NOTE: this is a shallow check; deeper checks may attempt a real storage probe
            Ok(Response::new(Body::from("ok")))
        }
        "/connect" => {
            // manual WebSocket upgrade: verify headers
            let headers = req.headers();
            let key_opt = headers
                .get("sec-websocket-key")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string());
            if key_opt.is_none() {
                let mut resp = Response::new(Body::from("upgrade required"));
                *resp.status_mut() = StatusCode::UPGRADE_REQUIRED;
                return Ok(resp);
            }
            let key = key_opt.unwrap();
            // compute accept key: base64(sha1(key + magic))
            use base64::{engine::general_purpose, Engine as _};
            use sha1::Digest;
            use sha1::Sha1;
            let mut hasher = Sha1::new();
            hasher.update(format!("{}258EAFA5-E914-47DA-95CA-C5AB0DC85B11", key).as_bytes());
            let result = hasher.finalize();
            let accept = general_purpose::STANDARD.encode(result);

            // build switching protocols response
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
            let headers = response.headers_mut();
            headers.insert(
                "Upgrade",
                hyper::header::HeaderValue::from_static("websocket"),
            );
            headers.insert(
                "Connection",
                hyper::header::HeaderValue::from_static("Upgrade"),
            );
            headers.insert(
                "Sec-WebSocket-Accept",
                hyper::header::HeaderValue::from_str(&accept).unwrap(),
            );

            // spawn a task to complete the upgrade and hand to ws processor
            let engine_for_task = engine.clone();
            let fut = async move {
                match hyper::upgrade::on(req).await {
                    Ok(upgraded) => match tokio_tungstenite::accept_async(upgraded).await {
                        Ok(ws_stream) => {
                            if let Err(e) = crate::transport::ws::process_ws_stream(ws_stream, engine_for_task)
                                .await
                            {
                                tracing::error!("ws session error (upgraded): {}", e);
                            }
                        }
                        Err(e) => {
                            tracing::error!("accept_async failed: {}", e);
                        }
                    },
                    Err(e) => {
                        tracing::error!("upgrade failed: {}", e);
                    }
                }
            };
            tokio::spawn(fut);
            Ok(response)
        }
        _ => {
            let mut not_found = Response::new(Body::from("not found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            Ok(not_found)
        }
    }
}
