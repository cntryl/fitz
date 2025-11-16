//! HTTP transport (health probes + websocket upgrade)
//!
//! - Hyper handles HTTP & upgrade
//! - After upgrade, tungstenite manages WS framing
//! - WS frames go directly to EngineHandle::on_frame()
//! - Engine is 100% synchronous
//!
//! This layer is just the async->sync boundary.

use crate::core::engine::EngineHandle;
use hyper::{
    service::{make_service_fn, service_fn},
    Body, Request, Response, Server, StatusCode,
};
use std::{convert::Infallible, net::SocketAddr};

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

        Server::bind(&self.addr).serve(make_svc).await?;
        Ok(())
    }
}

pub async fn handle_request(
    req: Request<Body>,
    engine: EngineHandle,
) -> Result<Response<Body>, Infallible> {
    let path = req.uri().path();

    match path {
        "/healthz" | "/livez" | "/startupz" | "/readyz" => {
            return Ok(Response::new(Body::from("ok")));
        }

        "/rpc/sys/token/issue" => handle_token_issue(req).await,

        "/connect" => {
            // --- WebSocket Upgrade ---
            if !is_websocket_request(&req) {
                let mut r = Response::new(Body::from("upgrade required"));
                *r.status_mut() = StatusCode::UPGRADE_REQUIRED;
                return Ok(r);
            }

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
                    Ok(upgraded) => {
                        match tokio_tungstenite::accept_async(upgraded).await {
                            Ok(ws_stream) => {
                                if let Err(e) = crate::transport::ws::handle_upgraded_connection(
                                    ws_stream,
                                    engine_for_ws,
                                )
                                .await
                                {
                                    tracing::error!("ws error: {}", e);
                                }
                            }
                            Err(e) => tracing::error!("ws handshake failed: {}", e),
                        }
                    }
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

async fn handle_token_issue(
    req: Request<Body>,
) -> Result<Response<Body>, Infallible> {
    use hyper::Method;

    if req.method() != Method::POST {
        let mut r = Response::new(Body::from("method not allowed"));
        *r.status_mut() = StatusCode::METHOD_NOT_ALLOWED;
        return Ok(r);
    }

    let body = hyper::body::to_bytes(req.into_body()).await.unwrap_or_default();
    let v: serde_json::Value =
        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);

    let client_id = v.get("client_id").and_then(|s| s.as_str()).unwrap_or("");
    let client_secret = v.get("client_secret").and_then(|s| s.as_str()).unwrap_or("");

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
