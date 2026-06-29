use super::{
    drain_session_tasks, record_connection_closed, record_connection_opened, session_tasks,
    websocket::handle_websocket, ListenerHandle,
};
use crate::api::http::Request;
use crate::api::ingress::IngressConfig;
use crate::boot::{BootConfig, BootResult};
use crate::session::manager::Ingress;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

/// Spawn HTTP/WebSocket listener on configured port (binds internally)
pub async fn spawn_http_listener(
    config: &BootConfig,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
) -> BootResult<ListenerHandle> {
    let http_addr = format!("{}:{}", config.bind_addr, config.http_port);
    let http_listener = TcpListener::bind(&http_addr).await?;
    info!("HTTP/WebSocket endpoint listening on {}", http_addr);

    spawn_http_listener_with_bound_socket(
        http_listener,
        ingress,
        ingress_config,
        runtime,
        config.ws_allowed_origins.clone(),
    )
}

/// Spawn HTTP/WebSocket listener with pre-bound socket (eliminates port reallocation race)
pub fn spawn_http_listener_with_bound_socket(
    http_listener: TcpListener,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
    ws_allowed_origins: Vec<crate::api::origin::ExactOrigin>,
) -> BootResult<ListenerHandle> {
    let http_config = ingress_config.clone();
    let runtime = Arc::new(runtime);
    let ws_allowed_origins = Arc::new(ws_allowed_origins);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let join = tokio::spawn(async move {
        let connections = session_tasks();
        let websockets = session_tasks();
        let _ = ready_tx.send(());

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::info!("HTTP listener shutdown requested");
                    break;
                }
                accept_result = http_listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            record_connection_opened();

                            info!("HTTP connection from {}", peer_addr);
                            let ingress = ingress.clone();
                            let config = http_config.clone();
                            let runtime = runtime.clone();
                            let websocket_tasks = websockets.clone();
                            let ws_allowed_origins = ws_allowed_origins.clone();
                            connections.lock().await.spawn(async move {
                                if let Err(e) = handle_http_upgrade(stream, ingress, config, runtime, websocket_tasks, ws_allowed_origins).await {
                                    tracing::error!("HTTP handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("HTTP accept error: {}", e);
                        }
                    }
                }
            }
        }

        drain_session_tasks("http", vec![connections, websockets]).await;
    });

    Ok(ListenerHandle {
        ready: ready_rx,
        shutdown: shutdown_tx,
        join,
    })
}

async fn handle_http_upgrade(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    websocket_tasks: super::SessionTasks,
    ws_allowed_origins: Arc<Vec<crate::api::origin::ExactOrigin>>,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;

    runtime.increment_connections();

    let runtime_clone = runtime.clone();
    let service = service_fn(move |req| {
        let ingress = ingress.clone();
        let config = config.clone();
        let runtime = runtime_clone.clone();
        let websocket_tasks = websocket_tasks.clone();
        let ws_allowed_origins = ws_allowed_origins.clone();

        async move {
            if is_websocket_upgrade(&req) {
                handle_websocket(
                    req,
                    ingress,
                    config,
                    runtime,
                    websocket_tasks,
                    ws_allowed_origins,
                )
                .await
            } else {
                crate::api::admin::handlers::handle_request(req, runtime).await
            }
        }
    });

    let io = TokioIo::new(stream);
    let conn = http1::Builder::new()
        .keep_alive(true)
        .serve_connection(io, service)
        .with_upgrades();

    if let Err(e) = conn.await {
        tracing::debug!("HTTP connection error: {}", e);
    }

    record_connection_closed();
    runtime.decrement_connections();

    Ok(())
}

fn is_websocket_upgrade(req: &Request) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}
