use super::{
    drain_session_tasks, reap_session_tasks, record_connection_opened, session_tasks,
    websocket::handle_websocket, ConnectionLifecycle, ListenerHandle,
};
use crate::api::http::Request;
use crate::api::ingress::IngressConfig;
use crate::api::runtime_ingress::Ingress;
use crate::boot::{BootConfig, BootResult};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::{TokioIo, TokioTimer};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

fn http_connection_limit(max_connections: usize) -> usize {
    max_connections.div_ceil(4).clamp(1, 1_024)
}

/// Spawn HTTP/WebSocket listener on configured port.
///
/// # Errors
///
/// Returns an error if the socket cannot be bound or the listener task fails to start.
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
        &ingress_config,
        runtime,
        config.ws_allowed_origins.clone(),
    )
}

/// Spawn HTTP/WebSocket listener with a pre-bound socket.
///
/// # Errors
///
/// Returns an error if the listener task cannot be started.
pub fn spawn_http_listener_with_bound_socket(
    http_listener: TcpListener,
    ingress: Arc<dyn Ingress>,
    ingress_config: &IngressConfig,
    runtime: crate::boot::Runtime,
    ws_allowed_origins: Vec<crate::api::origin::ExactOrigin>,
) -> BootResult<ListenerHandle> {
    let http_config = ingress_config.clone();
    let connection_limiter = ingress_config.connection_limiter();
    let http_connection_limiter = Arc::new(tokio::sync::Semaphore::new(http_connection_limit(
        ingress_config.max_connections,
    )));
    let runtime = Arc::new(runtime);
    let ws_allowed_origins = Arc::new(ws_allowed_origins);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let join = tokio::spawn(async move {
        let connections = session_tasks();
        let websockets = session_tasks();
        let mut reap_interval = tokio::time::interval(std::time::Duration::from_millis(250));
        reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
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
                            let Ok(permit) = connection_limiter.clone().try_acquire_owned() else {
                                tokio::spawn(reject_http_connection(stream));
                                continue;
                            };
                            let Ok(http_permit) =
                                http_connection_limiter.clone().try_acquire_owned()
                            else {
                                drop(permit);
                                tokio::spawn(reject_http_connection(stream));
                                continue;
                            };
                            record_connection_opened();

                            info!("HTTP connection from {}", peer_addr);
                            runtime.increment_connections();
                            let accepted_at = tokio::time::Instant::now();
                            let lifecycle = ConnectionLifecycle::new(runtime.clone(), permit);
                            let ingress = ingress.clone();
                            let config = http_config.clone();
                            let runtime = runtime.clone();
                            let websocket_tasks = websockets.clone();
                            let ws_allowed_origins = ws_allowed_origins.clone();
                            connections.lock().await.spawn(async move {
                                let _http_permit = http_permit;
                                if let Err(e) = handle_http_upgrade(stream, peer_addr, ingress, config, runtime, websocket_tasks, ws_allowed_origins, lifecycle, accepted_at).await {
                                    tracing::error!("HTTP handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("HTTP accept error: {}", e);
                        }
                    }
                }
                _ = reap_interval.tick() => {
                    reap_session_tasks("http", &connections).await;
                    reap_session_tasks("websocket", &websockets).await;
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

#[allow(clippy::too_many_arguments)]
async fn handle_http_upgrade(
    stream: TcpStream,
    peer_addr: std::net::SocketAddr,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    websocket_tasks: super::SessionTasks,
    ws_allowed_origins: Arc<Vec<crate::api::origin::ExactOrigin>>,
    lifecycle: ConnectionLifecycle,
    accepted_at: tokio::time::Instant,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.set_nodelay(true)?;

    let runtime_clone = runtime.clone();
    let connection_lifecycle = lifecycle.clone();
    let service = service_fn(move |mut req| {
        req.extensions_mut()
            .insert(crate::api::admin::auth::AdminClientIp(peer_addr.ip()));
        let ingress = ingress.clone();
        let config = config.clone();
        let runtime = runtime_clone.clone();
        let websocket_tasks = websocket_tasks.clone();
        let ws_allowed_origins = ws_allowed_origins.clone();
        let connection_lifecycle = connection_lifecycle.clone();
        let accepted_at = accepted_at;

        async move {
            if is_websocket_upgrade(&req) {
                handle_websocket(
                    req,
                    ingress,
                    config,
                    runtime,
                    websocket_tasks,
                    ws_allowed_origins,
                    connection_lifecycle,
                    accepted_at,
                )
                .await
            } else {
                crate::api::admin::handlers::handle_request(req, runtime).await
            }
        }
    });

    let io = TokioIo::new(stream);
    let mut builder = http1::Builder::new();
    builder
        .keep_alive(true)
        .timer(TokioTimer::new())
        .header_read_timeout(crate::api::ingress::HTTP_HEADER_READ_TIMEOUT);
    let conn = builder.serve_connection(io, service).with_upgrades();

    match tokio::time::timeout(crate::api::ingress::HTTP_CONNECTION_MAX_LIFETIME, conn).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::debug!("HTTP connection error: {error}"),
        Err(_) => tracing::debug!("HTTP connection reached its maximum lifetime"),
    }

    lifecycle.finish_http();

    Ok(())
}

async fn reject_http_connection(stream: TcpStream) {
    let service = service_fn(|_req: Request| async {
        Ok::<_, std::convert::Infallible>(
            hyper::http::Response::builder()
                .status(503)
                .header("Retry-After", "1")
                .body(crate::api::http::Body::from(
                    "Fitz connection limit reached",
                ))
                .unwrap(),
        )
    });
    let _ = http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .await;
}

fn is_websocket_upgrade(req: &Request) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reserve_global_connection_capacity_from_http() {
        assert_eq!(http_connection_limit(10_000), 1_024);
        assert_eq!(http_connection_limit(100), 25);
        assert_eq!(http_connection_limit(1), 1);
    }
}
