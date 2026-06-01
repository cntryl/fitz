use super::{
    drain_session_tasks, record_connection_opened, session_tasks,
    tcp_session::handle_tcp_connection, ListenerHandle,
};
use crate::api::ingress::IngressConfig;
use crate::boot::{BootConfig, BootResult};
use crate::session::manager::Ingress;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

/// Spawn TCP listener on configured port (binds internally)
pub async fn spawn_tcp_listener(
    config: &BootConfig,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
) -> BootResult<ListenerHandle> {
    let tcp_addr = format!("{}:{}", config.bind_addr, config.tcp_port);
    let tcp_listener = TcpListener::bind(&tcp_addr).await?;
    info!("TCP endpoint listening on {}", tcp_addr);

    spawn_tcp_listener_with_bound_socket(tcp_listener, ingress, ingress_config, runtime)
}

/// Spawn TCP listener with pre-bound socket (eliminates port reallocation race)
pub fn spawn_tcp_listener_with_bound_socket(
    tcp_listener: TcpListener,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
) -> BootResult<ListenerHandle> {
    let tcp_config = ingress_config.clone();
    let runtime = Arc::new(runtime);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    let join = tokio::spawn(async move {
        let sessions = session_tasks();
        let _ = ready_tx.send(());

        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    tracing::info!("TCP listener shutdown requested");
                    break;
                }
                accept_result = tcp_listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            record_connection_opened();

                            info!("TCP connection from {}", peer_addr);
                            let ingress = ingress.clone();
                            let config = tcp_config.clone();
                            let runtime = runtime.clone();
                            sessions.lock().await.spawn(async move {
                                if let Err(e) = handle_tcp_connection(stream, ingress, config, runtime).await {
                                    tracing::error!("TCP handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!("TCP accept error: {}", e);
                        }
                    }
                }
            }
        }

        drain_session_tasks("tcp", vec![sessions]).await;
    });

    Ok(ListenerHandle {
        ready: ready_rx,
        shutdown: shutdown_tx,
        join,
    })
}
