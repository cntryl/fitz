//! Transport handlers: TCP and WebSocket

use crate::api::ingress::IngressConfig;
use crate::boot::{BootConfig, BootResult};
use crate::session::manager::Ingress;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

/// Spawn TCP listener on configured port
pub async fn spawn_tcp_listener(
    config: &BootConfig,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
) -> BootResult<()> {
    let tcp_addr = format!("{}:{}", config.bind_addr, config.tcp_port);
    let tcp_listener = TcpListener::bind(&tcp_addr).await?;
    info!("TCP endpoint listening on {}", tcp_addr);

    let tcp_config = ingress_config.clone();
    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("TCP connection from {}", peer_addr);
                    let ingress = ingress.clone();
                    let config = tcp_config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_tcp_connection(stream, ingress, config).await {
                            tracing::error!("TCP handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("TCP accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}

/// Spawn HTTP/WebSocket listener on configured port
pub async fn spawn_http_listener(
    config: &BootConfig,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
) -> BootResult<()> {
    let http_addr = format!("{}:{}", config.bind_addr, config.http_port);
    let http_listener = TcpListener::bind(&http_addr).await?;
    info!("HTTP/WebSocket endpoint listening on {}", http_addr);

    let http_config = ingress_config.clone();
    tokio::spawn(async move {
        loop {
            match http_listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("HTTP connection from {}", peer_addr);
                    let ingress = ingress.clone();
                    let config = http_config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_http_upgrade(stream, ingress, config).await {
                            tracing::error!("HTTP handler error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    tracing::error!("HTTP accept error: {}", e);
                }
            }
        }
    });

    Ok(())
}

/// Handle an incoming TCP connection
///
/// # Protocol
/// - Length-prefixed frames: [u32 BE length][payload]
/// - Forwards to ingress for session and frame handling
async fn handle_tcp_connection(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::api::tcp::create_session;

    // Create session and handler
    let (frame_tx, _frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let handler = create_session(ingress, config, stream, frame_tx).await?;

    // Run TCP handler (reads frames and forwards to ingress)
    handler.run().await?;

    Ok(())
}

/// Handle HTTP upgrade to WebSocket
///
/// # Protocol
/// - HTTP upgrade request to WebSocket
/// - Binary frames only
/// - Forwards to ingress for session and frame handling
async fn handle_http_upgrade(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    use tokio_tungstenite::accept_async;

    // Perform WebSocket handshake
    let ws_stream = accept_async(stream).await?;
    info!("WebSocket connection established");

    // Create frame channel and session
    let (frame_tx, _frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let peer_addr = ws_stream.get_ref().peer_addr().ok();

    // Create session info and register with ingress
    let session = crate::session::Session::new(
        generate_session_id(),
        crate::session::TransportKind::WebSocket,
        peer_addr,
        crate::session::SessionPermissions::empty(),
        crate::session::SessionMetadata::new(),
        config.channel_capacity,
        None,
    );

    let session_id = ingress.on_open(session.info()).await?;

    // Create WebSocket handler
    let handler = crate::api::ws::WebSocketHandler::new(ingress, config, frame_tx, session_id);

    // Run WebSocket handler
    use futures::stream::StreamExt;
    let (_write, mut read) = ws_stream.split();

    while let Some(msg_result) = read.next().await {
        let msg = msg_result?;
        match handler.handle_message(msg).await {
            Ok(false) => break,
            Ok(true) => continue,
            Err(e) => {
                tracing::error!("WebSocket handler error: {}", e);
                break;
            }
        }
    }

    Ok(())
}

/// Generate a unique session ID
fn generate_session_id() -> u64 {
    static SESSION_COUNTER: AtomicU64 = AtomicU64::new(1);
    SESSION_COUNTER.fetch_add(1, Ordering::SeqCst)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_generate_unique_session_ids() {
        // Arrange

        // Act
        let id1 = generate_session_id();
        let id2 = generate_session_id();
        let id3 = generate_session_id();

        // Assert
        assert!(id1 < id2);
        assert!(id2 < id3);
    }
}
