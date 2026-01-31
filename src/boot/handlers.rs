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
    runtime: crate::boot::Runtime,
) -> BootResult<()> {
    let http_addr = format!("{}:{}", config.bind_addr, config.http_port);
    let http_listener = TcpListener::bind(&http_addr).await?;
    info!("HTTP/WebSocket endpoint listening on {}", http_addr);

    let http_config = ingress_config.clone();
    let runtime = Arc::new(runtime);
    tokio::spawn(async move {
        loop {
            match http_listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("HTTP connection from {}", peer_addr);
                    let ingress = ingress.clone();
                    let config = http_config.clone();
                    let runtime = runtime.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_http_upgrade(stream, ingress, config, runtime).await {
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
    runtime: Arc<crate::boot::Runtime>,
) -> Result<(), Box<dyn std::error::Error>> {
    use hyper::server::conn::Http;
    use hyper::service::service_fn;
    
    // Increment connection count
    runtime.increment_connections();
    
    // Clone runtime for the service closure
    let runtime_clone = runtime.clone();
    
    // Serve HTTP/WebSocket with Hyper
    let service = service_fn(move |req| {
        let ingress = ingress.clone();
        let config = config.clone();
        let runtime = runtime_clone.clone();
        
        async move {
            // Check if this is a WebSocket upgrade
            if is_websocket_upgrade(&req) {
                // Handle WebSocket upgrade
                handle_websocket(req, ingress, config, runtime).await
            } else {
                // Handle HTTP admin API
                crate::api::admin::handlers::handle_request(req, runtime).await
            }
        }
    });
    
    if let Err(e) = Http::new().serve_connection(stream, service).await {
        tracing::debug!("HTTP connection error: {}", e);
    }
    
    // Decrement connection count
    runtime.decrement_connections();

    Ok(())
}

/// Check if request is a WebSocket upgrade
fn is_websocket_upgrade(req: &hyper::Request<hyper::Body>) -> bool {
    req.headers()
        .get("upgrade")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("websocket"))
        .unwrap_or(false)
}

/// Handle WebSocket upgrade
async fn handle_websocket(
    _req: hyper::Request<hyper::Body>,
    _ingress: Arc<dyn Ingress>,
    _config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
) -> Result<hyper::Response<hyper::Body>, std::convert::Infallible> {
    // Increment session count
    runtime.increment_sessions();
    
    // TODO: Implement WebSocket upgrade using tungstenite
    // For now, return 501 Not Implemented
    Ok(hyper::Response::builder()
        .status(501)
        .body(hyper::Body::from("WebSocket upgrade not yet implemented"))
        .unwrap())
}

/// Generate a unique session ID
#[allow(dead_code)] // TODO: Remove or integrate with WebSocket upgrade
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
