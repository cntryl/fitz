//! Transport handlers: TCP and WebSocket

use crate::api::ingress::IngressConfig;
use crate::boot::{BootConfig, BootResult};
use crate::session::manager::Ingress;
use crate::session::{CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind};
use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

/// Spawn TCP listener on configured port
pub async fn spawn_tcp_listener(
    config: &BootConfig,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
) -> BootResult<()> {
    let tcp_addr = format!("{}:{}", config.bind_addr, config.tcp_port);
    let tcp_listener = TcpListener::bind(&tcp_addr).await?;
    info!("TCP endpoint listening on {}", tcp_addr);

    let tcp_config = ingress_config.clone();
    let runtime = Arc::new(runtime);
    tokio::spawn(async move {
        loop {
            match tcp_listener.accept().await {
                Ok((stream, peer_addr)) => {
                    info!("TCP connection from {}", peer_addr);
                    let ingress = ingress.clone();
                    let config = tcp_config.clone();
                    let runtime = runtime.clone();
                    tokio::spawn(async move {
                        if let Err(e) =
                            handle_tcp_connection(stream, ingress, config, runtime).await
                        {
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
                        if let Err(e) = handle_http_upgrade(stream, ingress, config, runtime).await
                        {
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

/// Handle an incoming TCP connection with outbound response support
///
/// # Protocol
/// - Length-prefixed frames: [u32 BE length][payload]
/// - Forwards to ingress for session and frame handling
/// - Registers outbound sink with router for response delivery
async fn handle_tcp_connection(
    stream: TcpStream,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
) -> Result<(), Box<dyn std::error::Error>> {
    use crate::api::tcp::create_session;
    use tokio::io::AsyncWriteExt;

    // Create session and handler
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let handler = create_session(ingress.clone(), config.clone(), stream, frame_tx).await?;

    // Get session_id and stream from handler
    let session_id = handler.session_id;
    let stream = handler.stream.clone();

    // Create outbound channel for responses
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Vec<u8>>(config.channel_capacity);

    // Register outbound sink with router under inbox route
    let sink = std::sync::Arc::new(crate::session::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let inbox_route = crate::runtime::routing::RouteAddress::new(
        crate::runtime::routing::RouteFamily::new(0),
        crate::runtime::routing::Route::new(format!("inbox://session/{}", session_id)),
    );
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering TCP outbound sink at inbox route"
    );
    runtime.router.register(
        inbox_route.clone(),
        sink as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );

    // Spawn task to send outbound frames through TCP
    let tcp_session_id = session_id;
    let stream_clone = stream.clone();
    tokio::spawn(async move {
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task started"
        );
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id = tcp_session_id,
                frame_len = frame.len(),
                "TCP outbound: sending frame to wire"
            );
            let mut stream_guard = stream_clone.lock().await;
            // Write length-prefixed frame
            let len = frame.len() as u32;
            if let Err(e) = stream_guard.write_all(&len.to_be_bytes()).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (header)");
                break;
            }
            if let Err(e) = stream_guard.write_all(&frame).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (payload)");
                break;
            }
            if let Err(e) = stream_guard.flush().await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound flush error");
                break;
            }
        }
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task ended"
        );
    });

    // Spawn task to process frames from the channel
    let ingress_clone = ingress.clone();
    let config_clone = config.clone();
    tokio::spawn(async move {
        // Create ONE session instance that persists for all frames on this connection
        let session_config = crate::session::NewSessionConfig::unauthenticated(
            crate::session::TransportKind::Tcp,
            None,
            crate::session::SessionPermissions::empty(),
            crate::session::SessionMetadata::new(),
            config_clone.channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(0),
        );

        let mut session = crate::session::Session::new(session_id, session_config);
        let mut frame_rx = frame_rx;

        // Process frames as they arrive, maintaining the session buffer state
        while let Some((_sid, frame)) = frame_rx.recv().await {
            // Process frame through session (decodes TLV and routes to ingress)
            if let Err(e) = session.on_frame(frame, ingress_clone.as_ref()).await {
                tracing::error!(session_id = session_id, error = %e, "TCP frame processing error");
                ingress_clone
                    .on_close(
                        session_id,
                        crate::session::CloseReason::Error(format!("{:?}", e)),
                    )
                    .await;
                break;
            }
        }
    });

    // Run TCP handler (reads frames and forwards to ingress) - this will block until client closes
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
                // Pass router to the handler so it can register session outbound sinks
                let router = runtime.router.clone();
                handle_websocket(req, ingress, config, runtime, router).await
            } else {
                // Handle HTTP admin API
                crate::api::admin::handlers::handle_request(req, runtime).await
            }
        }
    });

    // Configure HTTP/1.1 with upgrade support
    let conn = Http::new()
        .http1_only(true)
        .http1_keep_alive(true)
        .serve_connection(stream, service)
        .with_upgrades();

    if let Err(e) = conn.await {
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
    req: hyper::Request<hyper::Body>,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    runtime: Arc<crate::boot::Runtime>,
    router: Arc<crate::runtime::Router>,
) -> Result<hyper::Response<hyper::Body>, std::convert::Infallible> {
    // Increment session count
    runtime.increment_sessions();

    // Attempt WebSocket upgrade - hyper_tungstenite handles all validation
    match hyper_tungstenite::upgrade(req, None) {
        Ok((response, websocket_fut)) => {
            // Spawn task to handle WebSocket connection after response is sent
            let runtime_clone = runtime.clone();
            let router_clone = router.clone();
            tokio::spawn(async move {
                match websocket_fut.await {
                    Ok(ws_stream) => {
                        tracing::info!("WebSocket upgrade completed");
                        if let Err(e) =
                            run_websocket_session(ws_stream, ingress, config, router_clone).await
                        {
                            tracing::error!("WebSocket session error: {}", e);
                        }
                    }
                    Err(e) => {
                        tracing::error!("WebSocket upgrade error: {}", e);
                    }
                }
                // Decrement session count when done
                runtime_clone.decrement_sessions();
            });

            // Return the 101 Switching Protocols response immediately
            Ok(response)
        }
        Err(e) => {
            // Not a valid WebSocket upgrade request
            tracing::debug!("WebSocket upgrade rejected: {}", e);
            runtime.decrement_sessions();
            Ok(hyper::Response::builder()
                .status(400)
                .body(hyper::Body::from("Bad WebSocket upgrade request"))
                .unwrap())
        }
    }
}

/// Run a WebSocket session after successful upgrade
async fn run_websocket_session<S>(
    ws_stream: hyper_tungstenite::WebSocketStream<S>,
    ingress: Arc<dyn Ingress>,
    config: IngressConfig,
    router: Arc<crate::runtime::Router>,
) -> Result<(), String>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    use futures_util::SinkExt;
    use futures_util::StreamExt;
    use hyper_tungstenite::tungstenite::Message;

    // Generate session ID
    let session_id = generate_session_id();

    // Create transport-level session
    let session_config = crate::session::NewSessionConfig::unauthenticated(
        TransportKind::WebSocket,
        None, // No peer address for upgraded connection
        SessionPermissions::empty(),
        SessionMetadata::new(),
        config.channel_capacity,
        None,
        crate::runtime::routing::RouteFamily::new(0), // No auth = family 0
    );
    let mut session = Session::new(session_id, session_config);

    // Let ingress validate and accept the session
    let session_id = ingress.on_open(session.info()).await?;

    info!(
        session_id = session_id,
        "WebSocket session accepted by ingress"
    );

    // Split WebSocket stream
    let (mut ws_sender, mut ws_receiver) = ws_stream.split();

    // Create channel for outbound frames (from session to WebSocket)
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Vec<u8>>(config.channel_capacity);

    // Register outbound sink with router under inbox route
    let sink = std::sync::Arc::new(crate::session::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let inbox_route = crate::runtime::routing::RouteAddress::new(
        session.info().route_family,
        crate::runtime::routing::Route::new(format!("inbox://session/{}", session_id)),
    );
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering outbound sink at inbox route"
    );
    router.register(
        inbox_route.clone(),
        sink as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );

    // Spawn task to send outbound frames
    let ws_session_id = session_id;
    tokio::spawn(async move {
        tracing::debug!(
            session_id = ws_session_id,
            "WS outbound writer task started"
        );
        while let Some(frame) = outbound_rx.recv().await {
            tracing::debug!(
                session_id = ws_session_id,
                frame_len = frame.len(),
                "WS outbound: sending frame to wire"
            );
            if let Err(e) = ws_sender.send(Message::Binary(frame)).await {
                tracing::error!(session_id = ws_session_id, error = %e, "WS outbound send error");
                break;
            }
        }
        tracing::debug!(session_id = ws_session_id, "WS outbound writer task ended");
    });

    // Process inbound frames
    while let Some(msg_result) = ws_receiver.next().await {
        match msg_result {
            Ok(Message::Binary(data)) => {
                let frame = Bytes::from(data);
                tracing::debug!(
                    session_id = session_id,
                    frame_len = frame.len(),
                    "WS inbound: received binary frame"
                );

                // Check frame size
                if frame.len() > config.max_frame_size {
                    let reason = format!(
                        "frame too large: {} > {}",
                        frame.len(),
                        config.max_frame_size
                    );
                    tracing::warn!(
                        session_id = session_id,
                        frame_len = frame.len(),
                        max = config.max_frame_size,
                        "WS frame too large"
                    );
                    ingress
                        .on_close(session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(reason);
                }

                // Forward frame to session for processing (decoding + routing)
                if let Err(e) = session.on_frame(frame, ingress.as_ref()).await {
                    let reason = format!("session frame error: {:?}", e);
                    tracing::error!(session_id = session_id, error = %reason, "WS session frame processing error");
                    ingress
                        .on_close(session_id, CloseReason::Error(reason.clone()))
                        .await;
                    return Err(reason);
                }
                tracing::trace!(
                    session_id = session_id,
                    "WS inbound frame processed successfully"
                );
            }
            Ok(Message::Close(_)) => {
                tracing::debug!(session_id = session_id, "WS received Close frame");
                ingress.on_close(session_id, CloseReason::ClientClose).await;
                break;
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Text(_)) => {
                tracing::trace!(
                    session_id = session_id,
                    "WS received non-binary frame (ignored)"
                );
                continue;
            }
            Ok(_) => continue,
            Err(e) => {
                let reason = format!("WebSocket error: {}", e);
                ingress
                    .on_close(session_id, CloseReason::Error(reason.clone()))
                    .await;
                return Err(reason);
            }
        }
    }

    // Clean shutdown
    ingress.on_close(session_id, CloseReason::ClientClose).await;

    // Unregister outbound sink
    router.unregister(&inbox_route);

    info!("WebSocket connection closed, session {}", session_id);
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
