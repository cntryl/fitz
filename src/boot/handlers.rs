//! Transport handlers: TCP and WebSocket

use crate::api::ingress::IngressConfig;
use crate::boot::{BootConfig, BootResult};
use crate::observability as obs;
use crate::session::manager::Ingress;
use crate::session::{
    generate_session_id, CloseReason, Session, SessionMetadata, SessionPermissions, TransportKind,
};
use bytes::Bytes;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::info;

pub struct ListenerHandle {
    pub ready: tokio::sync::oneshot::Receiver<()>,
    pub shutdown: tokio::sync::oneshot::Sender<()>,
}

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

    tokio::spawn(async move {
        // Signal readiness before accepting connections
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
                            // Record connection opened counter
                            if let Ok(collector) =
                                std::panic::catch_unwind(crate::boot::observability::metrics)
                            {
                                collector.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
                            }

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
            }
        }
    });

    Ok(ListenerHandle {
        ready: ready_rx,
        shutdown: shutdown_tx,
    })
}

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

    spawn_http_listener_with_bound_socket(http_listener, ingress, ingress_config, runtime)
}

/// Spawn HTTP/WebSocket listener with pre-bound socket (eliminates port reallocation race)
pub fn spawn_http_listener_with_bound_socket(
    http_listener: TcpListener,
    ingress: Arc<dyn Ingress>,
    ingress_config: IngressConfig,
    runtime: crate::boot::Runtime,
) -> BootResult<ListenerHandle> {
    let http_config = ingress_config.clone();
    let runtime = Arc::new(runtime);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        // Signal readiness before accepting connections
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
                            // Record connection opened counter
                            if let Ok(collector) =
                                std::panic::catch_unwind(crate::boot::observability::metrics)
                            {
                                collector.counter_inc(obs::METRIC_CONNECTIONS_OPENED);
                            }

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
            }
        }
    });

    Ok(ListenerHandle {
        ready: ready_rx,
        shutdown: shutdown_tx,
    })
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
    use parking_lot::Mutex;
    use tokio::io::AsyncWriteExt;

    // Create session and handler (stream is split into read/write halves)
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel(config.channel_capacity);
    let (handler, write_half) =
        create_session(ingress.clone(), config.clone(), stream, frame_tx.clone()).await?;
    runtime.increment_connections();

    // Get session_id from handler
    let session_id = handler.session_id;

    // Create outbound channel for responses
    let (outbound_tx, mut outbound_rx) =
        tokio::sync::mpsc::channel::<Vec<u8>>(config.channel_capacity);

    // Register outbound sink with router under inbox route
    let sink = std::sync::Arc::new(crate::session::outbound::SessionOutboundSink::new(
        outbound_tx.clone(),
    ));
    let initial_family = ingress
        .get_route_family(session_id)
        .unwrap_or_else(|| crate::runtime::routing::RouteFamily::new(1));
    let inbox_route = crate::runtime::routing::session_inbox_address(initial_family, session_id);
    let current_inbox = Arc::new(Mutex::new(inbox_route.clone()));
    let registered_inboxes = Arc::new(Mutex::new(vec![inbox_route.clone()]));
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering TCP outbound sink at inbox route"
    );
    runtime.router.register(
        inbox_route.clone(),
        sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );

    // Spawn task to send outbound frames through TCP (owns the write half)
    let tcp_session_id = session_id;
    let runtime_for_writes = runtime.clone();
    let mut write_half = write_half;
    let writer_handle = tokio::spawn(async move {
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
            // Write length-prefixed frame directly to the owned write half (no mutex)
            let len = frame.len() as u32;
            if let Err(e) = write_half.write_all(&len.to_be_bytes()).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (header)");
                break;
            }
            if let Err(e) = write_half.write_all(&frame).await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound write error (payload)");
                break;
            }
            if let Err(e) = write_half.flush().await {
                tracing::error!(session_id = tcp_session_id, error = %e, "TCP outbound flush error");
                break;
            }
            runtime_for_writes.increment_messages_sent();
        }
        tracing::debug!(
            session_id = tcp_session_id,
            "TCP outbound writer task ended"
        );
    });

    // Spawn task to process frames from the channel
    let ingress_clone = ingress.clone();
    let config_clone = config.clone();
    let runtime_for_frames = runtime.clone();
    let outbound_sink = sink.clone();
    let current_inbox_for_frames = current_inbox.clone();
    let registered_inboxes_for_frames = registered_inboxes.clone();
    let mut frame_handle = tokio::spawn(async move {
        // Create ONE session instance that persists for all frames on this connection
        let session_config = crate::session::NewSessionConfig::unauthenticated(
            crate::session::TransportKind::Tcp,
            None,
            crate::session::SessionPermissions::empty(),
            crate::session::SessionMetadata::new(),
            config_clone.channel_capacity,
            None,
            crate::runtime::routing::RouteFamily::new(1),
        );

        let mut session = crate::session::Session::new(session_id, session_config);
        let mut frame_rx = frame_rx;
        let mut registered_inbox =
            crate::runtime::routing::session_inbox_address(initial_family, session_id);

        // Process frames as they arrive, maintaining the session buffer state
        while let Some((_sid, frame)) = frame_rx.recv().await {
            runtime_for_frames.increment_messages_received();
            // Process frame through session (decodes TLV and routes to ingress)
            if let Err(e) = session.on_frame(frame, ingress_clone.as_ref()).await {
                tracing::error!(session_id = session_id, error = %e, "TCP frame processing error");
                ingress_clone
                    .on_close(
                        session_id,
                        crate::session::CloseReason::Error(format!("{:?}", e)),
                    )
                    .await;
                return Err(format!("{e}"));
            }

            if let Some(updated_route_family) = ingress_clone.get_route_family(session_id) {
                if updated_route_family != *registered_inbox.family() {
                    runtime_for_frames.router.unregister(&registered_inbox);
                    registered_inbox = crate::runtime::routing::session_inbox_address(
                        updated_route_family,
                        session_id,
                    );
                    runtime_for_frames.router.register(
                        registered_inbox.clone(),
                        outbound_sink.clone()
                            as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
                    );
                    *current_inbox_for_frames.lock() = registered_inbox.clone();
                    let mut inboxes = registered_inboxes_for_frames.lock();
                    if !inboxes.contains(&registered_inbox) {
                        inboxes.push(registered_inbox.clone());
                    }
                }
            }
        }
        Ok::<(), String>(())
    });

    // Run TCP handler (reads frames and forwards to ingress). If frame processing
    // requests close (for example, CONNECT auth failure), stop waiting for the
    // client to hang up and actively tear down the transport so the peer sees EOF.
    let mut handler_task = tokio::spawn(async move { handler.run().await });
    let run_result = tokio::select! {
        res = &mut handler_task => {
            drop(frame_tx);
            if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), &mut frame_handle).await {
                tracing::warn!(session_id = session_id, error = %e, "TCP frame task did not terminate in time");
            }
            match res {
                Ok(result) => result,
                Err(e) => Err(format!("tcp handler task join error: {e}")),
            }
        }
        res = &mut frame_handle => {
            drop(frame_tx);
            match res {
                Ok(Ok(())) => Ok(()),
                Ok(Err(reason)) => {
                    tracing::debug!(
                        session_id = session_id,
                        reason = %reason,
                        "TCP frame task requested transport close"
                    );
                    handler_task.abort();
                    Err(reason)
                }
                Err(e) => {
                    handler_task.abort();
                    Err(format!("tcp frame task join error: {e}"))
                }
            }
        }
    };

    let inboxes = registered_inboxes.lock().clone();
    for inbox in &inboxes {
        runtime.router.unregister(inbox);
    }

    drop(current_inbox);
    drop(sink);
    drop(outbound_tx);

    if let Err(e) = tokio::time::timeout(std::time::Duration::from_secs(1), writer_handle).await {
        tracing::warn!(session_id = session_id, error = %e, "TCP writer task did not terminate in time");
    }

    // Record connection closed counter
    if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_CLOSED);
    }
    runtime.decrement_connections();

    run_result
        .map_err(std::io::Error::other)
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
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

    // Record connection closed counter and decrement connection count
    if let Ok(collector) = std::panic::catch_unwind(crate::boot::observability::metrics) {
        collector.counter_inc(obs::METRIC_CONNECTIONS_CLOSED);
    }
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
                        if let Err(e) = run_websocket_session(
                            ws_stream,
                            ingress,
                            config,
                            runtime_clone.clone(),
                            router_clone,
                        )
                        .await
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
    runtime: Arc<crate::boot::Runtime>,
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
        crate::runtime::routing::RouteFamily::new(1), // Default dev family = 1
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
    let mut inbox_route =
        crate::runtime::routing::session_inbox_address(session.info().route_family, session_id);
    tracing::debug!(
        session_id = session_id,
        inbox = %inbox_route,
        "Registering outbound sink at inbox route"
    );
    router.register(
        inbox_route.clone(),
        sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
    );

    // Spawn task to send outbound frames
    let ws_session_id = session_id;
    let runtime_for_writes = runtime.clone();
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
            runtime_for_writes.increment_messages_sent();
        }
        tracing::debug!(session_id = ws_session_id, "WS outbound writer task ended");
    });

    let result = loop {
        let Some(msg_result) = ws_receiver.next().await else {
            break Ok(());
        };

        match msg_result {
            Ok(Message::Binary(data)) => {
                let frame = Bytes::from(data);
                runtime.increment_messages_received();
                tracing::debug!(
                    session_id = session_id,
                    frame_len = frame.len(),
                    "WS inbound: received binary frame"
                );

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
                    break Err(reason);
                }

                if let Err(e) = session.on_frame(frame, ingress.as_ref()).await {
                    let reason = format!("session frame error: {:?}", e);
                    tracing::error!(session_id = session_id, error = %reason, "WS session frame processing error");
                    break Err(reason);
                }
                if let Some(updated_route_family) = ingress.get_route_family(session_id) {
                    if updated_route_family != *inbox_route.family() {
                        router.unregister(&inbox_route);
                        inbox_route = crate::runtime::routing::session_inbox_address(
                            updated_route_family,
                            session_id,
                        );
                        router.register(
                            inbox_route.clone(),
                            sink.clone() as std::sync::Arc<dyn crate::runtime::router::MailboxSink>,
                        );
                    }
                }
                tracing::trace!(
                    session_id = session_id,
                    "WS inbound frame processed successfully"
                );
            }
            Ok(Message::Close(_)) => {
                tracing::debug!(session_id = session_id, "WS received Close frame");
                break Ok(());
            }
            Ok(Message::Ping(_)) | Ok(Message::Pong(_)) | Ok(Message::Text(_)) => {
                tracing::trace!(
                    session_id = session_id,
                    "WS received non-binary frame (ignored)"
                );
            }
            Ok(_) => {}
            Err(e) => {
                break Err(format!("WebSocket error: {}", e));
            }
        }
    };

    let close_reason = match &result {
        Ok(()) => CloseReason::ClientClose,
        Err(reason) => CloseReason::Error(reason.clone()),
    };
    ingress.on_close(session_id, close_reason).await;
    router.unregister(&inbox_route);

    if result.is_ok() {
        info!("WebSocket connection closed, session {}", session_id);
    }
    result
}
